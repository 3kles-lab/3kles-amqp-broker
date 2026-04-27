import { Channel, ConsumeMessage, Options, Replies } from 'amqplib';
import { EventEmitter } from 'events';
import { v4 as uuidv4 } from 'uuid';
import pino, { Logger } from 'pino';

import { ConnectionManager } from './connection-manager';
import { ConsumerHandle } from './consumer-handle';
import { AmqpConsumerError, AmqpError, AmqpRpcTimeoutError } from './errors';

import { AssertExchangeInput, ExchangeDeclaration, ExchangeType } from './types/exchange';

import { AssertQueueInput, BindQueueInput, QueueBinding, QueueDeclaration } from './types/queue';

import { BrokerConfig, BrokerState } from './types/broker';

import { ConsumeContext, ConsumerRegistration, MessageHandler } from './types/consumer';

import { PublishInput, PublishPayload, SendToQueueInput } from './types/publish';

import { PendingRpcRequest, RpcContext, RpcHandler, RpcRequestExchangeInput, RpcRequestQueueInput } from './types/rpc';

import { bindingKey } from './utils/keys';
import { buildPublishOptions, parseJson, toBuffer } from './utils/message';

const defaultLogger = pino({
    level: process.env.NODE_ENV === 'production' ? 'info' : 'debug',
    timestamp: pino.stdTimeFunctions.isoTime,
    name: 'amqp-broker',
});

export class MessageBroker extends EventEmitter {
    private static readonly instances = new Map<string | number, Promise<MessageBroker>>();

    public static get(name: string | number, config: BrokerConfig): Promise<MessageBroker> {
        let instance = this.instances.get(name);

        if (!instance) {
            instance = this.create(name, config);
            this.instances.set(name, instance);
        }

        return instance;
    }

    public static async create(name: string | number, config: BrokerConfig): Promise<MessageBroker> {
        if (this.instances.has(name)) {
            throw new AmqpError(`[AMQP] Broker "${name}" already exists`);
        }

        const broker = new MessageBroker(name, config);
        await broker.start();

        return broker;
    }

    private channel: Channel | null = null;
    private state: BrokerState = 'idle';
    private startPromise?: Promise<void>;

    private readonly logger: Logger;
    private readonly connectionManager: ConnectionManager;

    private readonly exchanges = new Map<string, ExchangeDeclaration>();
    private readonly queues = new Map<string, QueueDeclaration>();
    private readonly bindings = new Map<string, QueueBinding>();
    private readonly consumers = new Map<string, ConsumerRegistration>();

    private rpcReplyQueue?: Replies.AssertQueue;
    private rpcConsumerTag?: string;
    private readonly pendingRpc = new Map<string, PendingRpcRequest>();

    private constructor(
        public readonly name: string | number,
        private readonly config: BrokerConfig,
    ) {
        super();

        this.connectionManager = config.connectionManager;
        this.logger = config.logger ?? defaultLogger;

        this.connectionManager.onConnected(async () => {
            try {
                await this.start();
            } catch (err) {
                this.logger.error({ err, broker: this.name }, '[AMQP] Broker restart failed');
                this.emitSafe('broker_error', err);
            }
        });

        this.connectionManager.onDisconnected(() => {
            this.channel = null;
            this.state = 'stopped';
            this.emit('disconnected');
        });
    }

    public get started(): boolean {
        return this.state === 'started' && !!this.channel;
    }

    public get currentChannel(): Channel {
        if (!this.channel || this.state !== 'started') {
            throw new AmqpError('[AMQP] Broker channel is not ready');
        }

        return this.channel;
    }

    private getChannel(): Channel {
        if (!this.channel) {
            throw new AmqpError('[AMQP] Broker channel is not ready');
        }

        return this.channel;
    }

    public async start(): Promise<void> {
        if (this.startPromise) {
            return this.startPromise;
        }

        this.startPromise = this.startInternal().finally(() => {
            this.startPromise = undefined;
        });

        return this.startPromise;
    }

    private async startInternal(): Promise<void> {
        if (this.state === 'started' && this.channel) {
            return;
        }

        if (!this.connectionManager.connected) {
            await this.connectionManager.connect();
        }

        this.state = 'starting';

        const oldChannel = this.channel;
        this.channel = null;

        if (oldChannel) {
            try {
                await oldChannel.close();
            } catch {}
        }

        const channel = await this.connectionManager.currentConnection.createChannel();

        channel.on('error', (err) => {
            this.logger.error({ err, broker: this.name }, '[AMQP] Channel error');
            this.emitSafe('channel_error', err);
        });

        channel.once('close', () => {
            if (this.channel === channel) {
                this.channel = null;
                this.state = 'stopped';
                this.emit('channel_closed');
            }
        });

        this.channel = channel;

        if (this.config.prefetch) {
            await channel.prefetch(this.config.prefetch);
            this.logger.debug({ broker: this.name, prefetch: this.config.prefetch }, '[AMQP] Prefetch configured');
        } else {
            this.logger.warn({ broker: this.name }, '[AMQP] No prefetch configured; consumers may receive many unacked messages');
        }

        await this.restoreTopology();
        await this.restoreConsumers();

        if (this.config.rpc?.enabled !== false) {
            await this.initRpcReplyQueue();
        }

        this.state = 'started';
        this.emit('started');
    }

    public async stop(): Promise<void> {
        this.state = 'stopping';

        for (const request of this.pendingRpc.values()) {
            clearTimeout(request.timeout);
            request.reject(new AmqpError('[AMQP] Broker stopped before RPC response'));
        }

        this.pendingRpc.clear();

        if (this.channel) {
            try {
                await this.channel.close();
            } catch (err) {
                this.logger.error({ err, broker: this.name }, '[AMQP] Error while closing broker channel');
            }
        }

        this.channel = null;
        this.state = 'stopped';
    }

    // ---------------------------------------------------------------------------
    // Topology
    // ---------------------------------------------------------------------------

    public async assertExchange(input: AssertExchangeInput): Promise<Replies.AssertExchange> {
        const type = input.type ?? 'direct';

        const result = await this.currentChannel.assertExchange(input.name, type, input.options);

        this.exchanges.set(input.name, {
            name: input.name,
            type,
            options: input.options,
        });

        return result;
    }

    public async assertQueue(input: AssertQueueInput): Promise<Replies.AssertQueue> {
        const result = await this.currentChannel.assertQueue(input.name, input.options);

        this.queues.set(result.queue, {
            requestedName: input.name,
            actualName: result.queue,
            options: input.options,
        });

        return result;
    }

    public async bindQueue(input: BindQueueInput): Promise<void> {
        await this.currentChannel.bindQueue(input.queue, input.exchange, input.routingKey, input.args);

        const binding: QueueBinding = {
            queue: input.queue,
            exchange: input.exchange,
            routingKey: input.routingKey,
            args: input.args,
        };

        this.bindings.set(bindingKey(binding), binding);
    }

    public async unbindQueue(input: BindQueueInput): Promise<void> {
        await this.currentChannel.unbindQueue(input.queue, input.exchange, input.routingKey, input.args);

        this.bindings.delete(bindingKey(input));
    }

    public async setupExchangeConsumer(
        input: {
            exchange: string;
            exchangeType?: ExchangeType;
            exchangeOptions?: Options.AssertExchange;
            queue: string;
            queueOptions?: Options.AssertQueue;
            routingKey?: string;
            routingKeys?: string[];
            consumeOptions?: Options.Consume;
        },
        handler: MessageHandler,
    ): Promise<ConsumerHandle> {
        const routingKeys = input.routingKeys ?? [input.routingKey ?? ''];

        await this.assertExchange({
            name: input.exchange,
            type: input.exchangeType ?? 'direct',
            options: input.exchangeOptions,
        });

        const queue = await this.assertQueue({
            name: input.queue,
            options: input.queueOptions,
        });

        for (const routingKey of routingKeys) {
            await this.bindQueue({
                queue: queue.queue,
                exchange: input.exchange,
                routingKey,
            });
        }

        return this.consumeQueue(queue.queue, handler, input.consumeOptions);
    }

    // ---------------------------------------------------------------------------
    // Publish
    // ---------------------------------------------------------------------------

    public async publish(exchange: string, routingKey: string, payload: PublishPayload, options?: Options.Publish): Promise<boolean> {
        const buffer = toBuffer(payload);

        return this.currentChannel.publish(exchange, routingKey, buffer, buildPublishOptions(payload, options));
    }

    public async publishInput(input: PublishInput): Promise<boolean> {
        if (input.assertExchange) {
            await this.assertExchange({
                name: input.exchange,
                type: input.assertExchange.type,
                options: input.assertExchange.options,
            });
        }

        return this.publish(input.exchange, input.routingKey, input.payload, input.options);
    }

    public async sendToQueue(queue: string, payload: PublishPayload, options?: Options.Publish): Promise<boolean> {
        const buffer = toBuffer(payload);

        return this.currentChannel.sendToQueue(queue, buffer, buildPublishOptions(payload, options));
    }

    public async sendToQueueInput(input: SendToQueueInput): Promise<boolean> {
        if (input.assertQueue) {
            await this.assertQueue({
                name: input.queue,
                options: input.assertQueue,
            });
        }

        return this.sendToQueue(input.queue, input.payload, input.options);
    }

    // ---------------------------------------------------------------------------
    // Consumers
    // ---------------------------------------------------------------------------

    public async consumeQueue(queue: string, handler: MessageHandler, options?: Options.Consume): Promise<ConsumerHandle> {
        const id = uuidv4();

        const registration: ConsumerRegistration = {
            id,
            queue,
            handler,
            options,
            state: 'active',
        };

        this.consumers.set(id, registration);

        await this.startConsumer(registration);

        return new ConsumerHandle(this, id);
    }

    public async pauseConsumer(id: string): Promise<void> {
        const consumer = this.consumers.get(id);

        if (!consumer || consumer.state !== 'active') {
            return;
        }

        if (consumer.consumerTag) {
            await this.currentChannel.cancel(consumer.consumerTag);
        }

        consumer.consumerTag = undefined;
        consumer.state = 'paused';
    }

    public async resumeConsumer(id: string): Promise<void> {
        const consumer = this.consumers.get(id);

        if (!consumer || consumer.state !== 'paused') {
            return;
        }

        consumer.state = 'active';
        await this.startConsumer(consumer);
    }

    public async closeConsumer(id: string): Promise<void> {
        const consumer = this.consumers.get(id);

        if (!consumer) {
            return;
        }

        const canCancelOnRabbit = consumer.state === 'active' && !!consumer.consumerTag && !!this.channel && this.state === 'started';

        if (canCancelOnRabbit && consumer.consumerTag) {
            try {
                await this.currentChannel.cancel(consumer.consumerTag);
            } catch (err) {
                this.logger.warn({ err, id }, '[AMQP] Error while cancelling consumer');
            }
        }

        consumer.state = 'closed';
        consumer.consumerTag = undefined;

        this.consumers.delete(id);
    }

    private async startConsumer(consumer: ConsumerRegistration): Promise<void> {
        if (consumer.state !== 'active') {
            return;
        }

        const channel = this.getChannel();
        const consumerTag = consumer.consumerTag ?? uuidv4();

        const result = await channel.consume(
            consumer.queue,
            async (msg) => {
                if (!msg) {
                    await this.handleConsumerCancelled(consumer);
                    return;
                }

                await this.handleMessage(consumer, msg);
            },
            {
                ...consumer.options,
                consumerTag,
            },
        );

        consumer.consumerTag = result.consumerTag;
    }

    private async handleMessage(consumer: ConsumerRegistration, msg: ConsumeMessage): Promise<void> {
        const ctx = this.createConsumeContext(msg);

        try {
            await consumer.handler(msg, ctx);

            if (!ctx.settled && consumer.options?.noAck !== true) {
                this.logger.warn({ consumerId: consumer.id, queue: consumer.queue }, '[AMQP] Message handler completed without ack/nack/reject');
            }
        } catch (err) {
            this.logger.error({ err, consumerId: consumer.id, queue: consumer.queue }, '[AMQP] Message handler failed');

            if (!ctx.settled && consumer.options?.noAck !== true) {
                const requeue = this.config.consumers?.requeueOnHandlerError ?? true;
                this.getChannel().nack(msg, false, requeue);
            }

            this.emitSafe('consumer_error', new AmqpConsumerError('[AMQP] Consumer handler failed', err));
        }
    }

    private async handleConsumerCancelled(consumer: ConsumerRegistration): Promise<void> {
        consumer.consumerTag = undefined;

        if (consumer.state !== 'active') {
            return;
        }

        const restart = this.config.consumers?.restartOnCancel ?? true;

        this.logger.warn({ consumerId: consumer.id, queue: consumer.queue, restart }, '[AMQP] Consumer cancelled');

        if (restart) {
            await this.startConsumer(consumer);
        }
    }

    private createConsumeContext(msg: ConsumeMessage): ConsumeContext {
        let settled = false;

        return {
            get settled() {
                return settled;
            },

            ack: (allUpTo?: boolean) => {
                if (settled) {
                    return;
                }

                settled = true;
                this.getChannel().ack(msg, allUpTo);
            },

            nack: (allUpTo?: boolean, requeue?: boolean) => {
                if (settled) {
                    return;
                }

                settled = true;
                this.getChannel().nack(msg, allUpTo, requeue);
            },

            reject: (requeue?: boolean) => {
                if (settled) {
                    return;
                }

                settled = true;
                this.getChannel().reject(msg, requeue);
            },

            buffer: () => msg.content,

            text: (encoding: BufferEncoding = 'utf8') => msg.content.toString(encoding),

            json: <T = unknown>() => parseJson<T>(msg.content),
        };
    }

    // ---------------------------------------------------------------------------
    // RPC client
    // ---------------------------------------------------------------------------

    public async requestQueue<T = unknown>(input: RpcRequestQueueInput): Promise<T> {
        await this.ensureRpcEnabled();

        const correlationId = uuidv4();
        const timeoutMs = input.timeoutMs ?? this.config.rpc?.timeoutMs ?? 30_000;

        return new Promise<T>((resolve, reject) => {
            const timeout = setTimeout(() => {
                this.pendingRpc.delete(correlationId);
                reject(new AmqpRpcTimeoutError(timeoutMs));
            }, timeoutMs);

            this.pendingRpc.set(correlationId, {
                correlationId,
                resolve: resolve as (value: unknown) => void,
                reject,
                timeout,
            });

            try {
                this.getChannel().sendToQueue(input.queue, toBuffer(input.payload), {
                    ...buildPublishOptions(input.payload, input.options),
                    replyTo: this.rpcReplyQueue!.queue,
                    correlationId,
                });
            } catch (err) {
                clearTimeout(timeout);
                this.pendingRpc.delete(correlationId);
                reject(err);
            }
        });
    }

    public async requestExchange<T = unknown>(input: RpcRequestExchangeInput): Promise<T> {
        await this.ensureRpcEnabled();

        const correlationId = uuidv4();
        const timeoutMs = input.timeoutMs ?? this.config.rpc?.timeoutMs ?? 30_000;

        return new Promise<T>((resolve, reject) => {
            const timeout = setTimeout(() => {
                this.pendingRpc.delete(correlationId);
                reject(new AmqpRpcTimeoutError(timeoutMs));
            }, timeoutMs);

            this.pendingRpc.set(correlationId, {
                correlationId,
                resolve: resolve as (value: unknown) => void,
                reject,
                timeout,
            });

            try {
                this.getChannel().publish(input.exchange, input.routingKey, toBuffer(input.payload), {
                    ...buildPublishOptions(input.payload, input.options),
                    replyTo: this.rpcReplyQueue!.queue,
                    correlationId,
                });
            } catch (err) {
                clearTimeout(timeout);
                this.pendingRpc.delete(correlationId);
                reject(err);
            }
        });
    }

    private async ensureRpcEnabled(): Promise<void> {
        if (this.config.rpc?.enabled === false) {
            throw new AmqpError(`[AMQP] RPC is disabled for broker "${this.name}"`);
        }

        if (!this.rpcReplyQueue) {
            await this.initRpcReplyQueue();
        }
    }

    private async initRpcReplyQueue(): Promise<void> {
        const channel = this.getChannel();
        const queueName = this.config.rpc?.replyQueue ?? '';

        this.rpcReplyQueue = await channel.assertQueue(queueName, {
            exclusive: !queueName,
            autoDelete: !queueName,
            durable: false,
        });

        const result = await channel.consume(
            this.rpcReplyQueue.queue,
            (msg) => {
                if (!msg) {
                    return;
                }

                const correlationId = msg.properties.correlationId;

                if (!correlationId) {
                    return;
                }

                const pending = this.pendingRpc.get(correlationId);

                if (!pending) {
                    return;
                }

                clearTimeout(pending.timeout);
                this.pendingRpc.delete(correlationId);

                try {
                    const contentType = msg.properties.contentType;

                    if (contentType === 'application/json') {
                        pending.resolve(parseJson(msg.content));
                    } else {
                        const text = msg.content.toString('utf8');

                        try {
                            pending.resolve(JSON.parse(text));
                        } catch {
                            pending.resolve(text);
                        }
                    }
                } catch (err) {
                    pending.reject(err);
                }
            },
            {
                noAck: true,
            },
        );

        this.rpcConsumerTag = result.consumerTag;
    }

    // ---------------------------------------------------------------------------
    // RPC server
    // ---------------------------------------------------------------------------

    public async consumeRpcQueue(queue: string, handler: RpcHandler, options?: Options.Consume): Promise<ConsumerHandle> {
        return this.consumeQueue(
            queue,
            async (msg) => {
                const ctx = this.createRpcContext(msg);

                try {
                    await handler(msg, ctx);

                    if (!ctx.settled) {
                        this.logger.warn({ queue }, '[AMQP] RPC handler completed without reply/nack/reject');
                    }
                } catch (err) {
                    this.logger.error({ err, queue }, '[AMQP] RPC handler failed');

                    if (!ctx.settled) {
                        this.getChannel().nack(msg, false, true);
                    }
                }
            },
            options,
        );
    }

    private createRpcContext(msg: ConsumeMessage): RpcContext {
        let settled = false;

        return {
            get settled() {
                return settled;
            },

            reply: (payload: PublishPayload, options?: Options.Publish) => {
                if (settled) {
                    return;
                }

                settled = true;

                if (!msg.properties.replyTo) {
                    this.getChannel().ack(msg);
                    return;
                }

                this.getChannel().sendToQueue(msg.properties.replyTo, toBuffer(payload), {
                    ...buildPublishOptions(payload, options),
                    correlationId: msg.properties.correlationId,
                });

                this.getChannel().ack(msg);
            },

            nack: (requeue?: boolean) => {
                if (settled) {
                    return;
                }

                settled = true;
                this.getChannel().nack(msg, false, requeue);
            },

            reject: (requeue?: boolean) => {
                if (settled) {
                    return;
                }

                settled = true;
                this.getChannel().reject(msg, requeue);
            },

            buffer: () => msg.content,

            text: (encoding: BufferEncoding = 'utf8') => msg.content.toString(encoding),

            json: <T = unknown>() => parseJson<T>(msg.content),
        };
    }

    // ---------------------------------------------------------------------------
    // Restore after reconnect
    // ---------------------------------------------------------------------------

    private async restoreTopology(): Promise<void> {
        const channel = this.getChannel();

        for (const exchange of this.exchanges.values()) {
            await channel.assertExchange(exchange.name, exchange.type, exchange.options);
        }

        const restoredQueues = new Map<string, QueueDeclaration>();

        for (const queue of this.queues.values()) {
            const result = await channel.assertQueue(queue.requestedName, queue.options);

            restoredQueues.set(result.queue, {
                requestedName: queue.requestedName,
                actualName: result.queue,
                options: queue.options,
            });
        }

        this.queues.clear();

        for (const queue of restoredQueues.values()) {
            this.queues.set(queue.actualName, queue);
        }

        for (const binding of this.bindings.values()) {
            await channel.bindQueue(binding.queue, binding.exchange, binding.routingKey, binding.args);
        }
    }

    private async restoreConsumers(): Promise<void> {
        for (const consumer of this.consumers.values()) {
            consumer.consumerTag = undefined;

            if (consumer.state === 'active') {
                await this.startConsumer(consumer);
            }
        }

        this.rpcReplyQueue = undefined;
        this.rpcConsumerTag = undefined;
    }

    private emitSafe(event: string, payload?: unknown): void {
        if (this.listenerCount(event) > 0) {
            this.emit(event, payload);
        }
    }
}
