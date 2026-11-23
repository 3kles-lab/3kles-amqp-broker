import { Channel, ChannelModel, ConfirmChannel, ConsumeMessage, Message, Options, Replies } from 'amqplib';
import { EventEmitter } from 'events';
import { setTimeout as wait } from 'node:timers/promises';
import { v4 as uuidv4 } from 'uuid';
import pino, { Logger } from 'pino';

import { ConnectionManager } from './connection-manager';
import { ConsumerHandle } from './consumer-handle';
import {
    AmqpConsumerError,
    AmqpError,
    AmqpPublishError,
    AmqpPublishUnknownError,
    AmqpRpcTimeoutError,
    AmqpRpcUnknownError,
    AmqpUnroutableError,
} from './errors';

import { AssertExchangeInput, ExchangeDeclaration, ExchangeType } from './types/exchange';

import { AssertQueueInput, BindQueueInput, QueueBinding, QueueDeclaration } from './types/queue';

import { BrokerConfig, BrokerState } from './types/broker';

import { ConsumeContext, ConsumerRegistration, MessageHandler } from './types/consumer';

import { PublishInput, PublishPayload, SendToQueueInput } from './types/publish';

import { PendingRpcRequest, RpcContext, RpcHandler, RpcRequestExchangeInput, RpcRequestQueueInput } from './types/rpc';

import { bindingKey } from './utils/keys';
import { reconnectDelayWithJitter } from './utils/delay';
import { buildPublishOptions, parseJson, toBuffer } from './utils/message';

const publicationIdHeader = 'x-3kles-publication-id';

function hasAmqpErrorCode(error: unknown, code: number): error is Error {
    return error instanceof Error && 'code' in error && error.code === code;
}

class ExclusiveQueueLockedError extends AmqpError {
    constructor(public readonly queue: string, cause: Error) {
        super('[AMQP] Exclusive queue is locked', cause);
    }
}

interface PendingPublication {
    messageId?: string;
    fail: (error: Error) => void;
}

interface DeliveryContext {
    context: ConsumeContext;
    settle(action: (channel: Channel) => void): void;
}

type RegisteredConsumer = Omit<ConsumerRegistration, 'handler'> & {
    handler(msg: ConsumeMessage, delivery: DeliveryContext): Promise<void> | void;
};

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

    private channel: Channel | ConfirmChannel | null = null;
    private topologyConnection?: ChannelModel;
    private state: BrokerState = 'idle';
    private startup?: { controller: AbortController; promise: Promise<void> };
    private stopPromise?: Promise<void>;
    private channelRecovery?: ReturnType<typeof setImmediate>;

    private readonly logger: Logger;
    private readonly connectionManager: ConnectionManager;

    private readonly exchanges = new Map<string, ExchangeDeclaration>();
    // The first declared name identifies each queue even when RabbitMQ assigns a new name.
    private readonly queues = new Map<string, QueueDeclaration>();
    private readonly bindings = new Map<string, QueueBinding>();
    private readonly consumers = new Map<string, RegisteredConsumer>();
    private readonly consumerOperationsByQueue = new Map<string, Promise<void>>();

    private rpcReplyQueue?: Replies.AssertQueue;
    private rpcConsumerTag?: string;
    private readonly pendingRpcByChannel = new WeakMap<Channel, Map<string, PendingRpcRequest>>();
    private readonly pendingPublications = new WeakMap<ConfirmChannel, Map<string, PendingPublication>>();

    private constructor(
        public readonly name: string | number,
        private readonly config: BrokerConfig,
    ) {
        super();

        const publishTimeoutMs = config.publishTimeoutMs ?? 10_000;
        if (!Number.isInteger(publishTimeoutMs) || publishTimeoutMs <= 0 || publishTimeoutMs > 2_147_483_647) {
            throw new AmqpError('[AMQP] publishTimeoutMs must be a positive integer no greater than 2147483647');
        }

        this.connectionManager = config.connectionManager;
        this.logger = config.logger ?? defaultLogger;

        this.connectionManager.onConnected(() => this.restart());

        this.connectionManager.onDisconnected(() => {
            this.cancelStartup();
            if (this.channel) {
                this.rejectPendingRpc(this.channel);
            }
            this.channel = null;
            this.topologyConnection = undefined;
            this.state = 'stopped';
            this.emit('disconnected');
        });
    }

    public get started(): boolean {
        return this.state === 'started' && !!this.channel;
    }

    public get currentChannel(): Channel | ConfirmChannel {
        if (!this.channel || this.state !== 'started') {
            throw new AmqpError('[AMQP] Broker channel is not ready');
        }

        return this.channel;
    }

    private getChannel(): Channel | ConfirmChannel {
        if (!this.channel) {
            throw new AmqpError('[AMQP] Broker channel is not ready');
        }

        return this.channel;
    }

    public async start(): Promise<void> {
        if (this.stopPromise) {
            await this.stopPromise;
        }

        if (this.startup) {
            return this.startup.promise;
        }

        const controller = new AbortController();
        const promise = this.startInternal(controller.signal).finally(() => {
            if (this.startup?.controller === controller) {
                this.startup = undefined;
            }
        });
        this.startup = { controller, promise };

        return promise;
    }

    private cancelStartup(): void {
        if (this.channelRecovery) {
            clearImmediate(this.channelRecovery);
            this.channelRecovery = undefined;
        }
        this.startup?.controller.abort();
        this.startup = undefined;
    }

    private async restart(): Promise<void> {
        try {
            await this.start();
        } catch (err) {
            this.logger.error({ err, broker: this.name }, '[AMQP] Broker restart failed');
            this.emitSafe('broker_error', err);
        }
    }

    private scheduleChannelRecovery(connection: ChannelModel): void {
        // Let a simultaneous connection close hand recovery back to the connection manager.
        this.channelRecovery = setImmediate(() => {
            this.channelRecovery = undefined;
            if (this.state === 'stopped' && !this.startup && !this.stopPromise &&
                this.connectionManager.connected && this.connectionManager.currentConnection === connection) {
                void this.restart();
            }
        });
    }

    private async startInternal(signal: AbortSignal): Promise<void> {
        if (this.started) {
            return;
        }

        if (!this.connectionManager.connected) {
            await this.connectionManager.connect();
        }

        if (signal.aborted) {
            return;
        }

        const connection = this.connectionManager.currentConnection;
        for (let attempt = 1; !signal.aborted; attempt++) {
            this.state = 'starting';
            try {
                await this.startChannel(connection, signal);
            } catch (err) {
                if (signal.aborted) {
                    return;
                }

                this.state = 'stopped';
                if (!(err instanceof ExclusiveQueueLockedError)) {
                    throw err;
                }

                const retryDelay = reconnectDelayWithJitter(250, attempt, 5_000);
                this.logger.warn(
                    { err: err.cause, queue: err.queue, broker: this.name, retryDelay },
                    '[AMQP] Exclusive queue is locked; retrying broker recovery',
                );
                try {
                    await wait(retryDelay, undefined, { signal });
                } catch (err) {
                    if (!signal.aborted) {
                        throw err;
                    }
                }
                continue;
            }

            if (!signal.aborted && this.channel) {
                this.state = 'started';
                this.emit('started');
            }
            return;
        }
    }

    private async startChannel(connection: ChannelModel, signal: AbortSignal): Promise<void> {
        const channel = this.config.confirm
            ? await connection.createConfirmChannel()
            : await connection.createChannel();

        let closedByNotFound = false;
        channel.on('error', (err) => {
            closedByNotFound = hasAmqpErrorCode(err, 404);
            this.logger.error({ err, broker: this.name }, '[AMQP] Channel error');
            this.emitSafe('channel_error', err);
        });

        channel.once('close', () => {
            this.rejectPendingRpc(channel);
            if (this.channel === channel) {
                const recover = this.started && closedByNotFound;
                this.channel = null;
                this.state = 'stopped';
                if (recover) {
                    this.scheduleChannelRecovery(connection);
                }
                this.emit('channel_closed');
            }
        });

        if (signal.aborted) {
            await channel.close().catch(() => undefined);
            return;
        }

        const consumersToRestore = [...this.consumers.values()];
        for (const consumer of consumersToRestore) {
            consumer.consumerTag = undefined;
        }

        this.channel = channel;
        this.pendingRpcByChannel.set(channel, new Map());
        this.rpcReplyQueue = undefined;
        this.rpcConsumerTag = undefined;

        try {
            if (this.config.prefetch) {
                await channel.prefetch(this.config.prefetch);
                this.logger.debug({ broker: this.name, prefetch: this.config.prefetch }, '[AMQP] Prefetch configured');
            } else {
                this.logger.warn({ broker: this.name }, '[AMQP] No prefetch configured; consumers may receive many unacked messages');
            }

            await this.restoreTopology(channel, connection, signal);
            await this.restoreConsumers(consumersToRestore, channel, signal);

            if (signal.aborted || this.channel !== channel) {
                return;
            }

            if (this.config.rpc?.enabled !== false) {
                await this.initRpcReplyQueue(channel);
            }
        } catch (err) {
            if (this.channel === channel) {
                this.rejectPendingRpc(channel);
                this.channel = null;
                await channel.close().catch(() => undefined);
            }
            throw err;
        }
    }

    public async stop(): Promise<void> {
        if (this.stopPromise) {
            return this.stopPromise;
        }

        this.cancelStartup();
        this.state = 'stopping';
        const channel = this.channel;
        if (channel) {
            this.rejectPendingRpc(channel, () => new AmqpError('[AMQP] Broker stopped before RPC response'));
        }

        this.stopPromise = Promise.resolve().then(async () => {
            if (channel) {
                try {
                    await channel.close();
                } catch (err) {
                    this.logger.error({ err, broker: this.name }, '[AMQP] Error while closing broker channel');
                }
            }

            this.channel = null;
            this.state = 'stopped';
        }).finally(() => {
            this.stopPromise = undefined;
        });
        return this.stopPromise;
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
        await this.currentChannel.bindQueue(this.resolveQueueName(input.queue), input.exchange, input.routingKey, input.args);

        const binding: QueueBinding = {
            queue: input.queue,
            exchange: input.exchange,
            routingKey: input.routingKey,
            args: input.args,
        };

        this.bindings.set(bindingKey(binding), binding);
    }

    public async unbindQueue(input: BindQueueInput): Promise<void> {
        await this.currentChannel.unbindQueue(this.resolveQueueName(input.queue), input.exchange, input.routingKey, input.args);

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

        if (this.config.confirm) {
            return this.publishWithConfirmation(exchange, routingKey, buffer, buildPublishOptions(payload, options));
        } else {
            return this.currentChannel.publish(exchange, routingKey, buffer, buildPublishOptions(payload, options));
        }
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
        const queueName = this.resolveQueueName(queue);

        if (this.config.confirm) {
            return this.publishWithConfirmation('', queueName, buffer, buildPublishOptions(payload, options));
        } else {
            return this.currentChannel.sendToQueue(queueName, buffer, buildPublishOptions(payload, options));
        }
    }

    private publishWithConfirmation(exchange: string, routingKey: string, buffer: Buffer, options: Options.Publish): Promise<boolean> {
        const channel = this.currentChannel as ConfirmChannel;
        const pending = this.trackPublications(channel);
        const publicationId = uuidv4();

        return new Promise<boolean>((resolve, reject) => {
            const finish = (error?: Error) => {
                if (!pending.delete(publicationId)) {
                    return;
                }
                clearTimeout(timeout);
                if (error) {
                    reject(error);
                } else {
                    resolve(true);
                }
            };
            const timeout = setTimeout(() => {
                finish(new AmqpPublishUnknownError('timeout', options.messageId));
            }, this.config.publishTimeoutMs ?? 10_000);

            pending.set(publicationId, { messageId: options.messageId, fail: finish });

            try {
                channel.publish(exchange, routingKey, buffer, {
                    ...options,
                    mandatory: true,
                    headers: { ...options.headers, [publicationIdHeader]: publicationId },
                }, (err) => {
                    finish(err ? new AmqpPublishError('[AMQP] Publication was not confirmed', err) : undefined);
                });
            } catch (err) {
                finish(new AmqpPublishError('[AMQP] Publication failed', err));
            }
        });
    }

    private trackPublications(channel: ConfirmChannel): Map<string, PendingPublication> {
        const existing = this.pendingPublications.get(channel);
        if (existing) {
            return existing;
        }

        const pending = new Map<string, PendingPublication>();
        this.pendingPublications.set(channel, pending);

        const onReturn = (message: Message & { fields: { replyCode: number; replyText: string } }) => {
            const publicationId = message.properties.headers?.[publicationIdHeader];
            const publication = pending.get(publicationId);
            if (publication) {
                publication.fail(new AmqpUnroutableError(
                    message.fields.replyCode,
                    message.fields.replyText,
                    message.fields.exchange,
                    message.fields.routingKey,
                    publication.messageId,
                ));
            }
        };

        channel.on('return', onReturn);
        // Run before amqplib rejects outstanding confirm callbacks on close.
        channel.prependOnceListener('close', () => {
            for (const publication of pending.values()) {
                publication.fail(new AmqpPublishUnknownError('channel_closed', publication.messageId));
            }
            channel.removeListener('return', onReturn);
            this.pendingPublications.delete(channel);
        });

        return pending;
    }

    public async sendToQueueInput(input: SendToQueueInput): Promise<boolean> {
        let queue = input.queue;
        if (input.assertQueue) {
            const declared = await this.assertQueue({
                name: input.queue,
                options: input.assertQueue,
            });
            queue = declared.queue;
        }

        return this.sendToQueue(queue, input.payload, input.options);
    }

    // ---------------------------------------------------------------------------
    // Consumers
    // ---------------------------------------------------------------------------

    public async consumeQueue(queue: string, handler: MessageHandler, options?: Options.Consume): Promise<ConsumerHandle> {
        return this.registerConsumer(queue, (msg, delivery) => handler(msg, delivery.context), options);
    }

    private async registerConsumer(queue: string, handler: RegisteredConsumer['handler'], options?: Options.Consume): Promise<ConsumerHandle> {
        const id = uuidv4();

        const registration: RegisteredConsumer = {
            id,
            queue,
            handler,
            options,
            state: 'active',
        };

        const channel = this.getChannel();
        await this.startConsumer(registration, channel);
        if (this.channel !== channel || this.state === 'stopping') {
            throw new AmqpConsumerError('[AMQP] Channel closed before consumer registration completed');
        }

        this.consumers.set(id, registration);

        return new ConsumerHandle(this, id);
    }

    public async pauseConsumer(id: string): Promise<void> {
        const consumer = this.consumers.get(id);
        if (!consumer) {
            return;
        }

        await this.runConsumerOperation(consumer.queue, async () => {
            if (consumer.state !== 'active') {
                return;
            }

            if (consumer.consumerTag) {
                await this.currentChannel.cancel(consumer.consumerTag);
            }

            consumer.consumerTag = undefined;
            consumer.state = 'paused';
        });
    }

    public async resumeConsumer(id: string): Promise<void> {
        const consumer = this.consumers.get(id);
        if (!consumer) {
            return;
        }

        await this.runConsumerOperation(consumer.queue, async () => {
            if (consumer.state !== 'paused') {
                return;
            }

            const channel = this.getChannel();
            await this.restoreAutoDeleteQueue(consumer.queue, channel);
            if (this.channel !== channel) {
                throw new AmqpConsumerError('[AMQP] Channel closed before consumer could resume');
            }

            await this.startConsumer(consumer, channel);
            if (this.channel !== channel) {
                throw new AmqpConsumerError('[AMQP] Channel closed before consumer could resume');
            }

            consumer.state = 'active';
        });
    }

    public async closeConsumer(id: string): Promise<void> {
        const consumer = this.consumers.get(id);
        if (!consumer) {
            return;
        }

        await this.runConsumerOperation(consumer.queue, async () => {
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
        });
    }

    private async runConsumerOperation(queue: string, operation: () => Promise<void>): Promise<void> {
        // Cancellation of the last consumer can delete the queue being resumed by another handle.
        const previous = this.consumerOperationsByQueue.get(queue) ?? Promise.resolve();
        const pending = previous.catch(() => undefined).then(operation);
        this.consumerOperationsByQueue.set(queue, pending);

        try {
            await pending;
        } finally {
            if (this.consumerOperationsByQueue.get(queue) === pending) {
                this.consumerOperationsByQueue.delete(queue);
            }
        }
    }

    private async restoreAutoDeleteQueue(queue: string, channel: Channel): Promise<void> {
        const declaration = this.queues.get(queue);
        if (!declaration?.options?.autoDelete) {
            return;
        }

        if (await this.queueExists(declaration.actualName, this.connectionManager.currentConnection)) {
            return;
        }

        if (this.channel !== channel) {
            return;
        }

        const bindings = [...this.bindings.values()].filter((binding) => binding.queue === queue);
        const exchanges = new Set(bindings.map((binding) => binding.exchange));
        for (const name of exchanges) {
            const exchange = this.exchanges.get(name);
            if (exchange) {
                await channel.assertExchange(exchange.name, exchange.type, exchange.options);
            }
        }

        const { queue: actualName } = await channel.assertQueue(declaration.requestedName, declaration.options);
        for (const binding of bindings) {
            await channel.bindQueue(actualName, binding.exchange, binding.routingKey, binding.args);
        }

        if (this.channel === channel) {
            this.queues.set(queue, { ...declaration, actualName });
        }
    }

    private async startConsumer(consumer: RegisteredConsumer, channel = this.getChannel()): Promise<void> {
        const consumerTag = consumer.consumerTag ?? uuidv4();

        const result = await channel.consume(
            this.resolveQueueName(consumer.queue),
            async (msg) => {
                if (!msg) {
                    if (this.channel === channel) {
                        await this.handleConsumerCancelled(consumer);
                    }
                    return;
                }

                await this.handleMessage(consumer, msg, channel);
            },
            {
                ...consumer.options,
                consumerTag,
            },
        );

        if (this.channel === channel) {
            consumer.consumerTag = result.consumerTag;
        }
    }

    private async handleMessage(consumer: RegisteredConsumer, msg: ConsumeMessage, channel: Channel): Promise<void> {
        const delivery = this.createDeliveryContext(consumer, msg, channel);
        const { context } = delivery;

        try {
            await consumer.handler(msg, delivery);

            if (!context.settled && consumer.options?.noAck !== true) {
                this.logger.warn({ consumerId: consumer.id, queue: consumer.queue }, '[AMQP] Message handler completed without ack/nack/reject');
            }
        } catch (err) {
            this.logger.error({ err, consumerId: consumer.id, queue: consumer.queue }, '[AMQP] Message handler failed');

            if (!context.settled && consumer.options?.noAck !== true) {
                const requeue = this.config.consumers?.requeueOnHandlerError ?? false;
                context.nack(false, requeue);
            }

            this.emitSafe('consumer_error', new AmqpConsumerError('[AMQP] Consumer handler failed', err));
        }
    }

    private async handleConsumerCancelled(consumer: RegisteredConsumer): Promise<void> {
        consumer.consumerTag = undefined;

        if (consumer.state !== 'active') {
            return;
        }

        const restart = this.config.consumers?.restartOnCancel ?? true;

        this.logger.warn({ consumerId: consumer.id, queue: consumer.queue, restart }, '[AMQP] Consumer cancelled');

        if (restart) {
            try {
                await this.startConsumer(consumer);
            } catch (err) {
                this.logger.error({ err, consumerId: consumer.id, queue: consumer.queue }, '[AMQP] Consumer restart failed');
                this.emitSafe('consumer_error', new AmqpConsumerError('[AMQP] Consumer restart failed', err));
            }
        }
    }

    private createDeliveryContext(consumer: RegisteredConsumer, msg: ConsumeMessage, channel: Channel): DeliveryContext {
        let settled = false;

        const settle: DeliveryContext['settle'] = (action) => {
            if (settled) {
                return;
            }

            settled = true;
            if (this.channel !== channel || this.state === 'stopping') {
                this.logger.warn(
                    { consumerId: consumer.id, queue: consumer.queue, deliveryTag: msg.fields.deliveryTag },
                    '[AMQP] Delivery could not be settled because its channel is no longer active',
                );
                return;
            }

            action(channel);
        };

        const context: ConsumeContext = {
            get settled() {
                return settled;
            },

            ack: (allUpTo) => settle((channel) => channel.ack(msg, allUpTo)),
            nack: (allUpTo, requeue) => settle((channel) => channel.nack(msg, allUpTo ?? false, requeue ?? false)),
            reject: (requeue) => settle((channel) => channel.reject(msg, requeue)),

            buffer: () => msg.content,

            text: (encoding: BufferEncoding = 'utf8') => msg.content.toString(encoding),

            json: <T = unknown>() => parseJson<T>(msg.content),
        };

        return { context, settle };
    }

    // ---------------------------------------------------------------------------
    // RPC client
    // ---------------------------------------------------------------------------

    public async requestQueue<T = unknown>(input: RpcRequestQueueInput): Promise<T> {
        return this.requestRpc<T>(input, (channel, content, options) =>
            channel.sendToQueue(this.resolveQueueName(input.queue), content, options),
        );
    }

    public async requestExchange<T = unknown>(input: RpcRequestExchangeInput): Promise<T> {
        return this.requestRpc<T>(input, (channel, content, options) =>
            channel.publish(input.exchange, input.routingKey, content, options),
        );
    }

    private async requestRpc<T>(
        input: RpcRequestQueueInput | RpcRequestExchangeInput,
        send: (channel: Channel, content: Buffer, options: Options.Publish) => boolean,
    ): Promise<T> {
        const channel = await this.ensureRpcEnabled();
        const replyQueue = this.rpcReplyQueue;
        const requests = this.pendingRpcByChannel.get(channel);
        if (this.channel !== channel || !replyQueue || !requests) {
            throw new AmqpError('[AMQP] Broker channel is not ready');
        }

        const correlationId = uuidv4();
        const timeoutMs = input.timeoutMs ?? this.config.rpc?.timeoutMs ?? 30_000;

        return new Promise<T>((resolve, reject) => {
            const timeout = setTimeout(() => {
                requests.delete(correlationId);
                reject(new AmqpRpcTimeoutError(timeoutMs));
            }, timeoutMs);

            requests.set(correlationId, {
                correlationId,
                resolve: resolve as (value: unknown) => void,
                reject,
                timeout,
            });

            try {
                send(channel, toBuffer(input.payload), {
                    ...buildPublishOptions(input.payload, input.options),
                    replyTo: replyQueue.queue,
                    correlationId,
                });
            } catch (err) {
                clearTimeout(timeout);
                requests.delete(correlationId);
                reject(err);
            }
        });
    }

    private rejectPendingRpc(
        channel: Channel,
        errorFor: (correlationId: string) => AmqpError = (correlationId) => new AmqpRpcUnknownError(correlationId),
    ): void {
        const requests = this.pendingRpcByChannel.get(channel);
        if (!requests) {
            return;
        }

        this.pendingRpcByChannel.delete(channel);
        for (const request of requests.values()) {
            clearTimeout(request.timeout);
            request.reject(errorFor(request.correlationId));
        }
        requests.clear();
    }

    private async ensureRpcEnabled(): Promise<Channel> {
        if (this.config.rpc?.enabled === false) {
            throw new AmqpError(`[AMQP] RPC is disabled for broker "${this.name}"`);
        }

        const channel = this.getChannel();
        if (!this.rpcReplyQueue) {
            await this.initRpcReplyQueue(channel);
        }
        return channel;
    }

    private async initRpcReplyQueue(channel: Channel): Promise<void> {
        const queueName = this.config.rpc?.replyQueue ?? '';

        const replyQueue = await channel.assertQueue(queueName, {
            exclusive: !queueName,
            autoDelete: !queueName,
            durable: false,
        });

        const result = await channel.consume(
            replyQueue.queue,
            (msg) => {
                if (!msg) {
                    return;
                }

                const correlationId = msg.properties.correlationId;

                if (!correlationId) {
                    return;
                }

                const requests = this.pendingRpcByChannel.get(channel);
                const pending = requests?.get(correlationId);

                if (!pending) {
                    return;
                }

                clearTimeout(pending.timeout);
                requests?.delete(correlationId);

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

        if (this.channel === channel) {
            this.rpcReplyQueue = replyQueue;
            this.rpcConsumerTag = result.consumerTag;
        }
    }

    // ---------------------------------------------------------------------------
    // RPC server
    // ---------------------------------------------------------------------------

    public async consumeRpcQueue(queue: string, handler: RpcHandler, options?: Options.Consume): Promise<ConsumerHandle> {
        return this.registerConsumer(
            queue,
            async (msg, delivery) => {
                const ctx = this.createRpcContext(msg, delivery);

                try {
                    await handler(msg, ctx);

                    if (!ctx.settled) {
                        this.logger.warn({ queue }, '[AMQP] RPC handler completed without reply/nack/reject');
                    }
                } catch (err) {
                    this.logger.error({ err, queue }, '[AMQP] RPC handler failed');

                    if (!ctx.settled) {
                        ctx.nack(true);
                    }
                }
            },
            options,
        );
    }

    private createRpcContext(msg: ConsumeMessage, { context, settle }: DeliveryContext): RpcContext {
        return {
            get settled() {
                return context.settled;
            },

            reply: (payload, options) => settle((channel) => {
                if (msg.properties.replyTo) {
                    channel.sendToQueue(msg.properties.replyTo, toBuffer(payload), {
                        ...buildPublishOptions(payload, options),
                        correlationId: msg.properties.correlationId,
                    });
                }

                channel.ack(msg);
            }),

            nack: (requeue) => context.nack(false, requeue ?? true),
            reject: context.reject,
            buffer: context.buffer,
            text: context.text,
            json: context.json,
        };
    }

    // ---------------------------------------------------------------------------
    // Restore after reconnect
    // ---------------------------------------------------------------------------

    private resolveQueueName(queue: string): string {
        return this.queues.get(queue)?.actualName ?? queue;
    }

    private async queueExists(queue: string, connection: ChannelModel): Promise<boolean> {
        // A failed passive declaration closes its channel. Exclusive queues must be checked on the same connection.
        const probe = await connection.createChannel();
        // checkQueue rejects with the protocol error as well as emitting it.
        probe.on('error', () => {});
        try {
            await probe.checkQueue(queue);
            return true;
        } catch (err) {
            if (hasAmqpErrorCode(err, 404)) {
                return false;
            }
            throw err;
        } finally {
            await probe.close().catch(() => undefined);
        }
    }

    private async restoreTopology(channel: Channel, connection: ChannelModel, signal: AbortSignal): Promise<void> {
        for (const exchange of this.exchanges.values()) {
            await channel.assertExchange(exchange.name, exchange.type, exchange.options);
        }

        const restoredQueues = new Map<string, QueueDeclaration>();
        const reuseGeneratedQueues = this.topologyConnection === connection;

        // Declare fixed names first so a lock cannot leave generated queues on the connection.
        const declarations = [...this.queues].sort(([, a], [, b]) => Number(!a.requestedName) - Number(!b.requestedName));
        for (const [name, queue] of declarations) {
            if (signal.aborted || this.channel !== channel) {
                return;
            }

            try {
                // A generated queue can survive a channel close when its connection stays open.
                if (!queue.requestedName && reuseGeneratedQueues && await this.queueExists(queue.actualName, connection)) {
                    restoredQueues.set(name, queue);
                } else {
                    const result = await channel.assertQueue(queue.requestedName, queue.options);
                    restoredQueues.set(name, { ...queue, actualName: result.queue });
                }
            } catch (err) {
                if (queue.requestedName && queue.options?.exclusive && hasAmqpErrorCode(err, 405)) {
                    throw new ExclusiveQueueLockedError(queue.requestedName, err);
                }
                throw err;
            }
        }

        for (const binding of this.bindings.values()) {
            const queueName = restoredQueues.get(binding.queue)?.actualName ?? binding.queue;
            await channel.bindQueue(queueName, binding.exchange, binding.routingKey, binding.args);
        }

        if (!signal.aborted && this.channel === channel) {
            for (const [name, queue] of restoredQueues) {
                this.queues.set(name, queue);
            }
            this.topologyConnection = connection;
        }
    }

    private async restoreConsumers(consumers: RegisteredConsumer[], channel: Channel, signal: AbortSignal): Promise<void> {
        for (const consumer of consumers) {
            if (signal.aborted || this.channel !== channel) {
                return;
            }

            if (consumer.state === 'active' && !consumer.consumerTag) {
                await this.startConsumer(consumer, channel);
            }
        }
    }

    private emitSafe(event: string, payload?: unknown): void {
        if (this.listenerCount(event) > 0) {
            this.emit(event, payload);
        }
    }
}
