import { Channel, connect, Connection, ConsumeMessage, Options, Replies } from 'amqplib';
import _ from 'lodash';
import { EventEmitter } from 'events';
import * as uuid from 'uuid';
import { ConnectionManager } from './connection-manager';
import { v4 as uuidv4 } from 'uuid';
import { Consumer } from './consumer';

enum Type {
    EXCHANGE,
    QUEUE
}

type Override<T1, T2> = Omit<T1, keyof T2> & T2;

type QueueConfig = {
    queue: string;
    options?: Options.AssertQueue;
    active: boolean;
    handler: ((msg: ConsumeMessage, ack: (response?: any, allUpTo?: boolean) => any, nack: (response?: any, allUpTo?: boolean, requeue?: boolean) => any) => Promise<any>)[];
    rpc?: boolean;
    consumerTag?: string;
};

type ExchangeConfig = Override<QueueConfig, {
    type: string;
    exchange: string;
    routingKey: string;
    options?: Options.AssertExchange;
    optionsQueue?: Options.AssertQueue;
}>;

type InstanceConfig = {
    connectionManager?: ConnectionManager,
    prefetch?: number,
    disableRPC?: boolean,
    cancelNotification?: boolean,
    recover?: boolean;
};

export class MessageBroker {

    public static async getInstance(index: number | string = 0): Promise<MessageBroker> {
        if (!MessageBroker.instance[index]) {
            await this.initInstance(index);
        }
        return MessageBroker.instance[index];
    }

    public static async initInstance(index: number | string = 0, config: InstanceConfig = {}): Promise<MessageBroker> {

        if (MessageBroker.instance[index]) {
            throw new Error(`[AMQP] Instance ${index} already exist`);
        }
        if (!config.connectionManager) {
            config.connectionManager = await ConnectionManager.getConnexion();
        }

        const broker = new MessageBroker(index, config);
        MessageBroker.instance[index] = broker;
        config.connectionManager.brokers.push(broker);
        await broker.start();

        return broker;
    }

    public static getAllInstances(): Promise<MessageBroker>[] {
        return MessageBroker.instance;
    }

    private static instance: Promise<MessageBroker>[] = [];
    public channel: Channel;

    private exchanges: Map<string, ExchangeConfig> = new Map<string, ExchangeConfig>();
    private queues: Map<string, QueueConfig> = new Map<string, QueueConfig>();

    private rpcQueue: Replies.AssertQueue;
    private correlationIds: any[] = [];
    private responseEmitter: EventEmitter = new EventEmitter();

    private constructor(private name: number | string, private config?: InstanceConfig) {
        this.responseEmitter.setMaxListeners(0);
    }

    public async sendToExchange(exchange: string, routingKey: string, msg: Buffer,
        type: string = 'direct', optionAssert?: Options.AssertExchange, optionPublish?: Options.Publish): Promise<boolean> {

        await this.channel.assertExchange(exchange, type, optionAssert);
        return this.channel.publish(exchange, routingKey, msg, { timestamp: Date.now(), ...optionPublish });
    }

    public async send(queue: string, msg: Buffer, optionPublish?: Options.Publish, optionAssert?: Options.AssertQueue): Promise<boolean> {
        await this.channel.assertQueue(queue, { durable: true, ...optionAssert });
        return this.channel.sendToQueue(queue, msg, { timestamp: Date.now(), ...optionPublish });
    }

    public async sendRPCToExchange(exchange: string, routingKey: string, msg: Buffer,
        type: string = 'direct', optionAssert?: Options.AssertExchange): Promise<any> {

        if (this.config?.disableRPC) {
            throw new Error(`[AMQP] RPC is disable for instance ${this.name}`);
        }

        await this.channel.assertExchange(exchange, type, { durable: false, autoDelete: true, ...optionAssert });

        return new Promise<any>((resolve) => {
            const correlationId = uuid.v4();
            this.correlationIds.push(correlationId);

            this.responseEmitter.once(correlationId, resolve);
            this.channel.publish(exchange, routingKey, msg, { timestamp: Date.now(), replyTo: this.rpcQueue.queue, correlationId });
        });

    }

    public async sendRPCMessage(queue: string, msg: Buffer): Promise<any> {

        if (this.config?.disableRPC) {
            throw new Error(`[AMQP] RPC is disable for instance ${this.name}`);
        }

        await this.channel.assertQueue(queue, { durable: false });

        return new Promise<any>((resolve) => {

            const correlationId = uuid.v4();
            this.correlationIds.push(correlationId);

            this.responseEmitter.once(correlationId, resolve);
            this.channel.sendToQueue(queue, msg, {
                correlationId,
                replyTo: this.rpcQueue.queue,
            });
        });
    }

    public async receiveRPCMessage(queue: string, handler: ((msg: ConsumeMessage, ack: (response: string, allUpTo?: boolean) => void,
        nack: (allUpTo?: boolean, requeue?: boolean) => void) => Promise<void>)): Promise<void> {
        await this.channel.assertQueue(queue, { durable: false });

        const key = JSON.stringify({ exchange: '', routingKey: '', queue });

        this.queues.set(key, {
            handler: [handler],
            queue,
            active: true,
            rpc: true
        });

        this.channel.consume(
            queue,
            async (msg) => {
                const ack = _.once((response: string) => {

                    this.channel.sendToQueue(msg.properties.replyTo,
                        Buffer.from(response),
                        {
                            correlationId: msg.properties.correlationId,
                            timestamp: Date.now()
                        }
                    );
                    this.channel.ack(msg);
                });
                const nack = _.once((allUpTo?: boolean, requeue?: boolean) => this.channel.nack(msg, allUpTo, requeue));
                await handler(msg, ack, nack);
            }
        );
    }

    private async subscribeExchangeMultiRoutingkey(queue: string, exchange: string, routingKeys: string[], type: string = 'direct',
        handler: ((msg: ConsumeMessage, ack: () => void, nack: () => void) => Promise<void>),
        options?: Options.AssertExchange, optionsQueue?: Options.AssertQueue): Promise<Consumer[]> {

        let keys = routingKeys.map((routingKey) => JSON.stringify({ exchange, routingKey, queue }));

        const unsubscribes = keys.filter((key) => this.exchanges.has(key) && this.exchanges.get(key).active)
            .map((key) => {
                const existingHandler = _.find(this.exchanges.get(key).handler, (h) => h === handler);
                if (existingHandler) {
                    /* Si on a déjà souscrit à la queue*/
                    // return () => this.unsubscribe(key, existingHandler);
                    return new Consumer(this, this.exchanges.get(key).consumerTag, key, handler);
                }
                this.exchanges.get(key).handler.push(handler);
                // return () => this.unsubscribe(key, handler);
                return new Consumer(this, this.exchanges.get(key).consumerTag, key, handler);
            });
        keys = keys.filter((key) => !(this.exchanges.has(key) && this.exchanges.get(key).active));

        if (keys.length) {
            await this.channel.assertExchange(exchange, type, options);
            const q = await this.channel.assertQueue(queue, { durable: true, autoDelete: !!!queue, exclusive: !!!queue, ...optionsQueue });
            await Promise.all(routingKeys.map((routingKey) => this.channel.bindQueue(q.queue, exchange, routingKey)));

            const consumerTag = uuidv4();

            keys.forEach((key, i) => {
                this.exchanges.set(key, {
                    exchange,
                    queue,
                    routingKey: routingKeys[i],
                    type,
                    options,
                    optionsQueue: { durable: true, autoDelete: !!!queue, exclusive: !!!queue, ...optionsQueue },
                    active: true,
                    handler: [handler],
                    consumerTag
                });
            });

            await this.consume(this.channel, q.queue, this.exchanges.get(keys[0]).handler, consumerTag, Type.EXCHANGE);

            return keys.map((key) => new Consumer(this, consumerTag, key, handler)).concat(unsubscribes);
        } else {
            return unsubscribes;
        }

    }

    public async subscribeExchange(queue: string, exchange: string, routingKey: string | string[], type: string = 'direct',
        handler: ((msg: ConsumeMessage, ack: (allUpTo?: boolean) => void, nack: (allUpTo?: boolean, requeue?: boolean) => void) => Promise<void>),
        options?: Options.AssertExchange, optionsQueue?: Options.AssertQueue): Promise<Consumer | Consumer[]> {

        if (Array.isArray(routingKey)) {
            return await this.subscribeExchangeMultiRoutingkey(queue, exchange, routingKey, type, handler, options, optionsQueue);
        }

        const key = JSON.stringify({ exchange, routingKey, queue });

        if (this.exchanges.has(key) && this.exchanges.get(key).active) {
            const existingHandler = _.find(this.exchanges.get(key).handler, (h) => h === handler);
            if (existingHandler) {
                /* Si on a déjà souscrit à la queue*/
                // return () => this.unsubscribe(key, existingHandler);
                return new Consumer(this, this.exchanges.get(key).consumerTag, key, handler);
            }
            this.exchanges.get(key).handler.push(handler);
            // return () => this.unsubscribe(key, handler);
            return new Consumer(this, this.exchanges.get(key).consumerTag, key, handler);
        }

        await this.channel.assertExchange(exchange, type, options);
        const q = await this.channel.assertQueue(queue, { durable: true, autoDelete: !!!queue, exclusive: !!!queue, ...optionsQueue });
        await this.channel.bindQueue(q.queue, exchange, routingKey);

        const consumerTag = uuidv4();

        this.exchanges.set(key, {
            exchange,
            queue,
            routingKey,
            type,
            options,
            optionsQueue: { durable: true, autoDelete: !!!queue, exclusive: !!!queue, ...optionsQueue },
            active: true,
            handler: [handler],
            consumerTag
        });

        await this.consume(this.channel, q.queue, this.exchanges.get(key).handler, consumerTag, Type.EXCHANGE);
        // return () => this.unsubscribe(key, handler);
        return new Consumer(this, consumerTag, key, handler);
    }

    public async subscribe(queue: string,
        handler: ((msg: ConsumeMessage, ack: (allUpTo?: boolean) => void, nack: (allUpTo?: boolean, requeue?: boolean) => void) => Promise<void>),
        options?: Options.AssertQueue, optionConsume?: Options.Consume): Promise<Consumer> {

        const key = JSON.stringify({ exchange: '', routingKey: '', queue });

        if (this.queues.has(key) && this.queues.get(key).active) {
            /*Si la queue existe déjà*/
            const existingHandler = _.find(this.queues.get(key).handler, (h) => h === handler);
            if (existingHandler) {
                /* Si on a déjà souscrit à la queue*/
                return new Consumer(this, this.queues.get(key).consumerTag, key, handler);
            }
            this.queues.get(key).handler.push(handler);
            return new Consumer(this, this.queues.get(key).consumerTag, key, handler);
        }

        await this.channel.assertQueue(queue, options);

        const consumerTag = uuidv4();

        this.queues.set(key, {
            handler: [handler],
            queue,
            active: true,
            options,
            consumerTag
        });

        await this.consume(this.channel, queue, this.queues.get(key).handler, consumerTag, Type.QUEUE, optionConsume);
        // return () => this.unsubscribe(key, handler);
        return new Consumer(this, consumerTag, key, handler);
    }

    public unsubscribe(key: string, handler: ((msg: ConsumeMessage, ack: () => void, nack: () => void) => Promise<void>)): void {
        if (this.queues.has(key)) {
            _.pull(this.queues.get(key).handler, handler);
        } else if (this.exchanges.has(key)) {
            _.pull(this.exchanges.get(key).handler, handler);
        }
    }

    public async disconnect(): Promise<void> {
        try {
            if (this.channel) {
                await this.clearRPC();
                if (this.config?.recover !== undefined ? this.config?.recover : true) {
                    await this.channel.recover();
                }
                await this.channel.close();
            }
        } catch (err) {
            console.error(`[AMQP] Error while disconnecting instance ${this.name}`);
            console.error(err);
        }
    }

    public async pause(consumerTag: string): Promise<void> {
        await this.channel.cancel(consumerTag);
    }

    public async resume(consumerTag: string): Promise<void> {
        const configs = this.getConfigFromConsumerTag(consumerTag);
        if (configs.length) {
            await this.channel.cancel(consumerTag);

            const q = await (this.isExchangeconfig(configs[0]) ? this.channel.assertQueue(configs[0].queue,
                { durable: true, autoDelete: !!!configs[0].queue, exclusive: !!!configs[0].queue, ...configs[0].optionsQueue })
                : this.channel.assertQueue(configs[0].queue, configs[0].options));

            for (const config of configs) {
                if (this.isExchangeconfig(config)) {
                    await this.channel.assertExchange(config.exchange, config.type, config.options);
                    await this.channel.bindQueue(config.queue, config.exchange, config.routingKey);
                }
            }
            await this.consume(this.channel, q.queue, configs[0].handler, configs[0].consumerTag, this.isExchangeconfig(configs[0]) ? Type.EXCHANGE : Type.QUEUE);
        }
    }

    private isExchangeconfig(config: QueueConfig | ExchangeConfig): config is ExchangeConfig {
        return (config as ExchangeConfig).exchange !== undefined;
    }

    private getConfigFromConsumerTag(consumerTag: string): (QueueConfig | ExchangeConfig)[] {
        return [...Array.from(this.exchanges.entries()), ...Array.from(this.queues.entries())].filter(([key, value]) => {
            return value.consumerTag === consumerTag;
        }).map(([key, value]) => value);
    }

    private async consume(channel: Channel, queue: string,
        handlers:
            ((
                msg: ConsumeMessage,
                ack: (response?: any, allUpTo?: boolean) => any,
                nack: (response?: any, allUpTo?: boolean, requeue?: boolean) => any
            ) => Promise<any>)[],
        consumerTag: string, type: Type, optionConsume?: Options.Consume): Promise<Replies.Consume> {
        return await channel.consume(
            queue,
            async (msg) => {
                if (!msg) {
                    if (type === Type.QUEUE) {
                        await this.restartQueueAfterKill(queue, consumerTag);
                    } else if (type === Type.EXCHANGE) {
                        await this.restartExchangeAfterKill(queue, consumerTag);
                    }
                }

                if (!msg && !this.config?.cancelNotification) {
                    return;
                }
                const ack = _.once((allUpTo?: boolean) => {
                    try {
                        channel.ack(msg, allUpTo);
                    } catch (err) {
                        console.error(`[AMQP] Error while ACK`);
                        console.error(err);
                    }

                });
                const nack = _.once((allUpTo?: boolean, requeue?: boolean) => {
                    try {
                        channel.nack(msg, allUpTo, requeue);
                    } catch (err) {
                        console.error(`[AMQP] Error while NACK`);
                        console.error(err);
                    }
                });
                await Promise.all(handlers.map((h) => h(msg, ack, nack)));
            },
            {
                ...optionConsume,
                consumerTag,
            }
        );
    }

    private async clearRPC(): Promise<void> {
        if (this.channel && this.rpcQueue?.queue) {
            await this.channel.deleteQueue(this.rpcQueue.queue);
        }
    }

    private async init(): Promise<MessageBroker> {
        this.channel = await this.config.connectionManager.connection.createChannel();

        let warningPrefetch = true;

        if (this.config?.prefetch) {
            await this.channel.prefetch(this.config?.prefetch);
            warningPrefetch = false;
        }
        else if (process.env.RABBITMQ_PREFETCH || process.env.PREFETCH) {

            if (isNaN(+process.env.RABBITMQ_PREFETCH || +process.env.PREFETCH)) {
                throw new Error('RABBITMQ_PREFETCH or PREFETCH is not a number');
            }

            await this.channel.prefetch(+process.env.RABBITMQ_PREFETCH || +process.env.PREFETCH);
            warningPrefetch = false;
        }

        if (warningPrefetch) {
            console.warn('\x1b[31m', '[AMQP] Warning, No prefetch defined for the instance', this.name);
        }

        if (!this.config?.disableRPC) {
            if (!this.rpcQueue) {
                this.rpcQueue = await this.channel.assertQueue(process.env.RABBITMQ_RPCQUEUE || '', {
                    exclusive: false,
                    expires: 1800000 /* expire after 30 minutes with no consumer and no activity*/
                });
            }

            await this.listenRPC();
        }
        await this.restartQueues();
        await this.restartExchanges();

        return this;
    }

    public async start(): Promise<MessageBroker> {
        Array.from(this.exchanges.entries()).forEach(([key, value]) => {
            this.exchanges.set(key, { ...value, active: false });
        });
        Array.from(this.queues.entries()).forEach(([key, value]) => {
            this.queues.set(key, { ...value, active: false });
        });
        return await this.init();
    }

    private async restartQueueAfterKill(queue: string, consumerTag?: string): Promise<void> {
        console.warn(`[AMQP] Warning consumer on queue ${queue} has been cancelled`);
        console.warn(`[AMQP] Re-creation of consumer on queue ${queue}`);
        await Promise.all(Array.from(this.queues.values())
            .filter((q) => q.queue === queue && !q.rpc && (consumerTag ? q.consumerTag === consumerTag : true))
            .flatMap((q) => {
                q.active = false;
                return q.handler.map((h) => {
                    return this.subscribe(q.queue, h, q.options);
                });
            }));
    }

    private async restartExchangeAfterKill(queue: string, consumerTag?: string): Promise<void> {
        console.warn(`[AMQP] Warning consumer on exchange with queue ${queue} has been cancelled`);
        console.warn(`[AMQP] Re-creation of consumer on queue ${queue}`);

        await Promise.all(Array.from(this.exchanges.values())
            .filter((q) => (consumerTag ? q.consumerTag === consumerTag : true) && q.queue === queue)
            .flatMap((q) => {
                q.active = false;
                return q.handler.map((h) => {
                    return this.subscribeExchange(q.queue, q.exchange, q.routingKey, q.type, h, q.options, q.optionsQueue);
                });
            }));
    }

    private async restartQueues(): Promise<void> {
        await Promise.all(Array.from(this.queues.values()).flatMap((q) => {
            return q.handler.map((h) => {
                if (q.rpc) {
                    return this.receiveRPCMessage(q.queue, h);
                } else {
                    return this.subscribe(q.queue, h, q.options);
                }
            });
        }));
    }

    private async restartExchanges(): Promise<void> {
        await Promise.all(Array.from(this.exchanges.values()).flatMap((q) => {
            return q.handler.map((h) => {
                return this.subscribeExchange(q.queue, q.exchange, q.routingKey, q.type, h, q.options, q.optionsQueue);
            });
        }));
    }

    private async listenRPC(): Promise<void> {
        try {
            await this.channel.consume(this.rpcQueue.queue, (msg) => {
                if (msg) {
                    const index = this.correlationIds.indexOf(msg.properties.correlationId, 0);
                    if (index !== -1) {
                        this.responseEmitter.emit(
                            msg.properties.correlationId,
                            JSON.parse(msg.content.toString('utf8')),
                        );
                        this.correlationIds.splice(index, 1);
                    }
                }
            }, { noAck: true });
        } catch (err) {
            console.error('[AMQP] Error on listen rpc');
            console.error(err);
            process.exit();
        }
    }
}
