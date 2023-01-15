import { Channel, connect, Connection, ConsumeMessage, Options, Replies } from 'amqplib';
import _ from 'lodash';
import { EventEmitter } from 'events';
import * as uuid from 'uuid';
import { exit } from 'process';

type Override<T1, T2> = Omit<T1, keyof T2> & T2;

type QueueConfig = {
    queue: string;
    options?: Options.AssertQueue;
    active: boolean;
    handler: ((msg: ConsumeMessage, ack: () => void) => Promise<void>)[];
};

type ExchangeConfig = Override<QueueConfig, {
    type: string;
    exchange: string;
    routingKey: string;
    options?: Options.AssertExchange;
}>;


export class MessageBroker {

    public static async getInstance(): Promise<MessageBroker> {
        if (!MessageBroker.instance) {
            const broker = new MessageBroker();
            MessageBroker.instance = broker.init();

            process.on('SIGINT', async () => await broker.disconnect());
            process.on('SIGTERM', async () => await broker.disconnect());
            process.on('SIGQUIT', async () => await broker.disconnect());
        }
        return MessageBroker.instance;
    }

    private static instance: Promise<MessageBroker>;
    private connection: Connection;
    public channel: Channel;

    private timeout: number = Number(process.env.RABBITMQ_TIMEOUT) || 5000;

    private exchanges: Map<string, ExchangeConfig> = new Map<string, ExchangeConfig>();
    private queues: Map<string, QueueConfig> = new Map<string, QueueConfig>();

    private rpcQueue: Replies.AssertQueue;
    private correlationIds: any[] = [];
    private responseEmitter: EventEmitter = new EventEmitter();

    private isDisconnected: boolean = false;

    private constructor() {
        this.responseEmitter.setMaxListeners(0);
    }

    public async sendToExchange(exchange: string, routingKey: string, msg: Buffer,
        type: string = 'direct', options?: Options.AssertExchange): Promise<boolean> {

        await this.channel.assertExchange(exchange, type, options);
        return this.channel.publish(exchange, routingKey, msg);
    }

    public async send(queue: string, msg: Buffer, options?: Options.Publish): Promise<boolean> {
        await this.channel.assertQueue(queue, { durable: true });
        return this.channel.sendToQueue(queue, msg, options);
    }

    public async sendRPCMessage(queue: string, msg: Buffer): Promise<any> {
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

    public async receiveRPCMessage(queue: string, handler: ((msg: ConsumeMessage, ack: (response: string) => void) => Promise<void>)): Promise<void> {
        await this.channel.assertQueue(queue, { durable: false });

        this.channel.consume(
            queue,
            async (msg) => {
                const ack = _.once((response: string) => {

                    this.channel.sendToQueue(msg.properties.replyTo,
                        Buffer.from(response),
                        {
                            correlationId: msg.properties.correlationId
                        }
                    );
                    this.channel.ack(msg);
                });

                await handler(msg, ack);
            }
        );
    }

    public async subscribeExchange(queue: string, exchange: string, routingKey: string, type: string = 'direct',
        handler: ((msg: ConsumeMessage, ack: () => void) => Promise<void>),
        options?: Options.AssertExchange): Promise<any> {

        const key = JSON.stringify({ exchange, routingKey, queue });

        if (this.exchanges.has(key) && this.exchanges.get(key).active) {
            const existingHandler = _.find(this.exchanges.get(key).handler, (h) => h === handler);
            if (existingHandler) {
                /* Si on a déjà souscrit à la queue*/
                return () => this.unsubscribe(key, existingHandler);
            }
            this.exchanges.get(key).handler.push(handler);
            return () => this.unsubscribe(key, handler);
        }

        await this.channel.assertExchange(exchange, type, options);
        const q = await this.channel.assertQueue(queue, { durable: true, autoDelete: !!!queue });
        await this.channel.bindQueue(queue, exchange, routingKey);

        this.exchanges.set(key, {
            exchange,
            queue,
            routingKey,
            type,
            options,
            active: true,
            handler: [handler]
        });

        this.channel.consume(
            q.queue,
            async (msg) => {
                const ack = _.once(() => this.channel.ack(msg));
                this.exchanges.get(key).handler.forEach((h) => h(msg, ack));
            }
        );
        return () => this.unsubscribe(key, handler);
    }

    public async subscribe(queue: string,
        handler: ((msg: ConsumeMessage, ack: () => void) => Promise<void>),
        options?: Options.AssertQueue): Promise<() => void> {

        const key = JSON.stringify({ exchange: '', routingKey: '', queue });

        if (this.queues.has(key) && this.queues.get(key).active) {
            /*Si la queue existe déjà*/
            const existingHandler = _.find(this.queues.get(key).handler, (h) => h === handler);
            if (existingHandler) {
                /* Si on a déjà souscrit à la queue*/
                return () => this.unsubscribe(key, existingHandler);
            }
            this.queues.get(key).handler.push(handler);
            return () => this.unsubscribe(key, handler);
        }

        await this.channel.assertQueue(queue, options);

        this.queues.set(key, {
            handler: [handler],
            queue,
            active: true,
            options
        });

        this.channel.consume(
            queue,
            async (msg) => {
                const ack = _.once(() => this.channel.ack(msg));
                this.queues.get(key).handler.map(async (h) => await h(msg, ack));
            }
        );
        return () => this.unsubscribe(key, handler);
    }

    public unsubscribe(key: string, handler: ((msg: ConsumeMessage, ack: () => void) => Promise<void>)): void {
        if (this.queues.has(key)) {
            _.pull(this.queues.get(key).handler, handler);
        } else if (this.exchanges.has(key)) {
            _.pull(this.exchanges.get(key).handler, handler);
        }
    }

    public async disconnect(): Promise<void> {
        this.isDisconnected = true;
        if (this.connection && this.channel) {
            await this.clearRPC();
            await this.channel.close();
            await this.connection.close();
        }
        exit();
    }

    private async clearRPC(): Promise<void> {
        if (this.channel && this.rpcQueue?.queue) {
            await this.channel.deleteQueue(this.rpcQueue.queue);
        }
    }

    private async init(): Promise<MessageBroker> {
        const config: Options.Connect = {
            hostname: process.env.RABBITMQ_URL || 'localhost',
            protocol: process.env.RABBITMQ_PROTOCOL || 'amqp',
            username: process.env.RABBITMQ_USERNAME,
            password: process.env.RABBITMQ_PASSWORD,
            port: Number(process.env.RABBITMQ_PORT) || 5672
        };

        while (!this.connection) {
            try {
                console.error("[AMQP] Connecting...");
                this.connection = await connect(config);
                console.error("[AMQP] Successfull connection");
            } catch (err) {
                console.error('[AMQP] Connection failed', err);
                console.error(`[AMQP] New connection attempt in ${this.timeout} ms`);
                await this.delay(this.timeout);
            }
        }

        this.connection.on('error', (err) => {
            if (err.message !== 'Connection closing') {
                console.error('[AMQP] Connection error', err.message);
            }
        });

        this.connection.on('close', async () => {
            if (!this.isDisconnected) {
                console.error('[AMQP] Connection has been closed');
                console.error('[AMQP] Restarting connection to RabbitMQ');
                this.connection = null;
                await this.restart();
            }

        });

        this.channel = await this.connection.createChannel();

        if (process.env.RABBITMQ_PREFETCH) {
            await this.channel.prefetch(+process.env.RABBITMQ_PREFETCH);
        } else if (process.env.PREFETCH) {
            await this.channel.prefetch(+process.env.PREFETCH);
        }

        if (!this.rpcQueue) {
            this.rpcQueue = await this.channel.assertQueue(process.env.RABBITMQ_RPCQUEUE || '', {
                exclusive: false,
            });
        }

        this.listenRPC();
        await this.restartQueues();
        await this.restartExchanges();

        return this;
    }

    private async restart(): Promise<void> {
        Array.from(this.exchanges.entries()).forEach(([key, value]) => {
            this.exchanges.set(key, { ...value, active: false });
        });
        Array.from(this.queues.entries()).forEach(([key, value]) => {
            this.queues.set(key, { ...value, active: false });
        });
        await this.init();
    }

    private delay(milliseconds: number): Promise<void> {
        return new Promise(resolve => {
            setTimeout(resolve, milliseconds);
        });
    }

    private async restartQueues(): Promise<void> {
        await Promise.all(Array.from(this.queues.values()).flatMap((q) => {
            return q.handler.map((h) => {
                return this.subscribe(q.queue, h, q.options);
            });
        }));
    }

    private async restartExchanges(): Promise<void> {
        await Promise.all(Array.from(this.exchanges.values()).flatMap((q) => {
            return q.handler.map((h) => {
                return this.subscribeExchange(q.queue, q.exchange, q.routingKey, q.type, h, q.options);
            });
        }));
    }

    private listenRPC(): void {
        this.channel.consume(this.rpcQueue.queue, (msg) => {
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
    }
}
