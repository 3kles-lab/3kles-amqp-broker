import { Channel, connect, Connection, ConsumeMessage, Options, Replies } from 'amqplib';
import _ from 'lodash';
import { EventEmitter } from 'events';
import * as uuid from 'uuid';

export class MessageBroker {

    public static async getInstance(): Promise<MessageBroker> {
        if (!MessageBroker.instance) {
            const broker = new MessageBroker();
            MessageBroker.instance = broker.init();
        }
        return MessageBroker.instance;
    }

    private static instance: Promise<MessageBroker>;
    private connection: Connection;
    private channel: Channel;

    private queues: { [key: string]: [((msg: ConsumeMessage, ack: () => void) => Promise<void>)] } = {};

    private rpcQueue: Replies.AssertQueue;
    private correlationIds: any[] = [];
    private responseEmitter: EventEmitter = new EventEmitter();

    private constructor() {
        this.responseEmitter.setMaxListeners(0);
    }

    public async sendToExchange(exchange: string, routingKey: string, msg: Buffer,
        type: string = 'direct', options?: Options.AssertExchange): Promise<boolean> {

        await this.channel.assertExchange(exchange, type, options);
        return this.channel.publish(exchange, routingKey, msg);
    }

    public async send(queue: string, msg: Buffer, options?: Options.Publish): Promise<boolean> {
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

    public async subscribeExchange(queue: string, exchange: string, routingKey: string, type: string = 'direct',
        handler: ((msg: ConsumeMessage, ack: () => void) => Promise<void>),
        options?: Options.AssertExchange): Promise<any> {

        const key = JSON.stringify({ exchange, routingKey });

        if (this.queues[key]) {
            const existingHandler = _.find(this.queues[key], (h) => h === handler);
            if (existingHandler) {
                /* Si on a déjà souscrit à la queue*/
                return () => this.unsubscribe(key, existingHandler);
            }
            this.queues[key].push(handler);
            return () => this.unsubscribe(key, handler);
        }


        await this.channel.assertExchange(exchange, type, options);
        const q = await this.channel.assertQueue(queue, { durable: true });
        await this.channel.bindQueue(queue, exchange, routingKey);

        this.queues[key] = [handler];

        this.channel.consume(
            q.queue,
            async (msg) => {
                const ack = _.once(() => this.channel.ack(msg));
                this.queues[key].forEach((h) => h(msg, ack));
            }
        );
        return () => this.unsubscribe(key, handler);
    }

    public async subscribe(queue: string,
        handler: ((msg: ConsumeMessage, ack: () => void) => Promise<void>),
        options?: Options.AssertQueue): Promise<() => void> {

        const key = JSON.stringify({ exchange: '', routingKey: queue });

        if (this.queues[key]) {
            /*Si la queue existe déjà*/
            const existingHandler = _.find(this.queues[key], (h) => h === handler);
            if (existingHandler) {
                /* Si on a déjà souscrit à la queue*/
                return () => this.unsubscribe(key, existingHandler);
            }
            this.queues[key].push(handler);
            return () => this.unsubscribe(key, handler);
        }

        await this.channel.assertQueue(queue, options);

        this.queues[key] = [handler];

        this.channel.consume(
            queue,
            async (msg) => {
                const ack = _.once(() => this.channel.ack(msg));
                this.queues[key].map(async (h) => await h(msg, ack));
            }
        );
        return () => this.unsubscribe(queue, handler);
    }

    public unsubscribe(key: string, handler: ((msg: ConsumeMessage, ack: () => void) => Promise<void>)): void {
        _.pull(this.queues[key], handler);
    }

    public async disconnect(): Promise<void> {
        await this.channel.close();
        await this.connection.close();
    }

    private async init(): Promise<MessageBroker> {
        const config: Options.Connect = {
            hostname: process.env.RABBITMQ_URL || 'localhost',
            protocol: process.env.RABBITMQ_PROTOCOL || 'amqp',
            username: process.env.RABBITMQ_USERNAME,
            password: process.env.RABBITMQ_PASSWORD,
            port: Number(process.env.RABBITMQ_PORT) || 5672
        };

        this.connection = await connect(config);
        this.channel = await this.connection.createChannel();

        this.rpcQueue = await this.channel.assertQueue('', {
            exclusive: false
        });

        this.listenRPC();

        return this;
    }

    private listenRPC(): void {
        this.channel.consume(this.rpcQueue.queue, (msg) => {
            const index = this.correlationIds.indexOf(msg.properties.correlationId, 0);
            if (index !== -1) {
                this.responseEmitter.emit(
                    msg.properties.correlationId,
                    JSON.parse(msg.content.toString('utf8')),
                );
                this.correlationIds.splice(index, 1);
            }
        }, { noAck: true });
    }

}
