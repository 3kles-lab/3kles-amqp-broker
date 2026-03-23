import { ChannelModel, connect, Connection, Options } from 'amqplib';
import { MessageBroker } from './message-broker';
import { EventEmitter } from 'events';
import { hostname } from 'os';
import * as pino from 'pino';
import { err as errSerializer } from 'pino-std-serializers';

process.on('SIGINT', async () => {
    await Promise.all(ConnectionManager.getConnexions().map((manager) => manager.disconnect()));
    process.exit();
});
process.on('SIGTERM', async () => {
    await Promise.all(ConnectionManager.getConnexions().map((manager) => manager.disconnect()));
    process.exit();
});
process.on('SIGQUIT', async () => {
    await Promise.all(ConnectionManager.getConnexions().map((manager) => manager.disconnect()));
    process.exit();
});

export interface ConnectOption {
    clientProperties?: { env?: string; service: string; extras?: { [key: string]: string } };
}

export class ConnectionManager extends EventEmitter {
    private static connections: ConnectionManager[] = [];

    public static async getConnexion(index: number | string = 0): Promise<ConnectionManager> {
        if (!ConnectionManager.connections[index]) {
            await this.initConnexion(index);
        }
        return ConnectionManager.connections[index];
    }

    public static async initConnexion(index: number | string = 0, config?: Options.Connect, option?: ConnectOption, timeout?: number) {
        if (ConnectionManager.connections[index]) {
            throw new Error(`[AMQP] Connection ${index} already exist`);
        }
        const connectionManager = new ConnectionManager(config ?? undefined, option ?? undefined, timeout ?? undefined);
        ConnectionManager.connections[index] = connectionManager;
        await ConnectionManager.connections[index].createConnexion();

        return connectionManager;
    }

    public static getConnexions(): ConnectionManager[] {
        return this.connections;
    }

    public connection: any;
    public brokers: MessageBroker[] = [];

    private isConnected = false;
    private isDisconnected: boolean;

    public logger = pino({
        level: process.env.NODE_ENV === 'production' ? 'info' : 'debug',
        timestamp: pino.stdTimeFunctions.isoTime,
        name: '3kles-amqpbroker',
        mixin() {
            return { ...(process.env.SERVICE_NAME && { service: process.env.SERVICE_NAME }) };
        },
        serializers: {
            err(error) {
                const e = errSerializer(error);
                return {
                    type: e.type,
                    message: e.message,
                    code: (e as any).code,
                    classId: (e as any).classId,
                    methodId: (e as any).methodId,
                };
            },
        },
    });

    public get connected(): boolean {
        return this.isConnected;
    }

    public onConnected(listener: (connection: Connection) => void): this {
        this.on('connected', listener);

        if (this.connection && this.isConnected) {
            listener(this.connection);
        }

        return this;
    }

    public onDisconnected(listener: (error?: Error) => void): this {
        this.on('disconnected', listener);
        return this;
    }

    private constructor(
        private config: Options.Connect = {
            hostname: process.env.RABBITMQ_URL || 'localhost',
            protocol: process.env.RABBITMQ_PROTOCOL || 'amqp',
            username: process.env.RABBITMQ_USERNAME,
            password: process.env.RABBITMQ_PASSWORD,
            port: Number(process.env.RABBITMQ_PORT) || 5672,
        },
        private option?: ConnectOption,
        private timeout: number = Number(process.env.RABBITMQ_TIMEOUT) || 5000,
    ) {
        super();
        this.setMaxListeners(0);
    }

    private async createConnexion(): Promise<void> {
        while (!this.connection) {
            try {
                this.logger.info('[AMQP] Connecting...');

                const env = this.option?.clientProperties?.env ?? process.env.NODE_ENV;
                const conn = await connect(this.config, {
                    clientProperties: {
                        ...(this.option?.clientProperties?.extras ?? {}),
                        connection_name: `${this.option?.clientProperties?.service ?? process.env.SERVICE_NAME ?? 'service'}${env ? `:${env}` : ''}:${process.pid}`,
                        service: this.option?.clientProperties?.service ?? process.env.SERVICE_NAME,
                        env,
                        hostname: hostname(),
                    },
                });
                this.connection = conn;
                this.isConnected = true;
                this.bindConnectionEvents(conn);
                this.logger.info('[AMQP] Successful connection');
                await Promise.all(this.brokers.map((b) => b.start()));
                this.emit('connected', conn);
            } catch (err) {
                this.isConnected = false;
                const error = err instanceof Error ? err : new Error(String(err));
                this.logger.error({ err: error }, '[AMQP] Connection failed');
                this.emit('error', error);
                if (!this.isDisconnected) {
                    this.logger.warn(`[AMQP] New connection attempt in ${this.timeout} ms`);
                    this.emit('reconnecting');
                    await this.delay(this.timeout);
                }
            }
        }
    }

    private bindConnectionEvents(conn: ChannelModel): void {
        conn.on('error', (err: Error) => {
            if (err.message !== 'Connection closing') {
                this.logger.error({ err }, '[AMQP] Connection error');
            }
            this.emit('error', err);
        });
        conn.on('blocked', (reason: string) => {
            this.logger.warn({ reason }, '[AMQP] Connection blocked');
            this.emit('blocked', reason);
        });
        conn.on('unblocked', () => {
            this.logger.warn('[AMQP] Connection unblocked');
            this.emit('unblocked');
        });
        conn.once('close', async (err?: Error) => {
            this.emit('disconnected', err);
            this.connection = null;
            this.isConnected = false;
            if (!this.isDisconnected) {
                this.logger.warn({ err }, '[AMQP] Connection has been closed');
                this.logger.warn('[AMQP] Restarting connection to RabbitMQ');
                this.emit('reconnecting');
                await this.createConnexion();
            }
        });
    }

    public async disconnect(): Promise<void> {
        this.isDisconnected = true;
        this.isConnected = false;
        if (this.connection) {
            await Promise.all(this.brokers.map((b) => b.disconnect()));
            await this.connection.close();
        }
    }

    private delay(milliseconds: number): Promise<void> {
        return new Promise((resolve) => {
            setTimeout(resolve, milliseconds);
        });
    }
}
