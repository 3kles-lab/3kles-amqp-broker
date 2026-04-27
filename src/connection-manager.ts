import { ChannelModel, connect } from 'amqplib';
import { EventEmitter } from 'events';
import { hostname } from 'os';
import pino, { Logger } from 'pino';
import { err as errSerializer } from 'pino-std-serializers';

import { ConnectionManagerConfig, ConnectionState } from './types/connection';
import { delay, reconnectDelayWithJitter } from './utils/delay';
import { AmqpConnectionError } from './errors';
import { MessageBroker } from './message-broker';

const defaultLogger = pino({
    level: process.env.NODE_ENV === 'production' ? 'info' : 'debug',
    timestamp: pino.stdTimeFunctions.isoTime,
    name: 'amqp-broker',
    serializers: {
        err(error) {
            const e = errSerializer(error);
            return {
                type: e.type,
                message: e.message,
                stack: e.stack,
                code: (e as any).code,
                classId: (e as any).classId,
                methodId: (e as any).methodId,
            };
        },
    },
});

export class ConnectionManager extends EventEmitter {
    private static readonly instances = new Map<string | number, ConnectionManager>();

    public static async get(name: string | number, config: ConnectionManagerConfig): Promise<ConnectionManager> {
        let instance = this.instances.get(name);

        if (!instance) {
            instance = await this.create(name, config);
        }

        return instance;
    }

    public static async create(name: string | number, config: ConnectionManagerConfig): Promise<ConnectionManager> {
        if (this.instances.has(name)) {
            throw new AmqpConnectionError(`[AMQP] Connection "${name}" already exists`);
        }

        const manager = new ConnectionManager(name, config);
        this.instances.set(name, manager);
        await manager.connect();
        if (config.enableGracefulShutdown !== false) {
            manager.enableGracefulShutdown();
        }

        return manager;
    }

    public static async disconnectAll(): Promise<void> {
        const managers = await Promise.all([...this.instances.values()]);

        await Promise.all(managers.map((manager) => manager.disconnect()));
    }

    public connection: ChannelModel | null = null;
    private state: ConnectionState = 'idle';
    private connectPromise?: Promise<void>;
    private reconnectAttempt = 0;

    private readonly logger: Logger;
    private readonly reconnectDelayMs: number;

    private constructor(
        public readonly name: string | number,
        private readonly config: ConnectionManagerConfig,
    ) {
        super();

        this.logger = config.logger ?? defaultLogger;
        this.reconnectDelayMs = config.reconnectDelayMs ?? 5_000;
    }

    public get connected(): boolean {
        return this.state === 'connected' && !!this.connection;
    }

    public get currentState(): ConnectionState {
        return this.state;
    }

    public get currentConnection(): ChannelModel {
        if (!this.connection || this.state !== 'connected') {
            throw new AmqpConnectionError('[AMQP] Connection is not ready');
        }

        return this.connection;
    }

    public enableGracefulShutdown() {
        const shutdown = async () => {
            await this.disconnect();
            process.exit();
        };

        process.once('SIGINT', shutdown);
        process.once('SIGTERM', shutdown);
        process.once('SIGQUIT', shutdown);
    }

    public onConnected(listener: (connection: ChannelModel) => void | Promise<void>): this {
        this.on('connected', listener);

        if (this.connection && this.state === 'connected') {
            void listener(this.connection);
        }

        return this;
    }

    public onDisconnected(listener: () => void | Promise<void>): this {
        this.on('disconnected', listener);
        return this;
    }

    public async connect(): Promise<void> {
        if (this.connectPromise) {
            return this.connectPromise;
        }

        this.connectPromise = this.connectLoop().finally(() => {
            this.connectPromise = undefined;
        });

        return this.connectPromise;
    }

    public async disconnect(): Promise<void> {
        this.state = 'disconnecting';

        const conn = this.connection;
        this.connection = null;

        if (conn) {
            try {
                await conn.close();
            } catch (err) {
                this.logger.error({ err }, '[AMQP] Error while closing connection');
            }
        }

        this.state = 'disconnected';
        this.emit('disconnected');
    }

    private async connectLoop(): Promise<void> {
        while (this.state !== 'disconnecting' && this.state !== 'disconnected') {
            try {
                this.state = this.reconnectAttempt === 0 ? 'connecting' : 'reconnecting';

                this.logger.info({ connection: this.name }, '[AMQP] Connecting');

                const conn = await this.openConnection();

                this.connection = conn;
                this.state = 'connected';
                this.reconnectAttempt = 0;

                this.bindConnectionEvents(conn);

                this.logger.info({ connection: this.name }, '[AMQP] Connected');
                this.emit('connected', conn);

                return;
            } catch (err) {
                const error = err instanceof Error ? err : new Error(String(err));

                this.connection = null;
                this.state = 'reconnecting';
                this.reconnectAttempt += 1;

                this.logger.error({ err: error, connection: this.name, attempt: this.reconnectAttempt }, '[AMQP] Connection failed');

                this.emitSafe('connection_error', error);

                const wait = reconnectDelayWithJitter(this.reconnectDelayMs, this.reconnectAttempt);

                this.logger.warn({ connection: this.name, wait }, '[AMQP] Reconnecting later');

                await delay(wait);
            }
        }
    }

    private async openConnection(): Promise<ChannelModel> {
        const clientProperties = this.buildClientProperties();

        if ('url' in this.config.connection) {
            return connect(this.config.connection.url, {
                ...(this.config.connection.socketOptions as object | undefined),
                clientProperties,
            });
        }

        return connect(this.config.connection.options, {
            ...(this.config.connection.socketOptions as object | undefined),
            clientProperties,
        });
    }

    private bindConnectionEvents(conn: ChannelModel): void {
        conn.on('error', (err: Error) => {
            if (err.message !== 'Connection closing') {
                this.logger.error({ err, connection: this.name }, '[AMQP] Connection error');
            }

            this.emitSafe('connection_error', err);
        });

        conn.on('blocked', (reason: string) => {
            this.logger.warn({ reason, connection: this.name }, '[AMQP] Connection blocked');
            this.emit('blocked', reason);
        });

        conn.on('unblocked', () => {
            this.logger.warn({ connection: this.name }, '[AMQP] Connection unblocked');
            this.emit('unblocked');
        });

        conn.once('close', () => {
            if (this.connection === conn) {
                this.connection = null;
            }

            if (this.state === 'disconnecting' || this.state === 'disconnected') {
                return;
            }

            this.state = 'reconnecting';

            this.logger.warn({ connection: this.name }, '[AMQP] Connection closed');
            this.emit('disconnected');

            void this.connect();
        });
    }

    private buildClientProperties(): Record<string, string | undefined> {
        const env = this.config.clientProperties?.env ?? process.env.NODE_ENV;
        const service = this.config.clientProperties?.service ?? process.env.SERVICE_NAME ?? 'service';

        return {
            ...(this.config.clientProperties?.extras ?? {}),
            connection_name: `${service}${env ? `:${env}` : ''}@${hostname()}:${process.pid}`,
            service,
            env,
            hostname: hostname(),
        };
    }

    private emitSafe(event: string, payload?: unknown): void {
        if (this.listenerCount(event) > 0) {
            this.emit(event, payload);
        }
    }
}
