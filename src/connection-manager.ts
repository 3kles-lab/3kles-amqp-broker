import { ChannelModel, connect } from 'amqplib';
import { EventEmitter } from 'events';
import { setTimeout as wait } from 'node:timers/promises';
import { hostname } from 'os';
import pino, { Logger } from 'pino';
import { err as errSerializer } from 'pino-std-serializers';

import { ConnectionManagerConfig, ConnectionState } from './types/connection';
import { reconnectDelayWithJitter } from './utils/delay';
import { AmqpConnectionError } from './errors';

const connectionForcedCode = 320;
const forcedCloseReplyPrefix = `Connection closed: ${connectionForcedCode} (CONNECTION-FORCED) with message "CONNECTION_FORCED - `;

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

function isRecoverableForcedClose(message: string): boolean {
    // RabbitMQ also uses CONNECTION_FORCED when a node stops or enters maintenance.
    return message.startsWith(`${forcedCloseReplyPrefix}broker forced connection closure with reason '`)
        || message === `${forcedCloseReplyPrefix}Node was put into maintenance mode"`;
}

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
    private connectAttempt?: { controller: AbortController; promise: Promise<void> };
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
        if (this.connectAttempt) {
            return this.connectAttempt.promise;
        }

        if (this.state === 'disconnecting' || this.state === 'disconnected') {
            return;
        }

        const controller = new AbortController();
        const promise = this.connectLoop(controller.signal).finally(() => {
            this.connectAttempt = undefined;
        });
        this.connectAttempt = { controller, promise };

        return promise;
    }

    public async disconnect(): Promise<void> {
        const wasConnected = this.connected;
        this.state = 'disconnecting';
        // Established connections are closed with the AMQP handshake.
        if (!wasConnected) {
            this.connectAttempt?.controller.abort();
        }

        const conn = this.connection;
        this.connection = null;

        if (conn) {
            await this.closeConnection(conn);
        }

        this.state = 'disconnected';
        this.emit('disconnected');
    }

    private async closeConnection(conn: ChannelModel): Promise<void> {
        try {
            await conn.close();
        } catch (err) {
            this.logger.error({ err }, '[AMQP] Error while closing connection');
        }
    }

    private async connectLoop(signal: AbortSignal): Promise<void> {
        while (!signal.aborted) {
            this.state = this.reconnectAttempt === 0 ? 'connecting' : 'reconnecting';
            this.logger.info({ connection: this.name }, '[AMQP] Connecting');

            let conn: ChannelModel;
            try {
                conn = await this.openConnection(signal);
            } catch (err) {
                if (signal.aborted) {
                    return;
                }

                const error = err instanceof Error ? err : new Error(String(err));

                this.connection = null;
                this.state = 'reconnecting';
                this.reconnectAttempt += 1;

                this.logger.error({ err: error, connection: this.name, attempt: this.reconnectAttempt }, '[AMQP] Connection failed');

                this.emitSafe('connection_error', error);

                const retryDelay = reconnectDelayWithJitter(this.reconnectDelayMs, this.reconnectAttempt);
                this.logger.warn({ connection: this.name, wait: retryDelay }, '[AMQP] Reconnecting later');

                try {
                    await wait(retryDelay, undefined, { signal });
                } catch (err) {
                    if (signal.aborted) {
                        return;
                    }
                    throw err;
                }
                continue;
            }

            if (signal.aborted) {
                conn.on('error', (err) => this.logger.error({ err }, '[AMQP] Error while closing cancelled connection'));
                await this.closeConnection(conn);
                return;
            }

            this.connection = conn;
            this.state = 'connected';
            this.reconnectAttempt = 0;

            this.bindConnectionEvents(conn);
            this.logger.info({ connection: this.name }, '[AMQP] Connected');
            try {
                this.emit('connected', conn);
            } catch (err) {
                const error = err instanceof Error ? err : new Error(String(err));
                this.logger.error({ err: error, connection: this.name }, '[AMQP] Connected listener failed');
                this.emitSafe('connection_error', error);
            }
            return;
        }
    }

    private openConnection(signal: AbortSignal): Promise<ChannelModel> {
        const connection = this.config.connection;
        const socketOptions = connection.socketOptions as { signal?: AbortSignal } | undefined;
        return connect('url' in connection ? connection.url : connection.options, {
            ...socketOptions,
            clientProperties: this.buildClientProperties(),
            signal: socketOptions?.signal ? AbortSignal.any([socketOptions.signal, signal]) : signal,
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

        conn.once('close', (err?: Error & { code?: number | string }) => {
            if (this.connection !== conn) {
                return;
            }
            this.connection = null;

            const reconnect = err?.code !== connectionForcedCode || isRecoverableForcedClose(err.message);
            this.logger.warn({ connection: this.name, reason: err?.message, code: err?.code, reconnect }, '[AMQP] Connection closed');
            if (!reconnect) {
                void this.disconnect();
                return;
            }

            this.state = 'reconnecting';
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
