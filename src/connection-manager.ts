import { connect, Connection, Options } from "amqplib";
import { MessageBroker } from "./message-broker";

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

export class ConnectionManager {

    private static connections: ConnectionManager[] = [];

    public static async getConnexion(index: number | string = 0, config?: Options.Connect, timeout?: number): Promise<ConnectionManager> {
        if (!ConnectionManager.connections[index]) {
            ConnectionManager.connections[index] = new ConnectionManager(config, timeout);
            await ConnectionManager.connections[index].createConnexion();
        }
        return ConnectionManager.connections[index];
    }

    public static getConnexions(): ConnectionManager[] {
        return this.connections;
    }

    public connection: Connection;
    public brokers: MessageBroker[] = [];
    private isDisconnected: boolean;

    private constructor(private config: Options.Connect = {
        hostname: process.env.RABBITMQ_URL || 'localhost',
        protocol: process.env.RABBITMQ_PROTOCOL || 'amqp',
        username: process.env.RABBITMQ_USERNAME,
        password: process.env.RABBITMQ_PASSWORD,
        port: Number(process.env.RABBITMQ_PORT) || 5672,
    }, private timeout: number = Number(process.env.RABBITMQ_TIMEOUT) || 5000) { }

    private async createConnexion(): Promise<void> {
        while (!this.connection) {
            try {
                console.error("[AMQP] Connecting...");
                this.connection = await connect(this.config);
                console.error("[AMQP] Successfull connection");
                await Promise.all(this.brokers.map((b) => b.start()));
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

        this.connection.once('close', async () => {
            if (!this.isDisconnected) {
                console.error('[AMQP] Connection has been closed');
                console.error('[AMQP] Restarting connection to RabbitMQ');
                this.connection = null;
                await this.createConnexion();
            }
        });
    }

    public async disconnect(): Promise<void> {
        this.isDisconnected = true;
        if (this.connection) {
            await Promise.all(this.brokers.map((b) => b.disconnect()));
            await this.connection.close();
        }
    }

    private delay(milliseconds: number): Promise<void> {
        return new Promise(resolve => {
            setTimeout(resolve, milliseconds);
        });
    }
}