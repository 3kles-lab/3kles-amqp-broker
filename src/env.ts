import { Options } from 'amqplib';
import { readFileSync } from 'fs';
import { AmqpConnectionConfig, ConnectionManagerConfig } from './types/connection';

export type RabbitMqEnv = NodeJS.ProcessEnv & {
    RABBITMQ_URL?: string;
    RABBITMQ_HOST?: string;
    RABBITMQ_PROTOCOL?: string;
    RABBITMQ_USERNAME?: string;
    RABBITMQ_PASSWORD?: string;
    RABBITMQ_PORT?: string;
    RABBITMQ_TIMEOUT?: string;
    RABBITMQ_PREFETCH?: string;
    RABBITMQ_VHOST?: string;
    RABBITMQ_CA_FILE?: string;
    SERVICE_NAME?: string;
    NODE_ENV?: string;
};

export function connectionConfigFromEnv(env: RabbitMqEnv = process.env): AmqpConnectionConfig {
    const socketOptions = env.RABBITMQ_CA_FILE
        ? { ca: readFileSync(env.RABBITMQ_CA_FILE), rejectUnauthorized: true }
        : undefined;
    if (env.RABBITMQ_URL) {
        return {
            url: env.RABBITMQ_URL,
            socketOptions,
        };
    }

    const options: Options.Connect = {
        hostname: env.RABBITMQ_HOST ?? 'localhost',
        protocol: env.RABBITMQ_PROTOCOL ?? 'amqp',
        username: env.RABBITMQ_USERNAME,
        password: env.RABBITMQ_PASSWORD,
        port: Number(env.RABBITMQ_PORT) || (env.RABBITMQ_PROTOCOL === 'amqps' ? 5671 : 5672),
        vhost: env.RABBITMQ_VHOST ?? '/',
    };

    return {
        options,
        socketOptions,
    };
}

export function connectionManagerConfigFromEnv(env: RabbitMqEnv = process.env): ConnectionManagerConfig {
    return {
        connection: connectionConfigFromEnv(env),
        reconnectDelayMs: Number(env.RABBITMQ_TIMEOUT) || 5_000,
        clientProperties: {
            service: env.SERVICE_NAME,
            env: env.NODE_ENV,
        },
    };
}
