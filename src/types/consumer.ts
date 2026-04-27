import { ConsumeMessage, Options } from 'amqplib';

export interface ConsumeContext {
    ack(allUpTo?: boolean): void;
    nack(allUpTo?: boolean, requeue?: boolean): void;
    reject(requeue?: boolean): void;

    buffer(): Buffer;
    text(encoding?: BufferEncoding): string;
    json<T = unknown>(): T;

    readonly settled: boolean;
}

export type MessageHandler = (msg: ConsumeMessage, ctx: ConsumeContext) => Promise<void> | void;

export type ConsumerState = 'active' | 'paused' | 'closed';

export interface ConsumerRegistration {
    id: string;
    queue: string;
    consumerTag?: string;
    handler: MessageHandler;
    options?: Options.Consume;
    state: ConsumerState;
}
