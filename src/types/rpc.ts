import { ConsumeMessage, Options } from 'amqplib';
import { PublishPayload } from './publish';

export interface RpcContext {
    reply(payload: PublishPayload, options?: Options.Publish): void;
    nack(requeue?: boolean): void;
    reject(requeue?: boolean): void;

    buffer(): Buffer;
    text(encoding?: BufferEncoding): string;
    json<T = unknown>(): T;

    readonly settled: boolean;
}

export type RpcHandler = (msg: ConsumeMessage, ctx: RpcContext) => Promise<void> | void;

export interface PendingRpcRequest<T = unknown> {
    correlationId: string;
    resolve(value: T): void;
    reject(reason?: unknown): void;
    timeout: NodeJS.Timeout;
}

export interface RpcRequestQueueInput {
    queue: string;
    payload: PublishPayload;
    timeoutMs?: number;
    options?: Options.Publish;
}

export interface RpcRequestExchangeInput {
    exchange: string;
    routingKey: string;
    payload: PublishPayload;
    timeoutMs?: number;
    options?: Options.Publish;
}
