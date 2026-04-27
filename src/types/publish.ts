import { Options } from 'amqplib';
import { ExchangeType } from './exchange';

export type PublishPayload = Buffer | string | object;

export interface PublishInput {
    exchange: string;
    routingKey: string;
    payload: PublishPayload;
    options?: Options.Publish;
    assertExchange?: {
        type?: ExchangeType;
        options?: Options.AssertExchange;
    };
}

export interface SendToQueueInput {
    queue: string;
    payload: PublishPayload;
    options?: Options.Publish;
    assertQueue?: Options.AssertQueue;
}
