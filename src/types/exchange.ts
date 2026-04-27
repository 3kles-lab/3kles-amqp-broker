import { Options } from 'amqplib';

export type ExchangeType = 'direct' | 'topic' | 'headers' | 'fanout' | 'match' | string;

export interface AssertExchangeInput {
    name: string;
    type?: ExchangeType;
    options?: Options.AssertExchange;
}

export interface ExchangeDeclaration {
    name: string;
    type: ExchangeType;
    options?: Options.AssertExchange;
}
