import { Options } from 'amqplib';

export interface AssertQueueInput {
    name: string;
    options?: Options.AssertQueue;
}

export interface QueueDeclaration {
    requestedName: string;
    actualName: string;
    options?: Options.AssertQueue;
}

export interface BindQueueInput {
    queue: string;
    exchange: string;
    routingKey: string;
    args?: Record<string, unknown>;
}

export interface QueueBinding {
    queue: string;
    exchange: string;
    routingKey: string;
    args?: Record<string, unknown>;
}
