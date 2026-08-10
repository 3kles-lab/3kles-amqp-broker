export interface MessageContext {
    messageId?: string;
    correlationId?: string;
    exchange?: string;
    routingKey?: string;
    headers: Record<string, unknown>;
    timestamp?: Date;
}

export interface Handler<T = unknown> {
    handle(message: T, context: MessageContext): Promise<void>;
}
