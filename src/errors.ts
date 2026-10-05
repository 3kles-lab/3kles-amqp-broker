export class AmqpError extends Error {
    constructor(
        message: string,
        public readonly cause?: unknown,
    ) {
        super(message);
        this.name = 'AmqpError';
    }
}

export class AmqpConnectionError extends AmqpError {
    constructor(message: string, cause?: unknown) {
        super(message, cause);
        this.name = 'AmqpConnectionError';
    }
}

export class AmqpPublishError extends AmqpError {
    constructor(message: string, cause?: unknown) {
        super(message, cause);
        this.name = 'AmqpPublishError';
    }
}

export class AmqpUnroutableError extends AmqpPublishError {
    constructor(
        public readonly replyCode: number,
        public readonly replyText: string,
        public readonly exchange: string,
        public readonly routingKey: string,
        public readonly messageId?: string,
    ) {
        super(`[AMQP] Publication could not be routed: ${replyCode} ${replyText}`);
        this.name = 'AmqpUnroutableError';
    }
}

/** The message may have been accepted; retrying can produce a duplicate. */
export class AmqpPublishUnknownError extends AmqpPublishError {
    constructor(
        public readonly reason: 'timeout' | 'channel_closed',
        public readonly messageId?: string,
        cause?: unknown,
    ) {
        super(`[AMQP] Publication outcome is unknown: ${reason}`, cause);
        this.name = 'AmqpPublishUnknownError';
    }
}

export class AmqpConsumerError extends AmqpError {
    constructor(message: string, cause?: unknown) {
        super(message, cause);
        this.name = 'AmqpConsumerError';
    }
}

export class AmqpRpcTimeoutError extends AmqpError {
    constructor(timeoutMs: number) {
        super(`[AMQP] RPC timeout after ${timeoutMs}ms`);
        this.name = 'AmqpRpcTimeoutError';
    }
}
