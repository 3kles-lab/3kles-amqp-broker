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
