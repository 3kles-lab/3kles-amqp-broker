export class RetryableError extends Error {
    constructor(message = 'Retryable error') {
        super(message);
        this.name = 'RetryableError';
    }
}
