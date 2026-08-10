export class NonRetryableError extends Error {
    constructor(message = 'Non retryable error') {
        super(message);
        this.name = 'NonRetryableError';
    }
}
