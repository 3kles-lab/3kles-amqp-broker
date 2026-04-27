import { Options } from 'amqplib';

export function toBuffer(payload: Buffer | string | object): Buffer {
    if (Buffer.isBuffer(payload)) {
        return payload;
    }

    if (typeof payload === 'string') {
        return Buffer.from(payload);
    }

    return Buffer.from(JSON.stringify(payload));
}

export function defaultContentType(payload: Buffer | string | object): string | undefined {
    if (Buffer.isBuffer(payload)) {
        return undefined;
    }

    if (typeof payload === 'string') {
        return 'text/plain';
    }

    return 'application/json';
}

export function buildPublishOptions(payload: Buffer | string | object, options?: Options.Publish): Options.Publish {
    return {
        timestamp: Date.now(),
        contentType: defaultContentType(payload),
        ...options,
    };
}

export function parseJson<T = unknown>(buffer: Buffer): T {
    return JSON.parse(buffer.toString('utf8')) as T;
}
