import { EventEmitter } from 'events';
import { MessageBroker } from '../src/message-broker';
import { AmqpPublishError, AmqpPublishUnknownError, AmqpUnroutableError } from '../src/errors';
import { Publisher } from '../src/messaging/publisher/publisher';

class FakeChannel extends EventEmitter {
    publications: { options: any; callback: (error?: Error) => void }[] = [];
    publish = jest.fn((_exchange, _routingKey, _buffer, options, callback) => {
        this.publications.push({ options, callback });
        return false; // Backpressure is not a negative confirmation.
    });
    sendToQueue = jest.fn(() => false);
    close = jest.fn(async () => { this.emit('close'); });
}

async function setup(confirm = true, publishTimeoutMs = 100) {
    const channel = new FakeChannel();
    // Reproduce amqplib's close callback order.
    channel.on('close', () => {
        for (const publication of channel.publications) {
            publication.callback?.(new Error('channel closed'));
        }
    });
    const connection = {
        createConfirmChannel: jest.fn(async () => channel),
        createChannel: jest.fn(async () => channel),
    };
    const manager = {
        connected: true,
        currentConnection: connection,
        onConnected: jest.fn(),
        onDisconnected: jest.fn(),
    };
    const logger = { warn: jest.fn(), debug: jest.fn(), error: jest.fn() };
    const broker = await MessageBroker.create('test', {
        connectionManager: manager as any, logger: logger as any,
        confirm, publishTimeoutMs, rpc: { enabled: false },
    });
    return { broker, channel, connection };
}

function returnPublication(channel: FakeChannel, index: number) {
    channel.emit('return', {
        properties: channel.publications[index].options,
        fields: { replyCode: 312, replyText: 'NO_ROUTE', exchange: 'orders', routingKey: 'created' },
    });
}

beforeEach(() => jest.useFakeTimers());
afterEach(() => jest.useRealTimers());

test('waits for confirmation despite backpressure and forces mandatory without mutating options', async () => {
    const { broker, channel } = await setup();
    const options = { mandatory: false, messageId: 'event-1', headers: { custom: 'value' } };
    const result = broker.publish('orders', 'created', {}, options);
    expect(channel.publications[0].options).toMatchObject({ mandatory: true, messageId: 'event-1', headers: { custom: 'value' } });
    expect(options).toEqual({ mandatory: false, messageId: 'event-1', headers: { custom: 'value' } });
    expect(jest.getTimerCount()).toBe(1);
    channel.publications[0].callback();
    await expect(result).resolves.toBe(true);
    expect(jest.getTimerCount()).toBe(0);
});

test('Publisher queue calls use the same confirmation path', async () => {
    const { broker, channel } = await setup();
    const result = new Publisher(broker).publishToQueue('queue', { id: 1 });
    expect(channel.publish).toHaveBeenCalledWith('', 'queue', expect.any(Buffer), expect.objectContaining({ mandatory: true }), expect.any(Function));
    channel.publications[0].callback();
    await expect(result).resolves.toBe(true);
});

test('returns reject immediately with routing details, independently of reused messageIds', async () => {
    const { broker, channel } = await setup();
    const first = broker.publish('orders', 'created', {}, { messageId: 'same' });
    const second = broker.publish('orders', 'created', {}, { messageId: 'same' });
    const rejected = expect(first).rejects.toMatchObject({
        name: 'AmqpUnroutableError', messageId: 'same', replyCode: 312,
        replyText: 'NO_ROUTE', exchange: 'orders', routingKey: 'created',
    });
    returnPublication(channel, 0);
    await rejected;
    channel.publications[1].callback();
    channel.publications[0].callback();
    await expect(second).resolves.toBe(true);
    expect(jest.getTimerCount()).toBe(0);
});

test('negative confirms reject with a typed error and preserve the cause', async () => {
    const { broker, channel } = await setup();
    const result = broker.publish('orders', 'created', {});
    const error = new Error('message nacked');
    channel.publications[0].callback(error);
    await expect(result).rejects.toMatchObject({ name: 'AmqpPublishError', cause: error });
    expect(jest.getTimerCount()).toBe(0);
});

test('timeout reports unknown and late returns cannot affect a retry with the same messageId', async () => {
    const { broker, channel } = await setup();
    const first = broker.sendToQueue('queue', {}, { messageId: 'same' });
    const rejected = expect(first).rejects.toMatchObject({ name: 'AmqpPublishUnknownError', reason: 'timeout' });
    jest.advanceTimersByTime(100);
    await rejected;
    const retry = broker.sendToQueue('queue', {}, { messageId: 'same' });
    returnPublication(channel, 0);
    channel.publications[0].callback();
    channel.publications[1].callback();
    await expect(retry).resolves.toBe(true);
    expect(jest.getTimerCount()).toBe(0);
});

test('close rejects all pending publications as unknown before library callbacks', async () => {
    const { broker, channel } = await setup();
    const first = broker.publish('orders', 'created', {});
    const second = broker.sendToQueue('queue', {});
    const checks = [first, second].map(result => expect(result).rejects.toMatchObject({
        name: 'AmqpPublishUnknownError', reason: 'channel_closed',
    }));
    channel.emit('close');
    await Promise.all(checks);
    expect(channel.listenerCount('return')).toBe(0);
    expect(jest.getTimerCount()).toBe(0);
});

test('uses one return listener per channel and supports a replacement channel', async () => {
    const { broker, channel, connection } = await setup();
    const first = broker.publish('orders', 'created', {});
    const second = broker.publish('orders', 'created', {});
    expect(channel.listenerCount('return')).toBe(1);
    channel.publications.forEach(publication => publication.callback());
    await Promise.all([first, second]);
    channel.emit('close');
    const replacement = new FakeChannel();
    connection.createConfirmChannel.mockResolvedValue(replacement);
    await broker.start();
    const result = broker.publish('orders', 'created', {});
    returnPublication(channel, 0);
    replacement.publications[0].callback();
    await expect(result).resolves.toBe(true);
    expect(replacement.listenerCount('return')).toBe(1);
});

test('synchronous publication failure cleans up tracking and timeout', async () => {
    const { broker, channel } = await setup();
    channel.publish.mockImplementationOnce(() => { throw new Error('write failed'); });
    await expect(broker.publish('orders', 'created', {})).rejects.toBeInstanceOf(AmqpPublishError);
    expect(jest.getTimerCount()).toBe(0);
});

test('non-confirm mode retains existing buffer result and options', async () => {
    const { broker, channel } = await setup(false);
    await expect(broker.publish('orders', 'created', {}, { mandatory: false })).resolves.toBe(false);
    await expect(broker.sendToQueue('queue', {})).resolves.toBe(false);
    expect(channel.publish.mock.calls[0][3].mandatory).toBe(false);
    expect(channel.listenerCount('return')).toBe(0);
    expect(jest.getTimerCount()).toBe(0);
});

test.each([0, -1, NaN, Infinity, 1.5, 2_147_483_648])('rejects invalid timeout %s', async timeout => {
    await expect(setup(true, timeout)).rejects.toThrow('publishTimeoutMs');
});

test('publication error classes remain compatible with AmqpPublishError', () => {
    expect(new AmqpUnroutableError(312, 'NO_ROUTE', '', 'queue')).toBeInstanceOf(AmqpPublishError);
    expect(new AmqpPublishUnknownError('timeout')).toBeInstanceOf(AmqpPublishError);
});
