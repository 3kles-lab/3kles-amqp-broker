import { ChannelModel, connect, ConsumeMessage, Options } from 'amqplib';
import { EventEmitter, once } from 'events';
import { setTimeout as wait } from 'node:timers/promises';
import { ConnectionManager } from '../src/connection-manager';
import { AmqpRpcTimeoutError } from '../src/errors';
import { MessageBroker } from '../src/message-broker';
import { reconnectDelayWithJitter } from '../src/utils/delay';

jest.mock('amqplib', () => ({ connect: jest.fn() }));
jest.mock('../src/utils/delay', () => ({
    ...jest.requireActual('../src/utils/delay'),
    reconnectDelayWithJitter: jest.fn(() => 1),
}));

const exclusiveQueue: Options.AssertQueue = { exclusive: true, durable: false };
const fixedQueue = 'events.exclusive';
const logger = { debug: jest.fn(), info: jest.fn(), warn: jest.fn(), error: jest.fn() };

class FakeChannel extends EventEmitter {
    private readonly activeConsumers = new Map<string, string>();
    private queueCount = 0;
    private closed = false;

    constructor(
        private readonly name: string,
        private readonly declarationError?: { queue: string; code: number },
        private readonly queues = new Map<string, Options.AssertQueue>(),
    ) {
        super();
    }

    assertExchange = jest.fn(async () => {});
    assertQueue = jest.fn(async (name: string, options: Options.AssertQueue) => {
        if (name === this.declarationError?.queue) {
            this.fail(this.declarationError.code);
        }
        const queue = name || `amq.gen-${this.name}-${this.queueCount + 1}`;
        if (!this.queues.has(queue)) {
            this.queueCount++;
        }
        this.queues.set(queue, options);
        return { queue, messageCount: 0, consumerCount: 0 };
    });

    checkQueue = jest.fn(async (queue: string) => {
        this.requireQueue(queue);
        return { queue, messageCount: 0, consumerCount: 0 };
    });

    bindQueue = jest.fn(async (queue: string, _exchange: string, _routingKey: string, _args?: object) => {
        this.requireQueue(queue);
    });

    unbindQueue = jest.fn(async (queue: string, _exchange: string, _routingKey: string, _args?: object) => {
        this.requireQueue(queue);
    });

    consume = jest.fn(async (queue: string, _handler: (message: ConsumeMessage | null) => void | Promise<void>, options: Options.Consume) => {
        this.requireQueue(queue);
        const consumerTag = options.consumerTag ?? 'rpc';
        this.activeConsumers.set(consumerTag, queue);
        return { consumerTag };
    });

    cancel = jest.fn(async (tag: string) => {
        const queue = this.activeConsumers.get(tag);
        this.activeConsumers.delete(tag);
        if (queue && this.queues.get(queue)?.autoDelete && ![...this.activeConsumers.values()].includes(queue)) {
            this.queues.delete(queue);
        }
    });
    ack = jest.fn();
    nack = jest.fn();
    reject = jest.fn();

    sendToQueue = jest.fn((queue: string, _message: Buffer, _options: Options.Publish) => {
        this.requireQueue(queue);
        return true;
    });
    publish = jest.fn((_exchange: string, _routingKey: string, _message: Buffer, _options: Options.Publish) => true);

    close = jest.fn(async () => {
        if (!this.closed) {
            this.closed = true;
            this.emit('close');
        }
    });

    private requireQueue(queue: string): void {
        if (!this.queues.has(queue)) {
            this.fail(404);
        }
    }

    private fail(code: number): never {
        const error = Object.assign(new Error(`Channel error ${code}`), { code });
        this.emit('error', error);
        void this.close();
        throw error;
    }
}

class FakeConnection extends EventEmitter {
    private readonly openedChannels: FakeChannel[] = [];

    constructor(private readonly channels: FakeChannel[]) {
        super();
    }

    createChannel = jest.fn(async () => {
        const channel = this.channels.shift();
        if (!channel) {
            throw new Error('Unexpected channel creation');
        }
        this.openedChannels.push(channel);
        return channel;
    });

    createConfirmChannel = this.createChannel;

    close = jest.fn(async () => { this.drop(); });

    drop(): void {
        for (const channel of this.openedChannels) {
            void channel.close();
        }
        this.emit('close');
    }
}

let nextManagerId = 0;

async function setup(connections: FakeConnection[], rpc = false, confirm = false) {
    const connectMock = jest.mocked(connect);
    connectMock.mockReset();
    for (const connection of connections) {
        connectMock.mockResolvedValueOnce(connection as unknown as ChannelModel);
    }

    const name = `reconnect-${++nextManagerId}`;
    const connectionManager = await ConnectionManager.create(name, {
        connection: { url: 'amqp://unused' },
        enableGracefulShutdown: false,
        logger: logger as any,
    });
    const broker = await MessageBroker.create(name, {
        connectionManager,
        logger: logger as any,
        confirm,
        rpc: { enabled: rpc },
    });
    return { broker, connectionManager };
}

function waitForRetry(delay: number): Promise<void> {
    return new Promise((resolve) => {
        jest.mocked(reconnectDelayWithJitter).mockImplementationOnce(() => {
            resolve();
            return delay;
        });
    });
}

function holdHandler() {
    let release!: () => void;
    const waiting = new Promise<void>((resolve) => { release = resolve; });
    return { waiting, release };
}

function delivery(body: string, replyTo?: string): ConsumeMessage {
    return {
        fields: { deliveryTag: 1, consumerTag: 'consumer', redelivered: false, exchange: '', routingKey: fixedQueue },
        properties: { replyTo, correlationId: body } as ConsumeMessage['properties'],
        content: Buffer.from(body),
    };
}

beforeEach(() => jest.clearAllMocks());

test('restores distinct generated queues and keeps bindings and consumer handles usable across reconnects', async () => {
    const first = new FakeChannel('first');
    const second = new FakeChannel('second');
    const third = new FakeChannel('third');
    const initial = new FakeConnection([first]);
    const replacement = new FakeConnection([second]);
    const { broker } = await setup([initial, replacement, new FakeConnection([third])]);

    const bound = await broker.assertQueue({ name: '', options: exclusiveQueue });
    const unbound = await broker.assertQueue({ name: '', options: exclusiveQueue });
    await broker.assertExchange({ name: 'events' });
    const binding = { queue: bound.queue, exchange: 'events', routingKey: 'created', args: { key: 'value' } };
    await broker.bindQueue(binding);
    await broker.consumeQueue(bound.queue, jest.fn());
    const paused = await broker.consumeQueue(unbound.queue, jest.fn());
    await paused.pause();

    const firstRecovery = once(broker, 'started');
    initial.drop();
    await firstRecovery;

    expect(second.bindQueue).toHaveBeenCalledWith('amq.gen-second-1', 'events', 'created', binding.args);
    expect(second.consume.mock.calls.map(([queue]) => queue)).toEqual(['amq.gen-second-1']);
    await paused.resume();
    expect(second.consume.mock.calls.map(([queue]) => queue)).toEqual(['amq.gen-second-1', 'amq.gen-second-2']);
    await broker.sendToQueue(unbound.queue, {});
    expect(second.sendToQueue).toHaveBeenCalledWith('amq.gen-second-2', expect.any(Buffer), expect.any(Object));
    await broker.unbindQueue(binding);
    expect(second.unbindQueue).toHaveBeenCalledWith('amq.gen-second-1', 'events', 'created', binding.args);

    const secondRecovery = once(broker, 'started');
    replacement.drop();
    await secondRecovery;

    expect(third.bindQueue).not.toHaveBeenCalled();
    expect(third.consume.mock.calls.map(([queue]) => queue)).toEqual(['amq.gen-third-1', 'amq.gen-third-2']);
});

test.each([['named', fixedQueue], ['server-named', '']])(
    'resume recreates a deleted %s auto-delete queue with its bindings',
    async (_label, name) => {
        const queues = new Map<string, Options.AssertQueue>();
        const channel = new FakeChannel('consumer', undefined, queues);
        const probe = new FakeChannel('probe', undefined, queues);
        const { broker } = await setup([new FakeConnection([channel, probe])]);
        const options = { ...exclusiveQueue, autoDelete: true };
        const exchange = { name: 'events', type: 'direct' as const, options: { durable: false, autoDelete: true } };
        await broker.assertExchange(exchange);
        const { queue } = await broker.assertQueue({ name, options });
        const binding = { queue, exchange: exchange.name, routingKey: 'created', args: { source: 'test' } };
        await broker.bindQueue(binding);
        const handle = await broker.consumeQueue(queue, (_message, context) => context.ack());
        await handle.pause();
        expect(queues.has(queue)).toBe(false);

        await handle.resume();

        const actualName = name || 'amq.gen-consumer-2';
        expect(probe.checkQueue).toHaveBeenCalledWith(queue);
        expect(channel.close).not.toHaveBeenCalled();
        expect(channel.assertExchange).toHaveBeenCalledTimes(2);
        expect(channel.assertQueue.mock.calls).toEqual([[name, options], [name, options]]);
        expect(channel.bindQueue).toHaveBeenLastCalledWith(actualName, exchange.name, binding.routingKey, binding.args);
        expect(channel.consume.mock.calls.map(([queue]) => queue)).toEqual([queue, actualName]);
        await broker.sendToQueue(queue, 'after resume');
        expect(channel.sendToQueue).toHaveBeenCalledWith(actualName, Buffer.from('after resume'), expect.any(Object));
        const message = delivery('after resume');
        await channel.consume.mock.calls[1][1](message);
        expect(channel.ack).toHaveBeenCalledWith(message, undefined);
    },
);

test('resume reuses an existing auto-delete queue and concurrent resumes share a recreated queue', async () => {
    const queues = new Map<string, Options.AssertQueue>();
    const channel = new FakeChannel('consumer', undefined, queues);
    const probes = ['existing', 'missing', 'recreated'].map((name) => new FakeChannel(name, undefined, queues));
    const { broker } = await setup([new FakeConnection([channel, ...probes])]);
    const { queue } = await broker.assertQueue({ name: '', options: { ...exclusiveQueue, autoDelete: true } });
    const first = await broker.consumeQueue(queue, jest.fn());
    const second = await broker.consumeQueue(queue, jest.fn());

    await first.pause();
    expect(queues.has(queue)).toBe(true);
    await first.resume();
    expect(channel.assertQueue).toHaveBeenCalledTimes(1);
    expect(channel.consume.mock.calls.map(([queue]) => queue)).toEqual([queue, queue, queue]);

    await Promise.all([first.pause(), second.pause()]);
    expect(queues.has(queue)).toBe(false);
    await Promise.all([first.resume(), second.resume(), first.resume()]);

    const recreatedQueue = 'amq.gen-consumer-2';
    expect(channel.assertQueue).toHaveBeenCalledTimes(2);
    expect(channel.consume.mock.calls.map(([queue]) => queue)).toEqual([queue, queue, queue, recreatedQueue, recreatedQueue]);
    expect([...queues.keys()]).toEqual([recreatedQueue]);
});

test('retries a locked named exclusive queue on fresh channels of the same connection', async () => {
    const lockedQueue = { queue: fixedQueue, code: 405 };
    const lockedChannels = [new FakeChannel('locked-1', lockedQueue), new FakeChannel('locked-2', lockedQueue)];
    const available = new FakeChannel('available');
    const initial = new FakeConnection([new FakeChannel('initial')]);
    const replacement = new FakeConnection([...lockedChannels, available]);
    const { broker, connectionManager } = await setup([initial, replacement], false, true);
    const brokerError = jest.fn();
    broker.on('broker_error', brokerError);

    await broker.setupExchangeConsumer({ exchange: 'events', queue: '', queueOptions: exclusiveQueue }, jest.fn());
    await broker.setupExchangeConsumer({ exchange: 'events', queue: fixedQueue, queueOptions: exclusiveQueue }, jest.fn());

    const recovered = once(broker, 'started');
    initial.drop();
    await recovered;

    expect(connect).toHaveBeenCalledTimes(2);
    expect(replacement.createConfirmChannel).toHaveBeenCalledTimes(3);
    for (const channel of lockedChannels) {
        expect(channel.close).toHaveBeenCalledTimes(1);
        expect(channel.assertQueue.mock.calls).toEqual([[fixedQueue, exclusiveQueue]]);
        expect(channel.consume).not.toHaveBeenCalled();
    }
    expect(available.bindQueue.mock.calls.map(([queue]) => queue)).toEqual(['amq.gen-available-2', fixedQueue]);
    expect(available.consume.mock.calls.map(([queue]) => queue)).toEqual(['amq.gen-available-2', fixedQueue]);
    expect(connectionManager.connected).toBe(true);
    expect(broker.started).toBe(true);
    expect(brokerError).not.toHaveBeenCalled();
});

test('cancels a delayed retry when the connection drops again', async () => {
    const locked = new FakeChannel('locked', { queue: fixedQueue, code: 405 });
    const available = new FakeChannel('available');
    const initial = new FakeConnection([new FakeChannel('initial')]);
    const replacement = new FakeConnection([locked]);
    const { broker } = await setup([initial, replacement, new FakeConnection([available])]);
    await broker.assertQueue({ name: fixedQueue, options: exclusiveQueue });

    const scheduled = waitForRetry(60_000);
    initial.drop();
    await scheduled;
    const cancelledStartup = broker.start();
    const recovered = once(broker, 'started');
    replacement.drop();
    await Promise.all([cancelledStartup, recovered]);

    expect(replacement.createChannel).toHaveBeenCalledTimes(1);
    expect(available.assertQueue).toHaveBeenCalledWith(fixedQueue, exclusiveQueue);
    expect(broker.started).toBe(true);
});

test('cancels a delayed retry when the broker stops', async () => {
    const initial = new FakeConnection([new FakeChannel('initial')]);
    const replacement = new FakeConnection([new FakeChannel('locked', { queue: fixedQueue, code: 405 })]);
    const { broker, connectionManager } = await setup([initial, replacement]);
    await broker.assertQueue({ name: fixedQueue, options: exclusiveQueue });

    const scheduled = waitForRetry(60_000);
    initial.drop();
    await scheduled;
    const cancelledStartup = broker.start();
    await broker.stop();
    await cancelledStartup;

    expect(replacement.createChannel).toHaveBeenCalledTimes(1);
    expect(connectionManager.connected).toBe(true);
    expect(broker.started).toBe(false);
});

test('an explicit start waits for the old broker channel to finish closing', async () => {
    const first = new FakeChannel('first');
    const second = new FakeChannel('second');
    const connection = new FakeConnection([first, second]);
    const { broker } = await setup([connection]);
    const close = holdHandler();
    const closeChannel = first.close.getMockImplementation()!;
    first.close.mockImplementationOnce(async () => {
        await close.waiting;
        await closeChannel();
    });

    const stopping = broker.stop();
    const restarting = broker.start();
    const channelCreationsWhileStopping = connection.createChannel.mock.calls.length;
    close.release();
    await Promise.all([stopping, restarting]);

    expect(channelCreationsWhileStopping).toBe(1);
    expect(broker.currentChannel).toBe(second);
});

test('a throwing started listener leaves the initialized broker channel usable', async () => {
    const second = new FakeChannel('second');
    const { broker } = await setup([new FakeConnection([new FakeChannel('first'), second])]);
    await broker.stop();
    const listenerError = new Error('Started listener failed');
    broker.once('started', () => { throw listenerError; });

    await expect(broker.start()).rejects.toBe(listenerError);

    expect(second.close).not.toHaveBeenCalled();
    expect(broker.currentChannel).toBe(second);
});

test('a throwing connected listener is reported without interrupting automatic recovery', async () => {
    const initial = new FakeConnection([new FakeChannel('first')]);
    const second = new FakeChannel('second');
    const replacement = new FakeConnection([second]);
    const { broker, connectionManager } = await setup([initial, replacement]);
    const listenerError = new Error('Connected listener failed');
    connectionManager.once('connected', () => { throw listenerError; });
    const reported = once(connectionManager, 'connection_error');
    const recovered = once(broker, 'started');

    initial.drop();
    const [[error]] = await Promise.all([reported, recovered]);

    expect(error).toBe(listenerError);
    expect(connectionManager.currentConnection).toBe(replacement);
    expect(broker.currentChannel).toBe(second);
    expect(connect).toHaveBeenCalledTimes(2);
});

test('disconnect cancels an AMQP handshake and closes a connection that arrives late', async () => {
    const initial = new FakeConnection([new FakeChannel('initial')]);
    const late = new FakeConnection([new FakeChannel('late')]);
    const { broker, connectionManager } = await setup([initial]);
    const handshake = holdHandler();
    let signal: AbortSignal | undefined;
    jest.mocked(connect).mockImplementationOnce(async (_target, socketOptions) => {
        signal = socketOptions?.signal;
        await handshake.waiting;
        return late as unknown as ChannelModel;
    });
    const connected = jest.fn();
    const started = jest.fn();
    connectionManager.on('connected', connected);
    broker.on('started', started);

    initial.drop();
    const reconnect = connectionManager.connect();
    await connectionManager.disconnect();
    expect(connectionManager.currentState).toBe('disconnected');
    handshake.release();
    await reconnect;

    expect(signal?.aborted).toBe(true);
    expect(late.close).toHaveBeenCalledTimes(1);
    expect(late.createChannel).not.toHaveBeenCalled();
    expect(connected).not.toHaveBeenCalled();
    expect(started).not.toHaveBeenCalled();
    expect(connectionManager.connection).toBeNull();
    expect(connectionManager.currentState).toBe('disconnected');
    expect(broker.started).toBe(false);
});

test('disconnect cancels the wait before another connection attempt', async () => {
    const initial = new FakeConnection([new FakeChannel('initial')]);
    const { broker, connectionManager } = await setup([initial]);
    const failure = new Error('Connection refused');
    jest.mocked(connect).mockRejectedValueOnce(failure);
    const scheduled = waitForRetry(1_000);

    initial.drop();
    const reconnect = connectionManager.connect();
    await scheduled;
    await connectionManager.disconnect();

    await expect(Promise.race([
        reconnect.then(() => 'cancelled'),
        wait(250, 'timed out', { ref: false }),
    ])).resolves.toBe('cancelled');
    expect(connect).toHaveBeenCalledTimes(2);
    expect(connectionManager.currentState).toBe('disconnected');
    expect(broker.started).toBe(false);
});

test('reports other queue declaration errors without retrying', async () => {
    const initial = new FakeConnection([new FakeChannel('initial')]);
    const replacement = new FakeConnection([new FakeChannel('mismatch', { queue: fixedQueue, code: 406 })]);
    const { broker } = await setup([initial, replacement]);
    await broker.assertQueue({ name: fixedQueue, options: exclusiveQueue });

    const failed = once(broker, 'broker_error');
    initial.drop();
    const [error] = await failed;

    expect(error).toMatchObject({ code: 406 });
    expect(reconnectDelayWithJitter).not.toHaveBeenCalled();
    expect(replacement.createChannel).toHaveBeenCalledTimes(1);
    expect(broker.started).toBe(false);
});

test('recreates and consumes the generated RPC reply queue on the replacement channel', async () => {
    const initial = new FakeConnection([new FakeChannel('initial')]);
    const available = new FakeChannel('available');
    const { broker } = await setup([initial, new FakeConnection([available])], true);

    const recovered = once(broker, 'started');
    initial.drop();
    await recovered;

    expect(available.assertQueue).toHaveBeenCalledWith('', { exclusive: true, autoDelete: true, durable: false });
    expect(available.consume).toHaveBeenCalledWith('amq.gen-available-1', expect.any(Function), { noAck: true });
});

test.each(['channel', 'connection'] as const)(
    'rejects pending RPCs with an unknown outcome when the %s closes and accepts new requests after reconnect',
    async (firstClose) => {
        jest.useFakeTimers();
        try {
            const first = new FakeChannel('first');
            const second = new FakeChannel('second');
            const initial = new FakeConnection([first]);
            const { broker } = await setup([initial, new FakeConnection([second])], true);
            await broker.assertQueue({ name: fixedQueue, options: exclusiveQueue });

            const queueRequest = broker.requestQueue({ queue: fixedQueue, payload: 'old queue request', timeoutMs: 60_000 });
            const exchangeRequest = broker.requestExchange({ exchange: 'events', routingKey: 'rpc', payload: 'old exchange request', timeoutMs: 60_000 });
            const outcomes = Promise.allSettled([queueRequest, exchangeRequest]);
            await Promise.resolve();
            const queueId = first.sendToQueue.mock.calls[0][2].correlationId;
            const exchangeId = first.publish.mock.calls[0][3].correlationId;
            expect(jest.getTimerCount()).toBe(2);

            const recovered = once(broker, 'started');
            if (firstClose === 'channel') {
                await first.close();
            } else {
                initial.emit('close');
            }

            expect(jest.getTimerCount()).toBe(0);
            expect(await outcomes).toEqual([queueId, exchangeId].map((correlationId) => ({
                status: 'rejected',
                reason: expect.objectContaining({ name: 'AmqpRpcUnknownError', reason: 'channel_closed', correlationId }),
            })));

            if (firstClose === 'channel') {
                initial.drop();
            }
            await recovered;
            expect(second.sendToQueue).not.toHaveBeenCalled();
            expect(second.publish).not.toHaveBeenCalled();

            const newRequest = broker.requestQueue({ queue: fixedQueue, payload: 'new request', timeoutMs: 60_000 });
            await Promise.resolve();
            const [, , options] = second.sendToQueue.mock.calls[0];
            expect(options.replyTo).toBe(second.consume.mock.calls[0][0]);
            expect(options.replyTo).not.toBe(first.consume.mock.calls[0][0]);
            // A late close or reply from the old channel cannot settle the new request.
            await first.close();
            await first.consume.mock.calls[0][1](delivery(options.correlationId));
            expect(jest.getTimerCount()).toBe(1);
            const reply = delivery('new response');
            reply.properties.correlationId = options.correlationId;
            await second.consume.mock.calls[0][1](reply);
            await expect(newRequest).resolves.toBe('new response');
            expect(jest.getTimerCount()).toBe(0);
        } finally {
            jest.useRealTimers();
        }
    },
);

test('a connected RPC request still times out when the server does not reply', async () => {
    jest.useFakeTimers();
    try {
        const channel = new FakeChannel('connected');
        const { broker } = await setup([new FakeConnection([channel])], true);
        const result = broker.requestExchange({ exchange: 'events', routingKey: 'rpc', payload: {}, timeoutMs: 50 });
        const timedOut = expect(result).rejects.toBeInstanceOf(AmqpRpcTimeoutError);
        await Promise.resolve();
        jest.advanceTimersByTime(50);
        await timedOut;
        expect(broker.started).toBe(true);
    } finally {
        jest.useRealTimers();
    }
});

test.each(['consumeQueue', 'consumeRpcQueue'] as const)(
    'restores valid consumers after %s is refused for a missing queue',
    async (register) => {
        const first = new FakeChannel('first');
        const second = new FakeChannel('second');
        const initial = new FakeConnection([first]);
        const { broker } = await setup([initial, new FakeConnection([second])]);
        const invalidHandler = jest.fn();
        await broker.assertQueue({ name: fixedQueue, options: exclusiveQueue });
        await broker.consumeQueue(fixedQueue, (_message, context) => context.ack());

        await expect(broker[register]('missing.queue', invalidHandler)).rejects.toMatchObject({ code: 404 });
        expect(broker.started).toBe(false);

        const recovered = Promise.race([
            once(broker, 'started'),
            once(broker, 'broker_error').then(([error]) => { throw error; }),
        ]);
        initial.drop();
        await recovered;

        expect(second.consume.mock.calls.map(([queue]) => queue)).toEqual([fixedQueue]);
        const message = delivery('after reconnect');
        await second.consume.mock.calls[0][1](message);
        expect(second.ack).toHaveBeenCalledWith(message, undefined);
        expect(invalidHandler).not.toHaveBeenCalled();
    },
);

test.each(['consumeQueue', 'consumeRpcQueue'] as const)(
    'recovers the channel on the same connection after %s gets a 404',
    async (register) => {
        const first = new FakeChannel('first');
        const replacement = new FakeChannel('replacement');
        const connection = new FakeConnection([first, replacement]);
        const { broker, connectionManager } = await setup([connection], true, true);
        const disconnected = jest.fn();
        connectionManager.on('disconnected', disconnected);
        await broker.assertQueue({ name: fixedQueue, options: exclusiveQueue });
        await broker.consumeQueue(fixedQueue, (_message, context) => context.ack());

        const recovered = once(broker, 'started', { signal: AbortSignal.timeout(1_500) });
        await expect(broker[register]('missing.queue', jest.fn())).rejects.toMatchObject({ code: 404 });
        await recovered;

        expect(connect).toHaveBeenCalledTimes(1);
        expect(disconnected).not.toHaveBeenCalled();
        expect(connectionManager.currentConnection).toBe(connection);
        expect(connection.createConfirmChannel).toHaveBeenCalledTimes(2);
        expect(broker.currentChannel).toBe(replacement);
        expect(replacement.consume.mock.calls.map(([queue]) => queue)).toEqual([fixedQueue, 'amq.gen-replacement-2']);
        const message = delivery('same connection');
        await replacement.consume.mock.calls[0][1](message);
        expect(replacement.ack).toHaveBeenCalledWith(message, undefined);
    },
);

test('a channel recovery preserves surviving generated queues and recreates deleted ones with their bindings', async () => {
    const queues = new Map<string, Options.AssertQueue>();
    const first = new FakeChannel('first', undefined, queues);
    const replacement = new FakeChannel('replacement', undefined, queues);
    const existing = new FakeChannel('existing', undefined, queues);
    const missing = new FakeChannel('missing', undefined, queues);
    const connection = new FakeConnection([first, replacement, existing, missing]);
    const { broker } = await setup([connection]);
    const { queue: survivingQueue } = await broker.assertQueue({ name: '', options: exclusiveQueue });
    const autoDelete = { ...exclusiveQueue, autoDelete: true };
    const { queue: deletedQueue } = await broker.assertQueue({ name: '', options: autoDelete });
    await broker.assertExchange({ name: 'events' });
    await broker.bindQueue({ queue: survivingQueue, exchange: 'events', routingKey: 'survives' });
    await broker.bindQueue({ queue: deletedQueue, exchange: 'events', routingKey: 'deleted' });
    await broker.consumeQueue(survivingQueue, jest.fn());
    await broker.consumeQueue(deletedQueue, jest.fn());
    first.once('close', () => { queues.delete(deletedQueue); });

    const recovered = once(broker, 'started', { signal: AbortSignal.timeout(1_500) });
    await expect(broker.consumeQueue('missing.queue', jest.fn())).rejects.toMatchObject({ code: 404 });
    await recovered;

    expect(replacement.assertQueue.mock.calls).toEqual([['', autoDelete]]);
    expect(existing.checkQueue).toHaveBeenCalledWith(survivingQueue);
    expect(missing.checkQueue).toHaveBeenCalledWith(deletedQueue);
    const recreatedQueue = 'amq.gen-replacement-1';
    expect(replacement.bindQueue.mock.calls.map(([queue, , key]) => [queue, key]))
        .toEqual([[survivingQueue, 'survives'], [recreatedQueue, 'deleted']]);
    expect(replacement.consume.mock.calls.map(([queue]) => queue)).toEqual([survivingQueue, recreatedQueue]);
    await broker.sendToQueue(deletedQueue, 'still routable');
    expect(replacement.sendToQueue).toHaveBeenCalledWith(recreatedQueue, Buffer.from('still routable'), expect.any(Object));
});

test('a 404 during automatic topology restoration reports the error and stops recovery', async () => {
    const first = new FakeChannel('first');
    const replacement = new FakeChannel('replacement', { queue: fixedQueue, code: 404 });
    const connection = new FakeConnection([first, replacement]);
    const { broker } = await setup([connection]);
    await broker.assertQueue({ name: fixedQueue, options: exclusiveQueue });
    const failed = once(broker, 'broker_error', { signal: AbortSignal.timeout(1_500) });

    await expect(broker.consumeQueue('missing.queue', jest.fn())).rejects.toMatchObject({ code: 404 });
    const [error] = await failed;
    await new Promise(setImmediate);

    expect(error).toMatchObject({ code: 404 });
    expect(connection.createChannel).toHaveBeenCalledTimes(2);
    expect(broker.started).toBe(false);
});

test('a 404 while restarting a cancelled consumer is handled and triggers channel recovery', async () => {
    const queues = new Map<string, Options.AssertQueue>();
    const first = new FakeChannel('first', undefined, queues);
    const replacement = new FakeChannel('replacement', undefined, queues);
    const { broker } = await setup([new FakeConnection([first, replacement])]);
    await broker.assertQueue({ name: fixedQueue, options: exclusiveQueue });
    await broker.consumeQueue(fixedQueue, jest.fn());
    queues.delete(fixedQueue);
    const recovered = once(broker, 'started', { signal: AbortSignal.timeout(1_500) });
    const reported = once(broker, 'consumer_error');

    await first.consume.mock.calls[0][1](null);
    const [error] = await reported;
    await recovered;

    expect(error).toMatchObject({ name: 'AmqpConsumerError', cause: { code: 404 } });
    expect(replacement.consume.mock.calls.map(([queue]) => queue)).toEqual([fixedQueue]);
    expect(connect).toHaveBeenCalledTimes(1);
});

test('stopping the broker when a 404 closes the channel cancels automatic recovery', async () => {
    const connection = new FakeConnection([new FakeChannel('first')]);
    const { broker } = await setup([connection]);
    const stopped = once(broker, 'channel_closed').then(() => broker.stop());

    await expect(broker.consumeQueue('missing.queue', jest.fn())).rejects.toMatchObject({ code: 404 });
    await stopped;
    await new Promise(setImmediate);

    expect(connection.createChannel).toHaveBeenCalledTimes(1);
    expect(broker.started).toBe(false);
});

test('rejects a late registration confirmation from a closed channel instead of restoring it', async () => {
    const first = new FakeChannel('first');
    const second = new FakeChannel('second');
    const initial = new FakeConnection([first]);
    const { broker } = await setup([initial, new FakeConnection([second])]);
    await broker.assertQueue({ name: fixedQueue, options: exclusiveQueue });

    const confirmation = holdHandler();
    first.consume.mockImplementationOnce(async () => {
        await confirmation.waiting;
        return { consumerTag: 'stale-tag' };
    });
    const registered = broker.consumeQueue(fixedQueue, jest.fn());
    const recovered = once(broker, 'started');
    initial.drop();
    await recovered;
    confirmation.release();

    await expect(registered).rejects.toThrow('Channel closed before consumer registration completed');
    expect(second.consume).not.toHaveBeenCalled();

    const handle = await broker.consumeQueue(fixedQueue, jest.fn());
    await handle.close();
    expect(second.consume).toHaveBeenCalledTimes(1);
    expect(second.cancel).toHaveBeenCalledTimes(1);
});

test('restoration keeps one subscription per handle when consumers are registered or resumed during recovery', async () => {
    const first = new FakeChannel('first');
    const second = new FakeChannel('second');
    const initial = new FakeConnection([first]);
    const { broker } = await setup([initial, new FakeConnection([second])]);
    const pausedQueue = 'paused.queue';
    const newQueue = 'new.queue';
    for (const name of [fixedQueue, pausedQueue, newQueue]) {
        await broker.assertQueue({ name, options: exclusiveQueue });
    }
    await broker.consumeQueue(fixedQueue, jest.fn());
    const paused = await broker.consumeQueue(pausedQueue, jest.fn());
    await paused.pause();

    const restoreStarted = holdHandler();
    const restore = holdHandler();
    const consume = second.consume.getMockImplementation()!;
    second.consume.mockImplementationOnce(async (...args) => {
        restoreStarted.release();
        await restore.waiting;
        return consume(...args);
    });

    const recovered = once(broker, 'started');
    initial.drop();
    await restoreStarted.waiting;
    await broker.consumeQueue(newQueue, jest.fn());
    await paused.resume();
    restore.release();
    await recovered;

    expect(second.consume.mock.calls.map(([queue]) => queue)).toEqual([fixedQueue, newQueue, pausedQueue]);
    await first.consume.mock.calls[0][1](null);
    expect(second.consume).toHaveBeenCalledTimes(3);
});

test.each(['ack', 'nack', 'reject', 'handler error'] as const)(
    'skips a late %s without settling a new delivery with the same tag',
    async (operation) => {
        const first = new FakeChannel('first');
        const second = new FakeChannel('second');
        const initial = new FakeConnection([first]);
        const { broker } = await setup([initial, new FakeConnection([second])]);
        const oldHandler = holdHandler();
        const newHandler = holdHandler();
        const oldMessage = delivery('old');
        const newMessage = delivery('new');

        await broker.assertQueue({ name: fixedQueue, options: exclusiveQueue });
        await broker.consumeQueue(fixedQueue, async (message, context) => {
            if (message === oldMessage) {
                await oldHandler.waiting;
                if (operation === 'handler error') {
                    throw new Error('Old handler failed');
                }
                context[operation]();
                context[operation]();
                return;
            }
            await newHandler.waiting;
            context.ack();
        });

        const oldDelivery = first.consume.mock.calls[0][1](oldMessage);
        const recovered = once(broker, 'started');
        initial.drop();
        await recovered;
        const newDelivery = second.consume.mock.calls[0][1](newMessage);

        oldHandler.release();
        await oldDelivery;
        for (const channel of [first, second]) {
            expect(channel.ack).not.toHaveBeenCalled();
            expect(channel.nack).not.toHaveBeenCalled();
            expect(channel.reject).not.toHaveBeenCalled();
        }
        const settlementWarnings = logger.warn.mock.calls.filter(([, message]) =>
            message === '[AMQP] Delivery could not be settled because its channel is no longer active');
        expect(settlementWarnings).toEqual([[expect.objectContaining({ queue: fixedQueue, deliveryTag: 1 }), expect.any(String)]]);

        newHandler.release();
        await newDelivery;
        expect(second.ack).toHaveBeenCalledTimes(1);
        expect(second.ack).toHaveBeenCalledWith(newMessage, undefined);
    },
);

test.each(['reply', 'nack', 'reject', 'handler error'] as const)(
    'skips a late RPC %s without affecting the new channel',
    async (operation) => {
        const first = new FakeChannel('first');
        const second = new FakeChannel('second');
        const initial = new FakeConnection([first]);
        const { broker } = await setup([initial, new FakeConnection([second])]);
        const oldHandler = holdHandler();
        const newHandler = holdHandler();
        const replyQueue = 'rpc.responses';
        const oldMessage = delivery('old', replyQueue);
        const newMessage = delivery('new', replyQueue);

        await broker.assertQueue({ name: fixedQueue, options: exclusiveQueue });
        await broker.assertQueue({ name: replyQueue, options: exclusiveQueue });
        await broker.consumeRpcQueue(fixedQueue, async (message, context) => {
            if (message === oldMessage) {
                await oldHandler.waiting;
                if (operation === 'handler error') {
                    throw new Error('Old RPC handler failed');
                }
                if (operation === 'reply') {
                    context.reply('old response');
                } else {
                    context[operation]();
                }
                return;
            }
            await newHandler.waiting;
            context.reply('new response');
        });

        const oldDelivery = first.consume.mock.calls[0][1](oldMessage);
        const recovered = once(broker, 'started');
        initial.drop();
        await recovered;
        const newDelivery = second.consume.mock.calls[0][1](newMessage);

        oldHandler.release();
        await oldDelivery;
        for (const channel of [first, second]) {
            expect(channel.sendToQueue).not.toHaveBeenCalled();
            expect(channel.ack).not.toHaveBeenCalled();
            expect(channel.nack).not.toHaveBeenCalled();
            expect(channel.reject).not.toHaveBeenCalled();
        }

        newHandler.release();
        await newDelivery;
        expect(second.sendToQueue).toHaveBeenCalledTimes(1);
        expect(second.sendToQueue).toHaveBeenCalledWith(replyQueue, Buffer.from('new response'), expect.objectContaining({ correlationId: 'new' }));
        expect(second.ack).toHaveBeenCalledTimes(1);
        expect(second.ack).toHaveBeenCalledWith(newMessage);
    },
);
