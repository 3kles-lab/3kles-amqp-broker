import { ConnectionManager } from '../src/connection-manager';
import { MessageBroker } from '../src/message-broker';
import { connectionManagerConfigFromEnv } from '../src/env';
import { KlesDefaultMaxPriority, KlesPriority } from '../src/enum/priority.enum';
import { ConsumerHandle } from '../src/consumer-handle';

// tslint:disable-next-line: no-floating-promises

let connectionManager: ConnectionManager;
let toto: ConsumerHandle;

let broker: MessageBroker;

process.once('SIGINT', async () => {
    console.log('ici1');
    // await toto?.close()

    
    const response = await broker?.publish(
        'orders.exchange',
        'orders.created',
        {
            orderId: 'order_123',
            userId: 'user_456',
            amount: 99.9,
        },
        {
            persistent: true,
        },
    );

    console.log('publis');

    console.log('response', response);

    await connectionManager?.disconnect();
    process.exit(0);
});

process.once('SIGTERM', async () => {
    console.log('ici2');
    await connectionManager?.disconnect();
    process.exit(0);
});

process.once('SIGTERM', async () => {
    console.log('ici3');
    await connectionManager?.disconnect();
    process.exit(0);
});

try {
    (async () => {
        process.env.RABBITMQ_USERNAME = 'guest';
        process.env.RABBITMQ_PASSWORD = 'guest';
        process.env.RABBITMQ_HOST = '192.168.111.63';
        process.env.RABBITMQ_PROTOCOL = 'amqp';
        process.env.RABBITMQ_PORT = '5672';

        connectionManager = await ConnectionManager.create(0, {
            ...connectionManagerConfigFromEnv(),
            enableGracefulShutdown: false,
        });

        await connectionManager.onDisconnected(() => {
            // toto.close();
        });

        broker = await MessageBroker.get('default', {
            connectionManager: connectionManager,
            confirm: true,
            prefetch: Number(process.env.RABBITMQ_PREFETCH) || 10,
        });

        await broker.assertExchange({
            name: 'orders.exchange',
            type: 'topic',
            options: { durable: true },
        });

        const queue = await broker.assertQueue({
            name: 'orders.created.queue',
            options: {
                durable: true,
                arguments: {
                    'x-max-priority': 5,
                },
            },
        });

        await broker.bindQueue({
            exchange: 'orders.exchange',
            queue: queue.queue,
            routingKey: 'orders.created',
        });
        // toto = await broker.consumeQueue(queue.queue, async (msg, ctx) => {

        //     try {

        //           const payload = ctx.json<any>();

        //     console.log('Received order:', payload);
        //         // traitement métier

        //         ctx.ack();
        //     } catch (err) {
        //         console.error('Failed to process order', err);
        //         ctx.nack(false, false);
        //     }
        // });

        // setInterval(async () => {
        //     try {
        //         await broker.publish(
        //             'orders.exchange',
        //             'orders.created',
        //             {
        //                 orderId: 'order_123',
        //                 userId: 'user_456',
        //                 amount: 99.9,
        //             },
        //             {
        //                 persistent: true,
        //             },
        //         );
        //     } catch (err) {
        //         console.error(err);
        //     }
        // }, 500);
    })();
} catch (err) {
    console.log(err);
}
