import { ConnectionManager } from '../src/connection-manager';
import { MessageBroker } from '../src/message-broker';
import { connectionManagerConfigFromEnv } from '../src/env';
import { KlesDefaultMaxPriority, KlesPriority } from '../src/enum/priority.enum';

// tslint:disable-next-line: no-floating-promises
try {
    (async () => {
        process.env.RABBITMQ_USERNAME = 'guest';
        process.env.RABBITMQ_PASSWORD = 'guest';
        process.env.RABBITMQ_HOST = 'localhost';
        process.env.RABBITMQ_PROTOCOL = 'amqp';
        process.env.RABBITMQ_PORT = '5672';

        const connection = await ConnectionManager.get('main', connectionManagerConfigFromEnv());

        const broker = await MessageBroker.get('default', {
            connectionManager: connection,
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

        await broker.publish(
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

        await broker.consumeQueue(queue.queue, async (msg, ctx) => {
            const payload = ctx.json<any>();

            console.log('Received order:', payload);

            try {
                // traitement métier

                ctx.ack();
            } catch (err) {
                console.error('Failed to process order', err);
                ctx.nack();
            }
        });
    })();
} catch (err) {
    console.log(err);
}
