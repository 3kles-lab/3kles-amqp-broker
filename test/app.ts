import { MessageBroker } from '../src/message-broker';

// tslint:disable-next-line: no-floating-promises
(async () => {
    process.env.RABBITMQ_USERNAME = 'rabbitmq';
    process.env.RABBITMQ_PASSWORD = 'rabbitmq';
    process.env.RABBITMQ_URL = 'localhost';
    process.env.RABBITMQ_PROTOCOL = 'amqp';
    process.env.RABBITMQ_PORT = '5672';

    const broker = await MessageBroker.getInstance();

    /*Queue*/
    await broker.subscribe('test.queue', async (msg, ack) => {
        console.log('Message from queue:', msg.content.toString());
        ack();
    });

    await broker.send('test.queue', Buffer.from('hello from queue'));
    /* ****/

    /*Exchange*/
    await broker.subscribeExchange('', 'test.exchange', 'test.routingKey', 'direct', async (msg, ack) => {
        console.log('Message from exchange :', msg.content.toString());
        ack();
    });

    await broker.sendToExchange('test.exchange', 'test.routingKey', Buffer.from('hello from exchange'));
    /* ***/

    /*Send RPC message and wait response*/
    const response = await broker.sendRPCMessage('m3api', Buffer.from(JSON.stringify({ api: 'MNS150MI', transaction: 'GetUserData' })));

    await broker.disconnect();

})();
