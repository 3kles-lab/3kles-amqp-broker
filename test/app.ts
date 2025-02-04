import { MessageBroker } from '../src/message-broker';

// tslint:disable-next-line: no-floating-promises
(async () => {
    process.env.RABBITMQ_USERNAME = 'guest';
    process.env.RABBITMQ_PASSWORD = 'guest';
    process.env.RABBITMQ_URL = '192.168.111.63';
    process.env.RABBITMQ_PROTOCOL = 'amqp';
    process.env.RABBITMQ_PORT = '5672';

    const broker = await MessageBroker.getInstance();

    /*Queue*/
    // await broker.subscribe('ceciestuntest', async (msg, ack) => {
    //     console.log('Message from queue:', msg.content.toString());
    //     ack();
    // });

    // await broker.receiveRPCMessage('addOneToNumber', async (msg, ack) => {
    //     const value = +msg.content.toString()
    //     console.log('receive from rpc', value);
    //     ack(`${value+1}`);
    // });

    // const response = await broker.sendRPCMessage('addOneToNumber', Buffer.from('55000'));
    // console.log('response',response)
    // const response2 = await broker.sendRPCMessage('addOneToNumber', Buffer.from('200'));
    // console.log('response',response2)
    // const response3 = await broker.sendRPCMessage('addOneToNumber', Buffer.from('88888'));
    // console.log('response',response3)

    // await broker.send('test.tata', Buffer.from('hello from queue'));
    /* ****/

    /*Exchange*/
    // await broker.subscribeExchange('', 'test.exchange', 'test.routingKey', 'direct', async (msg, ack) => {
    //     console.log('Message from exchange :', msg.content.toString());
    //     ack();
    // });

    // await broker.sendToExchange('test.exchange', 'test.routingKey', Buffer.from('hello from exchange'));
    /* ***/

    /*Send RPC message and wait response*/
    // const response = await broker.sendRPCMessage('m3api', Buffer.from(JSON.stringify({ api: 'MNS150MI', transaction: 'GetUserData' })));

    // await broker.disconnect();

})();
