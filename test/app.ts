import { ConnectionManager } from '../src/connection-manager';
import { MessageBroker } from '../src/message-broker';
import { KlesDefaultMaxPriority, KlesPriority } from '../src/enum/priority.enum';

// tslint:disable-next-line: no-floating-promises
try {


    (async () => {
        process.env.RABBITMQ_USERNAME = 'rabbitmq';
        process.env.RABBITMQ_PASSWORD = 'rabbitmq';
        process.env.RABBITMQ_URL = 'localhost';
        process.env.RABBITMQ_PROTOCOL = 'amqp';
        process.env.RABBITMQ_PORT = '5672';

        const broker = await MessageBroker.initInstance(0, { prefetch: 1 });


        // const broker = await MessageBroker.initInstance('bbbb', { prefetch: 1 });
        // const broker2 = await MessageBroker.initInstance('aaaa', { prefetch: 200 });
        /*Queue*/
        // await broker.subscribe('abcd', async (msg, ack) => {
        //     console.log('Message from queue:', msg.content.toString());
        //     ack();
        // }, {  exclusive: true });

        await broker.send('ceciestuntest1', Buffer.from('message LOW priority'), {priority: KlesPriority.LOW}, { exclusive: true, maxPriority: KlesDefaultMaxPriority });
        await broker.send('ceciestuntest1', Buffer.from('message HIGH priority'), {priority: KlesPriority.HIGH}, { exclusive: true, maxPriority: KlesDefaultMaxPriority });
        await broker.send('ceciestuntest1', Buffer.from('message VERY_HIGH priority'), {priority: KlesPriority.VERY_HIGH}, { exclusive: true, maxPriority: KlesDefaultMaxPriority });

        await broker.subscribe('ceciestuntest1', async (msg, ack) => {
            console.log('Message2 from queue:', msg.content.toString());
            ack();
        }, { exclusive: true, maxPriority: KlesDefaultMaxPriority });

        // await broker.receiveRPCMessage('addOneToNumber', async (msg, ack) => {
        //     if (msg) {
        //         const value = +msg.content.toString()
        //         console.log('receive from rpc', value);
        //         await new Promise(resolve => {
        //             setTimeout(resolve, 5000);
        //         });
        //         console.log('ack')
        //         ack(`${value + 1}`);
        //     }
        // });


        // while (true) {
        //     console.log('ici')

        //     await broker.sendRPCMessage('addOneToNumber', Buffer.from('200'));
        //     await new Promise(resolve => {
        //         setTimeout(resolve, 1000);
        //     });


        // }

        // console.log('response',response)
        // const response2 = await broker.sendRPCMessage('addOneToNumber', Buffer.from('200'));
        // console.log('response',response2)
        // const response3 = await broker.sendRPCMessage('addOneToNumber', Buffer.from('88888'));
        // console.log('response',response3)

        // await broker.subscribe('ceciestuntest', async (msg, ack) => {
        //     console.log('Message from queue:', msg.content.toString());
        //     ack();
        // });


        // await broker.send('', Buffer.from('hello from queue'));
        /* ****/

        // /*Exchange*/
        await broker.subscribeExchange('toto', 'test.exchange', 'test.routingKey', 'direct', async (msg, ack) => {
            console.log(msg.properties);
            console.log('Message from exchange :', msg.content.toString());
            ack();
        });

        // await broker.subscribeExchange('', 'test.exchange', 'test.routingKey', 'direct', async (msg, ack) => {
        //     console.log('Message from exchange :', msg.content.toString());
        //     ack();
        // });

        // await broker.subscribeExchange('toto', 'test.exchange', ['test.routingKey', 'test.routingkey2'], 'direct', async (msg, ack) => {
        //     console.log('Message from exchange2 :', msg.content.toString());
        //     ack();
        // });

        // await broker.sendToExchange('test.exchange', 'test.routingKey', Buffer.from('hello from exchange'));
        /* ***/

        /*Send RPC message and wait response*/
        const response = await broker.sendRPCMessage('m3api', Buffer.from(JSON.stringify({ api: 'MNS150MI', transaction: 'GetUserData', })));
        console.log(response)
        // await broker.disconnect();

    })();

} catch (err) {
    console.log(err)
}