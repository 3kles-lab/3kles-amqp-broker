import { ConnectionManager } from '../src/connection-manager';
import { MessageBroker } from '../src/message-broker';
import { KlesDefaultMaxPriority, KlesPriority } from '../src/enum/priority.enum';
import { Consumer } from '../src/consumer';

// tslint:disable-next-line: no-floating-promises
try {


    (async () => {
        process.env.RABBITMQ_USERNAME = 'rabbitmq';
        process.env.RABBITMQ_PASSWORD = 'rabbitmq';
        process.env.RABBITMQ_URL = 'localhost';
        process.env.RABBITMQ_PROTOCOL = 'amqp';
        process.env.RABBITMQ_PORT = '5672';

        const broker = await MessageBroker.initInstance(0, { prefetch: 1 });

        // for (let i = 5; i > 0; i--) {

        //     console.log('i',i)

        //     await broker.subscribeExchange('toto', 'eventhub', 'status', 'direct', async (msg, ack, nack) => {

        //         console.log('new message')
        //         console.log('Message from queue:', msg.content.toString());

        //         const { since } = JSON.parse(msg.content.toString());

        //         console.log('s', since)
        //         console.log('since', new Date(since));

        //         ack();
        //     })
        // }


        // const broker = await MessageBroker.initInstance('bbbb', { prefetch: 1 });
        // const broker2 = await MessageBroker.initInstance('aaaa', { prefetch: 200 });
        /*Queue*/
        // await broker.subscribe('abcd', async (msg, ack) => {
        //     try {
        //         console.log('Message from queue:', msg.content.toString());
        //         // await new Promise(resolve => {
        //         //     setTimeout(resolve, 1500);
        //         // });
        //         console.log('fin ')

        //     } catch (err) {
        //         console.error(err);
        //     }

        //     ack();
        // });

        // let i = 0;
        // while (true) {
        //     await broker.send('abcd', Buffer.from('message LOW priority ' + i), {});
        //     await new Promise(resolve => {
        //             setTimeout(resolve, 1500);
        //         });
        //     i++;
        // }



        // const consumer = await broker.subscribe('ceciestuntest1', async (msg, ack, nack) => {
        //     console.log('Message2 from queue:', msg.content.toString());
        //     ack();
        //     // nack(false, true);

        // }, { maxPriority: KlesDefaultMaxPriority });



        // (async () => {
        //     await new Promise(resolve => {
        //         setTimeout(resolve, 1500);
        //     });
        //     console.log('pause')
        //     await consumer.pause();
        //     // await broker.pause(consumer.consumerTag);

        //     await new Promise(resolve => {
        //         setTimeout(resolve, 10000);
        //     });
        //     console.log('resume')
        //     // await broker.resume(consumer.consumerTag);
        //     await consumer.resume();
        // })();

        // const data = {
        //     transaction: 'GetUserData',
        //     api: 'MNS150MI'

        // }

        // let i = 500000
        // console.log('start')
        // await Promise.all(Array.from(Array(i).keys()).map(() => broker.send('m3ionapi', Buffer.from(JSON.stringify(data)), {}, { durable: false })))
        // console.log('end')








        const consumers = (await broker.subscribeExchange('aaaa', 'test.exchange', ['test.routingKey1', 'test.routingKey2'], 'direct', async (msg, ack) => {
            console.log('Message from exchange :', msg.content.toString());
            ack();
        }, { autoDelete: true }, { maxPriority: 10, })) as Consumer[];

        await new Promise(resolve => {
            setTimeout(resolve, 2500);
        });
        console.log('pause')
        await consumers[0].pause()

        await new Promise(resolve => {
            setTimeout(resolve, 2500);
        });
        console.log('resume')
        await consumers[0].resume()

        // console.log(consumers);
        // (async () => {
        //     await new Promise(resolve => {
        //         setTimeout(resolve, 1500);
        //     });
        //     console.log('pause')
        //     await consumers[0].pause();
        //     // await broker.pause(consumer.consumerTag);

        //     await new Promise(resolve => {
        //         setTimeout(resolve, 10000);
        //     });
        //     console.log('resume')
        //     // await broker.resume(consumer.consumerTag);
        //     await consumers[0].resume();
        // })();


        // while (true) {
        //     await broker.sendToExchange('test.exchange', 'test.routingKey1', Buffer.from('message 1'), 'direct', { autoDelete: true }, { priority: 1 });
        //     await new Promise(resolve => {
        //         setTimeout(resolve, 500);
        //     });
        //     await broker.sendToExchange('test.exchange', 'test.routingKey2', Buffer.from('message 2'), 'direct', { autoDelete: true }, { priority: 1 });
        //     await new Promise(resolve => {
        //         setTimeout(resolve, 500);
        //     });
        // }






















        // await broker.send('ceciestuntest1', Buffer.from('message LOW priority'), { priority: KlesPriority.LOW }, { exclusive: true, maxPriority: KlesDefaultMaxPriority });
        // await broker.send('ceciestuntest1', Buffer.from('message HIGH priority'), { priority: KlesPriority.HIGH }, { exclusive: true, maxPriority: KlesDefaultMaxPriority });
        // await broker.send('ceciestuntest1', Buffer.from('message VERY_HIGH priority'), { priority: KlesPriority.VERY_HIGH }, { exclusive: true, maxPriority: KlesDefaultMaxPriority });



        // await broker.receiveRPCMessage('addOneToNumber', async (msg, ack, nack) => {
        //     if (msg) {
        //         const value = +msg.content.toString()
        //         console.log('receive from rpc', value);
        //         // await new Promise(resolve => {
        //         //     setTimeout(resolve, 5000);
        //         // });
        //         console.log('ack')
        //         // ack(`${value + 1}`);
        //         nack(false, false)
        //     }
        // });

        // await broker.sendRPCMessage('addOneToNumber', Buffer.from('200'));

        // while (true) {
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
        // await broker.subscribeExchange('toto2', 'test.exchange', 'test.routingKey', 'direct', async (msg, ack) => {
        //     console.log(msg.properties);
        //     console.log('Message from exchange :', msg.content.toString());
        //     ack();
        // }, { durable: true }, { maxPriority: 10,  });


        // await broker.sendToExchange('test.exchange', 'test.routingKey', Buffer.from('message 1'), 'direct', { durable: true }, { priority: 1 });
        // await new Promise(resolve => {
        //     setTimeout(resolve, 1000);
        // });

        // await broker.sendToExchange('test.exchange', 'test.routingKey', Buffer.from('message 2'), 'direct', { durable: true }, { priority: 5 });



        // await broker.subscribeExchange('toto', 'test.exchange', ['test.routingKey', 'test.routingkey2'], 'direct', async (msg, ack) => {
        //     console.log('Message from exchange2 :', msg.content.toString());
        //     ack();
        // });

        // await broker.sendToExchange('test.exchange', 'test.routingKey', Buffer.from('hello from exchange'));
        /* ***/

        /*Send RPC message and wait response*/
        // const response = await broker.sendRPCMessage('m3api', Buffer.from(JSON.stringify({ api: 'MNS150MI', transaction: 'GetUserData', })));
        // console.log(response)
        // await broker.disconnect();

    })();

} catch (err) {
    console.log(err)
}