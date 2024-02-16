# @3kles/3kles-amqpbroker

This package contains class to create broker with RabbitMQ

## App

**MessageBroker** is a class to create a broker with different methods
- **sendToExchange(exchange: string, routingKey: string, msg: Buffer,
        type: string = 'direct', options?: Options.AssertExchange): Promise\<boolean>** send the msg as a Buffer to the exchange with a specified routing key

- **send(queue: string, msg: Buffer, options?: Options.Publish): Promise\<boolean>** send the msg as a Buffer to a queue

- **sendRPCMessage(queue: string, msg: Buffer): Promise\<any>** send the msg as a Buffer to a queue and get the response

- **subscribeExchange(queue: string, exchange: string, routingKey: string, type: string = 'direct',
        handler: ((msg: ConsumeMessage, ack: () => void) => Promise\<void>),
        options?: Options.AssertExchange): Promise\<any>** subscribe to an exchange in a queue with a specified routingKey and a handler for the treatment of the messages we receive

- **subscribe(queue: string,
        handler: ((msg: ConsumeMessage, ack: () => void) => Promise\<void>),
        options?: Options.AssertQueue): Promise<() => void>** subscribe to a queue with a handler for the treatment of the messages we receive

## Install

### npm

```
npm install @3kles/3kles-amqpbroker --save
```

## How to use

First we need to create a broker

```javascript
const broker = await MessageBroker.getInstance();
```

How to subscribe to the messages of a queue

```javascript
await broker.subscribe('test.queue', async (msg, ack) => {
    console.log('Message from queue:', msg.content.toString());
    ack();
});
```

How to send a message to a queue

```javascript
await broker.send('test.queue', Buffer.from('hello from queue'));
```

How to subcribe to an exchange

```javascript
await broker.subscribeExchange('', 'test.exchange', 'test.routingKey', 'direct', async (msg, ack) => {
    console.log('Message from exchange :', msg.content.toString());
    ack();
});
```

How to send a message to an exchange

```javascript
await broker.sendToExchange('test.exchange', 'test.routingKey', Buffer.from('hello from exchange'));
```

How to send a message to an exchange and get the response

```javascript
const response = await broker.sendRPCMessage('api', Buffer.from(JSON.stringify({ name: 'toto' })));
```

Check the [`documentation`](https://doc.3kles-consulting.com) here.
