# 3KLES-AMQP-BROKER

This package contains interface and class to manage AMQP Broker

## Confirmed publications

Enable `confirm` on the broker to wait for RabbitMQ's confirmation and detect
messages that cannot be routed, without changing publication calls:

```ts
const broker = await MessageBroker.get('default', {
    connectionManager,
    confirm: true,
    publishTimeoutMs: 10_000, // Optional; defaults to 10 seconds.
});

await broker.publish('orders.exchange', 'orders.created', payload, {
    persistent: true,
    messageId: eventId,
});
await broker.sendToQueue('orders.queue', payload);
```

This also applies to `publishInput`, `sendToQueueInput`, and `Publisher`.
In confirm mode these methods always publish with `mandatory: true`, even if
the caller supplies `mandatory: false`. Their `Promise<boolean>` resolves with
`true` only after a positive confirmation without a routing return. It rejects with:

- `AmqpUnroutableError`: RabbitMQ returned the message. Includes `replyCode`,
  `replyText`, `exchange`, `routingKey`, and the caller's `messageId`, if supplied.
- `AmqpPublishError`: negative confirmation or synchronous channel publication failure.
- `AmqpPublishUnknownError`: confirmation timed out (`reason: 'timeout'`) or the
  channel closed before confirmation (`reason: 'channel_closed'`). The message
  may already have been accepted. A retry can produce duplicates; no automatic
  retry is performed.

The two specialized errors extend `AmqpPublishError`. The broker preserves the
caller's `messageId` and headers, reserving `x-3kles-publication-id` for a unique
identifier per publication attempt. Consumers should use a stable business
identifier for deduplication when retrying an uncertain publication.

Confirmation does not mean the consumer has processed the message. Persistence
also requires persistent messages and appropriate durable queue configuration.
With `confirm: false`, the existing boolean buffer/backpressure result is preserved.
Direct calls through `currentChannel` and the RPC methods do not use this
publication tracking.

## Enums

**KlesPriority**:

- **NOT**: 1
- **LOW**: 2
- **MEDIUM**: 3
- **HIGH**: 4
- **VERY_HIGH**: 5
  
**Type**:
- **EXCHANGE**: Type for exchange
- **QUEUE**: Type for queue
  
## Interfaces

**QueueConfig** to definy queue properties:

- **queue**: Queue name
- **options**: AssertQueue options
- **active**: Boolean to activate configuration
- **inihandlertRoute**: Create a handler to specify how to consume message and ack or nack
- **rpc**: Boolean to activate RPC mode
- **consumerTag**: Consumer name

**ExchangeConfig** override **QueueConfig**:

- **type**: Exchange type 
- **exchange**: Exchange name
- **routingKey**: Routing key for the exchange
- **options**: AssertExchange options
- **optionsQueue**: AssertQueue options

**InstanceConfig** is an interface defined as below:

- **queue**: Queue name
- **options**: AssertQueue options
- **active**: Boolean to activate configuratio
- **inihandlertRoute**: Create a handler to specify how consume message and ack or nack
- **rpc**: Boolean to activate RPC mode
- **consumerTag**: Consumer name

## Classes

**ConnectionManager** is a class to manage connections:
- **constructor**: options:Options.Connect, timeout:Number
- **createConnection**: Method to create connection with options
- **disconnect**: Close connection
- **delay**: Delay timeout before recreate connection if connection failed 
- **getConnections**: List all connections from index and create if not exist


**Consumer** is a class to create AMQP consumer from MessageBroker:
- **constructor**: broker:MessageBroker, consumerTag:string, key:string, handler:string
- **close**: Close connection
- **pause**: Method to create connection with options
- **close**: Delay timeout before recreate connection if connection failed 


**MessageBroker** is an interface with these methods:

- **getInstance**: Get instance from index
- **initInstance**: Init instance from index and config
- **getAllInstances**: List all broker instances
- **send**: Send data to a queue
- **sendToExchange**: Send data to an exchange with routingKey
- **sendRPCMessage**: Send data to a queue in RPC mode
- **sendRPCToExchange**: Send data to an exchange with routingKey in RPC mode
- **receiveRPCMessage**: Receive message from queue in RPC mode
- **subscribe**: Subscribe to a queue to consume message 
- **subscribeToExchange**: Subscribe to a exchange to consume message with routingKey 
- **subscribeToExchangeMutiRoutingKey**: Similar to **subscribeExchange** but with multiple routingKeys
- **unsubscribe**: Unsubscribe from a key or exchange
- **pause**: Pause a consumer 
- **resume**: Resume a consumer 
- **disconnect**: Disconnect channel


