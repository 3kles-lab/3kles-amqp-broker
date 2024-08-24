# 3KLES-AMQP-BROKER

This package contains interface and class to manage AMQP Broker

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


