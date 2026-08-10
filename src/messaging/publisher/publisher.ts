import { Options } from 'amqplib';
import { IPublisher } from './publisher.interface';
import { MessageBroker } from '../../message-broker';
import { PublishPayload } from '../../types/publish';

export class Publisher implements IPublisher {
    constructor(private readonly broker: MessageBroker) {}

    public publishToExchange(exchange: string, routingKey: string, payload: PublishPayload, options?: Options.Publish): Promise<boolean> {
        return this.broker.publish(exchange, routingKey, payload, {
            contentType: 'application/json',
            ...options,
        });
    }
    public publishToQueue(queue: string, payload: PublishPayload, options?: Options.Publish): Promise<boolean> {
        return this.broker.sendToQueue(queue, payload, {
            contentType: 'application/json',
            ...options,
        });
    }
}
