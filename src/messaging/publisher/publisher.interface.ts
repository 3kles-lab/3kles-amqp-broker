import { Options } from 'amqplib';
import { PublishPayload } from '../../types/publish';

export interface IPublisher {
    publishToExchange(exchange: string, routingKey: string, payload: PublishPayload, options?: Options.Publish): Promise<boolean>;
    publishToQueue(queue: string, payload: PublishPayload, options?: Options.Publish): Promise<boolean>;
}
