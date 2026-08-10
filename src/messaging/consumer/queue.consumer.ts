import { Options } from 'amqplib';
import { ConsumerHandle } from '../../consumer-handle';
import { MessageBroker } from '../../message-broker';
import { Handler } from '../handler/handler.interface';
import { AbstractConsumer } from './consumer.abstract';
import { ILogger } from '../logger/logger.interface';

export class QueueConsumer<TPayload> extends AbstractConsumer<TPayload> {
    private consumerHandle!: ConsumerHandle;

    constructor(
        broker: MessageBroker,
        queue: { name: string; options?: Options.Consume },
        handler: Handler<TPayload>,
        logger?: ILogger,
        parse?: (raw: string) => TPayload,
    ) {
        super(broker, queue, handler, logger, parse);
    }

    public async register(): Promise<void> {
        this.consumerHandle = await this.broker.consumeQueue(
            this.queue.name,
            async (msg, ctx) => {
                await this.onMessage(msg, ctx);
            },
            this.queue.options,
        );
    }

    public unregister(): void {
        this.consumerHandle?.close();
    }
}
