import { ConsumeMessage } from 'amqplib';
import { RetryableError } from '../errors/retryable.error';
import { NonRetryableError } from '../errors/non-retryable.error';
import { MessageBroker } from '../../message-broker';
import { ConsumeContext } from '../../types/consumer';
import { AssertQueueInput } from '../../types/queue';
import { Handler, MessageContext } from '../handler/handler.interface';
import { IConsumer } from './consumer.interface';
import { ILogger } from '../logger/logger.interface';


export abstract class AbstractConsumer<TPayload> implements IConsumer {
    protected constructor(
        protected readonly broker: MessageBroker,
        protected readonly queue: AssertQueueInput,
        protected readonly handler: Handler<TPayload>,
        protected readonly logger: ILogger = console,
        protected readonly parse: (raw: string) => TPayload = JSON.parse,
    ) {}

    public abstract register(): Promise<void>;
    public abstract unregister(): void;

    protected async onMessage(message: ConsumeMessage, ctx: ConsumeContext): Promise<void> {
        if (!message) {
            return;
        }

        try {
            const payload = this.parse(message.content.toString());

            const context: MessageContext = {
                messageId: message.properties.messageId,
                correlationId: message.properties.correlationId,
                routingKey: message.fields.routingKey,
                exchange: message.fields.exchange,
                timestamp: message.properties.timestamp ? new Date(message.properties.timestamp * 1000) : undefined,
                headers: (message.properties.headers ?? {}) as Record<string, unknown>,
            };

            await this.handler.handle(payload, context);
            ctx.ack();
        } catch (error) {
            this.handleError(error, ctx.nack);
        }
    }

    protected handleError(error: unknown, nack: (allUpTo?: boolean, requeue?: boolean) => void): void {
        if (error instanceof Error) {
            this.logger.error?.(error.message, {
                message: error.message,
                stack: error.stack,
                name: error.name,
            });
        } else {
            this.logger.error?.('Consumer error', {
                error,
            });
        }

        if (error instanceof RetryableError) {
            nack(false, true);
            return;
        }
        if (error instanceof NonRetryableError) {
            nack(false, false);
            return;
        }

        nack(false, false);
    }
}
