import { MessageBroker } from './message-broker';

export class ConsumerHandle {
    constructor(
        private readonly broker: MessageBroker,
        public readonly id: string,
    ) {}

    pause(): Promise<void> {
        return this.broker.pauseConsumer(this.id);
    }

    resume(): Promise<void> {
        return this.broker.resumeConsumer(this.id);
    }

    close(): Promise<void> {
        return this.broker.closeConsumer(this.id);
    }
}
