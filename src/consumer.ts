import { MessageBroker } from "./message-broker";

export class Consumer {

    constructor(private broker: MessageBroker,
        public readonly consumerTag: string,
        private readonly key: string,
        private readonly handler: any) {

    }

    async pause(): Promise<void> {
        await this.broker.pause(this.consumerTag);
    }

    async resume(): Promise<void> {
        await this.broker.resume(this.consumerTag);
    }

    close(): void {
        this.broker.unsubscribe(this.key, this.handler);
    }
}
