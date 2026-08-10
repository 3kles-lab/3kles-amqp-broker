import { MessageBroker } from '../../message-broker';
import { AssertExchangeInput } from '../../types/exchange';
import { AssertQueueInput, BindQueueInput } from '../../types/queue';

export class TopologyRegistry {
    constructor(private readonly broker: MessageBroker) {}

    public async register(topology: { exchanges?: AssertExchangeInput[]; queues?: AssertQueueInput[]; bindings?: BindQueueInput[] }): Promise<void> {
        if (topology.exchanges) {
            for (const exchange of topology.exchanges) {
                await this.broker.assertExchange(exchange);
            }
        }
        if (topology.queues) {
            for (const queue of topology.queues) {
                await this.broker.assertQueue(queue);
            }
        }
        if (topology.bindings) {
            for (const binding of topology.bindings) {
                await this.broker.bindQueue(binding);
            }
        }
    }
}
