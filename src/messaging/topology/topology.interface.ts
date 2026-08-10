import { AssertExchangeInput } from '../../types/exchange';
import { AssertQueueInput, BindQueueInput } from '../../types/queue';

export interface Topology {
    topology(): { exchanges: AssertExchangeInput[]; queues: AssertQueueInput[]; bindings: BindQueueInput[] };
}
