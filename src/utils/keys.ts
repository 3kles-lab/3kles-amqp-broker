import { QueueBinding } from "../types/queue";


export function bindingKey(binding: QueueBinding): string {
    return JSON.stringify({
        exchange: binding.exchange,
        queue: binding.queue,
        routingKey: binding.routingKey,
        args: binding.args ?? {},
    });
}
