export interface IProducer<TPayload = void, TResult = void> {
    publish(payload?: TPayload): Promise<TResult>;
}
