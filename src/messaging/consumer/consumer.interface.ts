export interface IConsumer {
    register(): Promise<void>;
    unregister(): void;
}
