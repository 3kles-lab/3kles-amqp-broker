export function delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

export function reconnectDelayWithJitter(baseMs: number, attempt: number, maxMs = 30_000): number {
    const exponential = Math.min(baseMs * 2 ** Math.min(attempt, 5), maxMs);
    const jitter = Math.floor(Math.random() * 1_000);

    return exponential + jitter;
}
