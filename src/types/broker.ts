import { Logger } from 'pino';
import { ConnectionManager } from '../connection-manager';

export interface BrokerRpcConfig {
    enabled?: boolean;
    timeoutMs?: number;
    replyQueue?: string;
}

export interface BrokerConsumerConfig {
    restartOnCancel?: boolean;
    requeueOnHandlerError?: boolean;
}

export interface BrokerConfig {
    connectionManager: ConnectionManager;
    prefetch?: number;
    logger?: Logger;

    rpc?: BrokerRpcConfig;

    consumers?: BrokerConsumerConfig;
}

export type BrokerState = 'idle' | 'starting' | 'started' | 'stopping' | 'stopped';
