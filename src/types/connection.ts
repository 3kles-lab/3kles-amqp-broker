import { Options } from 'amqplib';
import { Logger } from 'pino';

export type AmqpConnectionConfig =
    | {
          url: string;
          socketOptions?: unknown;
      }
    | {
          options: Options.Connect;
          socketOptions?: unknown;
      };

export interface ConnectionClientProperties {
    service?: string;
    env?: string;
    extras?: Record<string, string>;
}

export interface ConnectionManagerConfig {
    connection: AmqpConnectionConfig;
    reconnectDelayMs?: number;
    logger?: Logger;
    clientProperties?: ConnectionClientProperties;
    enableGracefulShutdown?: boolean;
}

export type ConnectionState = 'idle' | 'connecting' | 'connected' | 'reconnecting' | 'disconnecting' | 'disconnected';
