import { connectionConfigFromEnv } from '../src/env';
import { mkdtempSync, writeFileSync, rmSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';

test('configures a dedicated vhost and defaults to the TLS port', () => {
    const connection = connectionConfigFromEnv({ RABBITMQ_HOST: 'rabbitmq', RABBITMQ_PROTOCOL: 'amqps', RABBITMQ_VHOST: 'milo-dev' });
    expect(connection).toMatchObject({ options: { protocol: 'amqps', hostname: 'rabbitmq', port: 5671, vhost: 'milo-dev' } });
});

test('loads the CA for URL connections and retains certificate verification', () => {
    const dir = mkdtempSync(join(tmpdir(), 'milo-amqp-test-'));
    try {
        const file = join(dir, 'ca.pem');
        writeFileSync(file, 'public CA fixture');
        const config = connectionConfigFromEnv({ RABBITMQ_URL: 'amqps://rabbitmq:5671/milo-dev', RABBITMQ_CA_FILE: file });
        expect(config).toEqual({ url: 'amqps://rabbitmq:5671/milo-dev', socketOptions: { ca: Buffer.from('public CA fixture'), rejectUnauthorized: true } });
        expect(() => connectionConfigFromEnv({ RABBITMQ_CA_FILE: join(dir, 'missing') })).toThrow();
    } finally { rmSync(dir, { recursive: true }); }
});
