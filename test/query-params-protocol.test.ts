import assert from 'node:assert/strict';
import { after, before, beforeEach, describe, test } from 'node:test';
import { TetherClient } from '../src/index.js';

const URL = 'ws://example.test/tether';

type SentMessage = {
    type: string;
    location?: string;
    params?: Record<string, unknown>;
    query_key?: string;
    token?: string;
};

class FakeWebSocket {
    static CONNECTING = 0;
    static OPEN = 1;
    static CLOSING = 2;
    static CLOSED = 3;

    url: string;
    readyState = FakeWebSocket.CONNECTING;
    sent: string[] = [];
    onopen: (() => void) | null = null;
    onmessage: ((event: { data: string }) => void) | null = null;
    onclose: ((event: { code: number; reason: string; wasClean: boolean }) => void) | null = null;

    constructor(url: string) {
        this.url = url;
        sockets.push(this);
    }

    send(data: string) {
        if (this.readyState !== FakeWebSocket.OPEN) {
            throw new Error(`send while readyState=${this.readyState}`);
        }
        this.sent.push(String(data));
    }

    close() {
        this.readyState = FakeWebSocket.CLOSED;
        this.onclose?.({ code: 1000, reason: '', wasClean: true });
    }
}

const sockets: FakeWebSocket[] = [];

function parsed(socket: FakeWebSocket): SentMessage[] {
    return socket.sent.map((raw) => JSON.parse(raw) as SentMessage);
}

function connect(client: TetherClient) {
    client.connect(URL);
    const socket = sockets.at(-1);
    if (!socket) {
        throw new Error('expected a socket');
    }
    return socket;
}

function open(socket: FakeWebSocket) {
    socket.readyState = FakeWebSocket.OPEN;
    socket.onopen?.();
}

function queryMessage(queryKey: string, data: unknown, timestamp?: number) {
    return {
        data: JSON.stringify({
            type: 'query',
            query_key: queryKey,
            data,
            ...(timestamp !== undefined ? { timestamp } : {})
        })
    };
}

// encoding/json sorts map keys at every depth. Subscription identity on the
// server is the hash of that encoding, while query_key is echoed as sent.
function canonicalJSON(value: unknown): string {
    return JSON.stringify(sortKeys(value));
}

function sortKeys(value: unknown): unknown {
    if (Array.isArray(value)) {
        return value.map(sortKeys);
    }
    if (value !== null && typeof value === 'object') {
        const sorted: Record<string, unknown> = {};
        for (const key of Object.keys(value).sort()) {
            sorted[key] = sortKeys((value as Record<string, unknown>)[key]);
        }
        return sorted;
    }
    return value;
}

describe('query param protocol', { concurrency: 1 }, () => {
    const originalLog = console.log;
    const OriginalWebSocket = globalThis.WebSocket;

    before(() => {
        globalThis.WebSocket = FakeWebSocket as unknown as typeof WebSocket;
        console.log = () => {};
    });

    after(() => {
        globalThis.WebSocket = OriginalWebSocket;
        console.log = originalLog;
    });

    beforeEach(() => {
        sockets.length = 0;
    });

    test('nested key order shares one subscription and both listeners', () => {
        const client = new TetherClient();
        const socket = connect(client);
        open(socket);

        const firstSeen: unknown[] = [];
        const secondSeen: unknown[] = [];
        const reversed = {
            items: [{ b: 1, a: 2 }],
            filter: { b: 2, a: 1 },
            nested: { d: { z: 9, y: 8 }, c: 3 }
        };
        const forward = {
            nested: { c: 3, d: { y: 8, z: 9 } },
            filter: { a: 1, b: 2 },
            items: [{ a: 2, b: 1 }]
        };
        const canonical = {
            filter: { a: 1, b: 2 },
            items: [{ a: 2, b: 1 }],
            nested: { c: 3, d: { y: 8, z: 9 } }
        };
        const unsub1 = client.subscribe('q', reversed, (data) => {
            firstSeen.push(data);
        });
        const unsub2 = client.subscribe('q', forward, (data) => {
            secondSeen.push(data);
        });

        const subscribes = parsed(socket).filter((message) => message.type === 'subscribe');
        assert.equal(subscribes.length, 1);
        const subscribe = subscribes[0];
        const queryKey = subscribe?.query_key;
        assert.equal(typeof queryKey, 'string');
        assert.equal(canonicalJSON(subscribe?.params), canonicalJSON(reversed));
        assert.equal(queryKey, `q:${canonicalJSON(forward)}`);
        assert.equal(
            socket.sent.find((raw) => (JSON.parse(raw) as SentMessage).type === 'subscribe'),
            JSON.stringify({
                type: 'subscribe',
                location: 'q',
                params: canonical,
                query_key: queryKey
            })
        );

        socket.onmessage?.(queryMessage(queryKey!, { ok: true }));
        assert.deepEqual(firstSeen, [{ ok: true }]);
        assert.deepEqual(secondSeen, [{ ok: true }]);
        assert.deepEqual(client.getCache('q', reversed), { ok: true });
        assert.deepEqual(client.getCache('q', forward), { ok: true });

        unsub1();
        assert.equal(parsed(socket).filter((message) => message.type === 'unsubscribe').length, 0);

        socket.onmessage?.(queryMessage(queryKey!, { ok: 2 }));
        assert.deepEqual(firstSeen, [{ ok: true }]);
        assert.deepEqual(secondSeen, [{ ok: true }, { ok: 2 }]);

        unsub2();
        const unsubscribeFrames = socket.sent.filter((raw) => (JSON.parse(raw) as SentMessage).type === 'unsubscribe');
        assert.deepEqual(unsubscribeFrames, [
            JSON.stringify({
                type: 'unsubscribe',
                location: 'q',
                params: canonical,
                query_key: queryKey
            })
        ]);
        client.disconnect();
    });

    test('caller mutation after subscribe does not change reconnect or unsubscribe', () => {
        const client = new TetherClient();
        const socket = connect(client);
        open(socket);

        const params = {
            filter: { a: 1, b: 2 },
            items: [{ a: 2, b: 1 }],
            nested: { c: 3, d: { y: 8, z: 9 } }
        };
        const unsub = client.subscribe('q', params, () => {});
        const original = parsed(socket).find((message) => message.type === 'subscribe');
        assert.ok(original?.query_key);

        params.filter.a = 99;
        params.nested.d.z = 0;
        params.items[0].a = 5;
        params.items.push({ a: 1, b: 2 });

        const next = connect(client);
        open(next);
        const reconnectSub = parsed(next).find((message) => message.type === 'subscribe');
        assert.ok(reconnectSub);
        assert.equal(reconnectSub.query_key, original?.query_key);
        assert.equal(canonicalJSON(reconnectSub.params), canonicalJSON({
            filter: { a: 1, b: 2 },
            items: [{ a: 2, b: 1 }],
            nested: { c: 3, d: { y: 8, z: 9 } }
        }));
        assert.equal(
            next.sent.find((raw) => (JSON.parse(raw) as SentMessage).type === 'subscribe'),
            socket.sent.find((raw) => (JSON.parse(raw) as SentMessage).type === 'subscribe')
        );

        next.onmessage?.(queryMessage(original.query_key!, { ok: true }));
        assert.deepEqual(client.getCache('q', {
            filter: { b: 2, a: 1 },
            nested: { d: { z: 9, y: 8 }, c: 3 },
            items: [{ b: 1, a: 2 }]
        }), { ok: true });
        assert.equal(client.getCache('q', params), undefined);

        unsub();
        assert.equal(
            next.sent.find((raw) => (JSON.parse(raw) as SentMessage).type === 'unsubscribe'),
            JSON.stringify({
                type: 'unsubscribe',
                location: 'q',
                params: {
                    filter: { a: 1, b: 2 },
                    items: [{ a: 2, b: 1 }],
                    nested: { c: 3, d: { y: 8, z: 9 } }
                },
                query_key: original?.query_key
            })
        );
        client.disconnect();
    });

    test('drops a query result older than the cached timestamp', () => {
        const client = new TetherClient();
        const socket = connect(client);
        open(socket);
        const seen: unknown[] = [];
        const unsubscribe = client.subscribe('items', {}, (data) => {
            seen.push(data);
        });
        const queryKey = parsed(socket).find((message) => message.type === 'subscribe')?.query_key;
        assert.equal(typeof queryKey, 'string');

        socket.onmessage?.(queryMessage(queryKey!, { value: 'current' }, 20));
        socket.onmessage?.(queryMessage(queryKey!, { value: 'stale' }, 10));
        socket.onmessage?.(queryMessage(queryKey!, { value: 'same' }, 20));
        socket.onmessage?.(queryMessage(queryKey!, { value: 'next' }, 21));

        assert.deepEqual(seen, [
            { value: 'current' },
            { value: 'same' },
            { value: 'next' }
        ]);
        assert.deepEqual(client.getCache('items', {}), { value: 'next' });

        unsubscribe();
        const resubscribe = client.subscribe('items', {}, () => {});
        socket.onmessage?.(queryMessage(queryKey!, { value: 'fresh' }, 1));
        assert.deepEqual(client.getCache('items', {}), { value: 'fresh' });
        resubscribe();
        client.disconnect();
    });
});
