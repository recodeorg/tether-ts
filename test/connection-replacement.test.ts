import assert from 'node:assert/strict';
import { after, afterEach, before, beforeEach, describe, test } from 'node:test';
import { TetherClient } from '../src/index.js';

const URL_A = 'ws://a.example/tether';
const URL_B = 'ws://b.example/tether';
// First retry delay is backoff / 2 when Math.random() is 0. backoff starts at 1000ms.
const FIRST_RETRY_DELAY_MS = 500;

type SentMessage = {
    type: string;
    location?: string;
    params?: Record<string, unknown>;
    mutation_id?: string;
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
    onerror: (() => void) | null = null;

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

function connect(client: TetherClient, url = URL_A) {
    client.connect(url);
    const socket = sockets.at(-1);
    if (!socket) {
        throw new Error('expected a socket');
    }
    assert.equal(socket.url, url);
    assert.equal(socket.readyState, FakeWebSocket.CONNECTING);
    return socket;
}

function open(socket: FakeWebSocket) {
    socket.readyState = FakeWebSocket.OPEN;
    const onopen = socket.onopen;
    if (!onopen) {
        throw new Error('expected onopen');
    }
    onopen();
}

function closeFromServer(socket: FakeWebSocket) {
    socket.readyState = FakeWebSocket.CLOSED;
    const onclose = socket.onclose;
    if (!onclose) {
        throw new Error('expected onclose');
    }
    onclose({ code: 1006, reason: '', wasClean: false });
}

function queryKeyOf(socket: FakeWebSocket): string {
    const queryKey = parsed(socket).find((message) => message.type === 'subscribe')?.query_key;
    if (typeof queryKey !== 'string') {
        throw new Error('expected query_key');
    }
    return queryKey;
}

function queryMessage(queryKey: string, data: unknown) {
    return {
        data: JSON.stringify({
            type: 'query',
            query_key: queryKey,
            data
        })
    };
}

describe('connection replacement', { concurrency: 1 }, () => {
    const originalLog = console.log;
    const originalRandom = Math.random;
    const OriginalWebSocket = globalThis.WebSocket;

    before(() => {
        globalThis.WebSocket = FakeWebSocket as unknown as typeof WebSocket;
        console.log = () => {};
    });

    after(() => {
        globalThis.WebSocket = OriginalWebSocket;
        console.log = originalLog;
        Math.random = originalRandom;
    });

    beforeEach(() => {
        sockets.length = 0;
        Math.random = () => 0;
    });

    afterEach(() => {
        Math.random = originalRandom;
    });

    test('duplicate connect closes the previous socket and ignores its open and message', () => {
        const client = new TetherClient();
        const first = connect(client, URL_A);
        const staleOpen = first.onopen;
        const staleMessage = first.onmessage;
        open(first);
        const seen: unknown[] = [];
        client.subscribe('items', {}, (data) => {
            seen.push(data);
        });
        const queryKey = queryKeyOf(first);
        const sentOnFirst = first.sent.length;

        const second = connect(client, URL_A);
        assert.equal(sockets.length, 2);
        assert.equal(first.readyState, FakeWebSocket.CLOSED);
        assert.equal(first.sent.length, sentOnFirst);

        staleOpen?.();
        staleMessage?.(queryMessage(queryKey, { value: 'stale' }));
        assert.equal(second.sent.length, 0);
        assert.equal(client.getCache('items', {}), undefined);

        open(second);
        assert.deepEqual(parsed(second).map((message) => message.type), ['auth', 'subscribe']);

        second.onmessage?.(queryMessage(queryKey, { value: 'current' }));
        staleOpen?.();
        staleMessage?.(queryMessage(queryKey, { value: 'stale' }));

        assert.deepEqual(client.getCache('items', {}), { value: 'current' });
        assert.deepEqual(seen, [{ value: 'current' }]);
        assert.equal(parsed(second).filter((message) => message.type === 'auth').length, 1);
        assert.equal(first.readyState, FakeWebSocket.CLOSED);
        assert.equal(second.readyState, FakeWebSocket.OPEN);
        assert.equal(first.sent.length, sentOnFirst);

        client.disconnect();
        assert.equal(first.readyState, FakeWebSocket.CLOSED);
        assert.equal(second.readyState, FakeWebSocket.CLOSED);
    });

    test('endpoint change keeps the new query value when the old socket responds late', () => {
        const client = new TetherClient();
        const socketA = connect(client, URL_A);
        const staleOpen = socketA.onopen;
        const staleMessage = socketA.onmessage;
        open(socketA);
        const seen: unknown[] = [];
        client.subscribe('items', {}, (data) => {
            seen.push(data);
        });
        const queryKey = queryKeyOf(socketA);

        const socketB = connect(client, URL_B);
        assert.equal(socketA.readyState, FakeWebSocket.CLOSED);
        assert.notEqual(socketA.url, socketB.url);

        open(socketB);
        const subscribeB = parsed(socketB).find((message) => message.type === 'subscribe');
        assert.equal(subscribeB?.query_key, queryKey);

        socketB.onmessage?.(queryMessage(queryKey, { value: 'from-b' }));
        assert.deepEqual(client.getCache('items', {}), { value: 'from-b' });

        staleOpen?.();
        staleMessage?.(queryMessage(queryKey, { value: 'from-a' }));

        assert.deepEqual(client.getCache('items', {}), { value: 'from-b' });
        assert.deepEqual(seen, [{ value: 'from-b' }]);
        assert.equal(socketA.readyState, FakeWebSocket.CLOSED);
        assert.equal(socketB.readyState, FakeWebSocket.OPEN);
        assert.equal(parsed(socketB).filter((message) => message.type === 'auth').length, 1);
    });

    test('manual connect cancels a pending reconnect', (t) => {
        t.mock.timers.enable({ apis: ['setTimeout'] });
        const client = new TetherClient();
        const first = connect(client, URL_A);
        open(first);
        closeFromServer(first);
        assert.equal(sockets.length, 1);

        const manual = connect(client, URL_B);
        open(manual);

        t.mock.timers.tick(FIRST_RETRY_DELAY_MS);
        t.mock.timers.tick(30_000);
        assert.equal(sockets.length, 2);
        assert.equal(manual.readyState, FakeWebSocket.OPEN);
        assert.equal(manual.url, URL_B);

        client.subscribe('items', {}, () => {});
        assert.equal(parsed(manual).filter((message) => message.type === 'subscribe').length, 1);
        assert.equal(sockets.length, 2);
        assert.ok(sockets.every((socket) => socket === first || socket === manual));
    });

    test('reconnects after the server closes the socket and ignores that socket afterwards', (t) => {
        t.mock.timers.enable({ apis: ['setTimeout'] });
        const client = new TetherClient();
        const first = connect(client, URL_A);
        const staleMessage = first.onmessage;
        open(first);
        client.subscribe('items', {}, () => {});
        const queryKey = queryKeyOf(first);

        closeFromServer(first);
        staleMessage?.(queryMessage(queryKey, { value: 'after-close' }));
        assert.equal(client.getCache('items', {}), undefined);

        t.mock.timers.tick(FIRST_RETRY_DELAY_MS - 1);
        assert.equal(sockets.length, 1);
        t.mock.timers.tick(1);
        assert.equal(sockets.length, 2);

        const next = sockets[1];
        assert.equal(next?.url, URL_A);
        assert.equal(next?.readyState, FakeWebSocket.CONNECTING);
        open(next!);
        assert.ok(parsed(next!).some((message) => message.type === 'auth'));
        assert.ok(parsed(next!).some((message) => message.type === 'subscribe' && message.query_key === queryKey));

        next!.onmessage?.(queryMessage(queryKey, { value: 'fresh' }));
        staleMessage?.(queryMessage(queryKey, { value: 'stale' }));
        assert.deepEqual(client.getCache('items', {}), { value: 'fresh' });
        assert.equal(first.readyState, FakeWebSocket.CLOSED);
        assert.equal(next!.readyState, FakeWebSocket.OPEN);
    });

    test('duplicate connect keeps a queued mutation for the replacement socket', async (t) => {
        t.mock.timers.enable({ apis: ['setTimeout'] });
        const client = new TetherClient();
        const first = connect(client);
        const pending = client.sendMutation('charge', { amount: 1 });

        const second = connect(client);
        assert.equal(first.readyState, FakeWebSocket.CLOSED);
        open(second);

        const sent = parsed(second).filter((message) => message.type === 'mutation');
        assert.deepEqual(sent.map((message) => message.params), [{ amount: 1 }]);
        second.onmessage?.({
            data: JSON.stringify({
                type: 'mutation',
                mutation_id: sent[0].mutation_id,
                data: { ok: true }
            })
        });
        assert.deepEqual(await pending, { ok: true });
    });
});
