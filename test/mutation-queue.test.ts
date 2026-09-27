import assert from 'node:assert/strict';
import { after, afterEach, before, beforeEach, describe, mock, test } from 'node:test';
import { TetherClient } from '../src/index.js';

const MUTATION_TIMEOUT_MS = 10000;
const URL = 'ws://example.test/tether';

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

function mutations(socket: FakeWebSocket): SentMessage[] {
    return parsed(socket).filter((message) => message.type === 'mutation');
}

function rejectionOf(promise: Promise<unknown>): Promise<Error> {
    return promise.then(
        () => {
            throw new Error('expected mutation to reject');
        },
        (error: unknown) => {
            if (!(error instanceof Error)) {
                throw new Error('expected an Error rejection');
            }
            return error;
        }
    );
}

function connect(client = new TetherClient()) {
    client.connect(URL);
    const socket = sockets.at(-1);
    if (!socket) {
        throw new Error('expected a socket');
    }
    assert.equal(socket.readyState, FakeWebSocket.CONNECTING);
    return { client, socket };
}

function open(socket: FakeWebSocket) {
    socket.readyState = FakeWebSocket.OPEN;
    const onopen = socket.onopen;
    if (!onopen) {
        throw new Error('expected onopen');
    }
    onopen();
}

describe('queued mutations', { concurrency: 1 }, () => {
    const originalLog = console.log;
    const OriginalWebSocket = globalThis.WebSocket;

    before(() => {
        globalThis.WebSocket = FakeWebSocket as unknown as typeof WebSocket;
        console.log = () => {};
    });

    after(() => {
        globalThis.WebSocket = OriginalWebSocket;
        console.log = originalLog;
        mock.timers.reset();
    });

    beforeEach(() => {
        sockets.length = 0;
    });

    afterEach(() => {
        mock.timers.reset();
    });

    test('does not flush a mutation after its timeout once the socket opens', async () => {
        mock.timers.enable({ apis: ['setTimeout'] });
        const { client, socket } = connect();
        const settled = rejectionOf(client.sendMutation('charge', { amount: 1 }));

        mock.timers.tick(MUTATION_TIMEOUT_MS);
        open(socket);

        const error = await settled;
        assert.equal(error.message, 'Mutation timeout');
        assert.ok(parsed(socket).some((message) => message.type === 'auth'));
        assert.deepEqual(mutations(socket), []);
    });

    test('does not flush a mutation that timed out before connect', async () => {
        mock.timers.enable({ apis: ['setTimeout'] });
        const client = new TetherClient();
        const settled = rejectionOf(client.sendMutation('charge', { amount: 1 }));

        mock.timers.tick(MUTATION_TIMEOUT_MS);
        const { socket } = connect(client);
        open(socket);

        const error = await settled;
        assert.equal(error.message, 'Mutation timeout');
        assert.ok(parsed(socket).some((message) => message.type === 'auth'));
        assert.deepEqual(mutations(socket), []);
    });

    test('sends a retry after timeout and not the timed-out mutation', async () => {
        mock.timers.enable({ apis: ['setTimeout'] });
        const { client, socket } = connect();
        const first = rejectionOf(client.sendMutation('charge', { amount: 1 }));

        mock.timers.tick(MUTATION_TIMEOUT_MS);
        const second = client.sendMutation('charge', { amount: 2 });
        open(socket);

        assert.equal((await first).message, 'Mutation timeout');
        assert.deepEqual(mutations(socket).map((message) => message.params), [{ amount: 2 }]);

        const secondSettled = rejectionOf(second);
        client.disconnect();
        assert.equal((await secondSettled).message, 'Connection closed');
    });

    test('flushes a queued mutation that is still inside its timeout', async () => {
        mock.timers.enable({ apis: ['setTimeout'] });
        const { client, socket } = connect();
        const pending = client.sendMutation('charge', { amount: 1 });

        mock.timers.tick(MUTATION_TIMEOUT_MS - 1);
        open(socket);

        const sent = mutations(socket);
        assert.equal(sent.length, 1);
        assert.equal(sent[0].location, 'charge');
        assert.deepEqual(sent[0].params, { amount: 1 });
        assert.equal(typeof sent[0].mutation_id, 'string');

        socket.onmessage?.({
            data: JSON.stringify({
                type: 'mutation',
                mutation_id: sent[0].mutation_id,
                data: { ok: true }
            })
        });
        assert.deepEqual(await pending, { ok: true });
        assert.equal(mutations(socket).length, 1);
    });

    test('skips a queued mutation whose deadline has passed before the timeout callback', async () => {
        mock.timers.enable({ apis: ['setTimeout', 'Date'], now: 0 });
        const { client, socket } = connect();
        const settled = rejectionOf(client.sendMutation('charge', { amount: 1 }));

        mock.timers.setTime(MUTATION_TIMEOUT_MS);
        open(socket);

        assert.ok(parsed(socket).some((message) => message.type === 'auth'));
        assert.deepEqual(mutations(socket), []);

        mock.timers.tick(0);
        assert.equal((await settled).message, 'Mutation timeout');
        assert.deepEqual(mutations(socket), []);
    });

    test('times out an already sent mutation without sending it again', async () => {
        mock.timers.enable({ apis: ['setTimeout'] });
        const { client, socket } = connect();
        open(socket);

        const settled = rejectionOf(client.sendMutation('charge', { amount: 1 }));
        assert.equal(mutations(socket).length, 1);

        mock.timers.tick(MUTATION_TIMEOUT_MS);
        assert.equal((await settled).message, 'Mutation timeout');
        assert.equal(mutations(socket).length, 1);
    });

    test('keeps queued subscribe and unsubscribe frames when a mutation times out', async () => {
        mock.timers.enable({ apis: ['setTimeout'] });
        const { client, socket } = connect();
        const unsubscribe = client.subscribe('items', {}, () => {});
        const settled = rejectionOf(client.sendMutation('charge', { amount: 1 }));
        unsubscribe();

        mock.timers.tick(MUTATION_TIMEOUT_MS);
        open(socket);

        assert.equal((await settled).message, 'Mutation timeout');
        assert.deepEqual(parsed(socket).map((message) => message.type), [
            'auth',
            'subscribe',
            'unsubscribe'
        ]);
    });

    test('does not replay a queued mutation after disconnect', async () => {
        mock.timers.enable({ apis: ['setTimeout'] });
        const { client, socket } = connect();
        const settled = rejectionOf(client.sendMutation('charge', { amount: 1 }));

        client.disconnect();
        assert.equal((await settled).message, 'Connection closed');
        assert.deepEqual(mutations(socket), []);

        const next = connect(client);
        open(next.socket);
        assert.ok(parsed(next.socket).some((message) => message.type === 'auth'));
        assert.deepEqual(mutations(next.socket), []);
    });

    test('logout drops cached query data and tells subscribers to drop it', () => {
        const { client, socket } = connect();
        open(socket);
        const seen: unknown[] = [];
        client.subscribe('items', {}, (data) => {
            seen.push(data);
        });
        const queryKey = parsed(socket).find((message) => message.type === 'subscribe')?.query_key;
        assert.equal(typeof queryKey, 'string');

        socket.onmessage?.({
            data: JSON.stringify({
                type: 'query',
                query_key: queryKey,
                data: { secret: 'prior-user' }
            })
        });
        assert.deepEqual(client.getCache('items', {}), { secret: 'prior-user' });

        client.setToken('prior-user-token');
        client.logout();

        assert.equal(client.getCache('items', {}), undefined);
        assert.deepEqual(seen, [{ secret: 'prior-user' }, undefined]);
        const auths = parsed(socket).filter((message) => message.type === 'auth');
        assert.equal(auths.at(-1)?.token, '');

        socket.onmessage?.({
            data: JSON.stringify({
                type: 'query',
                query_key: queryKey,
                data: { secret: 'next-user' }
            })
        });
        assert.deepEqual(seen.at(-1), { secret: 'next-user' });
        assert.deepEqual(client.getCache('items', {}), { secret: 'next-user' });
    });

    test('logout does not send the prior user token or queued mutations', async () => {
        mock.timers.enable({ apis: ['setTimeout'] });
        const client = new TetherClient();
        client.setToken('prior-user-token');
        client.subscribe('items', { id: 1 }, () => {});
        const settled = rejectionOf(client.sendMutation('charge', { amount: 1 }));

        client.logout();
        const { socket } = connect(client);
        open(socket);

        assert.equal((await settled).message, 'Logged out');
        const messages = parsed(socket);
        assert.ok(messages.some((message) => message.type === 'subscribe'));
        assert.ok(messages.some((message) => message.type === 'auth'));
        assert.ok(messages.filter((message) => message.type === 'auth').every((message) => message.token === ''));
        assert.deepEqual(mutations(socket), []);
    });
});
