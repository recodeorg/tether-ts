import assert from 'node:assert/strict';
import { after, afterEach, before, beforeEach, describe, mock, test } from 'node:test';
import { TetherClient, TetherError, type AuthState } from '../src/index.js';

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

function connect(client = new TetherClient()) {
    client.connect(URL);
    const socket = sockets.at(-1);
    if (!socket) {
        throw new Error('expected a socket');
    }
    return { client, socket };
}

function open(socket: FakeWebSocket) {
    socket.readyState = FakeWebSocket.OPEN;
    socket.onopen?.();
}

function deliver(socket: FakeWebSocket, frame: string) {
    socket.onmessage?.({ data: frame });
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

// encoding/json sorts map keys. The hand-written auth and subscribe failures
// keep the spacing in the server's raw bytes.
function mutationError(mutationId: string) {
    return JSON.stringify({
        error: 'Failed to execute mutation',
        mutation: 'missing',
        mutation_id: mutationId,
        params: { password: 'secret' },
        type: 'error'
    });
}

function queryError(queryKey: string) {
    return JSON.stringify({
        error: 'Failed to execute query',
        params: { password: 'secret' },
        query_key: queryKey,
        type: 'error'
    });
}

const loggedOut: AuthState = { authenticated: false, userId: null, error: null };

describe('server error envelopes', { concurrency: 1 }, () => {
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

    test('rejects a pending mutation with the server error and clears the timeout', async () => {
        mock.timers.enable({ apis: ['setTimeout'] });
        const { client, socket } = connect();
        open(socket);
        const pending = client.sendMutation('missing', { password: 'secret' });
        const mutationId = parsed(socket).find((message) => message.type === 'mutation')?.mutation_id;
        assert.equal(typeof mutationId, 'string');

        const settled = rejectionOf(pending);
        deliver(socket, mutationError(mutationId!));
        const error = await settled;

        assert.ok(error instanceof TetherError);
        assert.equal(error.message, 'Failed to execute mutation');
        mock.timers.tick(MUTATION_TIMEOUT_MS);
        assert.equal(error.message, 'Failed to execute mutation');

        let resolved = false;
        pending.then(() => {
            resolved = true;
        }, () => {});
        deliver(socket, JSON.stringify({
            type: 'mutation',
            mutation_id: mutationId,
            data: { ok: true }
        }));
        await Promise.resolve();
        assert.equal(resolved, false);
        client.disconnect();
    });

    test('records a query error and clears it when a result arrives', () => {
        const { client, socket } = connect();
        open(socket);
        const seen: Array<{ data: unknown; error: Error | null }> = [];
        client.subscribe('items', { password: 'secret' }, (data, error) => {
            seen.push({ data, error });
        });
        const queryKey = parsed(socket).find((message) => message.type === 'subscribe')?.query_key;
        assert.equal(typeof queryKey, 'string');

        deliver(socket, queryError(queryKey!));
        const failure = client.getError('items', { password: 'secret' });
        assert.ok(failure instanceof TetherError);
        assert.equal(failure?.message, 'Failed to execute query');
        assert.equal(client.getCache('items', { password: 'secret' }), undefined);
        assert.equal(seen.length, 1);
        assert.equal(seen[0]?.data, undefined);
        assert.equal(seen[0]?.error, failure);

        const later = client.subscribe('items', { password: 'secret' }, (data, error) => {
            seen.push({ data, error });
        });
        assert.equal(seen.length, 2);
        assert.equal(seen[1]?.error, failure);

        deliver(socket, JSON.stringify({
            type: 'query',
            query_key: queryKey,
            data: { ok: true },
            timestamp: 1
        }));
        assert.equal(client.getError('items', { password: 'secret' }), undefined);
        assert.deepEqual(client.getCache('items', { password: 'secret' }), { ok: true });
        assert.equal(seen.at(-1)?.error, null);
        assert.deepEqual(seen.at(-1)?.data, { ok: true });

        later();
        client.disconnect();
    });

    test('a query error keeps the last result and does not block other listeners', () => {
        const { client, socket } = connect();
        open(socket);
        const seen: unknown[] = [];
        const logged: unknown[] = [];
        const originalError = console.error;
        console.error = (...args: unknown[]) => {
            logged.push(args[0]);
        };
        client.subscribe('items', {}, () => {
            throw new Error('subscriber failed');
        });
        client.subscribe('items', {}, (data, error) => {
            seen.push(error ?? data);
        });
        const queryKey = parsed(socket).find((message) => message.type === 'subscribe')?.query_key;
        assert.equal(typeof queryKey, 'string');
        try {
            deliver(socket, JSON.stringify({
                type: 'query',
                query_key: queryKey,
                data: { ok: true }
            }));
            deliver(socket, queryError(queryKey!));
        } finally {
            console.error = originalError;
        }

        const failure = client.getError('items', {});
        assert.equal(failure?.message, 'Failed to execute query');
        assert.deepEqual(client.getCache('items', {}), { ok: true });
        assert.deepEqual(seen, [{ ok: true }, failure]);
        assert.deepEqual(logged, [
            'Tether: Listener threw an exception during update:',
            'Tether: Listener threw an exception during query error:'
        ]);
        client.disconnect();
    });

    test('ignores a query error for a subscription that is already gone', () => {
        const { client, socket } = connect();
        open(socket);
        const unsubscribe = client.subscribe('items', {}, () => {});
        const queryKey = parsed(socket).find((message) => message.type === 'subscribe')?.query_key;
        unsubscribe();
        deliver(socket, queryError(queryKey!));
        assert.equal(client.getError('items', {}), undefined);
        client.disconnect();
    });

    test('auth success and token failure publish auth state', () => {
        const { client, socket } = connect();
        const states: AuthState[] = [];
        const initial = client.getAuthState();
        assert.equal(client.getAuthState(), initial);
        const stop = client.onAuthentication((state) => {
            states.push(state);
        });
        assert.deepEqual(states, [loggedOut]);
        assert.equal(states[0], initial);
        open(socket);

        deliver(socket, '{"type": "error", "error": "Failed to get user ID"}');
        assert.deepEqual(states, [loggedOut]);
        assert.deepEqual(client.getAuthState(), loggedOut);

        client.setToken('bad-token');
        deliver(socket, '{"type": "error", "error": "Failed to get user ID"}');
        assert.deepEqual(states.at(-1), {
            authenticated: false,
            userId: null,
            error: 'Failed to get user ID'
        });

        client.setToken('good-token');
        deliver(socket, '{"data":{"user_id":"user-1"},"success":true,"type":"auth"}');
        assert.deepEqual(client.getAuthState(), {
            authenticated: true,
            userId: 'user-1',
            error: null
        });
        assert.deepEqual(states.at(-1), client.getAuthState());

        deliver(socket, '{"type": "error", "error": "Failed to encode auth message"}');
        assert.deepEqual(client.getAuthState(), {
            authenticated: false,
            userId: null,
            error: 'Failed to encode auth message'
        });

        const published = states.length;
        stop();
        client.setToken('later-token');
        deliver(socket, '{"data":{"user_id":"user-2"},"success":true,"type":"auth"}');
        assert.equal(states.length, published);
        assert.equal(client.getAuthState().userId, 'user-2');

        const current: AuthState[] = [];
        client.onAuthentication((state) => {
            current.push(state);
        });
        assert.deepEqual(current, [client.getAuthState()]);
        client.disconnect();
    });

    test('logout and disconnect publish the logged-out state', () => {
        const { client, socket } = connect();
        open(socket);
        const states: AuthState[] = [];
        client.onAuthentication((state) => {
            states.push(state);
        });
        deliver(socket, '{"data":{"user_id":"user-1"},"success":true,"type":"auth"}');
        assert.equal(states.at(-1)?.authenticated, true);

        client.disconnect();
        assert.deepEqual(states.at(-1), loggedOut);
        assert.deepEqual(client.getAuthState(), loggedOut);

        const next = connect(client);
        open(next.socket);
        deliver(next.socket, '{"data":{"user_id":"user-1"},"success":true,"type":"auth"}');
        client.logout();
        assert.deepEqual(client.getAuthState(), loggedOut);
        assert.deepEqual(states.at(-1), loggedOut);
    });

    test('a throwing auth listener does not block the others', () => {
        const { client, socket } = connect();
        open(socket);
        const seen: AuthState[] = [];
        const logged: unknown[] = [];
        const originalError = console.error;
        console.error = (message: unknown) => {
            logged.push(message);
        };
        const stopThrowing = client.onAuthentication(() => {
            throw new Error('auth listener failed');
        });
        const stopWatching = client.onAuthentication((state) => {
            seen.push(state);
        });
        try {
            deliver(socket, '{"data":{"user_id":"user-1"},"success":true,"type":"auth"}');
            assert.deepEqual(logged, [
                'Tether: Authentication listener threw an exception:',
                'Tether: Authentication listener threw an exception:'
            ]);
            assert.deepEqual(seen.at(-1), {
                authenticated: true,
                userId: 'user-1',
                error: null
            });
        } finally {
            stopThrowing();
            stopWatching();
            console.error = originalError;
        }
        client.disconnect();
    });

    test('an auth failure does not settle an in-flight mutation', async () => {
        const { client, socket } = connect();
        open(socket);
        client.setToken('bad-token');
        const pending = client.sendMutation('charge', { amount: 1 });
        const mutationId = parsed(socket).find((message) => message.type === 'mutation')?.mutation_id;
        deliver(socket, '{"type": "error", "error": "Failed to get user ID"}');
        assert.deepEqual(client.getAuthState(), {
            authenticated: false,
            userId: null,
            error: 'Failed to get user ID'
        });

        let settled = false;
        pending.then(() => {
            settled = true;
        }, () => {
            settled = true;
        });
        await Promise.resolve();
        assert.equal(settled, false);

        deliver(socket, JSON.stringify({
            type: 'mutation',
            mutation_id: mutationId,
            data: { ok: true }
        }));
        assert.deepEqual(await pending, { ok: true });
        client.disconnect();
    });

    test('logs error frames that are not tied to an operation', () => {
        const { client, socket } = connect();
        open(socket);
        client.subscribe('items', {}, () => {});
        client.setToken('good-token');
        deliver(socket, '{"data":{"user_id":"user-1"},"success":true,"type":"auth"}');
        const logged: unknown[][] = [];
        const originalError = console.error;
        console.error = (...args: unknown[]) => {
            logged.push(args);
        };
        try {
            deliver(socket, '{"type": "error", "error": "Invalid message"}');
            deliver(socket, '{"type": "error", "error": "Failed to subscribe to query"}');
        } finally {
            console.error = originalError;
        }
        assert.deepEqual(logged, [
            ['Tether:', 'Invalid message'],
            ['Tether:', 'Failed to subscribe to query']
        ]);
        assert.deepEqual(client.getAuthState(), {
            authenticated: true,
            userId: 'user-1',
            error: null
        });
        assert.equal(client.getError('items', {}), undefined);
        client.disconnect();
    });
});
