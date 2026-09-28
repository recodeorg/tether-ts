import { WebSocketHandler } from './utils/websocket.js';

export class TetherError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'TetherError';
    }
}

export type AuthState = {
    authenticated: boolean;
    userId: string | null;
    error: string | null;
};

type AuthListener = (state: AuthState) => void;

type PendingMutation = {
    resolve: (value: unknown) => void;
    reject: (reason: Error) => void;
    timeoutId: ReturnType<typeof setTimeout>;
};

const MUTATION_TIMEOUT_MS = 10000;

type Listener = (data: any, error: Error | null) => void;

// Auth failures are type "error" and carry no mutation_id or query_key.
const AUTH_FAILURES = new Set([
    'Failed to get user ID',
    'Failed to encode auth message',
]);

type ActiveQuery = {
    queryName: string;
    params: Record<string, unknown>;
    queryKey: string;
};

export class TetherClient {
    private websocketHandler: WebSocketHandler = new WebSocketHandler();
    private pendingMutations = new Map<string, PendingMutation>();
    private token: string | null = null;
    private authenticated: boolean = false;
    private userInfo: Map<string, any> = new Map();
    private queryCache = new Map<string, any>();
    private queryErrors = new Map<string, TetherError>();
    private queryTimestamps = new Map<string, number>();
    private listeners = new Map<string, Set<Listener>>();
    private activeQueries = new Map<string, ActiveQuery>();
    private authListeners = new Set<AuthListener>();
    private authError: string | null = null;
    private lastAuthState: AuthState = { authenticated: false, userId: null, error: null };

    constructor() {
        this.websocketHandler.shouldSendQueuedMutation = (mutationId) => {
            return this.pendingMutations.has(mutationId);
        };
    }

    private normalizeParams = (params: unknown): Record<string, unknown> => {
        if (params == null || typeof params !== 'object' || Array.isArray(params)) {
            return {};
        }
        return { ...(params as Record<string, unknown>) };
    };

    private canonicalizeParams = (params: any): any => {
        if (params === null || typeof params !== 'object') {
            return params;
        }
        if (Array.isArray(params)) {
            return params.map(this.canonicalizeParams);
        }
        
        const sortedKeys = Object.keys(params).sort();
        const result: Record<string, any> = {};
        for (const key of sortedKeys) {
            result[key] = this.canonicalizeParams(params[key]);
        }
        return result;
    }

    // Detached recursive sort. The key and every later frame share this object.
    private snapshotParams = (params: unknown): Record<string, unknown> => {
        return this.canonicalizeParams(this.normalizeParams(params));
    };

    private readTimestamp = (timestamp: unknown): number | undefined => {
        if (typeof timestamp !== 'number' || !Number.isFinite(timestamp)) {
            return undefined;
        }
        return timestamp;
    };

    private getCacheKey = (queryName: string, params: Record<string, unknown>) => {
        return `${queryName}:${JSON.stringify(this.canonicalizeParams(params))}`;
    };

    private sendSubscribe = (query: ActiveQuery) => {
        this.websocketHandler.send(JSON.stringify({
            type: 'subscribe',
            location: query.queryName,
            params: query.params,
            query_key: query.queryKey
        }));
    };

    private callListener = (callback: Listener, data: unknown, error: Error | null, when: string) => {
        try {
            callback(data, error);
        } catch (thrown) {
            console.error(`Tether: Listener threw an exception during ${when}:`, thrown);
        }
    };

    private deliver = (queryKey: string, data: unknown, error: Error | null, when: string) => {
        const subs = this.listeners.get(queryKey);
        if (!subs) {
            return;
        }
        for (const callback of [...subs]) {
            this.callListener(callback, data, error, when);
        }
    };

    private readUserId = (): string | null => {
        const userId = this.userInfo.get('user_id');
        return typeof userId === 'string' && userId.length > 0 ? userId : null;
    };

    private currentAuthState = (): AuthState => {
        return {
            authenticated: this.authenticated,
            userId: this.readUserId(),
            error: this.authError,
        };
    };

    // Stable until the state changes, so a hook can use this as a snapshot.
    getAuthState = (): AuthState => {
        return this.lastAuthState;
    };

    private emitAuth = () => {
        const state = this.currentAuthState();
        if (
            this.lastAuthState.authenticated === state.authenticated &&
            this.lastAuthState.userId === state.userId &&
            this.lastAuthState.error === state.error
        ) {
            return;
        }
        this.lastAuthState = state;
        for (const listener of [...this.authListeners]) {
            this.callAuthListener(listener, state);
        }
    };

    private callAuthListener = (listener: AuthListener, state: AuthState) => {
        try {
            listener(state);
        } catch (error) {
            console.error('Tether: Authentication listener threw an exception:', error);
        }
    };

    private rememberUserId = (userId: unknown) => {
        if (typeof userId === 'string' && userId.length > 0) {
            this.userInfo.set('user_id', userId);
            return;
        }
        this.userInfo.delete('user_id');
    };

    // Execution failures carry mutation_id or query_key. Auth failures do not.
    // Params stay off the console: error frames can echo passwords and tokens.
    private handleServerError = (message: { error?: unknown; mutation_id?: unknown; query_key?: unknown }) => {
        const errorText = typeof message.error === 'string' && message.error.length > 0
            ? message.error
            : 'Unknown error';
        if (typeof message.mutation_id === 'string' && message.mutation_id.length > 0) {
            this.takePendingMutation(message.mutation_id)?.reject(new TetherError(errorText));
            return;
        }
        if (typeof message.query_key === 'string' && message.query_key.length > 0) {
            this.failQuery(message.query_key, errorText);
            return;
        }
        if (AUTH_FAILURES.has(errorText)) {
            // An empty token is the anonymous handshake sent on every open.
            // Rejecting it leaves the client logged out. A stored token that
            // the server rejects is a failed authentication.
            this.authenticated = false;
            this.userInfo.delete('user_id');
            this.authError = this.token ? errorText : null;
            this.emitAuth();
            return;
        }
        console.error('Tether:', errorText);
    };

    private failQuery = (queryKey: string, errorText: string) => {
        if (!this.activeQueries.has(queryKey)) {
            return;
        }
        const error = new TetherError(errorText);
        this.queryErrors.set(queryKey, error);
        this.deliver(queryKey, this.queryCache.get(queryKey), error, 'query error');
    };

    private takePendingMutation = (mutationId: string): PendingMutation | undefined => {
        const pending = this.pendingMutations.get(mutationId);
        if (!pending) {
            return undefined;
        }
        clearTimeout(pending.timeoutId);
        this.pendingMutations.delete(mutationId);
        this.websocketHandler.dropQueuedMutation(mutationId);
        return pending;
    };

    getCache = (queryName: string, params: any) => {
        return this.queryCache.get(this.getCacheKey(queryName, this.snapshotParams(params)));
    };

    getError = (queryName: string, params: any): TetherError | undefined => {
        return this.queryErrors.get(this.getCacheKey(queryName, this.snapshotParams(params)));
    };

    // Calls listener with the current auth state, and again whenever that
    // state changes. Returns an unsubscribe function.
    onAuthentication = (listener: AuthListener) => {
        this.authListeners.add(listener);
        this.callAuthListener(listener, this.getAuthState());
        return () => {
            this.authListeners.delete(listener);
        };
    };
    
    connect = (url: string) => {
        this.websocketHandler.onQuery = (queryKey, data, timestamp) => {
            if (!queryKey || !this.activeQueries.has(queryKey)) {
                return;
            }
            // Drop a result older than the copy already cached for this query.
            // A message with no timestamp still replaces the cache.
            const incomingTimestamp = this.readTimestamp(timestamp);
            if (incomingTimestamp !== undefined) {
                const cachedTimestamp = this.queryTimestamps.get(queryKey);
                if (cachedTimestamp !== undefined && incomingTimestamp < cachedTimestamp) {
                    return;
                }
                this.queryTimestamps.set(queryKey, incomingTimestamp);
            } else {
                this.queryTimestamps.delete(queryKey);
            }
            this.queryErrors.delete(queryKey);
            this.queryCache.set(queryKey, data);
            this.deliver(queryKey, data, null, 'update');
        };
        this.websocketHandler.onMutation = (incoming_id, data) => {
            this.takePendingMutation(incoming_id)?.resolve(data);
        };
        this.websocketHandler.onError = (message) => {
            this.handleServerError(message);
        };
        this.websocketHandler.onAuth = (message) => {
            if (message?.success === false) {
                this.authenticated = false;
                this.userInfo.delete('user_id');
                this.authError = typeof message?.error === 'string' ? message.error : null;
                this.emitAuth();
                return;
            }
            this.authenticated = true;
            this.authError = null;
            this.rememberUserId(message?.data?.user_id);
            this.emitAuth();
        };
        this.websocketHandler.onOpen = () => {
            this.websocketHandler.send(JSON.stringify({
                type: 'auth',
                token: this.token ?? ''
            }));
            this.activeQueries.forEach((query) => {
                this.sendSubscribe(query);
            });
        };
        this.websocketHandler.onClose = () => {
            for (const mutationId of [...this.pendingMutations.keys()]) {
                this.takePendingMutation(mutationId)?.reject(new Error('Connection closed'));
            }
            this.authenticated = false;
            this.userInfo.delete('user_id');
            this.emitAuth();
            this.queryTimestamps.clear();
        };
        this.websocketHandler.startConnection(url);
    };
    
    disconnect = () => {
        this.websocketHandler.close();
    };
    
    subscribe = (queryName: string, params: any, callback: (data: any, error: Error | null) => void) => {
        const snapshot = this.snapshotParams(params);
        const queryKey = this.getCacheKey(queryName, snapshot);
        let active = this.activeQueries.get(queryKey);
        if (!active) {
            this.listeners.set(queryKey, new Set());
            active = { queryName, params: snapshot, queryKey };
            this.activeQueries.set(queryKey, active);
            this.sendSubscribe(active);
        }
        this.listeners.get(queryKey)!.add(callback);

        if (this.queryCache.has(queryKey) || this.queryErrors.has(queryKey)) {
            this.callListener(
                callback,
                this.queryCache.get(queryKey),
                this.queryErrors.get(queryKey) ?? null,
                'initial update'
            );
        }

        const storedParams = active.params;
        return () => {
            const subs = this.listeners.get(queryKey);
            if (!subs) {
                return;
            }
            subs.delete(callback);
            if (subs.size === 0) {
                this.listeners.delete(queryKey);
                this.queryCache.delete(queryKey);
                this.queryErrors.delete(queryKey);
                this.queryTimestamps.delete(queryKey);
                this.activeQueries.delete(queryKey);
                this.websocketHandler.send(JSON.stringify({
                    type: 'unsubscribe',
                    location: queryName,
                    params: storedParams,
                    query_key: queryKey
                }));
            }
        };
    };
    
    sendMutation = (mutationName: string, params: any) => {
        const mutation_id = crypto.randomUUID();
        const deadline = Date.now() + MUTATION_TIMEOUT_MS;
        const promise = new Promise((resolve, reject) => {
            const timeoutId = setTimeout(() => {
                if (!this.pendingMutations.delete(mutation_id)) {
                    return;
                }
                // Not written yet. Remove it so a later open cannot run it after this rejection.
                this.websocketHandler.dropQueuedMutation(mutation_id);
                reject(new Error('Mutation timeout'));
            }, MUTATION_TIMEOUT_MS);
            this.pendingMutations.set(mutation_id, { resolve, reject, timeoutId });
        });
        this.websocketHandler.send(JSON.stringify({
            type: 'mutation',
            location: mutationName,
            params: this.normalizeParams(params),
            mutation_id: mutation_id
        }), { mutationId: mutation_id, deadline });
        return promise;
    };

    setToken = (token: string) => {
        this.token = token;
        this.websocketHandler.send(JSON.stringify({
            type: 'auth',
            token: this.token ?? ''
        }));
    };

    // Drop the socket and open another one. A new connection has no server
    // identity, so logout does not depend on the verifier accepting an empty
    // token. The retired socket is a previous connection generation: a late
    // query on it cannot refill the cache. Listeners run after the swap, and
    // a throw in one of them cannot cancel it.
    logout = () => {
        this.token = null;
        this.authenticated = false;
        this.authError = null;
        this.userInfo.clear();
        this.queryCache.clear();
        this.queryErrors.clear();
        this.queryTimestamps.clear();
        this.emitAuth();
        this.websocketHandler.restart();
        for (const mutationId of [...this.pendingMutations.keys()]) {
            this.takePendingMutation(mutationId)?.reject(new Error('Logged out'));
        }
        for (const queryKey of [...this.listeners.keys()]) {
            this.deliver(queryKey, undefined, null, 'logout');
        }
    };
}
