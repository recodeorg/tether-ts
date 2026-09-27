import { WebSocketHandler } from './utils/websocket.js';

type PendingMutation = {
    resolve: (value: unknown) => void;
    reject: (reason: Error) => void;
    timeoutId: ReturnType<typeof setTimeout>;
};

const MUTATION_TIMEOUT_MS = 10000;

type Listener = (data: any) => void;

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
    private listeners = new Map<string, Set<Listener>>();
    private activeQueries = new Map<string, ActiveQuery>();

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

    getCache = (queryName: string, params: any) => {
        return this.queryCache.get(this.getCacheKey(queryName, this.snapshotParams(params)));
    };
    
    connect = (url: string) => {
        this.websocketHandler.onQuery = (queryKey, data) => {
            if (!queryKey || !this.activeQueries.has(queryKey)) {
                return;
            }
            this.queryCache.set(queryKey, data);
            const subs = this.listeners.get(queryKey);
            if (subs) {
                subs.forEach(cb => {
                    try {
                        cb(data);
                    } catch (error) {
                        console.error('Tether: Listener threw an exception during update:', error);
                    }
                });
            }
        };
        this.websocketHandler.onMutation = (incoming_id, data) => {
            const pending = this.pendingMutations.get(incoming_id);
            if (!pending) {
                return;
            }
            clearTimeout(pending.timeoutId);
            this.pendingMutations.delete(incoming_id);
            pending.resolve(data);
        };
        this.websocketHandler.onAuth = (message) => {
            if (message?.success === false) {
                this.authenticated = false;
                return;
            }
            this.authenticated = true;
            const userId = message?.data?.user_id;
            if (userId !== undefined) {
                this.userInfo.set('user_id', userId);
            }
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
            this.pendingMutations.forEach(pending => {
                clearTimeout(pending.timeoutId);
                pending.reject(new Error('Connection closed'));
            });
            this.pendingMutations.clear();
            this.authenticated = false;
        };
        this.websocketHandler.startConnection(url);
    };
    
    disconnect = () => {
        this.websocketHandler.close();
    };
    
    subscribe = (queryName: string, params: any, callback: (data: any) => void) => {
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

        if (this.queryCache.has(queryKey)) {
            try {
                callback(this.queryCache.get(queryKey));
            } catch (error) {
                console.error('Tether: Listener threw an exception during initial update:', error);
            }
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

    logout = () => {
        this.token = null;
        this.authenticated = false;
        this.userInfo.clear();
        this.queryCache.clear();
        this.websocketHandler.dropQueuedUserData();
        this.pendingMutations.forEach((pending) => {
            clearTimeout(pending.timeoutId);
            pending.reject(new Error('Logged out'));
        });
        this.pendingMutations.clear();
        for (const subs of [...this.listeners.values()]) {
            for (const callback of [...subs]) {
                callback(undefined);
            }
        }
        this.websocketHandler.send(JSON.stringify({
            type: 'auth',
            token: ''
        }));
    }
}
