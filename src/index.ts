import { WebSocketHandler } from './utils/websocket.js';

type PendingMutation = {
    resolve: (value: unknown) => void;
    reject: (reason: Error) => void;
    timeoutId: ReturnType<typeof setTimeout>;
};

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

    private normalizeParams = (params: unknown): Record<string, unknown> => {
        if (params == null || typeof params !== 'object' || Array.isArray(params)) {
            return {};
        }
        return { ...(params as Record<string, unknown>) };
    };

    private getCacheKey = (queryName: string, params: Record<string, unknown>) => {
        const sortedParams = Object.keys(params).sort().reduce<Record<string, unknown>>((acc, key) => {
            acc[key] = params[key];
            return acc;
        }, {});
        return `${queryName}:${JSON.stringify(sortedParams)}`;
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
        return this.queryCache.get(this.getCacheKey(queryName, this.normalizeParams(params)));
    };
    
    connect = (url: string) => {
        this.websocketHandler.onQuery = (queryKey, data) => {
            if (!queryKey || !this.activeQueries.has(queryKey)) {
                return;
            }
            this.queryCache.set(queryKey, data);
            const subs = this.listeners.get(queryKey);
            if (subs) {
                subs.forEach(cb => cb(data));
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
        const normalized = this.normalizeParams(params);
        const queryKey = this.getCacheKey(queryName, normalized);
        if (!this.listeners.has(queryKey)) {
            this.listeners.set(queryKey, new Set());
            const active: ActiveQuery = { queryName, params: normalized, queryKey };
            this.activeQueries.set(queryKey, active);
            this.sendSubscribe(active);
        }
        this.listeners.get(queryKey)!.add(callback);

        if (this.queryCache.has(queryKey)) {
            callback(this.queryCache.get(queryKey));
        }

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
                    params: normalized,
                    query_key: queryKey
                }));
            }
        };
    };
    
    sendMutation = (mutationName: string, params: any) => {
        const mutation_id = crypto.randomUUID();
        const promise = new Promise((resolve, reject) => {
            const timeoutId = setTimeout(() => {
                this.pendingMutations.delete(mutation_id);
                reject(new Error('Mutation timeout'));
            }, 10000);
            this.pendingMutations.set(mutation_id, { resolve, reject, timeoutId });
        });
        this.websocketHandler.send(JSON.stringify({
            type: 'mutation',
            location: mutationName,
            params: this.normalizeParams(params),
            mutation_id: mutation_id
        }));
        return promise;
    };

    setToken = (token: string) => {
        this.token = token;
        this.websocketHandler.send(JSON.stringify({
            type: 'auth',
            token: this.token ?? ''
        }));
    };
}
