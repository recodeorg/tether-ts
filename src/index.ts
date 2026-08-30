import { WebSocketHandler } from './utils/websocket.js';

type PendingMutation = {
    resolve: (value: unknown) => void;
    reject: (reason: Error) => void;
    timeoutId: ReturnType<typeof setTimeout>;
};

type Listener = (data: any) => void;

export class TetherClient {
    private websocketHandler: WebSocketHandler = new WebSocketHandler();
    private pendingMutations = new Map<string, PendingMutation>();
    private token: string | null = null;
    private authenticated: boolean = false;
    private userInfo: Map<string, any> = new Map();
    private queryCache = new Map<string, any>();
    private listeners = new Map<string, Set<Listener>>();

    private getCacheKey = (queryName: string, params: any) => {
        const sortedParams = Object.keys(params).sort().reduce<Record<string, unknown>>((acc, key) => {
            acc[key] = params[key];
            return acc;
        }, {});
        return `${queryName}:${JSON.stringify(sortedParams)}`;
    }

    getCache = (queryName: string, params: any) => {
        return this.queryCache.get(this.getCacheKey(queryName, params));
    }
    
    connect = (url: string) => {
        this.websocketHandler.startConnection(url);
        this.websocketHandler.onQuery = (query_id, data) => {
            if (!query_id) {
                return;
            }
            this.queryCache.set(query_id, data);
            const subs = this.listeners.get(query_id);
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
        this.websocketHandler.onAuth = (data) => {
            this.authenticated = true;
            this.userInfo.set('user_id', data.user_id);
        };
        this.websocketHandler.onOpen = () => {
            this.websocketHandler.send(JSON.stringify({
                type: 'auth',
                token: this.token ?? ''
            }));
            this.listeners.forEach((listeners, queryId) => {
                this.websocketHandler.send(JSON.stringify({
                    type: 'subscribe',
                    location: queryId.split(':')[0],
                    params: JSON.parse(queryId.split(':')[1]),
                    query_id: queryId
                }));
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
    };
    
    disconnect = () => {
        this.websocketHandler.close();
    };
    
    subscribe = async (queryName: string, params: any, callback: (data: any) => void) => {
        const queryId = this.getCacheKey(queryName, params);
        if (!this.listeners.has(queryId)) {
            this.listeners.set(queryId, new Set())
            this.websocketHandler.send(JSON.stringify({
                type: 'subscribe',
                location: queryName,
                params: params,
                query_id: queryId
            }));
        }
        this.listeners.get(queryId)!.add(callback);

        return () => {
            const subs = this.listeners.get(queryId);
            if (subs) {
                subs.delete(callback);
                if (subs.size === 0) {
                    this.listeners.delete(queryId);
                    this.queryCache.delete(queryId);
                    this.websocketHandler.send(JSON.stringify({
                        type: 'unsubscribe',
                        query: queryId
                    }));
                }
            }
        }
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
            params: params,
            mutation_id: mutation_id
        }));
        return promise;
    };

    setToken = (token: string) => { // Function that returns a token
        this.token = token;
        this.websocketHandler.send(JSON.stringify({
            type: 'auth',
            token: this.token ?? ''
        }));
    };
}