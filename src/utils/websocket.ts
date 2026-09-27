type QueuedMessage = {
    payload: string;
    mutationId?: string;
    deadline?: number;
};

type ServerMessage = {
    type: string;
    location?: string;
    data?: unknown;
    error?: string;
    mutation_id?: string;
    query_key?: string;
    timestamp?: number;
    success?: boolean;
};

export class WebSocketHandler {
    private ws: WebSocket | null = null;
    private url: string = '';
    public onOpen: () => void = () => {};
    public onQuery: (queryKey: string | undefined, data: unknown, timestamp: unknown) => void = () => {};
    public onClose: () => void = () => {};
    private reconnectAttempts: number = 0;
    private reconnectInterval: number = 1000;
    private maxReconnectInterval: number = 30000;
    private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
    private shouldReconnect: boolean = true;
    // Bumped when startConnection replaces the socket and when close() drops it.
    // Handlers from an older generation must not touch callbacks, the send queue, or reconnect.
    private connectionGeneration: number = 0;
    // Offline mutations:
    // Frames wait here until the socket is OPEN. A mutation frame carries its id
    // and the deadline of its caller promise. If that promise is rejected before
    // the frame is written (timeout, or disconnect clearing this queue), the frame
    // is dropped and is not flushed on a later open. A frame already passed to
    // socket.send is not recalled. Other frames stay queued until open. Close
    // drops the whole queue; the client then sends auth and active subscriptions
    // again when the socket opens.
    private sendQueue: QueuedMessage[] = [];
    public shouldSendQueuedMutation: (mutationId: string) => boolean = () => true;
    public onMutation: (mutation_id: string, data: unknown) => void = () => {};
    public onAuth: (data: any) => void = () => {};
    startConnection = (url: string) => {
        this.url = url;
        this.shouldReconnect = true;
        this.cancelReconnect();

        const previous = this.ws;
        const generation = ++this.connectionGeneration;
        const ws = new WebSocket(url);
        this.ws = ws;

        ws.onopen = () => {
            if (!this.isCurrentSocket(generation, ws)) {
                return;
            }
            console.log('Connected to Tether');
            this.onOpen();
            this.flushSendQueue();
            this.reconnectAttempts = 0;
        };

        ws.onmessage = (event: MessageEvent) => {
            if (!this.isCurrentSocket(generation, ws)) {
                return;
            }
            let data: ServerMessage;
            try {
                data = JSON.parse(String(event.data));
            } catch (e) {
                console.error('Tether: invalid JSON message', event.data, e);
                return;
            }
            if (data.type === 'query') {
                this.onQuery(data.query_key, data.data, data.timestamp);
            } else if (data.type === 'mutation') {
                this.onMutation(data.mutation_id || '', data.data);
            } else if (data.type === 'error') {
                console.error(data.error);
            } else if (data.type === 'auth') {
                this.onAuth(data);
            }
        };

        ws.onclose = (event: CloseEvent) => {
            if (!this.isCurrentSocket(generation, ws)) {
                return;
            }
            console.log(
                'Disconnected from Tether',
                'code:',
                event.code,
                'reason:',
                event.reason || '(none)',
                'wasClean:',
                event.wasClean
            );
            this.ws = null;
            // Anything still queued never reached the server. Drop it so a
            // reconnect cannot deliver a mutation whose promise was rejected.
            this.sendQueue = [];
            this.onClose();
            if (this.shouldReconnect) {
                this.attemptReconnect();
            }
        };

        if (previous) {
            this.retireSocket(previous);
        }
    };

    attemptReconnect = () => {
        this.cancelReconnect();
        const generation = this.connectionGeneration;
        const backoff = Math.min(
            this.reconnectInterval * 2 ** Math.min(this.reconnectAttempts, 16),
            this.maxReconnectInterval
        );
        // Jitter in [backoff / 2, backoff) so clients dropped together don't retry in lockstep.
        const delay = backoff / 2 + Math.random() * (backoff / 2);
        this.reconnectAttempts++;
        this.reconnectTimer = setTimeout(() => {
            this.reconnectTimer = null;
            if (!this.shouldReconnect || generation !== this.connectionGeneration) {
                return;
            }
            this.startConnection(this.url);
        }, delay);
    };

    close = () => {
        this.shouldReconnect = false;
        this.cancelReconnect();
        this.connectionGeneration += 1;
        this.sendQueue = [];
        const ws = this.ws;
        this.ws = null;
        if (ws) {
            this.retireSocket(ws);
        }
        this.onClose();
    };

    private isCurrentSocket = (generation: number, ws: WebSocket) => {
        return generation === this.connectionGeneration && this.ws === ws;
    };

    private cancelReconnect = () => {
        if (this.reconnectTimer) {
            clearTimeout(this.reconnectTimer);
            this.reconnectTimer = null;
        }
    };

    private retireSocket = (ws: WebSocket) => {
        ws.onopen = null;
        ws.onmessage = null;
        ws.onclose = null;
        ws.onerror = null;
        if (ws.readyState === WebSocket.CONNECTING || ws.readyState === WebSocket.OPEN) {
            ws.close();
        }
    };

    dropQueuedMutation = (mutationId: string) => {
        this.sendQueue = this.sendQueue.filter((item) => item.mutationId !== mutationId);
    };

    // Auth frames carry a token. Mutation frames carry that user's params.
    // Subscribe and unsubscribe frames stay queued.
    dropQueuedUserData = () => {
        this.sendQueue = this.sendQueue.filter((item) => {
            if (item.mutationId !== undefined) {
                return false;
            }
            try {
                return (JSON.parse(item.payload) as { type?: string }).type !== 'auth';
            } catch {
                return true;
            }
        });
    };

    send = (message: string, options?: { mutationId: string; deadline: number }) => {
        if (this.ws?.readyState !== WebSocket.OPEN) {
            this.sendQueue.push({
                payload: message,
                mutationId: options?.mutationId,
                deadline: options?.deadline,
            });
            return;
        }
        this.ws.send(message);
    };

    private flushSendQueue = () => {
        const now = Date.now();
        const queued = this.sendQueue;
        this.sendQueue = [];
        for (const item of queued) {
            if (this.isStaleQueuedMutation(item, now)) {
                continue;
            }
            this.ws?.send(item.payload);
        }
    };

    private isStaleQueuedMutation = (item: QueuedMessage, now: number) => {
        if (item.mutationId === undefined) {
            return false;
        }
        if (item.deadline !== undefined && now >= item.deadline) {
            return true;
        }
        return !this.shouldSendQueuedMutation(item.mutationId);
    };

}
