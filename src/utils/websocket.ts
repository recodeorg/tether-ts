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
    success?: boolean;
};

export class WebSocketHandler {
    private ws: WebSocket | null = null;
    private url: string = '';
    public onOpen: () => void = () => {};
    public onQuery: (queryKey: string | undefined, data: unknown) => void = () => {};
    public onClose: () => void = () => {};
    private reconnectAttempts: number = 0;
    private maxReconnectAttempts: number = 5;
    private reconnectInterval: number = 1000;
    private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
    private shouldReconnect: boolean = true;
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
        this.ws = new WebSocket(url);
        const ws = this.ws;

        ws.onopen = () => {
            console.log('Connected to Tether');
            this.onOpen();
            this.flushSendQueue();
            this.reconnectAttempts = 0;
        };

        ws.onmessage = (event: MessageEvent) => {
            let data: ServerMessage;
            try {
                data = JSON.parse(String(event.data));
            } catch (e) {
                console.error('Tether: invalid JSON message', event.data, e);
                return;
            }
            if (data.type === 'query') {
                this.onQuery(data.query_key, data.data);
            } else if (data.type === 'mutation') {
                this.onMutation(data.mutation_id || '', data.data);
            } else if (data.type === 'error') {
                console.error(data.error);
            } else if (data.type === 'auth') {
                this.onAuth(data);
            }
        };

        ws.onclose = (event: CloseEvent) => {
            console.log(
                'Disconnected from Tether',
                'code:',
                event.code,
                'reason:',
                event.reason || '(none)',
                'wasClean:',
                event.wasClean
            );
            if (this.ws !== ws) {
                return;
            }
            this.ws = null;
            // Anything still queued never reached the server. Drop it so a
            // reconnect cannot deliver a mutation whose promise was rejected.
            this.sendQueue = [];
            this.onClose();
            if (this.shouldReconnect) {
                this.attemptReconnect();
            }
        };
    };

    attemptReconnect = () => {
        this.reconnectAttempts++;
        if (this.reconnectAttempts > this.maxReconnectAttempts) {
            console.error('Max reconnect attempts reached');
            return;
        }
        if (this.reconnectTimer) {
            clearTimeout(this.reconnectTimer);
        }
        this.reconnectTimer = setTimeout(() => {
            this.reconnectTimer = null;
            if (!this.shouldReconnect) {
                return;
            }
            this.startConnection(this.url);
        }, this.reconnectInterval);
    };

    close = () => {
        this.shouldReconnect = false;
        if (this.reconnectTimer) {
            clearTimeout(this.reconnectTimer);
            this.reconnectTimer = null;
        }
        this.sendQueue = [];
        const ws = this.ws;
        this.ws = null;
        ws?.close();
        this.onClose();
    };

    dropQueuedMutation = (mutationId: string) => {
        this.sendQueue = this.sendQueue.filter((item) => item.mutationId !== mutationId);
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
