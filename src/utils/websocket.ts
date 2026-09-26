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
    private sendQueue: string[] = [];
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
            if (this.sendQueue.length > 0) {
                this.sendQueue.forEach(message => this.ws?.send(message));
                this.sendQueue = [];
            }
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

    send = (message: string) => {
        if (this.ws?.readyState !== WebSocket.OPEN) {
            this.sendQueue.push(message);
            return;
        }
        this.ws.send(message);
    };

}
