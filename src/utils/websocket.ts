export class WebSocketHandler {
    private ws: WebSocket | null = null;
    private url: string = '';
    public onOpen: () => void = () => {};
    public onQuery: (location: string | undefined, data: any) => void = () => {};
    public onClose: () => void = () => {};
    private reconnectAttempts: number = 0;
    private maxReconnectAttempts: number = 5;
    private reconnectInterval: number = 1000;
    private sendQueue: string[] = [];
    public onMutation: (mutation_id: string, data: any) => void = () => {};
    public onAuth: (data: any) => void = () => {};
    startConnection = (url: string) => {
        this.url = url;
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
            let data: {
                type: string;
                location?: string;
                data?: unknown;
                error?: string;
                mutation_id?: string;
            };
            try {
                data = JSON.parse(String(event.data));
            } catch (e) {
                console.error('Tether: invalid JSON message', event.data, e);
                return;
            }
            if (data.type === 'query') {
                this.onQuery(data.location, data.data);
            } else if (data.type === 'mutation') {
                this.onMutation(data.mutation_id || '', data.data);
            } else if (data.type === 'error') {
                console.error(data.error);
            } else if (data.type === 'auth') {
                this.onAuth(data.data);
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
            this.attemptReconnect();
        };
    };

    attemptReconnect = () => {
        this.ws?.close();
        this.reconnectAttempts++;
        if (this.reconnectAttempts > this.maxReconnectAttempts) {
            console.error('Max reconnect attempts reached');
            return;
        }
        setTimeout(() => {
            this.startConnection(this.url);
        }, this.reconnectInterval);
    };

    close = () => {
        this.ws?.close();
        this.ws = null;
    };

    send = (message: string) => {
        if (this.ws?.readyState !== WebSocket.OPEN) {
            this.sendQueue.push(message);
            return;
        }
        this.ws?.send(message);
    };

}
