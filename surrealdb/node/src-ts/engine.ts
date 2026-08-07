import {
    ChannelIterator,
    type ConnectionState,
    ConnectionUnavailableError,
    type DriverContext,
    type EngineEvents,
    Features,
    type LiveAction,
    LiveDispatcher,
    type LiveMessage,
    Publisher,
    parseRpcError,
    type RecordId,
    RpcEngine,
    type RpcRequest,
    type SqlExportOptions,
    type SurrealEngine,
    UnexpectedConnectionError,
    type Uuid,
} from "surrealdb";
import { type ConnectionOptions, type NotificationReceiver, SurrealNodeEngine } from "../napi";
import { wrapSqonError } from "./wrap-sqon-error";

interface LivePayload {
    id: Uuid;
    action: LiveAction;
    result?: Record<string, unknown>;
    record?: RecordId;
}

/**
 * The engine implementation responsible for communicating with a SurrealDB
 * instance embedded in the host JavaScript runtime through a NAPI addon.
 */
export class NodeEngine extends RpcEngine implements SurrealEngine {
    #engine: SurrealNodeEngine | undefined;
    #notificationReceiver: NotificationReceiver | undefined;
    #publisher = new Publisher<EngineEvents>();
    #live = new LiveDispatcher();
    #active = false;
    #abort: AbortController | undefined;
    #options: ConnectionOptions | undefined;

    constructor(context: DriverContext, options?: ConnectionOptions) {
        super(context);
        this.#options = options;
    }

    // Annotated through `SurrealEngine` rather than left to inference: the
    // SDK's `Feature` class is not exported, so a declaration naming it
    // directly cannot be emitted.
    features: SurrealEngine["features"] = new Set([
        Features.LiveQueries,
        Features.Sessions,
        Features.Transactions,
        Features.Api,
        Features.ExportImportRaw,
    ]);

    open(state: ConnectionState): void {
        this.#abort?.abort();
        this.#abort = new AbortController();
        this.#active = true;
        this._state = state;
        this.#initialize(state, this.#abort.signal);
    }

    async close(): Promise<void> {
        this._state = undefined;
        this.#abort?.abort();
        this.#abort = undefined;
        this.#active = false;
        this.#engine?.free();
        this.#engine = undefined;
        this.#notificationReceiver = undefined;
        this.#live.clear();
        this.#publisher.publish("disconnected");
    }

    ready(): void {
        // No-op for Node engine - no pending calls to resend
    }

    subscribe<K extends keyof EngineEvents>(
        event: K,
        listener: (...payload: EngineEvents[K]) => void,
    ): () => void {
        return this.#publisher.subscribe(event, listener);
    }

    override liveQuery(id: Uuid): AsyncIterable<LiveMessage> {
        const channel = new ChannelIterator<LiveMessage>(() => {
            unsub1();
            unsub2();
        });

        const unsub1 = this.#live.subscribe(id.toString(), (msg) => {
            channel.submit(msg);
        });
        const unsub2 = this.#publisher.subscribe("disconnected", () => {
            channel.cancel();
        });

        return channel;
    }

    override async send<Method extends string, Params extends unknown[] | undefined, Result>(
        request: RpcRequest<Method, Params>,
    ): Promise<Result> {
        if (!this.#active || !this.#engine) {
            throw new ConnectionUnavailableError();
        }

        const id = this._context.uniqueId();
        const payload = wrapSqonError(() => this._context.codecs.cbor.encode({ id, ...request }));

        const response = await this.#engine.execute(payload);
        const decoded = wrapSqonError(() =>
            this._context.codecs.cbor.decode<Record<string, unknown>>(response),
        );

        if (decoded && typeof decoded === "object" && "error" in decoded) {
            throw parseRpcError(
                decoded.error as {
                    code: number;
                    message: string;
                    kind?: string;
                    details?: Record<string, unknown>;
                },
            );
        }

        return decoded as Result;
    }

    override async importSql(data: string | Blob | ReadableStream): Promise<void> {
        if (!this.#active || !this.#engine) {
            throw new ConnectionUnavailableError();
        }

        // NOTE We currently convert streams into strings as the
        // engine does not support streams yet.
        if (data instanceof ReadableStream) {
            const reader = data.getReader();
            const decoder = new TextDecoder();

            let sql = "";

            while (true) {
                const { done, value } = await reader.read();
                if (done) break;
                sql += decoder.decode(value, { stream: true });
            }

            return this.#engine.import(sql + decoder.decode());
        }

        // NOTE We currently convert blobs into strings as the
        // engine does not support blobs yet.
        if (data instanceof Blob) {
            return this.#engine.import(await data.text());
        }

        return this.#engine.import(data);
    }

    override async exportSql(options: Partial<SqlExportOptions>): Promise<Response> {
        if (!this.#active || !this.#engine) {
            throw new ConnectionUnavailableError();
        }

        const payload = wrapSqonError(() => this._context.codecs.cbor.encode(options));
        const sql = await this.#engine.export(payload);

        return new Response(sql);
    }

    async #initialize(state: ConnectionState, signal: AbortSignal) {
        try {
            this.#engine = await SurrealNodeEngine.connect(state.url.toString(), this.#options);

            if (signal.aborted) {
                return;
            }

            this.#notificationReceiver = await this.#engine.notifications();

            (async () => {
                while (this.#active && this.#notificationReceiver) {
                    const value = await this.#notificationReceiver.recv();

                    if (value === null) {
                        break; // Channel closed
                    }

                    const payload = wrapSqonError(() =>
                        this._context.codecs.cbor.decode<LivePayload>(value),
                    );

                    if (payload.id) {
                        this.#live.dispatch(
                            payload.id.toString(),
                            payload.action === "KILLED"
                                ? { queryId: payload.id, action: "KILLED" }
                                : {
                                      queryId: payload.id,
                                      action: payload.action,
                                      recordId: payload.record as RecordId,
                                      value: payload.result as Record<string, unknown>,
                                  },
                        );
                    }
                }
            })();

            this.#publisher.publish("connected");
        } catch (err) {
            this.#publisher.publish("error", new UnexpectedConnectionError(err));
        }
    }
}
