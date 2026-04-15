import { Delta } from "../../model/Delta";
import { InMemoryStore } from "../domain-store/InMemoryStore";

export interface StoreSyncClient {
    setStore(store: InMemoryStore): void;
    initialize(): Promise<void>;
    pushDelta(delta: Delta): void;
    dispose(): void;
}
