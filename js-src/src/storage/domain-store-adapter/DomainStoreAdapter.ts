import type { DeltaData } from "../../model/Delta";
import type { SerializedEntityData } from "../../model/Entity";
import type { RestApiAuthConfig } from "../rest-api/Auth";

/**
 * Adapter for an external domain store (e.g. desktop backend filesystem).
 *
 * Pure passive transport: provides bridge lifecycle, entity queries,
 * and change exchange. PSM coordinates the sync loop.
 */
export interface DomainStoreAdapter {
    setupBridge(projectUri: string): Promise<void>;
    discardBridge(): Promise<void>;

    find(filter?: Record<string, unknown>): Promise<SerializedEntityData[]>;
    applyDelta(deltaData: DeltaData): Promise<void>;
    getPendingDelta(timeoutMs?: number): Promise<DeltaData | null>;
}

export type DomainStoreAdapterNames = "RestApi";

export interface DomainStoreAdapterArgs {
    projectId: string;
    baseUrl: string;
    auth: RestApiAuthConfig;
}

export interface DomainStoreConfig {
    name: DomainStoreAdapterNames;
    args: DomainStoreAdapterArgs;
}
