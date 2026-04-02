import type { DeltaData } from "../../model/Delta";
import type { SerializedEntityData } from "../../model/Entity";
import { getLogger } from "../../logging";
import { buildRestApiAuthHeaders, RestApiAuthConfig } from "../rest-api/Auth";
import type { DomainStoreAdapter } from "./DomainStoreAdapter";

const log = getLogger('RestApiDomainStoreAdapter');

/**
 * Domain store adapter that talks to the desktop backend REST API.
 * Pure passive transport — no polling or callbacks.
 */
export class RestApiDomainStoreAdapter implements DomainStoreAdapter {
    private _projectId: string;
    private _baseUrl: string;
    private _auth: RestApiAuthConfig;

    constructor(projectId: string, baseUrl: string, auth: RestApiAuthConfig) {
        this._projectId = projectId;
        this._baseUrl = baseUrl;
        this._auth = auth;
    }

    private _headers(): HeadersInit {
        return buildRestApiAuthHeaders(this._auth);
    }

    private _projectUrl(path: string): string {
        return `${this._baseUrl}/desktop/projects/${encodeURIComponent(this._projectId)}${path}`;
    }

    // -- Bridge lifecycle --------------------------------------------------

    async setupBridge(projectUri: string): Promise<void> {
        const response = await fetch(this._projectUrl('/bridge'), {
            method: 'PUT',
            headers: { ...this._headers(), 'Content-Type': 'application/json' },
            body: JSON.stringify({ uri: projectUri }),
        });
        if (!response.ok) {
            throw new Error(`Failed to load project session (${response.status} ${response.statusText})`);
        }
    }

    async discardBridge(): Promise<void> {
        const response = await fetch(this._projectUrl('/bridge'), {
            method: 'DELETE',
            headers: this._headers(),
        });
        if (!response.ok) {
            throw new Error(`Failed to unload project session (${response.status} ${response.statusText})`);
        }
    }

    // -- Entity query -----------------------------------------------------

    async find(filter?: Record<string, unknown>): Promise<SerializedEntityData[]> {
        const url = new URL(this._projectUrl('/entities'));
        if (filter) {
            for (const [key, value] of Object.entries(filter)) {
                if (value !== undefined) {
                    url.searchParams.set(key, String(value));
                }
            }
        }
        const response = await fetch(url, {
            method: 'GET',
            headers: this._headers(),
        });
        if (!response.ok) {
            throw new Error(`Failed to find entities (${response.status} ${response.statusText})`);
        }
        const payload = await response.json() as { data?: { entities?: SerializedEntityData[] } };
        return payload.data?.entities ?? [];
    }

    // -- Changes exchange -------------------------------------------------

    async applyDelta(deltaData: DeltaData, snapshotHash?: string): Promise<void> {
        const body: Record<string, unknown> = { delta: deltaData };
        if (snapshotHash !== undefined) {
            body.snapshotHash = snapshotHash;
        }
        const response = await fetch(this._projectUrl('/changes'), {
            method: 'POST',
            headers: { ...this._headers(), 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
        });
        if (!response.ok) {
            throw new Error(`Failed to apply delta (${response.status} ${response.statusText})`);
        }
    }

    async getPendingDelta(timeoutMs: number = 0): Promise<DeltaData | null> {
        const url = new URL(this._projectUrl('/changes/pending'));
        url.searchParams.set('timeout_ms', String(timeoutMs));
        const response = await fetch(
            url,
            { method: 'GET', headers: this._headers() },
        );
        if (!response.ok) {
            throw new Error(`Failed to fetch pending delta (${response.status} ${response.statusText})`);
        }
        const payload = await response.json() as { data?: { pendingDelta?: DeltaData | null } };
        return payload.data?.pendingDelta ?? null;
    }

}
