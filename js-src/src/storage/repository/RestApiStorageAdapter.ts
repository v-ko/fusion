import { getLogger } from "../../logging";
import { Commit, CommitData } from "../version-control/Commit";
import { CommitGraph, CommitGraphData } from "../version-control/CommitGraph";
import { InternalRepoUpdate, StorageAdapter } from "./StorageAdapter";
import { buildRestApiAuthHeaders, RestApiAuthConfig } from "../rest-api/Auth";

const log = getLogger("RestApiStorageAdapter");

export class RestApiStorageAdapter implements StorageAdapter {
    private _projectId: string;
    private _localBranchName: string;
    private _baseUrl: string;
    private _auth: RestApiAuthConfig;
    private _warnedReadonlyUpdate = false;

    constructor(
        projectId: string,
        localBranchName: string,
        baseUrl: string,
        auth: RestApiAuthConfig,
    ) {
        this._projectId = projectId;
        this._localBranchName = localBranchName;
        this._baseUrl = baseUrl;
        this._auth = auth;
    }

    private _headers(): HeadersInit {
        return buildRestApiAuthHeaders(this._auth);
    }

    private _url(path: string, params?: URLSearchParams): string {
        const normalizedPath = path.startsWith("/") ? path : `/${path}`;
        const url = new URL(`${this._baseUrl}${normalizedPath}`);
        if (params) {
            url.search = params.toString();
        }
        return url.toString();
    }

    private async _getJson<T>(url: string): Promise<T> {
        const response = await fetch(url, {
            method: "GET",
            headers: this._headers(),
        });

        if (!response.ok) {
            throw new Error(
                `REST API request failed (${response.status} ${response.statusText}) for ${url}`,
            );
        }

        return (await response.json()) as T;
    }

    async getCommitGraph(): Promise<CommitGraph> {
        const params = new URLSearchParams();
        params.set("branch", this._localBranchName);

        const url = this._url(
            `/desktop/storage/project/${encodeURIComponent(this._projectId)}/commit-graph`,
            params,
        );
        const commitGraphData = await this._getJson<CommitGraphData>(url);
        return CommitGraph.fromData(commitGraphData);
    }

    async getCommits(ids: string[]): Promise<Commit[]> {
        if (!ids || ids.length === 0) {
            return [];
        }

        const params = new URLSearchParams();
        params.set("branch", this._localBranchName);
        for (const id of ids) {
            params.append("ids", id);
        }

        const url = this._url(
            `/desktop/storage/project/${encodeURIComponent(this._projectId)}/commits`,
            params,
        );
        const commitDataList = await this._getJson<CommitData[]>(url);
        const commitDataById = new Map(commitDataList.map((c) => [c.id, c]));

        return ids.map((id) => {
            const commitData = commitDataById.get(id);
            if (!commitData) {
                throw new Error(`Commit ${id} not found on REST API backend`);
            }
            return new Commit(commitData);
        });
    }

    async applyUpdate(update: InternalRepoUpdate): Promise<void> {
        const hasChanges =
            update.addedCommits.length > 0
            || update.updatedCommits.length > 0
            || update.removedCommits.length > 0
            || update.addedBranches.length > 0
            || update.updatedBranches.length > 0
            || update.removedBranches.length > 0;

        if (hasChanges && !this._warnedReadonlyUpdate) {
            this._warnedReadonlyUpdate = true;
            log.warning(
                "applyUpdate called on read-only RestApiStorageAdapter; update ignored.",
            );
        }
    }

    close(): void {
        // Nothing to release for HTTP adapter.
    }

    async eraseStorage(): Promise<void> {
        // Read-only adapter does not own persistent storage.
    }
}
