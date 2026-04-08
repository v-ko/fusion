import { getVcsAdapter, Repository, RepositoryIntegrityError, MissingBranchError, VcsAdapterConfig, verifyRepositoryIntegrity } from "../repository/Repository";
import { FileStoreAdapter } from "../file-store/FileStoreAdapter";
import { InMemoryFileStoreAdapter } from "../file-store/InMemoryFileStoreAdapter";
import { CacheFileStoreAdapter } from "../file-store/CacheFileStoreAdapter";
import { RestApiFileStoreAdapter } from "../file-store/RestApiFileStoreAdapter";
import { getLogger } from "../../logging";
import { RestApiAuthConfig } from "../rest-api/Auth";
import { ProjectData } from "./StorageService";
import type { StorageServiceActual } from "./StorageService";
import type { DomainStoreAdapter, DomainStoreConfig } from "../domain-store-adapter/DomainStoreAdapter";
import { RestApiDomainStoreAdapter } from "../domain-store-adapter/RestApiDomainStoreAdapter";
import { loadFromDict } from "../../model/Entity";
import { Delta, DeltaData } from "../../model/Delta";
import { Change } from "../../model/Change";

let log = getLogger('ProjectStorageManager');

export type { DomainStoreAdapter } from "../domain-store-adapter/DomainStoreAdapter";
export type { DomainStoreConfig } from "../domain-store-adapter/DomainStoreAdapter";

export interface ProjectStorageConfig {
    projectId: string;
    deviceBranchName: string;
    onDeviceVcsAdapter: VcsAdapterConfig;  // IndexedDB, RestApi, Cloud (for thin clients), InMemory for testing
    onDeviceFileStore: FileStoreConfig;  // CacheAPI, RestApi, Cloud (for thin clients), InMemory for testing
    domainStore?: DomainStoreConfig;  // External domain store config (e.g. desktop backend filesystem bridge)
}

export interface FileStoreConfig {
    name: FileStoreAdapterNames;
    args: FileStoreAdapterArgs;
}

export interface FileStoreAdapterArgs {
    projectId: string;
    baseUrl?: string;
    auth?: RestApiAuthConfig;
}

export type FileStoreAdapterNames = "InMemory" | "CacheAPI" | "RestApi";

async function initFileStore(config: FileStoreConfig): Promise<FileStoreAdapter> {
    let fileStore: FileStoreAdapter;

    switch (config.name) {
        case "InMemory": {
            let inMemFileStore = new InMemoryFileStoreAdapter();
            fileStore = inMemFileStore
            break;
        }
        case "CacheAPI": {
            let cacheFileStore = new CacheFileStoreAdapter(config.args.projectId);
            await cacheFileStore.init();
            log.info('Initialized CacheFileStoreAdapter for project', config.args.projectId);
            fileStore = cacheFileStore
            break;
        }
        case "RestApi": {
            const { projectId, baseUrl, auth } = config.args;
            if (!baseUrl) {
                throw new Error("RestApi file store requires args.baseUrl in config");
            }
            if (!auth) {
                throw new Error("RestApi file store requires args.auth in config");
            }
            fileStore = new RestApiFileStoreAdapter(projectId, baseUrl, auth);
            break;
        }
        default: {
            throw new Error(`Unknown file store name: ${config.name}`)
        }
    }

    return fileStore
}


function initDomainStoreAdapter(config: DomainStoreConfig): DomainStoreAdapter {
    switch (config.name) {
        case "RestApi": {
            const { projectId, baseUrl, auth } = config.args;
            return new RestApiDomainStoreAdapter(projectId, baseUrl, auth);
        }
        default:
            throw new Error(`Unknown domain store adapter type: ${config.name}`);
    }
}


export class ProjectStorageManager {
    /**
     * A class to manage the storage adapters and sync graph operations for a
     * repository (of a project).
     *
     * Each project needs storage. A client will always have a local storage
     * adapter to keep offline state (indexdb or device (desktop/mobile)).
     *
     * To allow for inter-device-sync or collab/sharing we apply new commits from
     * each device to their own branch (like in git). The resulting graph is the
     * "sync graph" (since it's used for state synchronization).
     * The synchronization process consists of communicating sync graph changes -
     * e.g. through yjs/webrtc or other means we convey changes in a CRDT-like
     * manner, where the order of update arrival does not matter.
     * The other operations on the graph (squashing, merging) are applied by
     * each client to their own branch, which allows for the above properties of
     * sync graph updates (arrival order irrelevance, ..?). Merge-conflicts
     * (where two clients commit at the same time on the same data) are resolved
     * deterministically by seniority. I.e. the junior node always adapts first,
     * and the senior node adopts only commits made on top of its own branch head.
     */
    private _config: ProjectStorageConfig;
    _onDeviceRepo: Repository | null = null;
    private _localFileStore: FileStoreAdapter | null = null;
    private _parentStorageService: StorageServiceActual | null = null;
    private _domainStoreAdapter: DomainStoreAdapter | null = null;
    private _bridgeActive: boolean = false;
    private _polling: boolean = false;

    constructor(config: ProjectStorageConfig, parentStorageService: StorageServiceActual | null = null) {
        this._config = config
        this._parentStorageService = parentStorageService;

        // Assert that the adapter local branches and the device branch are the same
        if (this._config.deviceBranchName !== this._config.onDeviceVcsAdapter.args.localBranchName) {
            throw new Error("Device branch name and on-device repo local branch name must match")
        }

        if (this._config.domainStore) {
            this._domainStoreAdapter = initDomainStoreAdapter(this._config.domainStore);
        }
    }
    get onDeviceRepo() {
        if (!this._onDeviceRepo) {
            throw new Error("Local storage repo not set. Have you called init?")
        }
        return this._onDeviceRepo
    }
    get fileStore() {
        if (!this._localFileStore) {
            throw new Error("File store not set. Have you called init?")
        }
        return this._localFileStore
    }
    get config(): ProjectStorageConfig {
        return this._config
    }
    get currentBranchName() {
        return this.config.deviceBranchName
    }
    get parentStorageService(): StorageServiceActual | null {
        return this._parentStorageService;
    }

    async getProjectProperties(): Promise<ProjectData | null> {
        return await this.onDeviceRepo.getProjectProperties() as ProjectData | null;
    }

    async setProjectProperties(projectProperties: ProjectData): Promise<void> {
        await this.onDeviceRepo.setProjectProperties(projectProperties);
    }

    // ---- Project lifecycle ----

    async loadProject(projectUri?: string) {
        const ds = this._domainStoreAdapter;

        // 1. Setup bridge (PFM reads .canvas files into memory)
        if (ds) {
            if (!projectUri) {
                throw new Error('projectUri is required when a domain store is configured');
            }
            await ds.setupBridge(projectUri);
            this._bridgeActive = true;
        }

        // 2. Open VCS (with domain store entities if available)
        let headStoreData;
        if (ds) {
            const entityDicts = await ds.find();
            headStoreData = entityDicts.map(d => loadFromDict(d));
            log.info(`Fetched ${headStoreData.length} entities from domain store`);
        }

        try {
            this._onDeviceRepo = await Repository.open(this.config.onDeviceVcsAdapter, true, headStoreData)
        } catch (e) {
            if (e instanceof MissingBranchError && headStoreData) {
                log.info(`VCS store empty, creating repository from ${headStoreData.length} domain store entities`);
                this._onDeviceRepo = await Repository.create(this.config.onDeviceVcsAdapter, true);
                const initialDelta = Delta.fromChanges(headStoreData.map(e => Change.create(e)));
                await this._onDeviceRepo.commit(initialDelta, 'Initial commit from domain store');
            } else {
                if (ds && this._bridgeActive) {
                    await ds.discardBridge().catch((unloadError: unknown) => {
                        log.error('Failed to discard bridge after load failure', unloadError);
                    });
                    this._bridgeActive = false;
                }
                if (e instanceof RepositoryIntegrityError) {
                    let storageAdapter = await getVcsAdapter(this.config.onDeviceVcsAdapter);
                    log.error('Attempting repository integrity verification after load failure', e);
                    await verifyRepositoryIntegrity(storageAdapter, this.config.deviceBranchName);
                }
                throw e;
            }
        }

        // 3. If domain store exists: verify hash integrity
        if (ds) {
            const commitGraph = await this._onDeviceRepo.getCommitGraph();
            const headCommit = commitGraph.headCommit(this.config.deviceBranchName);
            if (headCommit) {
                const diskHash = this._onDeviceRepo.hashTree.rootHash();
                const vcsHash = headCommit.snapshotHash;
                if (diskHash !== vcsHash) {
                    throw new Error(`Integrity check failed: filesystem hash (${diskHash}) does not match VCS head hash (${vcsHash}). integrityPatch not yet implemented.`);
                }
            }

            // 4. Check for immediate pending FS changes
            const pendingDelta = await ds.getPendingDelta(0);
            if (pendingDelta && Object.keys(pendingDelta).length > 0) {
                throw new Error('NotImplementedError: unexpected pending changes on project load');
            }
        }

        this._localFileStore = await initFileStore(this.config.onDeviceFileStore)

        // Start pulling external changes from domain store
        if (ds) {
            this._polling = true;
            void this._pollLoop();
        }
    }

    async createProject(projectProperties?: ProjectData): Promise<void> {
        if (this._domainStoreAdapter) {
            throw new Error("Desktop project creation is not implemented yet. Folder selection and project initialization still need to be added.");
        }
        this._onDeviceRepo = await Repository.create(this.config.onDeviceVcsAdapter, true);
        this._localFileStore = await initFileStore(this.config.onDeviceFileStore);
        if (projectProperties) {
            await this.setProjectProperties(projectProperties);
        }
    }

    async unloadProject(): Promise<void> {
        log.info('Closing project storage manager')
        this._polling = false;
        if (this._onDeviceRepo) {
            this._onDeviceRepo.close()
        }
        this._onDeviceRepo = null;
        this._localFileStore = null;
        if (this._domainStoreAdapter && this._bridgeActive) {
            await this._domainStoreAdapter.discardBridge();
            this._bridgeActive = false;
        }
    }

    async shutdown() {
        await this.unloadProject();
    }

    // ---- Domain store sync ----

    /** Push a user-initiated delta to the domain store (filesystem). */
    async syncDeltaToDomainStore(deltaData: DeltaData): Promise<void> {
        if (!this._domainStoreAdapter) return;
        await this._domainStoreAdapter.applyDelta(deltaData);
    }

    /** Pull loop: fetch pending FS changes and commit them to VCS. */
    private static readonly POLL_TIMEOUT_MS = 30_000;
    private async _pollLoop(): Promise<void> {
        const ds = this._domainStoreAdapter!;
        while (this._polling) {
            try {
                const delta = await ds.getPendingDelta(ProjectStorageManager.POLL_TIMEOUT_MS);
                if (!this._polling) break;

                if (delta && Object.keys(delta).length > 0) {
                    if (!this._parentStorageService || !this._onDeviceRepo) break;
                    await this._parentStorageService.commit(
                        this.config.projectId,
                        delta,
                        'external filesystem change',
                    );
                }
            } catch (e) {
                if (!this._polling) break;
                log.error('Change polling error', e);
                await new Promise(resolve => setTimeout(resolve, 2000));
            }
        }
    }

    async eraseLocalStorage() {
        if (this._onDeviceRepo) {
            await this._onDeviceRepo.eraseStorage();
        } else {
            log.warning('VCS storage not initialized. Nothing to erase.');
        }
        if (this._localFileStore) {
            await this._localFileStore.eraseStorage();
        } else {
            log.warning('File store not initialized. Nothing to erase.');
        }
    }
}
