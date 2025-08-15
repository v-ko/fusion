import { getStorageAdapter, Repository, RepositoryIntegrityError, StorageAdapterConfig, verifyRepositoryIntegrity } from "../repository/Repository";
import { MediaStoreAdapter } from "../media-store/MediaStoreAdapter";
import { InMemoryMediaStoreAdapter } from "../media-store/InMemoryMediaStoreAdapter";
import { CacheMediaStoreAdapter } from "../media-store/CacheMediaStoreAdapter";
import { getLogger } from "../../logging";
import { IndexConfig } from "../domain-store/InMemoryStore";

let log = getLogger('ProjectStorageManager');

export interface ProjectStorageConfig {
    deviceBranchName: string;
    storeIndexConfigs: readonly IndexConfig[];
    onDeviceStorageAdapter: StorageAdapterConfig;  // IndexedDB, DesktopServer, Cloud (for thin clients), InMemory for testing
    onDeviceMediaStore: MediaStoreConfig;  // CacheAPI, DesktopServer, Cloud (for thin clients), InMemory for testing
}

export interface MediaStoreConfig {
    name: MediaStoreAdapterNames;
    args: MediaStoreAdapterArgs;
}

export interface MediaStoreAdapterArgs {
    projectId: string;
}

export type MediaStoreAdapterNames = "InMemory" | "CacheAPI" | "DesktopServer";

async function initMediaStore(config: MediaStoreConfig): Promise<MediaStoreAdapter> {
    let mediaStore: MediaStoreAdapter;

    switch (config.name) {
        case "InMemory": {
            let inMemMediaStore = new InMemoryMediaStoreAdapter();
            mediaStore = inMemMediaStore
            break;
        }
        case "CacheAPI": {
            let cacheMediaStore = new CacheMediaStoreAdapter(config.args.projectId);
            await cacheMediaStore.init();
            log.info('Initialized CacheMediaStoreAdapter for project', config.args.projectId);
            mediaStore = cacheMediaStore
            break;
        }
        case "DesktopServer": {
            throw new Error("DesktopServer not implemented yet")
        }
        default: {
            throw new Error(`Unknown media store name: ${config.name}`)
        }
    }

    return mediaStore
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
     * The other operations on the graph (squishing, merging) are applied by
     * each client to their own branch, which allows for the above properties of
     * sync graph updates (arrival order irrelevance, ..?). Merge-conflicts
     * (where two clients commit at the same time on the same data) are resolved
     * deterministically by seniority. I.e. the junior node always adapts first,
     * and the senior node adopts only commits made on top of its own branch head.
     */
    private _config: ProjectStorageConfig;
    _onDeviceRepo: Repository | null = null;
    private _localMediaStore: MediaStoreAdapter | null = null;

    constructor(config: ProjectStorageConfig) {
        this._config = config

        // Assert that the adapter local branches and the device branch are the same
        if (this._config.deviceBranchName !== this._config.onDeviceStorageAdapter.args.localBranchName) {
            throw new Error("Device branch name and on-device repo local branch name must match")
        }
    }
    get onDeviceRepo() {
        if (!this._onDeviceRepo) {
            throw new Error("Local storage repo not set. Have you called init?")
        }
        return this._onDeviceRepo
    }
    get mediaStore() {
        if (!this._localMediaStore) {
            throw new Error("Media store not set. Have you called init?")
        }
        return this._localMediaStore
    }
    get config(): ProjectStorageConfig {
        return this._config
    }
    get currentBranchName() {
        return this.config.deviceBranchName
    }
    async loadProject() {
        try{
            this._onDeviceRepo = await Repository.open(this.config.onDeviceStorageAdapter, true, this.config.storeIndexConfigs)
        } catch (e) {
            if (e instanceof RepositoryIntegrityError) {
                let storageAdapter = await getStorageAdapter(this.config.onDeviceStorageAdapter);
                await verifyRepositoryIntegrity(storageAdapter, this.config.deviceBranchName);
            } else {
                throw e;
            }
        }
        this._localMediaStore = await initMediaStore(this.config.onDeviceMediaStore)
    }
    async createProject(): Promise<void> {
        this._onDeviceRepo = await Repository.create(this.config.onDeviceStorageAdapter, true, this.config.storeIndexConfigs);
        this._localMediaStore = await initMediaStore(this.config.onDeviceMediaStore);
    }

    shutdown() {
        log.info('Closing project storage manager')
        if (this._onDeviceRepo) {
            this._onDeviceRepo.close()
        }
    }

    async eraseLocalStorage() {
        // Erase the local storage
        if (this._onDeviceRepo) {
            await this._onDeviceRepo.eraseStorage()
        } else {
            log.warning('Local storage not initialized. Nothing to erase.')
        }
    }
}
