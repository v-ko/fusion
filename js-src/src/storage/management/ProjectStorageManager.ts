import { getStorageAdapter, Repository, RepositoryIntegrityError, StorageAdapterConfig, verifyRepositoryIntegrity } from "../repository/Repository";
import { FileStoreAdapter } from "../file-store/FileStoreAdapter";
import { InMemoryFileStoreAdapter } from "../file-store/InMemoryFileStoreAdapter";
import { CacheFileStoreAdapter } from "../file-store/CacheFileStoreAdapter";
import { RestApiFileStoreAdapter } from "../file-store/RestApiFileStoreAdapter";
import { getLogger } from "../../logging";
import { IndexConfig } from "../domain-store/InMemoryStore";
import { RestApiAuthConfig } from "../rest-api/Auth";

let log = getLogger('ProjectStorageManager');

export interface ProjectStorageConfig {
    deviceBranchName: string;
    storeIndexConfigs: readonly IndexConfig[];
    onDeviceStorageAdapter: StorageAdapterConfig;  // IndexedDB, RestApi, Cloud (for thin clients), InMemory for testing
    onDeviceFileStore: FileStoreConfig;  // CacheAPI, RestApi, Cloud (for thin clients), InMemory for testing
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
        this._localFileStore = await initFileStore(this.config.onDeviceFileStore)
    }
    async createProject(): Promise<void> {
        this._onDeviceRepo = await Repository.create(this.config.onDeviceStorageAdapter, true, this.config.storeIndexConfigs);
        this._localFileStore = await initFileStore(this.config.onDeviceFileStore);
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
