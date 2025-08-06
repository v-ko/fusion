import * as Comlink from 'comlink';
import { ProjectStorageManager, ProjectStorageConfig } from './ProjectStorageManager';
import { SerializedStoreData } from './BaseStore';
import { Delta, DeltaData } from './Delta';
import { RepoUpdateData } from "./BaseRepository"
import { createId } from '../base-util';
import { buildHashTree } from './HashTree';
import { MediaItem, MediaItemData } from '../libs/MediaItem';
import { generateUniquePathWithSuffix } from "../util";
import { getLogger } from '../logging';

let log = getLogger('StorageService')

export interface MediaRequest {
    projectId: string;
    mediaItemId: string;
    mediaItemContentHash?: string;
}

export class RepositoryConfigMismatchError extends Error {
    constructor(message: string) {
        super(message);
        this.name = "RepositoryConfigMismatchError";
    }
}

export type MediaRequestParser = (storageService: StorageServiceActual, url: string) => MediaRequest | null;

export type RepoUpdateNotifiedSignature = (update: RepoUpdateData) => void;

export interface StorageServiceActualInterface {
    loadProject: (projectId: string, repoManagerConfig: ProjectStorageConfig) => Promise<void>;
    unloadProject: (projectId: string) => Promise<void>;
    deleteProject: (projectId: string, projectStorageConfig: ProjectStorageConfig) => Promise<void>;
    headState: (projectId: string) => Promise<SerializedStoreData>;
    _storageOperationRequest: (request: StorageOperationRequest) => Promise<void>;

    // Media operations
    addMedia: (projectId: string, blob: Blob, path: string) => Promise<MediaItemData>;
    getMedia: (projectId: string, mediaId: string, mediaHash: string) => Promise<Blob>;
    removeMedia: (projectId: string, mediaId: string, mediaHash: string) => Promise<void>;
    moveMediaItemToTrash: (projectId: string, mediaId: string, mediaHash: string) => Promise<void>;

    test(): boolean;
}

interface StorageOperationRequest {
    type: string;
}
interface CommitRequest extends StorageOperationRequest {
    projectId: string;
    deltaData: DeltaData;
    message: string;
}
function createCommitRequest(projectId: string, deltaData: DeltaData, message: string): CommitRequest {
    return {
        type: 'commit',
        projectId: projectId,
        deltaData: deltaData,
        message: message
    }
}

export interface LocalStorageUpdateMessage {
    projectId: string
    storageServiceId: string
    update: RepoUpdateData
}


const LOCAL_STORAGE_UPDATE_CHANNEL = 'storage-service-local-storage-update-channel'

export class StorageService {
    /**
     * This is a wrapper that allows the storage service to be run in either
     * a service worker or the main thread.
     *
     * Commit, squish, merge and other storage operations are wrapped as requests
     * so that they can be executed in request order as to avoid consistency problems
     *
     * Normally each tab/window will only load a single project. Those can be
     * different though, so the service should accomodate that with minimal overhead.
     * Therefore it will act on a subscription principle. Each request for loading
     * a repo will constitue a subscription to that repos changes. The first load
     * inits the repo, and the last close(=unsubscribe) closes the repo and frees
     * memory.
     *
     * There's a local repo and at some point - a sync service
     * > The local repo does not issue changes, since it's solely owned by the client.
     * It may be index-db based or device (desktop/mobile) based.
     * > The sync service will push local sync graph changes and notify for remote
     *   sync graph changes.
     */
    private _service: Comlink.Remote<StorageServiceActualInterface> | StorageServiceActualInterface | null = null;
    _worker: ServiceWorker | null = null;
    _workerRegistration: ServiceWorkerRegistration | null = null;
    _localUpdateBroadcastChannel: BroadcastChannel | null = null;
    _currentProjectId: string | null = null; // Only one project allowed per tab
    _localStorageUpdateCallback: RepoUpdateNotifiedSignature | null = null; // Callback for current project

    constructor() {
        // Create the broadcast channel for receiving updates
        this._localUpdateBroadcastChannel = new BroadcastChannel(LOCAL_STORAGE_UPDATE_CHANNEL);
        this._localUpdateBroadcastChannel.onmessage = (event) => {
            const update: LocalStorageUpdateMessage = event.data;
            if (this._currentProjectId === update.projectId && this._localStorageUpdateCallback) {
                this._localStorageUpdateCallback(update.update);
            }
        };
        log.info('Broadcast channel created for updates', this._localUpdateBroadcastChannel.name);
    }

    get service(): Comlink.Remote<StorageServiceActualInterface> | StorageServiceActualInterface {
        if (!this._service) {
            throw new Error("Service not setup. Call setupInMainThread or setupInServiceWorker first.");
        }
        return this._service;
    }

    setupInMainThread() {
        log.info('Setting up storage service in main thread');
        this._service = new StorageServiceActual();
    }

    async registerServiceWorker(serviceWorkerUrl: any): Promise<ServiceWorker | null> {
        if (!("serviceWorker" in navigator)) {
            log.warning("Service workers are not supported.");
            return null;
        }

        log.info('Registering service worker ', serviceWorkerUrl)
        let registration: ServiceWorkerRegistration;

        if (!this._workerRegistration) {
            try {
                registration = await navigator.serviceWorker.register(
                    serviceWorkerUrl, { type: "module", scope: "/" })
                this.setWorkerRegistration(registration);
            } catch (error) {
                log.warning(`Service worker registration failed: ${error}`);
                return null;
            }
        }

        await navigator.serviceWorker.ready;

        const controller = navigator.serviceWorker.controller;
        if (!controller) {
            // A hard reset disables workers. There might be other similar
            // reliability issues, like private browsing, etc
            log.warning("Service worker controller not found.");
            return null;
        }

        return controller;
    }

    async setupInServiceWorker(serviceWorkerUrl: any) {
        log.info('Setting up storage service in service worker', serviceWorkerUrl);

        let controller = await this.registerServiceWorker(serviceWorkerUrl);
        if (!controller) {
            throw new Error("Service worker not available");
        }

        // Create MessageChannel for proper Comlink communication
        const messageChannel = new MessageChannel();
        const port1 = messageChannel.port1;
        const port2 = messageChannel.port2;

        log.info('Created MessageChannel for Comlink communication');

        // Send port2 to the service worker
        controller.postMessage({ type: 'CONNECT_STORAGE' }, [port2]);
        log.info('Sent MessageChannel port to service worker');

        // Store reference to the worker
        this._worker = controller;

        // Wrap port1 for communication
        let service = Comlink.wrap<StorageServiceActualInterface>(port1);
        log.info('Wrapped MessageChannel port1 with Comlink');

        // Confirm the connection works
        try {
            let testResult = await service.test();
            log.info('Service worker test passed, result:', testResult);
        } catch (e) {
            log.error('Service worker test failed:', e);
            throw Error(`Service worker test failed: ${e}`);
        }

        this._service = service;
        console.log('Remote service initialized', service)
    }


    setWorkerRegistration(registration: ServiceWorkerRegistration) {
        this._workerRegistration = registration;
        if (registration.installing) {
            console.log("Setting registration. State: installing");
        } else if (registration.waiting) {
            console.log("Setting registration. State: installed");
        } else if (registration.active) {
            console.log("Setting registration. State: active");
        }
        console.log("Scope: ", registration.scope);
    }

    // Proxy interface methods
    async loadProject(projectId: string, projectStorageConfig: ProjectStorageConfig, commitNotify: RepoUpdateNotifiedSignature): Promise<void> {
        log.info('Loading project', projectId)

        // Enforce one project per tab restriction
        if (this._currentProjectId) {
            throw new Error(`Cannot load project ${projectId}. Project ${this._currentProjectId} is already loaded. Only one project per tab is allowed.`);
        }

        // Store current project and callback
        this._currentProjectId = projectId;
        this._localStorageUpdateCallback = commitNotify;

        log.info('Loading project with storage config', projectStorageConfig)
        await this.service.loadProject(projectId, projectStorageConfig);
        log.info('Loaded project', projectId)
    }
    async unloadProject(projectId: string): Promise<void> {
        log.info('Unloading project', projectId)

        if (this._currentProjectId !== projectId) {
            throw new Error(`Trying to unload a project that is not the current project: ${projectId}`);
        }

        // Clear current project and callback
        this._currentProjectId = null;
        this._localStorageUpdateCallback = null;

        await this.service.unloadProject(projectId);
        log.info('Unloaded project', projectId)
    }
    async deleteProject(projectId: string, projectStorageConfig: ProjectStorageConfig): Promise<void> {
        return this.service.deleteProject(projectId, projectStorageConfig);
    }
    async _storageOperationRequest(request: StorageOperationRequest): Promise<any> {
        return this.service._storageOperationRequest(request);
    }
    commit(projectId: string, deltaData: DeltaData, message: string) {
        let request = createCommitRequest(projectId, deltaData, message)
        this._storageOperationRequest(request).catch((error) => {
            log.error('Error committing', error)
        })
    }
    headState(projectId: string): Promise<SerializedStoreData> {
        return this.service.headState(projectId);
    }

    // Media operations
    async addMedia(projectId: string, blob: Blob, path: string): Promise<MediaItemData> {
        return this.service.addMedia(projectId, blob, path);
    }

    async getMedia(projectId: string, mediaId: string, mediaHash: string): Promise<Blob> {
        return this.service.getMedia(projectId, mediaId, mediaHash);
    }

    async removeMedia(projectId: string, mediaId: string, mediaHash: string): Promise<void> {
        return this.service.removeMedia(projectId, mediaId, mediaHash);
    }

    async moveMediaItemToTrash(projectId: string, mediaId: string, mediaHash: string): Promise<void> {
        return this.service.moveMediaItemToTrash(projectId, mediaId, mediaHash);
    }


    async test() {
        return this.service.test();
    }
}

export class StorageServiceActual implements StorageServiceActualInterface {
    /**
     * This service provides an interface for the storage management.
     *
     * When the service worker is available - it's run in it and is used via
     * wrappers in all windows/tabs (to save on resources).
     *
     * Uses reference counting to manage repository lifecycle - repos are loaded
     * on first request and unloaded when no more references exist.
     */
    id: string = createId(8)
    private repoManagers: { [key: string]: ProjectStorageManager } = {}; // Per projectId
    private repoRefCounts: { [key: string]: number } = {}; // Per projectId - reference counting
    private _storageOperationBroadcaster: BroadcastChannel;
    private _storageOperationReceiver: BroadcastChannel;
    private _storageOperationQueue: StorageOperationRequest[] = [];

    // Media deletion tracking - sorted by timeDeleted for efficient cleanup
    private deletedMediaItems: { [projectId: string]: MediaItem[] } = {}; // Per projectId, sorted by timeDeleted
    private mediaRequestParser?: MediaRequestParser;

    constructor(mediaRequestParser?: MediaRequestParser) {
        this.mediaRequestParser = mediaRequestParser;
        this._storageOperationBroadcaster = new BroadcastChannel(LOCAL_STORAGE_UPDATE_CHANNEL);
        this._storageOperationReceiver = new BroadcastChannel(LOCAL_STORAGE_UPDATE_CHANNEL);

        this._storageOperationReceiver.onmessage = (message) => {
            this._onLocalStorageUpdate(message.data);
        };

        // setInterval(() => {
        //     log.info(`StorageServiceActual heartbeat. ID: ${this.id}. Loaded repos: ${Object.keys(this.repoManagers).join(', ')}`);
        // }, 5000);
    }
    get inWorker(): boolean { // Might need to be more specific?
        return typeof self !== 'undefined';
    }

    test() {
        log.info('Test!!!!!!!!!')
        return true
    }

    setupMediaRequestInterception() {
        /**
         * Sets up the media request interception for the service worker.
         * For the desktop-app and offline-webapp scenarios we intercept
         * media requests to serve the media files from the respective storage
         */
        if (!this.inWorker) {
            throw new Error('Media request interception can only be set up in a service worker context');
        }

        // Check if fetch event interception is available
        if (typeof self.addEventListener !== 'function') {
            throw new Error('Service worker fetch event interception is not available. Cannot set up media cache interception.');
        }


        // Set up fetch event listener for media requests only if a handler is provided
        if (this.mediaRequestParser) {
            self.addEventListener('fetch', this.handleFetch);
            log.info('Set up global fetch interception for media requests in service worker');
        } else {
            log.info('No media request handler set up. Skipping fetch interception setup.')
        }
    }

    // Set up fetch event listener for media requests
    handleFetch = (event: Event) => {
        const fetchEvent = event as FetchEvent;
        const url = fetchEvent.request.url;

        log.info(`Intercepting fetch request for URL: ${url}`);

        // Parse the URL using the media request handler
        const mediaRequest = this.mediaRequestParser!(this, url);

        if (mediaRequest) {
            fetchEvent.respondWith(this.handleMediaRequest(fetchEvent.request, mediaRequest));
        }
    };

    async handleMediaRequest(request: Request, mediaRequest: MediaRequest): Promise<Response> {
        log.info(`Handling media request for URL: ${request.url}`, mediaRequest);
        try {
            if (!mediaRequest.mediaItemId || !mediaRequest.projectId) {
                log.warning(`Invalid media URL format: ${request.url}`);
                return new Response('Invalid media URL format', {
                    status: 400,
                    statusText: 'Bad Request',
                    headers: { 'Content-Type': 'text/plain' }
                });
            }

            // Get project storage manager for the project
            const repoManager = this.repoManagers[mediaRequest.projectId];
            if (!repoManager) {
                log.warning(`No repo manager found for project: ${mediaRequest.projectId}`);
                return new Response('Project not found', {
                    status: 404,
                    statusText: 'Not Found',
                    headers: { 'Content-Type': 'text/plain' }
                });
            }
            if (mediaRequest.mediaItemContentHash === undefined) {
                log.warning(`Media item content hash is required: ${request.url}`);
                return new Response('Media item content hash is required', {
                    status: 400,
                    statusText: 'Bad Request',
                    headers: { 'Content-Type': 'text/plain' }
                });
            }

            const blob = await repoManager.mediaStore.getMedia(mediaRequest.mediaItemId, mediaRequest.mediaItemContentHash);
            log.info(`Serving media from storage: ${mediaRequest.mediaItemId}, hash: ${mediaRequest.mediaItemContentHash}`);
            return new Response(blob, {
                headers: {
                    'Content-Type': blob.type,
                    'Content-Length': blob.size.toString(),
                }
            });

        } catch (error) {
            log.warning(`Media not found: ${request.url}`, error);
            return new Response('Media not found', {
                status: 404,
                statusText: 'Not Found',
                headers: { 'Content-Type': 'text/plain' }
            });
        }
    }

    async loadProject(projectId: string, projectStorageConfig: ProjectStorageConfig): Promise<void> {
        /**
         * Creates the Repo manager (if not already present) and increments reference count.
         */
        let repoManager = this.repoManagers[projectId];
        if (!repoManager) {
            repoManager = new ProjectStorageManager(this, projectStorageConfig);
            this.repoManagers[projectId] = repoManager;
            await repoManager.init();

            this.repoRefCounts[projectId] = 0;

            // Initialize deleted media items tracking for this project
            if (!this.deletedMediaItems[projectId]) {
                this.deletedMediaItems[projectId] = [];
            }

            log.info('Initialized repo manager for project', projectId);

            // Perform cleanup of old deleted media items on startup
            await this.cleanupExpiredTrashItems(projectId);
        } else { // Repo already loaded
            log.info('Repo already loaded', projectId);
            log.info('Loaded repo manager for project', JSON.stringify(repoManager.config));
            // Check that the configs are the same
            // deep compare the configs
            let configsAreTheSame = JSON.stringify(repoManager.config) === JSON.stringify(projectStorageConfig);
            if (!configsAreTheSame) {
                log.error('Repo already loaded with different config', projectId);
                log.error('Loaded config', repoManager.config);
                log.error('Requested config', projectStorageConfig);
                throw new RepositoryConfigMismatchError(`Repository config mismatch for project ${projectId}. Loaded config: ${JSON.stringify(repoManager.config)}, requested config: ${JSON.stringify(projectStorageConfig)}`);
            }
        }

        // Increment reference count
        this.repoRefCounts[projectId]++;
    }

    async unloadProject(projectId: string): Promise<void> {
        /**
         * Decrements reference count and unloads the repo if no more references exist.
         */
        if (!this.repoRefCounts[projectId]) {
            log.warning('Trying to unload a project that is not loaded:', projectId);
            return;
        }

        this.repoRefCounts[projectId]--;

        if (this.repoRefCounts[projectId] <= 0) {
            let repoManager = this.repoManagers[projectId];
            delete this.repoManagers[projectId];
            delete this.repoRefCounts[projectId];
            delete this.deletedMediaItems[projectId];
            repoManager.shutdown();
            log.info('Unloaded repo for project', projectId);
        }
    }
    async deleteProject(projectId: string, projectStorageConfig: ProjectStorageConfig): Promise<void> {
        // Delete the local storage
        let projectStorageManager = this.repoManagers[projectId];

        // If the repo manager is not loaded - load it temporarily
        let wasTemporarilyLoaded = false;
        if (projectStorageManager === undefined) {
            log.info('Loading repo temporarily for deletion', projectId);
            await this.loadProject(projectId, projectStorageConfig);
            wasTemporarilyLoaded = true;
            log.info('Loaded repo temporarily for deletion', projectId);
        }
        projectStorageManager = this.repoManagers[projectId];
        await projectStorageManager.localStorageRepo.eraseStorage();
        log.info('Erased local storage for project', projectId);

        if (wasTemporarilyLoaded) { // Unload tmp repo
            await this.unloadProject(projectId);
            log.info('Unloaded temporary repo', projectId);
        }
    }

    async _storageOperationRequest(request: StorageOperationRequest): Promise<void> {
        log.info('Storage operation request made', request)
        // This is a wrapper for the actual storage operation
        // It's used to queue operations and execute them in order
        this._storageOperationQueue.push(request);

        // Call queue processing deferred
        setTimeout(() => {
            this.processStorageOperationQueue().catch((error) => {
                log.error('Error processing storage operation queue', error)
            });
        });
    }
    async _exectuteCommitRequest(request: CommitRequest): Promise<void> {
        console.log('Type is commit')
        let commitRequest = request as CommitRequest;
        let projectStorageManager = this.repoManagers[commitRequest.projectId];

        // Commit to in-mem
        let commit = await projectStorageManager.inMemoryRepo.commit(new Delta(commitRequest.deltaData), commitRequest.message);
        console.log('Created commit', commit)

        // Integrity check (TMP)
        let hashTree = await buildHashTree(projectStorageManager.inMemoryRepo.headStore);
        let currentHash = projectStorageManager.inMemoryRepo.hashTree.rootHash();

        if (currentHash !== hashTree.rootHash()) {
            log.error('Hash tree integrity check failed',
                'Current hash:', currentHash,
                'Expected hash:', hashTree.rootHash());
            return;
        }

        // Save in local storage
        log.info('Pulling the new commit from the adapter into the project inMem repo')
        await projectStorageManager.localStorageRepo.pull(projectStorageManager.inMemoryRepo);

        // Notify subscribers
        this.broadcastLocalUpdate({
            projectId: commitRequest.projectId,
            storageServiceId: this.id,
            update: {
                commitGraph: this.repoManagers[commitRequest.projectId].inMemoryRepo.commitGraph.data(),
                newCommits: [commit.data()]
            }
        });
    }
    async _executeStorageOperationRequest(request: StorageOperationRequest): Promise<void> {
        log.info('Executing storage operation request', request)
        if (request.type === 'commit') {
            await this._exectuteCommitRequest(request as CommitRequest);
        } else {
            log.error('Unknown storage operation request', request)
        }
    }

    async processStorageOperationQueue() {
        log.info('Processing storage operation queue')
        for (let request of this._storageOperationQueue) {
            try {
                await this._executeStorageOperationRequest(request);
            } catch (error) {
                log.error('Error processing storage operation request', request, error)
            }
        }
        this._storageOperationQueue = [];
    }

    broadcastLocalUpdate(update: LocalStorageUpdateMessage) {
        log.info('Broadcasting local storage update', update);
        this._storageOperationBroadcaster.postMessage(update);
    }

    _onLocalStorageUpdate(updateMessage: LocalStorageUpdateMessage) {
        log.info('Received local storage update', updateMessage)

        // (Edge case) If there's more than one local storage service
        // (e.g. hard refresh and no service worker) - react on updates by pulling
        if (updateMessage.storageServiceId === this.id) {
            if (updateMessage.projectId in this.repoManagers) {
                let repoManager = this.repoManagers[updateMessage.projectId];
                repoManager.inMemoryRepo.pull(repoManager.localStorageRepo).then(
                    () => {
                        this._notifySubscribers(updateMessage.projectId, updateMessage.update);
                    }
                ).catch((error) => {
                    log.error('Error pulling local storage update', error)
                });
            }
        } else {
            // Notify all subscribers
            this._notifySubscribers(updateMessage.projectId, updateMessage.update);
        }
    }

    _notifySubscribers(projectId: string, update: RepoUpdateData) {
        // No-op in service worker - notifications handled via BroadcastChannel
        // The main thread StorageService will handle callback notifications
    }
    async headState(projectId: string): Promise<SerializedStoreData> {
        let repoManager = this.repoManagers[projectId];
        if (!repoManager) {
            throw new Error("Repo not loaded");
        }
        return repoManager.inMemoryRepo.headStore.data();
    }
    // Media operations
    async addMedia(projectId: string, blob: Blob, path: string): Promise<MediaItemData> {
        let repoManager = this.repoManagers[projectId];
        if (!repoManager) {
            throw new Error("Repo not loaded");
        }

        // Generate a unique path using the utility function and check against the in-memory repository
        const uniquePath = generateUniquePathWithSuffix(path, (checkPath: string) => {
            // Check if any entity with this path exists in the in-memory repository
            return !!repoManager.inMemoryRepo.headStore.findOne({ path: checkPath });
        });

        log.info(`Adding media to project ${projectId} with unique path: ${uniquePath}`);
        return repoManager.mediaStore.addMedia(blob, uniquePath); // Use the unique path
    }

    async getMedia(projectId: string, mediaId: string, mediaHash: string): Promise<Blob> {
        let repoManager = this.repoManagers[projectId];
        if (!repoManager) {
            throw new Error("Repo not loaded");
        }
        return repoManager.mediaStore.getMedia(mediaId, mediaHash);
    }

    async removeMedia(projectId: string, mediaId: string, mediaHash: string): Promise<void> {
        let repoManager = this.repoManagers[projectId];
        if (!repoManager) {
            throw new Error("Repo not loaded");
        }

        // Create a MediaItem instance for the adapter
        const mediaItem = new MediaItem({ id: mediaId, path: '', contentHash: mediaHash, width: 0, height: 0, mimeType: '', size: 0 });
        return repoManager.mediaStore.removeMedia(mediaItem);
    }

    async moveMediaItemToTrash(projectId: string, mediaId: string, mediaHash: string): Promise<void> {
        let repoManager = this.repoManagers[projectId];
        if (!repoManager) {
            throw new Error("Repo not loaded");
        }

        // Create a MediaItem instance and mark it as deleted
        const mediaItem = new MediaItem({ id: mediaId, path: '', contentHash: mediaHash, width: 0, height: 0, mimeType: '', size: 0 });
        mediaItem.markDeleted();

        // Add to deleted media items index
        if (!this.deletedMediaItems[projectId]) {
            this.deletedMediaItems[projectId] = [];
        }

        const deletedItems = this.deletedMediaItems[projectId];
        deletedItems.push(mediaItem);

        // Sort by timeDeleted to maintain order for efficient cleanup
        deletedItems.sort((a, b) => a.timeDeleted! - b.timeDeleted!);

        log.info(`Moved media item to trash: ${mediaItem.path}, timeDeleted: ${mediaItem.timeDeleted}`);
    }

    /**
     * Clean up deleted media items older than the specified retention period
     * Default retention period is 30 days (30 * 24 * 60 * 60 * 1000 ms)
     */
    private async cleanupExpiredTrashItems(projectId: string, retentionPeriodMs: number = 30 * 24 * 60 * 60 * 1000): Promise<void> {
        const deletedItems = this.deletedMediaItems[projectId];
        if (!deletedItems || deletedItems.length === 0) {
            return;
        }

        const cutoffTime = Date.now() - retentionPeriodMs;
        let itemsToRemove = 0;

        // Since the array is sorted by timeDeleted, we can find the cutoff point efficiently
        for (let i = 0; i < deletedItems.length; i++) {
            if (deletedItems[i].timeDeleted! > cutoffTime) {
                break;
            }
            itemsToRemove++;
        }

        if (itemsToRemove === 0) {
            log.info(`No deleted media items to clean up for project ${projectId}`);
            return;
        }

        const repoManager = this.repoManagers[projectId];
        if (!repoManager) {
            log.warning(`Repo not loaded for project ${projectId}, skipping cleanup`);
            return;
        }

        // Remove the expired items from storage
        const itemsToDelete = deletedItems.splice(0, itemsToRemove);
        for (const item of itemsToDelete) {
            try {
                await repoManager.mediaStore.removeMedia(item);
                log.info(`Permanently deleted expired media item: ${item.path}`);
            } catch (error) {
                log.error(`Failed to delete expired media item ${item.path}:`, error);
            }
        }

        log.info(`Cleaned up ${itemsToDelete.length} expired deleted media items for project ${projectId}`);
    }
}
