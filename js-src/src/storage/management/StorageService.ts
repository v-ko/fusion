import * as Comlink from 'comlink';
import { ProjectStorageManager, ProjectStorageConfig } from './ProjectStorageManager';
import { SerializedStoreData } from '../domain-store/BaseStore';
import { Delta, DeltaData } from '../../model/Delta';
import { RepoUpdateData } from "../repository/Repository"
import { createId } from '../../util/base';
import { buildHashTree } from '../version-control/HashTree';
import { MediaItemData } from '../../model/MediaItem';
import { generateUniquePathWithSuffix } from "../../util/secondary";
import { getLogger } from '../../logging';
import { addChannel, Channel, getChannel, Subscription } from '../../registries/Channel';

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
    createProject: (projectId: string, projectStorageConfig: ProjectStorageConfig) => Promise<void>;
    unloadProject: (projectId: string) => Promise<void>;
    deleteProject: (projectId: string, projectStorageConfig: ProjectStorageConfig) => Promise<void>;
    headState: (projectId: string) => Promise<SerializedStoreData>;

    // Repo changes (mostly commits to the domain store as of now) can come from
    // different sources (the UI/FDS, remote storage sync adapters), so they operate
    // like a queue - many sources push requests, and the tabs receive the
    // updates via the broadcast channel to consume any changes that don'l
    // source from them.
    _storageOperationRequest: (request: StorageOperationRequest) => Promise<void>;

    // Media operations
    addMedia: (projectId: string, blob: Blob, path: string, parentId: string) => Promise<MediaItemData>;
    getMedia: (projectId: string, mediaId: string, mediaHash: string) => Promise<Blob>;
    removeMedia: (projectId: string, mediaId: string, mediaHash: string) => Promise<void>;
    moveMediaToTrash: (projectId: string, mediaId: string, mediaHash: string) => Promise<void>;

    test(): boolean;
    disconnect: () => void;
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


export const LOCAL_STORAGE_UPDATE_CHANNEL = 'storage-service-local-storage-update-channel';

// Helper to create a storage channel with backend auto-selection
// The policy here is to create the channel for the app scope and reuse it
// Cleanup is not a concern until it becomes one
export function getStorageUpdatesChannel(name: string): Channel {
    const backend = (typeof BroadcastChannel !== 'undefined') ? 'broadcast' : 'local';
    let channel = getChannel(name);
    if (!channel) {
        channel = addChannel(name, { backend: backend });
    }
    return channel
}

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
    _localUpdateChannel: Channel | null = null;
    _localUpdateSubscription: Subscription | null = null;
    _currentProjectId: string | null = null; // Only one project allowed per tab
    _localStorageUpdateCallback: RepoUpdateNotifiedSignature | null = null; // Callback for current project
    _isWrapper: boolean = false; // whether this is a wrapper for the service worker or a main thread instance

    constructor() {
        // Create the channel for receiving updates (broadcast if available, else local)
        this._localUpdateChannel = getStorageUpdatesChannel(LOCAL_STORAGE_UPDATE_CHANNEL);
        this._localUpdateSubscription = this._localUpdateChannel.subscribe(this._handleChannelMessage.bind(this));
    }

    _handleChannelMessage(updateMessage: LocalStorageUpdateMessage) {
        log.info('Received local storage update', updateMessage);

        if (this._currentProjectId === updateMessage.projectId && this._localStorageUpdateCallback) {
            this._localStorageUpdateCallback(updateMessage.update);
        }
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

    async setupInServiceWorker(serviceWorkerUrl: string) {
        this._isWrapper = true;
        await this.registerServiceWorker(serviceWorkerUrl);
        await this.waitForController();      // <-- handles first install
        await this.connectToWorker();

        // Reconnect after updates / skipWaiting
        navigator.serviceWorker.addEventListener('controllerchange', () => {
            this.connectToWorker().catch(err => log.error('Reconnect after update failed', err));
        });
    }

    async registerServiceWorker(serviceWorkerUrl: string): Promise<ServiceWorkerRegistration> {
        if (!('serviceWorker' in navigator)) {
            throw new Error('Service workers are not supported in this browser.');
        }
        if (this._workerRegistration) {
            throw new Error('Service worker already registered. Cannot register again.');
        }
        log.info('Registering service worker ', serviceWorkerUrl);
        const registration = await navigator.serviceWorker.register(serviceWorkerUrl, { type: 'module', scope: '/' });
        this.setWorkerRegistration(registration);
        return registration;
    }

    private async waitForController(): Promise<void> {
        if (navigator.serviceWorker.controller) return;
        await new Promise<void>((resolve, reject) => {
            const to = setTimeout(() => reject(new Error('SW never took control')), 15000);
            navigator.serviceWorker.addEventListener('controllerchange', () => {
                clearTimeout(to);
                resolve();
            }, { once: true });
        });
    }

    private async connectToWorker(): Promise<void> {
        const controller = navigator.serviceWorker.controller!;
        const { port1, port2 } = new MessageChannel();
        controller.postMessage({ type: 'CONNECT_STORAGE' }, [port2]);
        this._worker = controller;

        const service = Comlink.wrap<StorageServiceActualInterface>(port1);

        try {
            await service.test();
        } catch (e) {
            log.error('Service worker test failed:', e);
            throw new Error(`Service worker test failed: ${e}`);
        }
        this._service = service;
        log.info('Remote service initialized');
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
        // Enforce one project per tab restriction
        if (this._currentProjectId) {
            throw new Error(`Cannot load project ${projectId}. Project ${this._currentProjectId} is already loaded. Only one project per tab is allowed.`);
        }

        log.info('Loading project with storage config', projectStorageConfig)
        await this.service.loadProject(projectId, projectStorageConfig);

        // Store current project and callback
        this._currentProjectId = projectId;
        this._localStorageUpdateCallback = commitNotify;
    }
    async unloadProject(projectId: string): Promise<void> {
        if (this._currentProjectId !== projectId) {
            throw new Error(`Trying to unload a project that is not the current project: ${projectId}`);
        }

        await this.service.unloadProject(projectId);
        log.info('Unloaded project', projectId)

        // Clear current project and callback
        this._currentProjectId = null;
        this._localStorageUpdateCallback = null;
    }
    async deleteProject(projectId: string, projectStorageConfig: ProjectStorageConfig): Promise<void> {
        return this.service.deleteProject(projectId, projectStorageConfig);
    }
    async createProject(projectId: string, projectStorageConfig: ProjectStorageConfig): Promise<void> {
        return this.service.createProject(projectId, projectStorageConfig);
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
    async addMedia(projectId: string, blob: Blob, path: string, parentId: string): Promise<MediaItemData> {
        return this.service.addMedia(projectId, blob, path, parentId);
    }

    async getMedia(projectId: string, mediaId: string, mediaHash: string): Promise<Blob> {
        return this.service.getMedia(projectId, mediaId, mediaHash);
    }

    async removeMedia(projectId: string, mediaId: string, mediaHash: string): Promise<void> {
        return this.service.removeMedia(projectId, mediaId, mediaHash);
    }

    async moveMediaToTrash(projectId: string, mediaId: string, mediaHash: string): Promise<void> {
        return this.service.moveMediaToTrash(projectId, mediaId, mediaHash);
    }


    async test() {
        return this.service.test();
    }

    async unregisterServiceWorker() {
        if (!this._workerRegistration) {
            throw new Error('Service worker registration not found. Cannot restart service worker.');
        }

        log.info('Unregistering service worker...');
        await this._workerRegistration.unregister();
        log.info('Service worker unregistered. Reloading page to re-register...');
        window.location.reload();
    }

    async checkForUpdate(): Promise<'waiting' | 'none' | 'no-reg'> {
        const reg = this._workerRegistration;
        if (!reg) return 'no-reg';
        await reg.update();
        return reg.waiting ? 'waiting' : 'none';
    }

    async applyUpdateNow(): Promise<void> {
        const reg = this._workerRegistration;
        if (!reg?.waiting) return;
        const swapped = new Promise<void>(resolve =>
            navigator.serviceWorker.addEventListener('controllerchange', () => resolve(), { once: true })
        );
        reg.waiting.postMessage({ type: 'SKIP_WAITING' });
        await swapped; // controller replaced; connectToWorker() listener will fire
    }

    disconnect() {
        this._localUpdateSubscription?.unsubscribe();

        if (this._service && !this._isWrapper) {
            // eslint-disable-next-line
            this._service.disconnect();
        }
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
    private _storageUpdateChannel: Channel;
    private _storageUpdateSubscription: Subscription | null = null;
    private _storageOperationQueue: StorageOperationRequest[] = [];
    private _processing = false;
    private mediaRequestParser?: MediaRequestParser;

    constructor(mediaRequestParser?: MediaRequestParser) {
        this.mediaRequestParser = mediaRequestParser;

        // Single channel for broadcasting and receiving storage updates
        this._storageUpdateChannel = getStorageUpdatesChannel(LOCAL_STORAGE_UPDATE_CHANNEL);
        this._storageUpdateSubscription = this._storageUpdateChannel.subscribe(
            (message: LocalStorageUpdateMessage) => this._onLocalStorageUpdate(message)
        );

        // setInterval(() => {
        //     log.info(`StorageServiceActual heartbeat. ID: ${this.id}. Loaded repos: ${Object.keys(this.repoManagers).join(', ')}`);
        // }, 5000);
    }
    get inWorker(): boolean {
        // @ts-ignore: ServiceWorkerGlobalScope is global only in SW
        return typeof ServiceWorkerGlobalScope !== 'undefined' && self instanceof ServiceWorkerGlobalScope;
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

        // log.info(`Intercepting fetch request for URL: ${url}`);

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
            repoManager = new ProjectStorageManager(projectStorageConfig);
            this.repoManagers[projectId] = repoManager;
            await repoManager.loadProject();

            this.repoRefCounts[projectId] = 0;

            log.info('Initialized repo manager for project', projectId);

            // Perform cleanup of old deleted media items on startup
            await repoManager.mediaStore.cleanTrash();
        } else { // Repo already loaded
            log.info(`Repo manager already loaded for project ${projectId}. Skipping initialization.`);
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
            repoManager.shutdown();
            log.info('Unloaded repo for project', projectId);
        }
    }

    async createProject(projectId: string, projectStorageConfig: ProjectStorageConfig): Promise<void> {
        let repoManager = this.repoManagers[projectId];
        if (repoManager) {
            throw new Error(`Project ${projectId} already exists.`);
        }

        repoManager = new ProjectStorageManager(projectStorageConfig);
        await repoManager.createProject();

        console.log(`[createProject] Created repo with head store ${JSON.stringify(repoManager._onDeviceRepo?.headStore)}`)
        // this.repoManagers[projectId] = repoManager;
        await repoManager.shutdown();  // Just create it, load separately

        this.repoRefCounts[projectId] = 0;

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
        await projectStorageManager.onDeviceRepo.eraseStorage();
        log.info('Erased local storage for project', projectId);

        if (wasTemporarilyLoaded) { // Unload tmp repo
            await this.unloadProject(projectId);
            log.info('Unloaded temporary repo', projectId);
        }
    }

    async _storageOperationRequest(request: StorageOperationRequest): Promise<void> {
        this._storageOperationQueue.push(request);
        if (!this._processing) await this.processStorageOperationQueue();
    }

    private async processStorageOperationQueue(): Promise<void> {
        if (this._processing) return;
        this._processing = true;
        try {
            while (this._storageOperationQueue.length) {
                const req = this._storageOperationQueue.shift()!;
                await this._executeStorageOperationRequest(req);
            }
        } catch (error) {
            log.error('Error processing storage operation queue', error);
        } finally {
            this._processing = false;
        }
    }

    async _executeCommitRequest(request: CommitRequest): Promise<void> {
        console.log('Type is commit')
        let commitRequest = request as CommitRequest;
        let projectStorageManager = this.repoManagers[commitRequest.projectId];

        // Commit to in-mem
        let commit = await projectStorageManager.onDeviceRepo.commit(new Delta(commitRequest.deltaData), commitRequest.message);
        console.log('Created commit', commit)

        // Integrity check (TMP)
        let hashTree = await buildHashTree(projectStorageManager.onDeviceRepo.headStore);
        let currentHash = projectStorageManager.onDeviceRepo.hashTree.rootHash();

        if (currentHash !== hashTree.rootHash()) {
            log.error('Hash tree integrity check failed',
                'Current hash:', currentHash,
                'Expected hash:', hashTree.rootHash());
            return;
        }

        // // Save in local storage
        // log.info('Pulling the new commit from the adapter into the project inMem repo')
        // await projectStorageManager.onDeviceRepo.pull(projectStorageManager.onDeviceRepo);

        // Notify subscribers
        let commitGraph = await this.repoManagers[commitRequest.projectId].onDeviceRepo.getCommitGraph()
        this.broadcastLocalUpdate({
            projectId: commitRequest.projectId,
            storageServiceId: this.id,
            update: {
                commitGraph: commitGraph.data(),
                newCommits: [commit.data()]
            }
        });
    }
    async _executeStorageOperationRequest(request: StorageOperationRequest): Promise<void> {
        if (request.type === 'commit') {
            await this._executeCommitRequest(request as CommitRequest);
        } else {
            log.error('Unknown storage operation request', request)
        }
    }


    broadcastLocalUpdate(update: LocalStorageUpdateMessage) {
        log.info('Broadcasting local storage update', update);
        this._storageUpdateChannel.push(update);
    }

    _onLocalStorageUpdate(updateMessage: LocalStorageUpdateMessage) {
        log.info('Received local storage update', updateMessage)

        // (Edge case) If there's more than one local storage service
        // (e.g. hard refresh and no service worker) - react on updates by pulling
        // Should be done only for updates that are not from this service
        if (updateMessage.storageServiceId !== this.id && updateMessage.projectId in this.repoManagers) {
            let repoManager = this.repoManagers[updateMessage.projectId];
            repoManager.onDeviceRepo.pull(repoManager.onDeviceRepo).catch((error) => {
                log.error('Error pulling local storage update', error)
            });
        }
    }

    async headState(projectId: string): Promise<SerializedStoreData> {
        let repoManager = this.repoManagers[projectId];
        if (!repoManager) {
            throw new Error("Repo not loaded");
        }
        return repoManager.onDeviceRepo.headStore.data();
    }
    // Media operations
    async addMedia(projectId: string, blob: Blob, path: string, parentId: string): Promise<MediaItemData> {
        let repoManager = this.repoManagers[projectId];
        if (!repoManager) {
            throw new Error("Repo not loaded");
        }

        // Generate a unique path using the utility function and check against the in-memory repository
        const uniquePath = generateUniquePathWithSuffix(path, (checkPath: string) => {
            // Check if any entity with this path exists in the in-memory repository
            return !!repoManager.onDeviceRepo.headStore.findOne({ path: checkPath });
        });

        log.info(`Adding media to project ${projectId} with unique path: ${uniquePath}`);
        return repoManager.mediaStore.addMedia(blob, uniquePath, parentId); // Use the unique path
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

        return repoManager.mediaStore.removeMedia(mediaId, mediaHash);
    }

    async moveMediaToTrash(projectId: string, mediaId: string, mediaHash: string): Promise<void> {
        let repoManager = this.repoManagers[projectId];
        if (!repoManager) {
            throw new Error("Repo not loaded");
        }
        return repoManager.mediaStore.moveMediaToTrash(mediaId, mediaHash);
    }

    disconnect() {
        this._storageUpdateSubscription?.unsubscribe();
    }
}
