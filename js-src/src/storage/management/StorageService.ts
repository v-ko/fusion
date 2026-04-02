import * as Comlink from 'comlink';
import { ProjectStorageManager, ProjectStorageConfig } from './ProjectStorageManager';
import { Delta, DeltaData } from '../../model/Delta';
import { RepoUpdateData } from "../repository/Repository"
import { createId } from '../../util/base';
import { FileItemData, FileItemMetadata } from '../../model/FileItem';
import { generateUniquePathWithSuffix } from "../../util/secondary";
import { getLogger } from '../../logging';
import { addChannel, Channel, getChannel, Subscription } from '../../registries/Channel';
import { CommitData } from '../version-control/Commit';
import { CommitGraphData } from '../version-control/CommitGraph';
import { squashBranchHistory } from './sync-utils';

let log = getLogger('StorageService')

// Simple policy: squash commits older than this TTL into the first commit (J=0)
const SQUASH_TTL_MS = 15 * 60 * 1000 // TODO: Should be passed as a parameter

export interface FileRequest {
    projectId: string;
    fileItemId: string;
    fileItemContentHash?: string;
}

export interface ProjectData {
    id: string;
    title: string;
    description: string;
    created: string;
}

export class RepositoryConfigMismatchError extends Error {
    constructor(message: string) {
        super(message);
        this.name = "RepositoryConfigMismatchError";
    }
}

export type FileRequestParser = (storageService: StorageServiceActual, url: string) => FileRequest | null;

export type RepoUpdateNotifiedSignature = (update: RepoUpdateData) => void;

export function deriveProjectUri(projectId: string, adapterName: string): string {
    switch (adapterName) {
        case 'IndexedDB':
            return `indexeddb:///${projectId}`;
        case 'RestApi':
            return `file:///${projectId}`;
        default:
            return `project:///${projectId}`;
    }
}

export interface StorageServiceActualInterface {
    loadProject: (projectId: string, repoManagerConfig: ProjectStorageConfig, projectUri?: string) => Promise<void>;
    createProject: (projectId: string, projectStorageConfig: ProjectStorageConfig, projectProperties?: ProjectData) => Promise<string>;
    unloadProject: (projectId: string) => Promise<void>;
    removeProject: (projectId: string, projectStorageConfig: ProjectStorageConfig) => Promise<void>;
    getCommitGraph: (projectId: string) => Promise<CommitGraphData>;
    getCommits: (projectId: string, commitIds: string[]) => Promise<CommitData[]>;
    getProjectProperties: (projectId: string) => Promise<ProjectData | null>;
    setProjectProperties: (projectId: string, projectProperties: ProjectData) => Promise<void>;

    // Repo changes (mostly commits to the domain store as of now) can come from
    // different sources (the UI/FDS, remote storage sync adapters), so they operate
    // like a queue - many sources push requests, and the tabs receive the
    // updates via the broadcast channel to consume any changes that don't
    // source from them.
    _storageOperationRequest: (request: StorageOperationRequest) => Promise<StorageOperationResult>;

    // File operations
    addFile: (projectId: string, blob: Blob, path: string, parentId: string, metadata: FileItemMetadata) => Promise<FileItemData>;
    getFile: (projectId: string, fileId: string, fileHash: string) => Promise<Blob>;
    removeFile: (projectId: string, fileId: string, fileHash: string) => Promise<void>;

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
export interface StorageOperationResult {
    type: string;
}

export interface CommitOperationResult extends StorageOperationResult {
    type: 'commit';
    commit: CommitData;
}

function createCommitRequest(projectId: string, deltaData: DeltaData, message: string): CommitRequest {
    return {
        type: 'commit',
        projectId: projectId,
        deltaData: deltaData,
        message: message,
    }
}

export interface LocalStorageUpdateMessage {
    projectId: string
    storageServiceId: string
    update: RepoUpdateData
}

interface PendingStorageOperationRequest {
    request: StorageOperationRequest;
    resolve: (result: StorageOperationResult) => void;
    reject: (error: unknown) => void;
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

const STORAGE_SERVICE_CALL_TIMEOUT_MS = 10000;
const STORAGE_SERVICE_CONTROLLER_TIMEOUT_MS = 15000;

export type StorageBackendKind =
    | 'none'
    | 'service-worker'
    | 'main-thread';

export type StorageConnectionPhase =
    | 'uninitialized'
    | 'main-thread-ready'
    | 'registering'
    | 'waiting-for-controller'
    | 'connecting'
    | 'ready'
    | 'disconnected'
    | 'fatal';

export type StorageProjectPhase =
    | 'detached'
    | 'attaching'
    | 'attached'
    | 'detaching';

export type ServiceWorkerLifecycleState =
    | 'none'
    | 'installing'
    | 'installed'
    | 'activating'
    | 'activated'
    | 'redundant'
    | 'unknown';

export type StorageServiceErrorCode =
    | 'none'
    | 'sw-api-unavailable'
    | 'sw-register-failed'
    | 'sw-controller-timeout'
    | 'sw-connect-failed'
    | 'sw-connect-timeout'
    | 'sw-worker-redundant'
    | 'storage-call-timeout'
    | 'storage-call-failed';

export interface StorageServiceErrorState {
    code: StorageServiceErrorCode;
    message: string;
    operation: string | null;
    timestamp: number;
    recoverable: boolean;
    reloadRecommended: boolean;
}

export interface StorageServiceRuntimeState {
    backend: StorageBackendKind;
    connectionPhase: StorageConnectionPhase;
    projectPhase: StorageProjectPhase;
    activeProjectId: string | null;
    usingServiceWorker: boolean;
    hasController: boolean;
    connected: boolean;
    workerLifecycle: ServiceWorkerLifecycleState;
    lastReadyAt: number | null;
    lastError: StorageServiceErrorState | null;
}

interface StorageCallTrace {
    operationId: string;
    operationName: string;
    detail: string;
    startedAt: number;
}

export function createInitialStorageServiceRuntimeState(): StorageServiceRuntimeState {
    return {
        backend: 'none',
        connectionPhase: 'uninitialized',
        projectPhase: 'detached',
        activeProjectId: null,
        usingServiceWorker: false,
        hasController: false,
        connected: false,
        workerLifecycle: 'none',
        lastReadyAt: null,
        lastError: null,
    };
}

export class StorageService {
    /**
     * This is a wrapper that allows the storage service to be run in either
     * a service worker or the main thread.
     *
     * Commit, squash, merge and other storage operations are wrapped as requests
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
    _projectUpdateCallbacks: Map<string, RepoUpdateNotifiedSignature> = new Map();
    _isWrapper: boolean = false; // whether this is a wrapper for the service worker or a main thread instance
    state: StorageServiceRuntimeState = createInitialStorageServiceRuntimeState();
    private _stateChangeHandler: ((state: StorageServiceRuntimeState) => void) | null = null;
    private _workerStateListeners: WeakSet<ServiceWorker> = new WeakSet();
    private _activeProjectConfig: ProjectStorageConfig | null = null;
    private _activeProjectUpdateCallback: RepoUpdateNotifiedSignature | undefined;
    private _activeProjectUri: string | undefined;
    private _reconnectInFlight: Promise<void> | null = null;

    constructor() {
        // Create the channel for receiving updates (broadcast if available, else local)
        this._localUpdateChannel = getStorageUpdatesChannel(LOCAL_STORAGE_UPDATE_CHANNEL);
        this._localUpdateSubscription = this._localUpdateChannel.subscribe(this._handleChannelMessage.bind(this));
    }

    _handleChannelMessage(updateMessage: LocalStorageUpdateMessage) {
        log.info('Received local storage update', updateMessage);

        const callback = this._projectUpdateCallbacks.get(updateMessage.projectId);
        if (callback) {
            callback(updateMessage.update);
        }
    }

    get service(): Comlink.Remote<StorageServiceActualInterface> | StorageServiceActualInterface {
        if (!this._service) {
            throw new Error("Service not setup. Call setupInMainThread or setupInServiceWorker first.");
        }
        return this._service;
    }

    setStateChangeHandler(handler: ((state: StorageServiceRuntimeState) => void) | null) {
        this._stateChangeHandler = handler;
        this.emitState();
    }

    setupInMainThread() {
        log.info('Setting up storage service in main thread');
        this._isWrapper = false;
        this._service = new StorageServiceActual();
        this.setState({
            backend: 'main-thread',
            connectionPhase: 'main-thread-ready',
            usingServiceWorker: false,
            hasController: false,
            connected: true,
            workerLifecycle: 'none',
            lastReadyAt: Date.now(),
        });
    }

    async setupInServiceWorker(serviceWorkerUrl: string) {
        this._isWrapper = true;
        this.setState({
            backend: 'service-worker',
            connectionPhase: 'registering',
            usingServiceWorker: true,
            hasController: this.hasServiceWorkerController(),
            connected: false,
            workerLifecycle: 'unknown',
        });
        try {
            await this.registerServiceWorker(serviceWorkerUrl);
            this.setState({
                connectionPhase: 'waiting-for-controller',
                hasController: this.hasServiceWorkerController(),
            });
            await this.waitForController();
        } catch (error) {
            if (!(error instanceof Error && error.message.includes('SW never took control'))) {
                this.setError('sw-register-failed', `Failed to register the service worker: ${this.errorMessage(error)}`, {
                    connectionPhase: 'fatal',
                    recoverable: false,
                    reloadRecommended: true,
                });
                throw error;
            }
            this.setError('sw-controller-timeout', 'Timed out while waiting for the service worker to take control.', {
                connectionPhase: 'fatal',
                recoverable: false,
                reloadRecommended: true,
            });
            throw error;
        }

        await this.reconnectToWorker('fatal');

        // Reconnect after updates / skipWaiting
        navigator.serviceWorker.addEventListener('controllerchange', () => {
            this.setState({
                hasController: this.hasServiceWorkerController(),
            });
            this.reconnectToWorker('disconnected').catch(err => {
                log.error('Reconnect after update failed', err);
            });
        });
    }

    async registerServiceWorker(serviceWorkerUrl: string): Promise<ServiceWorkerRegistration> {
        if (!('serviceWorker' in navigator)) {
            this.setError('sw-api-unavailable', 'Service workers are not supported in this browser.', {
                connectionPhase: 'fatal',
                recoverable: false,
                reloadRecommended: false,
            });
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
            const to = setTimeout(() => reject(new Error('SW never took control')), STORAGE_SERVICE_CONTROLLER_TIMEOUT_MS);
            navigator.serviceWorker.addEventListener('controllerchange', () => {
                clearTimeout(to);
                resolve();
            }, { once: true });
        });
    }

    private async connectToWorker(): Promise<void> {
        if (!this.hasServiceWorkerController()) {
            throw new Error('No service worker controller available.');
        }
        const controller = navigator.serviceWorker.controller!;
        const { port1, port2 } = new MessageChannel();
        controller.postMessage({ type: 'CONNECT_STORAGE' }, [port2]);
        this._worker = controller;

        const service = Comlink.wrap<StorageServiceActualInterface>(port1);

        try {
            await this.runWithTimeout('connectToWorker', Promise.resolve(service.test()), STORAGE_SERVICE_CALL_TIMEOUT_MS);
        } catch (e) {
            log.error('Service worker test failed:', e);
            throw new Error(`Service worker test failed: ${e}`);
        }
        this._service = service;
        log.info('Remote service initialized');
    }

    setWorkerRegistration(registration: ServiceWorkerRegistration) {
        this._workerRegistration = registration;
        registration.addEventListener('updatefound', () => {
            if (registration.installing) {
                this.attachWorkerStateListener(registration.installing);
            }
        });
        if (registration.installing) {
            this.attachWorkerStateListener(registration.installing);
        }
        if (registration.waiting) {
            this.attachWorkerStateListener(registration.waiting);
        }
        if (registration.active) {
            this.attachWorkerStateListener(registration.active);
        }
        log.info('Service worker registration set. Scope:', registration.scope);
    }

    // Proxy interface methods
    async loadProject(projectId: string, projectStorageConfig: ProjectStorageConfig, commitNotify?: RepoUpdateNotifiedSignature, projectUri?: string): Promise<void> {
        log.info('Loading project with storage config', projectStorageConfig)
        this.setState({
            activeProjectId: projectId,
            projectPhase: 'attaching',
        });
        try {
            await this.runStorageCall('loadProject', `projectId=${projectId}`, () => this.service.loadProject(projectId, projectStorageConfig, projectUri));
            this._activeProjectConfig = projectStorageConfig;
            this._activeProjectUpdateCallback = commitNotify;
            this._activeProjectUri = projectUri;
            if (commitNotify) {
                this._projectUpdateCallbacks.set(projectId, commitNotify);
            }
            this.setState({
                projectPhase: 'attached',
            });
        } catch (error) {
            this._activeProjectConfig = null;
            this._activeProjectUpdateCallback = undefined;
            this._activeProjectUri = undefined;
            this._projectUpdateCallbacks.delete(projectId);
            this.setState({
                activeProjectId: null,
                projectPhase: 'detached',
            });
            throw error;
        }
    }
    async unloadProject(projectId: string): Promise<void> {
        const shouldClearActiveProject = this.state.activeProjectId === projectId;
        if (shouldClearActiveProject) {
            this.setState({
                projectPhase: 'detaching',
            });
        }
        try {
            await this.runStorageCall('unloadProject', `projectId=${projectId}`, () => this.service.unloadProject(projectId));
            log.info('Unloaded project', projectId)
        } finally {
            this._projectUpdateCallbacks.delete(projectId);
            if (shouldClearActiveProject) {
                this._activeProjectConfig = null;
                this._activeProjectUpdateCallback = undefined;
                this._activeProjectUri = undefined;
                this.setState({
                    activeProjectId: null,
                    projectPhase: 'detached',
                });
            }
        }
    }
    async removeProject(projectId: string, projectStorageConfig: ProjectStorageConfig): Promise<void> {
        return this.runStorageCall('removeProject', `projectId=${projectId}`, () => this.service.removeProject(projectId, projectStorageConfig));
    }
    async createProject(projectId: string, projectStorageConfig: ProjectStorageConfig, projectProperties?: ProjectData): Promise<string> {
        return this.runStorageCall('createProject', `projectId=${projectId}`, () => this.service.createProject(projectId, projectStorageConfig, projectProperties));
    }
    async getProjectProperties(projectId: string): Promise<ProjectData | null> {
        return this.runStorageCall('getProjectProperties', `projectId=${projectId}`, () => this.service.getProjectProperties(projectId));
    }
    async setProjectProperties(projectId: string, projectProperties: ProjectData): Promise<void> {
        return this.runStorageCall('setProjectProperties', `projectId=${projectId}`, () => this.service.setProjectProperties(projectId, projectProperties));
    }
    async _storageOperationRequest(request: StorageOperationRequest): Promise<StorageOperationResult> {
        const detail = 'projectId' in request
            ? `type=${request.type} projectId=${request.projectId}`
            : `type=${request.type}`;
        return this.runStorageCall('_storageOperationRequest', detail, () => this.service._storageOperationRequest(request));
    }
    async commit(projectId: string, deltaData: DeltaData, message: string): Promise<CommitOperationResult> {
        let request = createCommitRequest(projectId, deltaData, message)
        const result = await this._storageOperationRequest(request);
        return result as CommitOperationResult;
    }
    getCommitGraph(projectId: string): Promise<CommitGraphData> {
        return this.runStorageCall('getCommitGraph', `projectId=${projectId}`, () => this.service.getCommitGraph(projectId));
    }

    getCommits(projectId: string, commitIds: string[]): Promise<CommitData[]> {
        return this.runStorageCall('getCommits', `projectId=${projectId} commitCount=${commitIds.length}`, () => this.service.getCommits(projectId, commitIds));
    }

    // File operations
    async addFile(projectId: string, blob: Blob, path: string, parentId: string, metadata: FileItemMetadata): Promise<FileItemData> {
        return this.runStorageCall('addFile', `projectId=${projectId} path=${path} parentId=${parentId} size=${blob.size}`, () => this.service.addFile(projectId, blob, path, parentId, metadata));
    }

    async getFile(projectId: string, fileId: string, fileHash: string): Promise<Blob> {
        return this.runStorageCall('getFile', `projectId=${projectId} fileId=${fileId} hash=${fileHash}`, () => this.service.getFile(projectId, fileId, fileHash));
    }

    async removeFile(projectId: string, fileId: string, fileHash: string): Promise<void> {
        return this.runStorageCall('removeFile', `projectId=${projectId} fileId=${fileId} hash=${fileHash}`, () => this.service.removeFile(projectId, fileId, fileHash));
    }

    async test() {
        return this.runStorageCall('test', '', () => Promise.resolve(this.service.test()));
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

    private cloneState(): StorageServiceRuntimeState {
        return {
            ...this.state,
            lastError: this.state.lastError ? { ...this.state.lastError } : null,
        };
    }

    private emitState() {
        if (this._stateChangeHandler) {
            this._stateChangeHandler(this.cloneState());
        }
    }

    private setState(patch: Partial<StorageServiceRuntimeState>) {
        const nextState = { ...this.state, ...patch };
        if (JSON.stringify(nextState) === JSON.stringify(this.state)) {
            return;
        }
        this.state = nextState;
        this.emitState();
    }

    private setError(
        code: StorageServiceErrorCode,
        message: string,
        options: {
            operation?: string | null;
            recoverable?: boolean;
            reloadRecommended?: boolean;
            connectionPhase?: StorageConnectionPhase;
            workerLifecycle?: ServiceWorkerLifecycleState;
        } = {}
    ) {
        this.setState({
            connectionPhase: options.connectionPhase ?? this.state.connectionPhase,
            workerLifecycle: options.workerLifecycle ?? this.state.workerLifecycle,
            lastError: {
                code,
                message,
                operation: options.operation ?? null,
                timestamp: Date.now(),
                recoverable: options.recoverable ?? true,
                reloadRecommended: options.reloadRecommended ?? false,
            },
        });
    }

    private clearRemoteConnection() {
        this._service = null;
        this._worker = null;
        this.setState({
            connected: false,
            hasController: this.hasServiceWorkerController(),
            projectPhase: this.state.activeProjectId ? 'detached' : this.state.projectPhase,
        });
    }

    private hasServiceWorkerController() {
        return typeof navigator !== 'undefined'
            && 'serviceWorker' in navigator
            && navigator.serviceWorker.controller !== null;
    }

    private attachWorkerStateListener(worker: ServiceWorker) {
        if (this._workerStateListeners.has(worker)) {
            return;
        }
        this._workerStateListeners.add(worker);
        const updateLifecycle = () => {
            const workerLifecycle = this.mapWorkerLifecycle(worker.state);
            this.setState({ workerLifecycle });
            if (workerLifecycle === 'redundant') {
                this.clearRemoteConnection();
                this.setError('sw-worker-redundant', 'The service worker became redundant.', {
                    connectionPhase: 'disconnected',
                    recoverable: true,
                    reloadRecommended: true,
                    workerLifecycle,
                });
            }
        };
        worker.addEventListener('statechange', updateLifecycle);
        updateLifecycle();
    }

    private mapWorkerLifecycle(state: ServiceWorkerState): ServiceWorkerLifecycleState {
        switch (state) {
            case 'installing':
                return 'installing';
            case 'installed':
                return 'installed';
            case 'activating':
                return 'activating';
            case 'activated':
                return 'activated';
            case 'redundant':
                return 'redundant';
            default:
                return 'unknown';
        }
    }

    private async runWithTimeout<T>(operationName: string, work: Promise<T>, timeoutMs: number): Promise<T> {
        return new Promise<T>((resolve, reject) => {
            const timeoutHandle = setTimeout(() => {
                reject(new Error(`${operationName} timed out after ${timeoutMs}ms`));
            }, timeoutMs);
            work.then((value) => {
                clearTimeout(timeoutHandle);
                resolve(value);
            }).catch((error) => {
                clearTimeout(timeoutHandle);
                reject(error);
            });
        });
    }

    private isTimeoutError(error: unknown) {
        return error instanceof Error && error.message.includes('timed out after');
    }

    private errorMessage(error: unknown) {
        if (error instanceof Error) {
            return error.message;
        }
        return String(error);
    }

    private async reconnectToWorker(failurePhase: StorageConnectionPhase): Promise<void> {
        if (!this._isWrapper) {
            return;
        }
        if (this._reconnectInFlight) {
            return this._reconnectInFlight;
        }

        this._reconnectInFlight = (async () => {
            this.setState({
                connectionPhase: 'connecting',
                hasController: this.hasServiceWorkerController(),
            });
            try {
                await this.connectToWorker();
                await this.restoreActiveProject();
                this.setState({
                    connectionPhase: 'ready',
                    connected: true,
                    hasController: this.hasServiceWorkerController(),
                    workerLifecycle: this.state.workerLifecycle === 'unknown' ? 'activated' : this.state.workerLifecycle,
                    lastReadyAt: Date.now(),
                });
            } catch (error) {
                this.clearRemoteConnection();
                const code = this.isTimeoutError(error) ? 'sw-connect-timeout' : 'sw-connect-failed';
                this.setError(code, `Failed to connect to the service worker: ${this.errorMessage(error)}`, {
                    connectionPhase: failurePhase,
                    recoverable: failurePhase !== 'fatal',
                    reloadRecommended: failurePhase === 'fatal',
                });
                throw error;
            } finally {
                this._reconnectInFlight = null;
            }
        })();

        return this._reconnectInFlight;
    }

    private async restoreActiveProject(): Promise<void> {
        if (!this.state.activeProjectId || !this._activeProjectConfig) {
            return;
        }

        this.setState({
            projectPhase: 'attaching',
        });
        try {
            await this.runWithTimeout(
                'restoreActiveProject',
                this.service.loadProject(this.state.activeProjectId, this._activeProjectConfig, this._activeProjectUri),
                STORAGE_SERVICE_CALL_TIMEOUT_MS
            );
            if (this._activeProjectUpdateCallback) {
                this._projectUpdateCallbacks.set(this.state.activeProjectId, this._activeProjectUpdateCallback);
            }
            this.setState({
                projectPhase: 'attached',
            });
        } catch (error) {
            this.setState({
                projectPhase: 'detached',
            });
            throw error;
        }
    }

    private async ensureConnectedForCall(): Promise<void> {
        if (!this._isWrapper) {
            return;
        }
        if (this._service && this.state.connectionPhase === 'ready') {
            return;
        }
        if (this.state.connectionPhase === 'fatal') {
            throw new Error('Storage service is in a fatal state.');
        }
        await this.reconnectToWorker('disconnected');
    }

    private createCallTrace(operationName: string, detail: string): StorageCallTrace {
        return {
            operationId: createId(6),
            operationName,
            detail,
            startedAt: Date.now(),
        };
    }

    private formatCallTrace(trace: StorageCallTrace): string {
        return `[op:${trace.operationId}] ${trace.operationName}${trace.detail ? ` (${trace.detail})` : ''}`;
    }

    private async callWithTimeout<T>(trace: StorageCallTrace, work: () => Promise<T>): Promise<T> {
        try {
            const result = await this.runWithTimeout(trace.operationName, work(), STORAGE_SERVICE_CALL_TIMEOUT_MS);
            log.info(`${this.formatCallTrace(trace)} completed in ${Date.now() - trace.startedAt}ms`);
            return result;
        } catch (error) {
            if (this.isTimeoutError(error)) {
                log.error(`${this.formatCallTrace(trace)} timed out after ${Date.now() - trace.startedAt}ms`, error);
                this.clearRemoteConnection();
                this.setError('storage-call-timeout', `${trace.operationName} timed out.`, {
                    operation: trace.operationName,
                    connectionPhase: 'disconnected',
                    recoverable: true,
                });
            } else {
                log.error(`${this.formatCallTrace(trace)} failed after ${Date.now() - trace.startedAt}ms`, error);
                this.setError('storage-call-failed', `${trace.operationName} failed: ${this.errorMessage(error)}`, {
                    operation: trace.operationName,
                });
            }
            throw error;
        }
    }

    private async runStorageCall<T>(operationName: string, detail: string, work: () => Promise<T>): Promise<T> {
        const trace = this.createCallTrace(operationName, detail);
        log.info(`${this.formatCallTrace(trace)} started`);
        if (!this._isWrapper) {
            try {
                const result = await work();
                log.info(`${this.formatCallTrace(trace)} completed in ${Date.now() - trace.startedAt}ms`);
                return result;
            } catch (error) {
                log.error(`${this.formatCallTrace(trace)} failed after ${Date.now() - trace.startedAt}ms`, error);
                throw error;
            }
        }
        await this.ensureConnectedForCall();
        return this.callWithTimeout(trace, work);
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
    private _storageOperationQueue: PendingStorageOperationRequest[] = [];
    private _processing = false;
    private fileRequestParser?: FileRequestParser;

    // Runtime tracking for files created via addFile during this session.
    // Keys are `${fileId}#${contentHash}`.
    private _createdFilesThisSession: Set<string> = new Set();

    constructor(fileRequestParser?: FileRequestParser) {
        this.fileRequestParser = fileRequestParser;

        // Single channel for broadcasting and receiving storage updates
        this._storageUpdateChannel = getStorageUpdatesChannel(LOCAL_STORAGE_UPDATE_CHANNEL);
        this._storageUpdateSubscription = this._storageUpdateChannel.subscribe(
            (message: LocalStorageUpdateMessage) => this._onLocalStorageUpdate(message)
        );

        // setInterval(() => {
        //     log.info(`StorageServiceActual heartbeat. ID: ${this.id}. Loaded repos: ${Object.keys(this.repoManagers).join(', ')}`);
        // }, 5000);
    }
    inWorker(): boolean {
        // @ts-ignore: ServiceWorkerGlobalScope is global only in SW
        return typeof ServiceWorkerGlobalScope !== 'undefined' && self instanceof ServiceWorkerGlobalScope;
    }

    test() {
        log.info('Test!!!!!!!!!')
        return true
    }

    setupFileRequestInterception() {
        /**
         * Sets up the file request interception for the service worker.
         * For the desktop-app and offline-webapp scenarios we intercept
         * file requests to serve the files from the respective storage
         */
        if (!this.inWorker()) {
            throw new Error('File request interception can only be set up in a service worker context');
        }

        // Check if fetch event interception is available
        if (typeof self.addEventListener !== 'function') {
            throw new Error('Service worker fetch event interception is not available. Cannot set up file request interception.');
        }


        // Set up fetch event listener for file requests only if a handler is provided
        if (this.fileRequestParser) {
            self.addEventListener('fetch', this.handleFetch);
            log.info('Set up global fetch interception for file requests in service worker');
        } else {
            log.info('No file request handler set up. Skipping fetch interception setup.')
        }
    }

    // Set up fetch event listener for file requests
    handleFetch = (event: Event) => {
        const fetchEvent = event as FetchEvent;
        const url = fetchEvent.request.url;

        // log.info(`Intercepting fetch request for URL: ${url}`);

        // Parse the URL using the file request handler
        const fileRequest = this.fileRequestParser!(this, url);

        if (fileRequest) {
            fetchEvent.respondWith(this.handleFileRequest(fetchEvent.request, fileRequest));
        }
    };

    async handleFileRequest(request: Request, fileRequest: FileRequest): Promise<Response> {
        log.info(`Handling file request for URL: ${request.url}`, fileRequest);
        try {
            if (!fileRequest.fileItemId || !fileRequest.projectId) {
                log.warning(`Invalid file URL format: ${request.url}`);
                return new Response('Invalid file URL format', {
                    status: 400,
                    statusText: 'Bad Request',
                    headers: { 'Content-Type': 'text/plain' }
                });
            }

            // Get project storage manager for the project
            const repoManager = this.repoManagers[fileRequest.projectId];
            if (!repoManager) {
                log.warning(`No repo manager found for project: ${fileRequest.projectId}`);
                return new Response(`Project not found ${fileRequest.projectId}`, {
                    status: 404,
                    statusText: 'Not Found',
                    headers: { 'Content-Type': 'text/plain' }
                });
            }
            if (fileRequest.fileItemContentHash === undefined) {
                log.warning(`File item content hash is required: ${request.url}`);
                return new Response('File item content hash is required', {
                    status: 400,
                    statusText: 'Bad Request',
                    headers: { 'Content-Type': 'text/plain' }
                });
            }

            const blob = await repoManager.fileStore.getFile(fileRequest.fileItemId, fileRequest.fileItemContentHash);
            log.info(`Serving file from storage: ${fileRequest.fileItemId}, hash: ${fileRequest.fileItemContentHash}`);
            return new Response(blob, {
                headers: {
                    'Content-Type': blob.type,
                    'Content-Length': blob.size.toString(),
                }
            });

        } catch (error) {
            log.warning(`File not found: ${request.url}`, error);
            return new Response('File not found', {
                status: 404,
                statusText: 'Not Found',
                headers: { 'Content-Type': 'text/plain' }
            });
        }
    }

    async loadProject(projectId: string, projectStorageConfig: ProjectStorageConfig, projectUri?: string): Promise<void> {
        /**
         * Creates the Repo manager (if not already present) and increments reference count.
         */
        let repoManager = this.repoManagers[projectId];
        if (!repoManager) {
            repoManager = new ProjectStorageManager(projectStorageConfig, this);
            await repoManager.loadProject(projectUri);
            this.repoManagers[projectId] = repoManager;
            this.repoRefCounts[projectId] = 0;

            log.info('Initialized repo manager for project', projectId);

            // Perform cleanup of old deleted file items on startup
            await repoManager.fileStore.cleanTrash();
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
            await repoManager.shutdown();

            delete this.repoRefCounts[projectId];
            delete this.repoManagers[projectId];
            log.info('Unloaded repo for project', projectId);
        }
    }

    async createProject(projectId: string, projectStorageConfig: ProjectStorageConfig, projectProperties?: ProjectData): Promise<string> {
        let repoManager = this.repoManagers[projectId];
        if (repoManager) {
            throw new Error(`Project ${projectId} already exists.`);
        }

        repoManager = new ProjectStorageManager(projectStorageConfig, this);
        await repoManager.createProject(projectProperties);

        console.log(`[createProject] Created repo with head store ${JSON.stringify(repoManager._onDeviceRepo?.headStore)}`)
        // this.repoManagers[projectId] = repoManager;
        await repoManager.shutdown();  // Just create it, load separately

        this.repoRefCounts[projectId] = 0;

        return deriveProjectUri(projectId, projectStorageConfig.onDeviceVcsAdapter.name);
    }

    async getProjectProperties(projectId: string): Promise<ProjectData | null> {
        const repoManager = this.repoManagers[projectId];
        if (!repoManager) {
            throw new Error(`Project ${projectId} is not loaded. Properties are only accessible for loaded projects.`);
        }
        return repoManager.getProjectProperties();
    }

    async setProjectProperties(projectId: string, projectProperties: ProjectData): Promise<void> {
        const repoManager = this.repoManagers[projectId];
        if (!repoManager) {
            throw new Error(`Project ${projectId} is not loaded. Properties are only accessible for loaded projects.`);
        }
        await repoManager.setProjectProperties(projectProperties);
    }

    async removeProject(projectId: string, projectStorageConfig: ProjectStorageConfig): Promise<void> {
        let repoManager = this.repoManagers[projectId];
        const wasLoaded = !!repoManager;

        if (!repoManager) {
            // Temporarily create a PSM to access adapters for erasure
            repoManager = new ProjectStorageManager(projectStorageConfig, this);
            await repoManager.loadProject();
        }

        // Each adapter's eraseStorage() does the right thing:
        // IndexedDB/CacheAPI erase their data; RestApi adapters are no-ops.
        await repoManager.eraseLocalStorage();

        log.info(`Removed project ${projectId}`);

        if (wasLoaded) {
            await this.unloadProject(projectId);
        } else {
            await repoManager.shutdown();
        }
    }

    async commit(projectId: string, deltaData: DeltaData, message: string): Promise<CommitOperationResult> {
        const request = createCommitRequest(projectId, deltaData, message);
        const result = await this._storageOperationRequest(request);
        return result as CommitOperationResult;
    }

    async _storageOperationRequest(request: StorageOperationRequest): Promise<StorageOperationResult> {
        return new Promise<StorageOperationResult>((resolve, reject) => {
            this._storageOperationQueue.push({
                request,
                resolve,
                reject,
            });

            void this.processStorageOperationQueue();
        });
    }

    private async processStorageOperationQueue(): Promise<void> {
        if (this._processing) return;
        this._processing = true;
        try {
            while (this._storageOperationQueue.length) {
                const queuedRequest = this._storageOperationQueue.shift()!;
                try {
                    const result = await this._executeStorageOperationRequest(queuedRequest.request);
                    queuedRequest.resolve(result);
                } catch (error) {
                    log.error('Error processing storage operation request', queuedRequest.request, error);
                    queuedRequest.reject(error);
                }
            }
        } finally {
            this._processing = false;
            if (this._storageOperationQueue.length > 0) {
                void this.processStorageOperationQueue();
            }
        }
    }

    async _executeCommitRequest(request: CommitRequest): Promise<CommitOperationResult> {
        console.log('Type is commit')
        const commitRequest = request as CommitRequest;
        const repoManager = this.repoManagers[commitRequest.projectId];

        if (!repoManager) {
            throw new Error("Repo not loaded");
        }

        const delta = new Delta(commitRequest.deltaData);

        // Commit to in-mem, skipping conflicting changes against the current head.
        const commit = await repoManager.onDeviceRepo.commit(delta, commitRequest.message, {
            skipConflictingChanges: true,
        });
        console.log('Created commit', commit)

        // Try squash before notifying subscribers, so the broadcast reflects latest graph
        const squashedUpserts = await squashBranchHistory(
            repoManager.onDeviceRepo,
            repoManager.currentBranchName,
            SQUASH_TTL_MS
        )//.catch(err => { log.error('[squash] Failed to squash history', err); return []; });

        // Build upserted set conditionally: only augment when squash returned updates
        let upsertedCommits: CommitData[];
        if (squashedUpserts.length > 0) {
            const map = new Map<string, CommitData>();
            for (const c of squashedUpserts) map.set(c.id, c);
            const newCommitData = commit.data();
            if (!map.has(newCommitData.id)) map.set(newCommitData.id, newCommitData);
            upsertedCommits = Array.from(map.values());
        } else {
            upsertedCommits = [commit.data()];
        }

        // Notify subscribers (graph reflects any squash because we fetch after applying)
        const commitGraph = await this.repoManagers[commitRequest.projectId].onDeviceRepo.getCommitGraph()
        this.broadcastLocalUpdate({
            projectId: commitRequest.projectId,
            storageServiceId: this.id,
            update: {
                commitGraph: commitGraph.data(),
                upsertedCommits
            }
        });

        // Push committed delta to domain store (filesystem bridge).
        // PSM's syncDeltaToDomainStore is a no-op when no domain store is configured.
        // For commits originating from external FS changes (pulled by PSM's poll loop),
        // this writes back the same data — the backend PFM detects no diff and it's a no-op.
        const commitData = commit.data();
        await repoManager.syncDeltaToDomainStore(commitData.deltaData, commitData.snapshotHash);

        return {
            type: 'commit',
            commit: commit.data(),
        };
    }
    async _executeStorageOperationRequest(request: StorageOperationRequest): Promise<StorageOperationResult> {
        if (request.type === 'commit') {
            return await this._executeCommitRequest(request as CommitRequest);
        } else {
            throw new Error(`Unknown storage operation request: ${request.type}`);
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

    async getCommitGraph(projectId: string): Promise<CommitGraphData> {
        let repoManager = this.repoManagers[projectId];
        if (!repoManager) {
            throw new Error("Repo not loaded");
        }
        const graph = await repoManager.onDeviceRepo.getCommitGraph();
        return graph.data();
    }

    async getCommits(projectId: string, commitIds: string[]): Promise<CommitData[]> {
        let repoManager = this.repoManagers[projectId];
        if (!repoManager) {
            throw new Error("Repo not loaded");
        }
        const commits = await repoManager.onDeviceRepo.getCommits(commitIds);
        return commits.map(commit => commit.data());
    }

    // File operations
    async addFile(projectId: string, blob: Blob, path: string, parentId: string, metadata: FileItemMetadata): Promise<FileItemData> {
        let repoManager = this.repoManagers[projectId];
        if (!repoManager) {
            throw new Error("Repo not loaded");
        }

        // Generate a unique path using the utility function and check against the in-memory repository
        const uniquePath = generateUniquePathWithSuffix(path, (checkPath: string) => {
            // Check if any entity with this path exists in the in-memory repository
            return !!repoManager.onDeviceRepo.headStore.findOne({ path: checkPath });
        });

        log.info(`Adding file to project ${projectId} with unique path: ${uniquePath}`);
        const fileItemData = await repoManager.fileStore.addFile(blob, uniquePath, parentId, metadata);

        // Mark created in this session to skip restore logic during commit processing
        const key = `${fileItemData.id}#${fileItemData.content.hash}`;
        this._createdFilesThisSession.add(key);

        return fileItemData;
    }

    async getFile(projectId: string, fileId: string, fileHash: string): Promise<Blob> {
        let repoManager = this.repoManagers[projectId];
        if (!repoManager) {
            throw new Error("Repo not loaded");
        }
        return repoManager.fileStore.getFile(fileId, fileHash);
    }

    async removeFile(projectId: string, fileId: string, fileHash: string): Promise<void> {
        let repoManager = this.repoManagers[projectId];
        if (!repoManager) {
            throw new Error("Repo not loaded");
        }

        await repoManager.fileStore.removeFile(fileId, fileHash);

        // Unset runtime-created mark when removed explicitly
        this._createdFilesThisSession.delete(`${fileId}#${fileHash}`);
    }

    disconnect() {
        this._storageUpdateSubscription?.unsubscribe();
    }

}
