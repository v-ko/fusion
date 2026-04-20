import * as Comlink from 'comlink';
import { getLogger } from '../../logging';
import { StorageServiceActualInterface } from './StorageService';
import { createId } from '../../util/base';

const log = getLogger('service-worker-utils');

declare const self: ServiceWorkerGlobalScope;

type LoggedStorageServiceBridge = {
    [K in keyof StorageServiceActualInterface]: StorageServiceActualInterface[K];
};

console.log('test')

function createLoggedStorageServiceBridge(storageService: StorageServiceActualInterface): LoggedStorageServiceBridge {
    return {
        loadProject: async (projectId, repoManagerConfig, projectUri) => {
            return logWorkerOperation('loadProject', `projectId=${projectId}`, () => storageService.loadProject(projectId, repoManagerConfig, projectUri));
        },
        createProject: async (projectId, projectStorageConfig) => {
            return logWorkerOperation('createProject', `projectId=${projectId}`, () => storageService.createProject(projectId, projectStorageConfig));
        },
        unloadProject: async (projectId) => {
            return logWorkerOperation('unloadProject', `projectId=${projectId}`, () => storageService.unloadProject(projectId));
        },
        removeProject: async (projectId, projectStorageConfig) => {
            return logWorkerOperation('removeProject', `projectId=${projectId}`, () => storageService.removeProject(projectId, projectStorageConfig));
        },
        getCommitGraph: async (projectId) => {
            return logWorkerOperation('getCommitGraph', `projectId=${projectId}`, () => storageService.getCommitGraph(projectId));
        },
        getCommits: async (projectId, commitIds) => {
            return logWorkerOperation('getCommits', `projectId=${projectId} commitCount=${commitIds.length}`, () => storageService.getCommits(projectId, commitIds));
        },
        _storageOperationRequest: async (request) => {
            const details = 'projectId' in request
                ? `type=${request.type} projectId=${request.projectId}`
                : `type=${request.type}`;
            return logWorkerOperation('_storageOperationRequest', details, () => storageService._storageOperationRequest(request));
        },
        addFile: async (projectId, blob, path) => {
            return logWorkerOperation('addFile', `projectId=${projectId} path=${path} size=${blob.size}`, () => storageService.addFile(projectId, blob, path));
        },
        getFile: async (projectId, path) => {
            return logWorkerOperation('getFile', `projectId=${projectId} path=${path}`, () => storageService.getFile(projectId, path));
        },
        removeFile: async (projectId, path) => {
            return logWorkerOperation('removeFile', `projectId=${projectId} path=${path}`, () => storageService.removeFile(projectId, path));
        },
        test: () => {
            return storageService.test();
        },
        disconnect: () => {
            storageService.disconnect();
        }
    };
}

async function logWorkerOperation<T>(operationName: string, detail: string, work: () => Promise<T>): Promise<T> {
    const operationId = createId(6);
    const startedAt = Date.now();
    log.info(`[op:${operationId}] Received ${operationName}${detail ? ` (${detail})` : ''}`);
    try {
        const result = await work();
        log.info(`[op:${operationId}] Completed ${operationName} in ${Date.now() - startedAt}ms`);
        return result;
    } catch (error) {
        log.error(`[op:${operationId}] Failed ${operationName} after ${Date.now() - startedAt}ms`, error);
        throw error;
    }
}

export function setupServiceWorker(storageService: StorageServiceActualInterface) {
    const loggedStorageService = createLoggedStorageServiceBridge(storageService);
    // Configure the service worker to update immediately and claim clients upon activation
    self.addEventListener('install', event => {
        log.info('Service worker installed');
        // We are not calling skipWaiting() here anymore. The client will decide when to activate the new SW.
    });

    self.addEventListener('activate', event => {
        // Claim clients immediately
        log.info('Service worker activated');
        event.waitUntil(self.clients.claim());
    });

    // Handle MessageChannel connections from main thread
    self.addEventListener("message", (event: ExtendableMessageEvent) => {
        log.info('Service worker received message:', event.data);

        const { type } = event.data || {};
        if (type === 'SKIP_WAITING') {
            self.skipWaiting().catch(err => {
                log.error('Service worker: Error skipping waiting state', err);
            });
            return;
        }

        // Handle Comlink connection setup
        if (type === 'CONNECT_STORAGE') {
            const port = event.ports[0];
            if (port) {
                log.info('Service worker: Setting up Comlink on MessageChannel port');
                Comlink.expose(loggedStorageService, port);
                log.info('Service worker: Comlink exposed on port');
            } else {
                log.error('Service worker: No port received in CONNECT_STORAGE message');
            }
            return;
        }

        log.info('Service worker: Unhandled message type', event.data.type);
    });
}
