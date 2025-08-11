import * as Comlink from 'comlink';
import { getLogger } from '../../logging';
import { StorageServiceActualInterface } from './StorageService';

const log = getLogger('service-worker-utils');

declare const self: ServiceWorkerGlobalScope;

export function setupServiceWorker(storageService: StorageServiceActualInterface) {
    // Configure the service worker to update immediately and claim clients upon activation
    self.addEventListener('install', event => {
        // Skip waiting to activate the service worker immediately
        log.info('Service worker installed');
        event.waitUntil(self.skipWaiting());
    });

    self.addEventListener('activate', event => {
        // Claim clients immediately
        log.info('Service worker activated');
        event.waitUntil(self.clients.claim());
    });

    // Handle MessageChannel connections from main thread
    self.addEventListener("message", (event: ExtendableMessageEvent) => {
        log.info('Service worker received message:', event.data);

        // Handle Comlink connection setup
        if (event.data && event.data.type === 'CONNECT_STORAGE') {
            const port = event.ports[0];
            if (port) {
                log.info('Service worker: Setting up Comlink on MessageChannel port');
                // Expose the storage service on this specific port
                Comlink.expose(storageService, port);
                log.info('Service worker: Comlink exposed on port');
            } else {
                log.error('Service worker: No port received in CONNECT_STORAGE message');
            }
            return;
        }

        // Handle other messages (legacy logging)
        log.info('Service worker: Unhandled message type');
    });
}
