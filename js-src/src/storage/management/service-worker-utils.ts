import * as Comlink from 'comlink';
import { getLogger } from '../../logging';
import { StorageServiceActualInterface } from './StorageService';

const log = getLogger('service-worker-utils');

declare const self: ServiceWorkerGlobalScope;

export function setupServiceWorker(storageService: StorageServiceActualInterface) {
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
                Comlink.expose(storageService, port);
                log.info('Service worker: Comlink exposed on port');
            } else {
                log.error('Service worker: No port received in CONNECT_STORAGE message');
            }
            return;
        }

        log.info('Service worker: Unhandled message type', event.data.type);
    });
}
