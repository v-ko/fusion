import { getLogger } from "../../logging";
import { MediaStoreAdapter } from "./MediaStoreAdapter";
import { extractImageDimensions, generateContentHash } from "../../util/media";
import { MediaItem, MediaItemData } from "../../model/MediaItem";

const log = getLogger('InMemoryMediaStoreAdapter');

export class InMemoryMediaStoreAdapter implements MediaStoreAdapter {
    private _media: Map<string, Blob> = new Map();
    private _trash: Map<string, { blob: Blob, timeDeleted: number }> = new Map();

    private _getStorageKey(id: string, contentHash: string): string {
        return `${id}#${contentHash}`;
    }

    async addMedia(blob: Blob, path: string, parentId: string): Promise<MediaItemData> {
        log.info(`Adding media to in-memory store: ${path} (${blob.type}, ${blob.size} bytes)`);
        const contentHash = (await generateContentHash(blob)).slice(0, 32);

        let width = 0;
        let height = 0;
        if (blob.type.startsWith('image/')) {
            try {
                const dimensions = await extractImageDimensions(blob);
                width = dimensions.width;
                height = dimensions.height;
            } catch (error) {
                log.warning('Failed to extract image dimensions:', error);
            }
        }

        const mediaItem = MediaItem.create({
            path: path,
            contentHash,
            width,
            height,
            mimeType: blob.type,
            size: blob.size,
            timeDeleted: undefined,
            parent_id: parentId
        });

        const storageKey = this._getStorageKey(mediaItem.id, contentHash);
        this._media.set(storageKey, blob);
        log.info(`Added media to in-memory store: ${storageKey}`);

        return mediaItem.data();
    }

    async getMedia(mediaId: string, mediaHash: string): Promise<Blob> {
        const storageKey = this._getStorageKey(mediaId, mediaHash);
        const blob = this._media.get(storageKey);
        if (!blob) {
            log.error(`Media not found in in-memory store: ${storageKey}`);
            throw new Error(`Media not found: ${storageKey}`);
        }
        return blob;
    }

    async removeMedia(mediaId: string, contentHash: string): Promise<void> {
        const storageKey = this._getStorageKey(mediaId, contentHash);
        const deleted = this._media.delete(storageKey);

        if (deleted) {
            log.info(`Removed media from in-memory store: ${storageKey}`);
        } else {
            log.warning(`Media not found for deletion in in-memory store: ${storageKey}`);
        }
    }

    async moveMediaToTrash(mediaId: string, contentHash: string): Promise<void> {
        const storageKey = this._getStorageKey(mediaId, contentHash);
        const blob = this._media.get(storageKey);

        if (!blob) {
            log.warning(`Attempted to trash non-existent media: ${storageKey}`);
            return;
        }

        this._media.delete(storageKey);
        this._trash.set(storageKey, { blob: blob, timeDeleted: Date.now() });
        log.info(`Moved media to trash: ${storageKey}`);
    }

    async cleanTrash(): Promise<void> {
        const trashedCount = this._trash.size;
        this._trash.clear();
        log.info(`Cleaned ${trashedCount} items from in-memory trash.`);
    }

    async close(): Promise<void> {
        this._media.clear();
        this._trash.clear();
        log.info('Closed and cleared InMemoryMediaStoreAdapter.');
    }
}
