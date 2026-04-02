import { getLogger } from "../../logging";
import { FileStoreAdapter } from "./FileStoreAdapter";
import { generateContentHash } from "../../util/media";
import { FileItem, FileItemData, FileItemMetadata } from "../../model/FileItem";

const log = getLogger('InMemoryFileStoreAdapter');

export class InMemoryFileStoreAdapter implements FileStoreAdapter {
    private _files: Map<string, Blob> = new Map();
    private _trash: Map<string, { blob: Blob, timeDeleted: number }> = new Map();

    private _getStorageKey(id: string, contentHash: string): string {
        return `${id}#${contentHash}`;
    }

    async addFile(blob: Blob, path: string, parentId: string, metadata: FileItemMetadata): Promise<FileItemData> {
        log.info(`Adding file to in-memory store: ${path} (${blob.type}, ${blob.size} bytes)`);
        const contentHash = (await generateContentHash(blob)).slice(0, 32);

        const fileItem = FileItem.create({
            path: path,
            content: { hash: contentHash },
            parent_id: parentId,
            metadata: metadata
        });

        const storageKey = this._getStorageKey(fileItem.id, contentHash);
        this._files.set(storageKey, blob);
        log.info(`Added file to in-memory store: ${storageKey}`);

        return fileItem.data();
    }

    async getFile(mediaId: string, mediaHash: string): Promise<Blob> {
        const storageKey = this._getStorageKey(mediaId, mediaHash);
        let blob = this._files.get(storageKey);
        if (!blob) {
            // Allow fetching from trash to support "trashed-but-servable" behavior
            const trashed = this._trash.get(storageKey);
            blob = trashed?.blob;
        }
        if (!blob) {
            log.error(`File not found in in-memory store (active or trash): ${storageKey}`);
            throw new Error(`File not found: ${storageKey}`);
        }
        return blob;
    }

    async removeFile(mediaId: string, contentHash: string): Promise<void> {
        const storageKey = this._getStorageKey(mediaId, contentHash);
        const deleted = this._files.delete(storageKey);

        if (deleted) {
            log.info(`Removed file from in-memory store: ${storageKey}`);
        } else {
            log.warning(`File not found for deletion in in-memory store: ${storageKey}`);
        }
    }

    async moveFileToTrash(mediaId: string, contentHash: string): Promise<void> {
        const storageKey = this._getStorageKey(mediaId, contentHash);
        const blob = this._files.get(storageKey);

        if (!blob) {
            log.warning(`Attempted to trash non-existent file: ${storageKey}`);
            return;
        }

        this._files.delete(storageKey);
        this._trash.set(storageKey, { blob: blob, timeDeleted: Date.now() });
        log.info(`Moved file to trash: ${storageKey}`);
    }

    async restoreFileFromTrash(mediaId: string, contentHash: string): Promise<void> {
        const storageKey = this._getStorageKey(mediaId, contentHash);
        const trashed = this._trash.get(storageKey);

        if (!trashed) {
            log.warning(`Attempted to restore non-existent trashed file: ${storageKey}`);
            return;
        }

        // Move blob back to active storage
        this._trash.delete(storageKey);
        this._files.set(storageKey, trashed.blob);
        log.info(`Restored file from trash: ${storageKey}`);
    }

    async cleanTrash(): Promise<void> {
        const trashedCount = this._trash.size;
        this._trash.clear();
        log.info(`Cleaned ${trashedCount} items from in-memory trash.`);
    }

    async eraseStorage(): Promise<void> {
        this._files.clear();
        this._trash.clear();
        log.info('Erased InMemoryFileStoreAdapter.');
    }

    async close(): Promise<void> {
        this._files.clear();
        this._trash.clear();
        log.info('Closed and cleared InMemoryFileStoreAdapter.');
    }
}
