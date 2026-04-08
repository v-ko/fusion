import { getLogger } from "../../logging";
import { FileStoreAdapter } from "./FileStoreAdapter";
import { generateContentHash } from "../../util/media";
import { FileItem, FileItemData, FileItemMetadata } from "../../model/FileItem";

const log = getLogger('InMemoryFileStoreAdapter');

export class InMemoryFileStoreAdapter implements FileStoreAdapter {
    private _files: Map<string, Blob> = new Map();

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

    async getFile(fileItemId: string, contentHash: string): Promise<Blob> {
        const storageKey = this._getStorageKey(fileItemId, contentHash);
        const blob = this._files.get(storageKey);
        if (!blob) {
            log.error(`File not found in in-memory store: ${storageKey}`);
            throw new Error(`File not found: ${storageKey}`);
        }
        return blob;
    }

    async removeFile(fileItemId: string, contentHash: string): Promise<void> {
        const storageKey = this._getStorageKey(fileItemId, contentHash);
        const deleted = this._files.delete(storageKey);

        if (deleted) {
            log.info(`Removed file from in-memory store: ${storageKey}`);
        } else {
            log.warning(`File not found for deletion in in-memory store: ${storageKey}`);
        }
    }

    async eraseStorage(): Promise<void> {
        this._files.clear();
        log.info('Erased InMemoryFileStoreAdapter.');
    }

    async close(): Promise<void> {
        this._files.clear();
        log.info('Closed and cleared InMemoryFileStoreAdapter.');
    }
}
