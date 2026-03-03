import { FileStoreAdapter } from "./FileStoreAdapter";
import { generateContentHash } from "../../util/media";
import { getLogger } from "../../logging";
import { FileItem, FileItemData, FileItemMetadata } from "../../model/FileItem";

const log = getLogger('CacheFileStoreAdapter');

export class CacheFileStoreAdapter implements FileStoreAdapter {
  private _projectId: string;
  private _cacheName: string;
  private _cache: Cache | null = null;

  constructor(projectId: string) {
    this._projectId = projectId;
    this._cacheName = `pamet-file-${projectId}`;
  }

  async init(): Promise<void> {
    try {
      this._cache = await caches.open(this._cacheName);
      log.info(`Opened Cache API store: ${this._cacheName}`);

      // // Set up fetch interception for file requests
      // this.setupFetchInterception();
    } catch (error) {
      log.error('Failed to initialize Cache API:', error);
      throw new Error(`Failed to initialize CacheFileStoreAdapter: ${error}`);
    }
  }

  private get cache(): Cache {
    if (!this._cache) {
      throw new Error('Cache not initialized. Call init() first.');
    }
    return this._cache;
  }

  private _getCacheKey(id: string, contentHash: string): string {
    // Create relative cache key without user/project prefix: file/item/{id}#{contentHash}
    // This makes storage user-agnostic, with security handled by cleanup on logout
    return `file/item/${id}#${contentHash}`;
  }

  async addFile(blob: Blob, path: string, parentId: string, metadata: FileItemMetadata): Promise<FileItemData> {
    log.info(`Adding file to cache: ${path} (${blob.type}, ${blob.size} bytes)`);
    // Generate content hash first
    const contentHash = (await generateContentHash(blob)).slice(0, 32); // Cut down the sha-256 hash to 32 characters

    // Create the FileItem
    const fileItem = FileItem.create({
      path: path, // Store relative path (e.g. "images/photo.jpg")
      content: { hash: contentHash },
      parent_id: parentId,
      metadata: metadata
    });

    // Create the cache key using FileItem ID
    const cacheKey = this._getCacheKey(fileItem.id, contentHash);

    // Store the blob in the Cache API using the cache key
    const response = new Response(blob.slice(), {
      headers: {
        'Content-Type': blob.type,
        'Content-Length': blob.size.toString(),
        'X-Content-Hash': contentHash, // Store hash in header for reference
      }
    });

    await this.cache.put(cacheKey, response);
    log.info(`Added file to cache: ${cacheKey}`);

    return fileItem.data(); // Return the data instead of the ImageItem instance
  }

  async getFile(fileId: string, fileHash: string): Promise<Blob> {
    // Create cache key from fileId and fileHash
    const cacheKey = this._getCacheKey(fileId, fileHash);

    const response = await this.cache.match(cacheKey);

    if (!response) {
      throw new Error(`File not found in cache: ${cacheKey}`);
    }

    return await response.blob();
  }

  async removeFile(fileId: string, contentHash: string): Promise<void> {
    const cacheKey = this._getCacheKey(fileId, contentHash);
    const deleted = await this.cache.delete(cacheKey);

    if (deleted) {
      log.info(`Removed file from cache: ${cacheKey}`);
    } else {
      log.warning(`File not found for deletion: ${cacheKey}`);
    }
  }

  async moveFileToTrash(fileId: string, contentHash: string): Promise<void> {
    const cacheKey = this._getCacheKey(fileId, contentHash);
    const request = new Request(cacheKey);
    const response = await this.cache.match(request);

    if (!response) {
      log.warning(`File not found for trashing: ${cacheKey}`);
      return;
    }

    const blob = await response.blob();
    const newHeaders = new Headers(response.headers);
    newHeaders.set('X-Delete-After', (Date.now() + 7 * 24 * 60 * 60 * 1000).toString());

    const newResponse = new Response(blob, {
        status: response.status,
        statusText: response.statusText,
        headers: newHeaders
    });

    await this.cache.put(request, newResponse);
    log.info(`Marked file for trashing in cache: ${cacheKey}`);
  }

  async restoreFileFromTrash(fileId: string, contentHash: string): Promise<void> {
    const cacheKey = this._getCacheKey(fileId, contentHash);
    const request = new Request(cacheKey);
    const response = await this.cache.match(request);

    if (!response) {
      log.warning(`File not found for restore: ${cacheKey}`);
      return;
    }

    const blob = await response.blob();
    // Recreate response without trash-related headers
    const headers = new Headers(response.headers);
    headers.delete('X-Delete-After');

    const newResponse = new Response(blob, {
      status: response.status,
      statusText: response.statusText,
      headers
    });

    await this.cache.put(request, newResponse);
    log.info(`Restored file in cache: ${cacheKey}`);
  }

  async cleanTrash(): Promise<void> {
    log.info('Cache API file trash is cleaned automatically by the browser based on expiry headers.');
    // Potentially, we could iterate and count items, but it's not necessary.
  }


  async close(): Promise<void> {
    // Cache API doesn't need explicit closing
    this._cache = null;
    log.info(`Closed CacheFileStoreAdapter: ${this._cacheName}`);
  }
}
