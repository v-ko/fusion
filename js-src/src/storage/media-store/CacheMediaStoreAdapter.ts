import { MediaStoreAdapter } from "./MediaStoreAdapter";
import { extractImageDimensions, generateContentHash } from "../../util/media";
import { getLogger } from "../../logging";
import { MediaItem, MediaItemData } from "../../model/MediaItem";

const log = getLogger('CacheMediaStoreAdapter');

export class CacheMediaStoreAdapter implements MediaStoreAdapter {
  private _projectId: string;
  private _cacheName: string;
  private _cache: Cache | null = null;

  constructor(projectId: string) {
    this._projectId = projectId;
    this._cacheName = `pamet-media-${projectId}`;
  }

  async init(): Promise<void> {
    try {
      this._cache = await caches.open(this._cacheName);
      log.info(`Opened Cache API store: ${this._cacheName}`);

      // // Set up fetch interception for media requests
      // this.setupFetchInterception();
    } catch (error) {
      log.error('Failed to initialize Cache API:', error);
      throw new Error(`Failed to initialize CacheMediaStoreAdapter: ${error}`);
    }
  }

  private get cache(): Cache {
    if (!this._cache) {
      throw new Error('Cache not initialized. Call init() first.');
    }
    return this._cache;
  }

  private _getCacheKey(id: string, contentHash: string): string {
    // Create relative cache key without user/project prefix: media/item/{id}#{contentHash}
    // This makes storage user-agnostic, with security handled by cleanup on logout
    return `media/item/${id}#${contentHash}`;
  }

  async addMedia(blob: Blob, path: string, parentId: string): Promise<MediaItemData> {
    log.info(`Adding media to cache: ${path} (${blob.type}, ${blob.size} bytes)`);
    // Generate content hash first
    const contentHash = (await generateContentHash(blob)).slice(0, 32); // Cut down the sha-256 hash to 32 characters

    // Extract image dimensions if it's an image
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

    // Create the MediaItem
    const mediaItem = MediaItem.create({
      path: path, // Store relative path (e.g. "images/photo.jpg")
      contentHash,
      width,
      height,
      mimeType: blob.type,
      size: blob.size,
      timeDeleted: undefined,
      parent_id: parentId
    });

    // Create the cache key using MediaItem ID
    const cacheKey = this._getCacheKey(mediaItem.id, contentHash);

    // Store the blob in the Cache API using the cache key
    const response = new Response(blob.slice(), {
      headers: {
        'Content-Type': blob.type,
        'Content-Length': blob.size.toString(),
        'X-Content-Hash': contentHash, // Store hash in header for reference
      }
    });

    await this.cache.put(cacheKey, response);
    log.info(`Added media to cache: ${cacheKey}`);

    return mediaItem.data(); // Return the data instead of the MediaItem instance
  }

  async getMedia(mediaId: string, mediaHash: string): Promise<Blob> {
    // Create cache key from mediaId and mediaHash
    const cacheKey = this._getCacheKey(mediaId, mediaHash);

    const response = await this.cache.match(cacheKey);

    if (!response) {
      throw new Error(`Media not found in cache: ${cacheKey}`);
    }

    return await response.blob();
  }

  async removeMedia(mediaId: string, contentHash: string): Promise<void> {
    const cacheKey = this._getCacheKey(mediaId, contentHash);
    const deleted = await this.cache.delete(cacheKey);

    if (deleted) {
      log.info(`Removed media from cache: ${cacheKey}`);
    } else {
      log.warning(`Media not found for deletion: ${cacheKey}`);
    }
  }

  async moveMediaToTrash(mediaId: string, contentHash: string): Promise<void> {
    const cacheKey = this._getCacheKey(mediaId, contentHash);
    const request = new Request(cacheKey);
    const response = await this.cache.match(request);

    if (!response) {
      log.warning(`Media not found for trashing: ${cacheKey}`);
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
    log.info(`Marked media for trashing in cache: ${cacheKey}`);
  }

  async cleanTrash(): Promise<void> {
    log.info('Cache API media trash is cleaned automatically by the browser based on expiry headers.');
    // Potentially, we could iterate and count items, but it's not necessary.
  }


  async close(): Promise<void> {
    // Cache API doesn't need explicit closing
    this._cache = null;
    log.info(`Closed CacheMediaStoreAdapter: ${this._cacheName}`);
  }
}
