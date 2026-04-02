import { FileStoreAdapter } from "./FileStoreAdapter";
import { generateContentHash } from "../../util/media";
import { FileItem, FileItemData, FileItemMetadata } from "../../model/FileItem";
import { buildRestApiAuthHeaders, RestApiAuthConfig } from "../rest-api/Auth";

export class RestApiFileStoreAdapter implements FileStoreAdapter {
  private _projectId: string;
  private _baseUrl: string;
  private _auth: RestApiAuthConfig;

  constructor(projectId: string, baseUrl: string, auth: RestApiAuthConfig) {
    this._projectId = projectId;
    this._baseUrl = baseUrl;
    this._auth = auth;
  }

  private _url(path: string): string {
    const normalizedPath = path.startsWith('/') ? path : `/${path}`;
    const url = new URL(`${this._baseUrl}${normalizedPath}`);
    url.searchParams.set('project_id', this._projectId);
    return url.toString();
  }

  private _headers(): HeadersInit {
    return buildRestApiAuthHeaders(this._auth);
  }

  async addFile(blob: Blob, path: string, parentId: string, metadata: FileItemMetadata): Promise<FileItemData> {
    // Compute metadata on the client so we can create the FileItem locally
    const contentHash = (await generateContentHash(blob)).slice(0, 32);

    const fileItem = FileItem.create({
      path,
      content: { hash: contentHash },
      parent_id: parentId,
      metadata: metadata
    });

    // Upload blob to desktop server under id + hash mapping
    const form = new FormData();
    form.append('path', path);
    // Use original filename from path if available
    const filename = path.split(/[\\/]/).pop() || `${fileItem.id}`;
    form.append('file', blob, filename);

    const url = this._url(`/media/item/${encodeURIComponent(fileItem.id)}/${encodeURIComponent(contentHash)}`);
    const response = await fetch(url, {
      method: 'POST',
      body: form,
      headers: this._headers(),
    });
    if (!response.ok) {
      throw new Error(`Failed to upload file: ${response.status} ${response.statusText}`);
    }

    return fileItem.data();
  }

  async getFile(mediaId: string, mediaHash: string): Promise<Blob> {
    const url = this._url(`/media/item/${encodeURIComponent(mediaId)}/${encodeURIComponent(mediaHash)}`);
    const response = await fetch(url, { headers: this._headers() });
    if (!response.ok) {
      throw new Error(`Failed to fetch media: ${response.status} ${response.statusText}`);
    }
    return await response.blob();
  }

  async removeFile(mediaId: string, contentHash: string): Promise<void> {
    const url = this._url(`/media/item/${encodeURIComponent(mediaId)}/${encodeURIComponent(contentHash)}`);
    const response = await fetch(url, { method: 'DELETE', headers: this._headers() });
    if (!response.ok) {
      throw new Error(`Failed to remove media: ${response.status} ${response.statusText}`);
    }
  }

  async moveFileToTrash(mediaId: string, contentHash: string): Promise<void> {
    // Same as remove - server moves to trash
    return this.removeFile(mediaId, contentHash);
  }

  async restoreFileFromTrash(mediaId: string, contentHash: string): Promise<void> {
    const url = this._url(`/media/item/${encodeURIComponent(mediaId)}/${encodeURIComponent(contentHash)}/restore`);
    const response = await fetch(url, { method: 'POST', headers: this._headers() });
    if (!response.ok) {
      throw new Error(`Failed to restore media: ${response.status} ${response.statusText}`);
    }
  }

  async cleanTrash(): Promise<void> {
    const url = this._url(`/media/trash/clean`);
    const response = await fetch(url, { method: 'POST', headers: this._headers() });
    if (!response.ok) {
      throw new Error(`Failed to clean trash: ${response.status} ${response.statusText}`);
    }
  }

  async eraseStorage(): Promise<void> {
    // RestApi adapter does not own persistent storage — nothing to erase.
  }
}
