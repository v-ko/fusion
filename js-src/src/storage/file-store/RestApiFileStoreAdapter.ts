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
    return `${this._baseUrl}/desktop/projects/${encodeURIComponent(this._projectId)}${normalizedPath}`;
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

    // Upload blob to desktop server under file item id
    const form = new FormData();
    form.append('content_hash', contentHash);
    form.append('path', path);
    // Use original filename from path if available
    const filename = path.split(/[\\/]/).pop() || `${fileItem.id}`;
    form.append('file', blob, filename);

    const url = this._url(`/files/${encodeURIComponent(fileItem.id)}`);
    const response = await fetch(url, {
      method: 'POST',
      body: form,
      headers: this._headers(),
    });
    if (!response.ok) {
      throw new Error(`Failed to upload file item: ${response.status} ${response.statusText}`);
    }

    return fileItem.data();
  }

  async getFile(fileItemId: string, contentHash: string): Promise<Blob> {
    const url = this._url(`/files/${encodeURIComponent(fileItemId)}/content`);
    const response = await fetch(url, { headers: this._headers(), cache: 'no-store' });
    if (!response.ok) {
      throw new Error(`Failed to fetch file item: ${response.status} ${response.statusText}`);
    }
    return await response.blob();
  }

  async removeFile(fileItemId: string, contentHash: string): Promise<void> {
    const url = this._url(`/files/${encodeURIComponent(fileItemId)}`);
    const response = await fetch(url, { method: 'DELETE', headers: this._headers() });
    if (!response.ok) {
      throw new Error(`Failed to remove file item: ${response.status} ${response.statusText}`);
    }
  }

  async eraseStorage(): Promise<void> {
    // RestApi adapter does not own persistent storage — nothing to erase.
  }
}
