import { FileItemData, FileItemMetadata } from "../../model/FileItem";

// Global cleanup parameters
export const MAX_VERSIONS_PER_FILE_ITEM = 5; // Maximum versions to keep in trash per file item
export const TRASHED_ITEM_EXPIRY_TIME = 7 * 24 * 60 * 60 * 1000; // 1 week in milliseconds

export class FileItemExistsInPathError extends Error {
  constructor(path: string) {
    super(`File item already exists at path: ${path}`);
    this.name = 'FileItemExistsInPathError';
  }
}

export interface FileStoreAdapter {
  addFile: (blob: Blob, path: string, parentId: string, metadata: FileItemMetadata) => Promise<FileItemData>;
  getFile: (fileId: string, contentHash: string) => Promise<Blob>;
  removeFile: (fileId: string, contentHash: string) => Promise<void>;
  moveFileToTrash: (fileId: string, contentHash: string) => Promise<void>;
  restoreFileFromTrash: (fileId: string, contentHash: string) => Promise<void>;
  cleanTrash: () => Promise<void>;
}
