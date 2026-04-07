import { FileItemData, FileItemMetadata } from "../../model/FileItem";

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
  eraseStorage: () => Promise<void>;
}
