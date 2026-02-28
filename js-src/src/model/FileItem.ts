import { Entity, EntityData, entityType, getEntityId } from "./Entity";

export interface FileItemMetadata {
    // Base metadata – subclasses extend this with type-specific fields
    // (e.g. ImageItemMetadata adds width/height)
}

export interface FileItemData extends EntityData {
    path: string;  // For project organization and FS storage (e.g. path relative to the project root)
    contentHash: string;  // Updated on content edit, enables id persistence through edits
    mimeType: string;  // MIME type of the file (e.g. 'image/jpeg', 'application/pdf')
    size: number;  // Size of the file in bytes
    metadata: FileItemMetadata;  // Extensible metadata object (subclasses add their own fields)
}

@entityType('FileItem')
export class FileItem extends Entity<FileItemData> {
    static create(data: Omit<FileItemData, 'id'>): FileItem {
        return new FileItem({
            id: getEntityId(),
            ...data
        });
    }

    get path(): string {
        return this._data.path;
    }

    get contentHash(): string {
        return this._data.contentHash;
    }

    get mimeType(): string {
        return this._data.mimeType;
    }

    get size(): number {
        return this._data.size;
    }

    get metadata(): FileItemMetadata {
        return this._data.metadata;
    }
}
