import { Entity, EntityData, entityType, getEntityId } from "./Entity";

// Content object – holds the content hash (analogous to NoteContent)
export interface FileItemContent {
    hash: string;  // Updated on content edit, enables id persistence through edits
}

// Base metadata – standardized to match Note pattern.
// Subclasses extend with type-specific fields (e.g. ImageItemMetadata adds width/height).
export interface FileItemMetadata {
    size: number;       // Size of the file in bytes
    mime_type: string;   // MIME type of the file (e.g. 'image/jpeg', 'application/pdf')
    [key: string]: unknown; // Allow subtype-specific fields (e.g. width, height for images)
}

export interface FileItemData extends EntityData {
    path: string;       // For project organization and FS storage (e.g. path relative to the project root)
    content: FileItemContent;
    metadata: FileItemMetadata;
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

    get content(): FileItemContent {
        return this._data.content;
    }

    get contentHash(): string {
        return this._data.content.hash;
    }

    get mimeType(): string {
        return this._data.metadata.mime_type;
    }

    get size(): number {
        return this._data.metadata.size;
    }

    get metadata(): FileItemMetadata {
        return this._data.metadata;
    }
}
