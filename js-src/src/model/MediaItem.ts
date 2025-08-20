import { Entity, EntityData, entityType, getEntityId } from "./Entity";

export interface MediaItemData extends EntityData {
    // It's flatter than other entities because it was designed for metadata and
    // nesting into note objects. But that may change
    path: string;  // For project organization and FS storage (e.g. path relative to the project root and a name for the item)
    contentHash: string;  // Updated on content edit, enables id persistence through edits
    width: number;  // For rendering placeholders and doing geometry calculations
    height: number;  // For audio those are 0. That's the only rendundancy. Better to have that than a complex model
    mimeType: string;  // MIME type of the image (e.g. 'image/jpeg', 'image/png')
    size: number;  // Size of the image in bytes
}

@entityType('MediaItem')
export class MediaItem extends Entity<MediaItemData> {
    // Static factory method for creating new MediaItems
    static create(data: Omit<MediaItemData, 'id'>): MediaItem {
        return new MediaItem({
            id: getEntityId(),
            ...data
        });
    }

    // Data access properties
    get path(): string {
        return this._data.path;
    }

    get contentHash(): string {
        return this._data.contentHash;
    }

    get width(): number {
        return this._data.width;
    }

    get height(): number {
        return this._data.height;
    }

    get mimeType(): string {
        return this._data.mimeType;
    }

    get size(): number {
        return this._data.size;
    }
}
