import { entityType, getEntityId } from "./Entity";
import { FileItem, FileItemData, FileItemMetadata } from "./FileItem";

export interface ImageItemMetadata extends FileItemMetadata {
    width: number;  // For rendering placeholders and doing geometry calculations
    height: number;  // For audio those are 0. That's the only redundancy. Better to have that than a complex model
}

export interface ImageItemData extends FileItemData {
    metadata: ImageItemMetadata;
}

@entityType('ImageItem')
export class ImageItem extends FileItem {
    // Static factory method for creating new ImageItems
    static create(data: Omit<ImageItemData, 'id'>): ImageItem {
        return new ImageItem({
            id: getEntityId(),
            ...data
        });
    }

    // Override to narrow return type
    data(): ImageItemData {
        return this._data as ImageItemData;
    }

    get imageMetadata(): ImageItemMetadata {
        return this._data.metadata as ImageItemMetadata;
    }

    get width(): number {
        return this.imageMetadata.width;
    }

    get height(): number {
        return this.imageMetadata.height;
    }
}
