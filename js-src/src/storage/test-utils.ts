import { EntityData, entityType, Entity } from "fusion/model/Entity";
import { IndexConfig, ENTITY_TYPE_INDEX_KEY } from "fusion/storage/domain-store/InMemoryStore";


export const indexConfigs: readonly IndexConfig[] = [
    {
        fields: [{ indexKey: "id" }],
        isUnique: true,
        name: "id"
    },
    {
        fields: [{
            indexKey: ENTITY_TYPE_INDEX_KEY,
            allowedTypes: ['Page', 'Note']
        }],
        isUnique: false,
        name: 'by_class_name'
    }
];
interface PageData extends EntityData {
    name: string;
}
// mock Page entity subclass

@entityType("Page")
export class Page extends Entity<PageData> {
    constructor(data: PageData) {
        super(data);
    }
    get name(): string {
        return this._data.name;
    }
    set name(name: string) {
        this._data.name = name;
    }
    get parentId(): string {
        return "";
    }
}
interface NoteData extends EntityData {
    name: string;
    pageId: string;
}

@entityType("Note")
export class Note extends Entity<NoteData> {
    constructor(data: NoteData) {
        super(data);
    }
    get parentId(): string {
        return this._data.pageId;
    }
    get name(): string {
        return this._data.name;
    }
    set name(name: string) {
        this._data.name = name;
    }
}
