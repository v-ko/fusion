import { Change } from "../Change";
import { Entity, EntityData, dumpToDict } from "../libs/Entity";
import { SerializedStoreData, changesFromDelta } from "./BaseStore";
import { Delta } from "./Delta";

export abstract class BaseAsyncStore {
    abstract insertOne(entity: Entity<EntityData>): Promise<Change>;
    abstract updateOne(entity: Entity<EntityData>): Promise<Change>;
    abstract removeOne(entity: Entity<EntityData>): Promise<Change>;
    abstract find(filter: any): Promise<Entity<EntityData>[]>;

    async data(): Promise<SerializedStoreData> {
        const entities = await this.find({});
        return { entities: entities.map(entity => dumpToDict(entity)) };
    }
    abstract applyChange(change: Change): Promise<void>;
    abstract applyDelta(delta: Delta): Promise<Change[]>;

}
