import { Change } from "../../model/Change";
import { Entity, EntityData, SerializedEntityData, dumpToDict, loadFromDict, transformedEntity } from "../../model/Entity";
import { Delta } from "../../model/Delta";

// a searchFilter is a dict, where there can be an id, a parent_id, a type or any other property
export interface SearchFilter {
    id?: string;
    parentId?: string;
    [key: string]: any;
}

export interface SerializedStoreData {
    entities: SerializedEntityData[];
}

export abstract class Store {
    abstract insertOne(entity: Entity<EntityData>): Change;

    abstract find(filter: SearchFilter): Generator<Entity<EntityData>>;

    abstract findOne(filter: SearchFilter): Entity<EntityData> | undefined;

    abstract updateOne(entity: Entity<EntityData>): Change;

    abstract removeOne(entity: Entity<EntityData>): Change;

    // Batch operations (inefficient implementations)
    insert(entities: Entity<EntityData>[]): Change[] {
        return entities.map(entity => this.insertOne(entity))
    }
    remove(entities: Entity<EntityData>[]): Change[] {
        return entities.map(entity => this.removeOne(entity))
    }
    update(entities: Entity<EntityData>[]): Change[] {
        return entities.map(entity => this.updateOne(entity))
    }
    applyChange(change: Change): void {
        if (change.isCreate()) {
            let entityState = loadFromDict(change.forwardComponent as SerializedEntityData);
            this.insertOne(entityState);
        } else if (change.isUpdate()) {
            let entity = this.findOne({ id: change.entityId });
            if (entity === undefined) {
                throw new Error(`Last state retreival error for ${change.data} (type update)`);
            }
            entity = transformedEntity(entity, change);
            this.updateOne(entity);
        } else if (change.isDelete()) {
            let entity = this.findOne({ id: change.entityId });
            if (entity === undefined) {
                throw new Error(`Last state retreival error for ${change.data} (type delete)`);
            }
            this.removeOne(entity);
        }
    }
    data(): SerializedStoreData {
        const entities = Array.from(this.find({}));
        return { entities: entities.map(entity => dumpToDict(entity)) };
    }
    loadData(data: SerializedStoreData): void {
        for (const entityData of data.entities) {
            const entity = loadFromDict(entityData);
            this.insertOne(entity);
        }
    }

    applyDelta(delta: Delta) {
        for (const change of delta.changes()) {
            this.applyChange(change);
        }
    }
}
