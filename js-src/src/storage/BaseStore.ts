import { Change, ChangeType } from "../Change";
import { Entity, EntityData, SerializedEntityData, dumpToDict, loadFromDict } from "../libs/Entity";
import { Delta } from "./Delta";

// a searchFilter is a dict, where there can be an id, a parent_id, a type or any other property
export interface SearchFilter {
    id?: string;
    parentId?: string;
    type?: new (...args: any[]) => Entity<EntityData>;
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
            this.insertOne(lastEntityState(this, change));
        } else if (change.isUpdate()) {
            this.updateOne(lastEntityState(this, change));
        } else if (change.isDelete()) {
            this.removeOne(lastEntityState(this, change));
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
        console.log('Applying delta', delta)

        for (const change of delta.changes()) {
            this.applyChange(change);
        }
    }
}

export function lastEntityState(store: Store, change: Change): Entity<EntityData> {
    let entity: Entity<EntityData> | undefined

    switch (change.type()) {
        case ChangeType.CREATE:
            return loadFromDict(change.forwardComponent as SerializedEntityData);

        case ChangeType.UPDATE:
            entity = store.findOne({ id: change.entityId });
            if (entity === undefined) {
                throw new Error(`Last state retreival error for ${change} (type update)`);
            }
            return entity.withAppliedChange(change);

        case ChangeType.DELETE:
            entity = store.findOne({ id: change.entityId });
            if (entity === undefined) {
                throw new Error(`Last state retreival error for ${change} (type delete)`);
            }
            return entity;

        case ChangeType.EMPTY:
            throw new Error(`Cannot get last state from empty change`);

        default:
            throw new Error(`Unknown change type ${change.type()}`);

    }
}
