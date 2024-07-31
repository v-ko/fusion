import { Change, ChangeTypes } from "../Change";
import { Entity, EntityData, SerializedEntityData, dumpToDict, loadFromDict } from "../libs/Entity";
import { Delta, entityDeltaFromChange } from "./Delta";

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
            this.insertOne(change.lastState);
        } else if (change.isUpdate()) {
            this.updateOne(change.lastState);
        } else if (change.isDelete()) {
            this.removeOne(change.lastState);
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

    applyDelta(delta: Delta): Change[] {
        // console.log('Applying delta', delta)
        // console.trace()
        const changes = changesFromDelta(this, delta);
        for (const change of changes) {
            this.applyChange(change);
        }
        return changes;
    }
}


export function deltaFromChanges(changes: Change[]): Delta {
    /**
     * Returns a delta object representing the given change set. Order matters.
     * If there are multiple changes for the same object - they are aggregated.
     *
     * Opertion: Iterate over all changes. Add the deltas to an index by id.
     * If there are aggregations possible (id already present): do them. Mainly:
     * - Create > Update: Merge update into Create delta
     * - Update > Update: Merge
     * - Delete > Create: Remove Delete, readd as Create
     * - Create > Delete: Remove Create, don't add Delete
     * Detect irrational change sequences:
     * - Create > Create, Delete > Delete, Update > Create, Delete > Update
     */
    // const deltaData: DeltaData = [];

    // rather use a Delta and add helper methods
    // let deltaEntriesById: Map<string, EntityDeltaData> = new Map();
    let delta = new Delta([]);

    // HERE: refactor the below code according to the above spec

    for (const change of changes) {
        if (change.isEmpty()) {
            continue;
        }

        // Get the change delta
        let entityDelta = entityDeltaFromChange(change);

        // Merge it into the delta object
        delta.mergeEntityDelta(entityDelta);
    }

    // console.log('Delta from changes', changes, delta.data)
    return delta
}

export function changesFromDelta(store: Store, delta: Delta): Change[] {
    const changes: Change[] = [];

    // console.log('Changes from delta', delta.data);

    for (const entityId of delta.entityIds()) {
        let [entityId_, reverseDelta, forwardDelta] = delta.entityDelta(entityId)!;
        let changeType = delta.changeType(entityId);

        // If both forward and reverse deltas are present - it's an update operation
        if (changeType === ChangeTypes.UPDATE) {
            const entity = store.findOne({ id: entityId });

            if (!entity) {
                throw new Error(`Entity with ID ${entityId} not found in store.`);
            }

            const oldState = entity;
            const newState = entity.copy();
            newState.replace(forwardDelta);

            const change = new Change({
                old_state: oldState,
                new_state: newState,
            });

            changes.push(change);
            continue;

        // If no forward delta - it's a delete operation
        } else if (changeType === ChangeTypes.DELETE) {
            const entity = store.findOne({ id: entityId });

            if (!entity) {
                throw new Error(`Entity with ID ${entityId} not found in store.`);
            }

            const oldState = entity;
            const change = Change.delete(oldState);

            changes.push(change);
            continue;

        // If no reverse delta - it's a create operation
        } else if (changeType === ChangeTypes.CREATE) {
            const entity = store.findOne({ id: entityId });

            if (entity) {
                throw new Error(`Entity with ID ${entityId} already exists in store.`);
            }

            const newState = loadFromDict(forwardDelta as SerializedEntityData)
            const change = Change.create(newState);

            changes.push(change);
        }
    }

    return changes;
}


