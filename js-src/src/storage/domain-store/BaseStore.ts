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

export class IrrationalStorageOperation extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'IrrationalStorageOperation';
    }
}

export abstract class Store {
    onChanges: ((delta: Delta) => void) | null = null;

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
    /** Apply a single change, firing onChange. For bulk/internal use, see loadChange. */
    applyChange(change: Change): Change {
        return this._applyChangeCore(change);
    }

    /** Apply a single change without firing onChange (used by repos, hydration, etc). */
    loadChange(change: Change): Change {
        const saved = this.onChanges;
        this.onChanges = null;
        try {
            return this._applyChangeCore(change);
        } finally {
            this.onChanges = saved;
        }
    }

    private _applyChangeCore(change: Change): Change {
        if (change.isCreate()) {
            let entityState = loadFromDict(change.forwardComponent as SerializedEntityData);
            return this.insertOne(entityState);
        } else if (change.isUpdate()) {
            let entity = this.findOne({ id: change.entityId });
            if (entity === undefined) {
                throw new IrrationalStorageOperation(`Last state retreival error for ${change.data} (type update)`);
            }
            entity = transformedEntity(entity, change);
            return this.updateOne(entity);
        } else if (change.isDelete()) {
            let entity = this.findOne({ id: change.entityId });
            if (entity === undefined) {
                throw new IrrationalStorageOperation(`Last state retreival error for ${change.data} (type delete)`);
            }
            return this.removeOne(entity);
        }
        return change;
    }

    data(): SerializedStoreData {
        const entities = Array.from(this.find({}));
        return { entities: entities.map(entity => dumpToDict(entity)) };
    }
    loadData(data: SerializedStoreData): void {
        const saved = this.onChanges;
        this.onChanges = null;
        try {
            for (const entityData of data.entities) {
                const entity = loadFromDict(entityData);
                this.insertOne(entity);
            }
        } finally {
            this.onChanges = saved;
        }
    }

    /** Apply a delta, firing onChange once at the end. For bulk/internal use, see loadDelta. */
    applyDelta(delta: Delta, skipIrrationalOperations: boolean = false): Delta {
        const appliedDelta = this._applyDeltaCore(delta, skipIrrationalOperations);

        if (this.onChanges && !appliedDelta.isEmpty()) {
            this.onChanges(appliedDelta);
        }
        return appliedDelta;
    }

    /** Apply a delta without firing onChange (used by repos, hydration, reconciliation internals). */
    loadDelta(delta: Delta, skipIrrationalOperations: boolean = false): Delta {
        return this._applyDeltaCore(delta, skipIrrationalOperations);
    }

    private _applyDeltaCore(delta: Delta, skipIrrationalOperations: boolean): Delta {
        // Suppress per-item onChange — callers (applyDelta/loadDelta) handle notification
        const saved = this.onChanges;
        this.onChanges = null;

        const appliedDelta = new Delta({});
        try {
            for (const change of delta.changes()) {
                try {
                    const appliedChange = this._applyChangeCore(change);
                    appliedDelta.addChangeFromData(appliedChange.data);
                } catch (error) {
                    if (!skipIrrationalOperations || !(error instanceof IrrationalStorageOperation)) {
                        throw error;
                    }
                }
            }
        } finally {
            this.onChanges = saved;
        }
        return appliedDelta;
    }
}
