import { Change, ChangeData, ChangeType } from "../Change";
import { getLogger } from "../logging";

let log = getLogger('Delta')

export type DeltaData = {
    [key: string]: ChangeData;
}

export class Delta {
    // Max entity key depth: e.g. entity.content.image.height - 3 levels
    // 1 property scope, 2 property object, 3 key
    private _data: DeltaData = {};

    constructor(data: DeltaData) {
        this._data = data;
    }

    static fromChanges(changes: Change[]): Delta {
        let delta = new Delta({});
        for (const change of changes) {
            delta.addChangeFromData(change.data)
        }
        return delta;
    }

    get data(): DeltaData {
        return this._data;
    }

    setData(data: DeltaData) {
        this._data = data;
    }

    *changes(): Generator<Change> {
        for (const changeData of Object.values(this._data)) {
            yield new Change(changeData);
        }
    }

    copy() {
        return new Delta(structuredClone(this._data));
    }

    reversed(): Delta {
        // Makes the changes in place
        const reversedData: DeltaData = {};
        for (const [entityId, reverseComponent, forwardComponent] of Object.values(this._data)) {
            reversedData[entityId] = [entityId, forwardComponent, reverseComponent];
        }
        return new Delta(reversedData);
    }

    addChangeFromData(changeData: ChangeData) {
        const [entityId] = changeData;
        if (this._data[entityId] !== undefined) {
            // Merge the changes
            this.mergeChangeWithPriority(new Change(changeData));
        } else {
            this._data[entityId] = changeData;
        }
    }

    removeChange(entityId: string) {
        if (this._data[entityId] === undefined) {
            throw Error('Entity delta with that key not found');
        }
        delete this._data[entityId];
    }

    isEmpty(): boolean {
        for (const [_, oldState, newState] of Object.values(this._data)) {
            if (Object.keys(oldState).length > 0 || Object.keys(newState).length > 0) {
                return false;
            }
        }
        return true;
    }

    entityIds(): string[] {
        return Object.keys(this._data);
    }

    changeData(entityId: string): ChangeData | undefined {
        return this._data[entityId];
    }

    change(entityId: string): Change | undefined{
        const changeData = this.changeData(entityId);
        if (changeData === undefined) {
            return undefined;
        }
        return new Change(changeData);
    }
    mergeChangeWithPriority(change: Change) {
        /**
         * Merges a new change (second/next) into the existing delta (first)
         * for a specific entity, as if it was applied afterward.
         *
         * This function enforces a priority or ordering, meaning
         * "the second/next change overrides or merges into the first."
         *
         * Supported patterns for merging:
         *  1) Update > Update   : Combine forward/reverse fields
         *  2) Create > Update   : Extend the 'create' forward component
         *  3) Delete > Create   : Treat as an 'update' (old data => new data)
         *  4) Create > Delete   : The creation is effectively canceled
         *  5) Otherwise         : Log as an error or do nothing
         *
         * Notes:
         *  - 'forwardComponent' describes how to go from the "old" to "new" state.
         *  - 'reverseComponent' describes how to go back from the "new" to "old" state.
         */
        const firstChange = this.change(change.entityId);

        // If no existing (first) change for this entity, just add the new one
        if (!firstChange) {
            this.addChangeFromData(change.data);
            return;
        }

        // Identify what kind of changes we're merging:
        const firstCT = firstChange.type();  // e.g. CREATE, UPDATE, DELETE
        const nextCT = change.type();        // the new change being merged in

        /**
         * 1) UPDATE > UPDATE
         *    We combine the forward components to reflect the sum of both updates,
         *    and merge reverse components to preserve the ability to revert back
         *    to the earliest "old" state if necessary.
         */
        if (firstCT === ChangeType.UPDATE && nextCT === ChangeType.UPDATE) {
            const firstForward = { ...firstChange.forwardComponent };
            const firstReverse = { ...firstChange.reverseComponent };
            const nextForward = change.forwardComponent;
            const nextReverse = change.reverseComponent;

            // Forward props: new update fields appended on top
            if (nextForward) {
                firstChange.forwardComponent = { ...firstForward, ...nextForward };
            }
            // Reverse props: nextReverse overrides firstReverse to revert properly
            if (nextReverse) {
                firstChange.reverseComponent = { ...nextReverse, ...firstReverse };
            }

        /**
         * 2) CREATE > UPDATE
         *    We interpret the second update as simply extending the forward part
         *    of the original CREATE. Because the entity was just created, we can
         *    "stack" these updates into its forward component.
         */
        } else if (firstCT === ChangeType.CREATE && nextCT === ChangeType.UPDATE) {
            if (change.forwardComponent) {
                firstChange.forwardComponent = {
                    ...firstChange.forwardComponent,
                    ...change.forwardComponent
                };
            }

        /**
         * 3) DELETE > CREATE
         *    The "delete" said "this entity used to exist, now it doesn't."
         *    The "create" says "here's a new version of the entity."
         *    Interpreting them together means we effectively have an 'update' from
         *    the old data to the newly created data (old => new).
         */
        } else if (firstCT === ChangeType.DELETE && nextCT === ChangeType.CREATE) {
            const oldData = firstChange.reverseComponent;  // Pre-delete data
            const newData = change.forwardComponent;       // Newly created data
            this._data[change.entityId] = [
                change.entityId,
                oldData,    // reverse => revert to "deleted" or old state
                newData     // forward => adopt the new data
            ];

        /**
         * 4) CREATE > DELETE
         *    Means we created an entity and are now deleting it before any usage.
         *    Net effect = remove both changes; there's no entity at all.
         */
        } else if (firstCT === ChangeType.CREATE && nextCT === ChangeType.DELETE) {
            this.removeChange(change.entityId);

        /**
         * 5) EMPTY or unhandled
         *    If the new change is empty, do nothing; if it's an irrational
         *    sequence (like 'Update > Create' on an entity that didn't exist),
         *    log an error.
         */
        } else if (nextCT === ChangeType.EMPTY) {
            // Nothing to do for empty changes
        } else {
            log.error(
                'Irrational or unhandled delta sequence:',
                firstChange,
                '->',
                change
            );
        }

    }

    changeType(entityId: string, reverse: boolean = false): ChangeType {
        let change = this.change(entityId);
        if (!change) {
            throw Error('No entity delta for the given entityId');
        }
        if (reverse) {
            change = change.reversed();
        }
        return change.type();
    }

    mergeWithPriority(next: Delta) { // In place
        /**
         * Union with another delta up to the level of entity.key. I.e. if
         * there's an update for an entity - if the updated key in both deltas
         * is the same - the second one ('other') will override the key in
         * the forward operation. And the first one (this) will override in
         * the backward direction
         */
        // Iterate over all entity deltas in the next delta
        for (const change of next.changes()) {
            this.mergeChangeWithPriority(change);
        }
    }
}

export function squishDeltas(deltas: DeltaData[]) {
    const squishedDelta = new Delta({});

    for (let delta of deltas) {
        squishedDelta.mergeWithPriority(new Delta(delta));
    }

    return squishedDelta;
}

function changeTypeFromData(
    entityDelta: ChangeData,
    reverse: boolean = false
): ChangeType {
    const [_, reverseDelta, forwardDelta] = entityDelta;
    const [componentToCheck, otherComponent] = reverse ? [forwardDelta, reverseDelta] : [reverseDelta, forwardDelta];

    const forwardPresent = Object.keys(otherComponent).length > 0;
    const reversePresent = Object.keys(componentToCheck).length > 0;

    if (forwardPresent && reversePresent) {
        return ChangeType.UPDATE;
    } else if (forwardPresent) {
        return ChangeType.CREATE;
    } else if (reversePresent) {
        return ChangeType.DELETE;
    } else {
        return ChangeType.EMPTY;
    }
}

