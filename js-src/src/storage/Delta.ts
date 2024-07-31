import { Change, ChangeTypes } from "../Change";
import { SerializedEntityData, dumpToDict, entityKeysAreEqual } from "../libs/Entity";

export type EntityDeltaComponent = Partial<SerializedEntityData>

export type EntityDeltaData = [
    string, // entity id
    EntityDeltaComponent, // old state delta (reverse delta component)
    EntityDeltaComponent // new state delta (forward delta component)
]

export type DeltaData = EntityDeltaData[] // Ordered by change sequence

export class Delta {
    // Max entity key depth: e.g. entity.content.image.height - 3 levels
    // 1 property scope, 2 property object, 3 key
    private _data: DeltaData = [];
    private _byId: Map<string, EntityDeltaData> = new Map();

    constructor(data: DeltaData) {
        this._data = data;

        // Index by entity id
        for (let entityDelta of data) {
            this._byId.set(entityDelta[0], entityDelta)
        }
    }

    get data(): DeltaData {
        return this._data;
    }

    copy() {
        return new Delta(structuredClone(this._data))
    }

    reversed(): Delta {
        let reversedData: DeltaData = []
        for (let [entityId, forward, reverse] of this._data.slice().reverse()) {
            reversedData.push([entityId, reverse, forward])
        }

        return new Delta(reversedData)
    }

    addEntityDelta(entityDelta: EntityDeltaData) {
        if (this._byId.get(entityDelta[0]) !== undefined) {
            throw Error('Entity delta with that key already present')
        }

        this._data.push(entityDelta)
        this._byId.set(entityDelta[0], entityDelta)
    }

    removeEntityDelta(entityId: string) {
        let entityDelta = this._byId.get(entityId)
        if (entityDelta === undefined) {
            throw Error('Entity delta with that key not found')
        }

        this._data = this._data.filter((ed) => ed[0] !== entityId)
        this._byId.delete(entityId)
    }

    isEmpty(): boolean {
        let empty = true
        for (let entityDelta of Object.values(this._data)) {
            if (entityDelta[1] || entityDelta[2]) {
                empty = false
                break
            }
        }
        return empty
    }

    entityIds(): string[] {
        return Array.from(this._byId.keys())
    }

    entityDelta(entityId: string): EntityDeltaData | undefined {
        // Is expected to be mutable in some places
        return this._byId.get(entityId)
    }

    mergeEntityDelta(entityDelta: EntityDeltaData) {
        /**
         * Merges an entity delta into this delta as if it was applied after it.
         * - Create > Update: Merge update into Create delta
         * - Update > Update: Merge both forward and reverse components
         * - Delete > Create: Remove Delete, readd as Create
         * - Create > Delete: Remove Create, don't add Delete
         * Detect irrational change sequences:
         * - Create > Create, Delete > Delete, Update > Create, Delete > Update
         *
         * It's not the most efficient implementation, but given that we should
         * keep change order, that collisions are rare and that the number of
         * entities is expected to be small - it's fine...?
         */
        // Get entity delta from this delta
        let firstEntityDelta = this.entityDelta(entityDelta[0])

        // If there's no present delta: just merge it
        if (firstEntityDelta === undefined) {
            this.addEntityDelta(entityDelta)
            return
        }

        // Infer change type for both
        let firstCT = entityDeltaChangeType(firstEntityDelta)
        let nextCT = entityDeltaChangeType(entityDelta)

        // Else if Update > Update
        if (firstCT === ChangeTypes.UPDATE && nextCT === ChangeTypes.UPDATE) {
            let firstForward = firstEntityDelta[2]
            let firstReverse = firstEntityDelta[1]
            let nextForward = entityDelta[2]
            let nextReverse = entityDelta[1]

            // Merge forward
            if (nextForward) {
                firstForward = { ...firstForward, ...nextForward }
            }
            // Merge reverse
            if (nextReverse) {
                firstReverse = { ...nextReverse, ...firstReverse }
            }

            // Else if Create > Update
        } else if (firstCT === ChangeTypes.CREATE && nextCT === ChangeTypes.UPDATE) {
            let firstForward = firstEntityDelta[2]
            let nextForward = entityDelta[2]

            // Merge forward
            if (nextForward) {
                firstEntityDelta[2] = { ...firstForward, ...nextForward }
            }
            // Disregard the rest of the other/next delta

            // Else if Delete > Create
        } else if (firstCT === ChangeTypes.DELETE && nextCT === ChangeTypes.CREATE) {
            this.removeEntityDelta(entityDelta[0])
            this.addEntityDelta(entityDelta)

            // Else if Create > Delete
        } else if (firstCT === ChangeTypes.CREATE && nextCT === ChangeTypes.DELETE) {
            this.removeEntityDelta(entityDelta[0])

            // Else if irrational sequence
        } else {
            throw Error('Irrational delta sequence detected')
        }
    }

    // equals(other: Delta): boolean {
    //     let thisData = this.data()
    //     let otherData = other.data()

    //     if (Object.keys(thisData).length !== Object.keys(otherData).length){
    //         return false
    //     }

    //     function areEqual(a: Partial<EntityData>, b: Partial<EntityData>): boolean {
    //         // 3-lvl check
    //         for (let key in (a as any)) {
    //             if (!(b as any)[key]){
    //                 return false
    //             }
    //             for (let subKey in (a as any)[key]) {
    //                 if (!(b as any)[key][subKey]){
    //                     return false
    //                 }
    //                 if ((a as any)[key][subKey] !== (b as any)[key][subKey]){
    //                     return false
    //                 }
    //             }
    //         }
    //         return true
    //     }

    //     for (let entityId in thisData) {
    //         if (!otherData[entityId]){
    //             return false
    //         }
    //         if (!areEqual(thisData[entityId], otherData[entityId])){
    //             return false
    //         }
    //     }

    //     return true
    // }

    changeType(entityId: string, reverse: boolean = false): ChangeTypes {
        const entityDelta = this.entityDelta(entityId)
        if (!entityDelta) {
            throw Error('No entity delta for the given entityId')
        }
        return entityDeltaChangeType(entityDelta, reverse)
    }

    mergeWith(next: Delta) { // In place
        /**
         * Union with another delta up to the level of entity.key. I.e. if
         * there's an update for an entity - if the updated key in both deltas
         * is the same - the socond one ('other') will override the key in
         * the forward operation. And the first one (this) will override in
         * the backward direction
         */

        //     // Get a union of all entity deltas in both deltas
        //     let entityIds = new Set([...this.entityIds(), ...next.entityIds()])

        //     for (let entityId of entityIds) {
        //         // Where not present in either delta: just keep

        //         // Where changes are present in both deltas for an entity:
        //         // Merge by replacing in both the forward and reverse components
        //         // (in the appropriate direction) while dropping empty changes and
        //         // checking for irrational sequnces

        //         let entityDeltaThis = this.entityDelta(entityId)
        //         let entityDeltaNext = next.entityDelta(entityId)
        //         let thisECT = this.changeType(entityId)
        //         let otherECT = this.changeType(entityId)


        //         // Add any changes for entity ids missing in this delta
        //         //(no conflicts are possible)
        //         if (!entityDeltaThis) {
        //             this._data[entityId] = structuredClone(entityDeltaNext!)
        //             continue

        //         } else if (!entityDeltaNext) { // Nothing to do
        //             continue
        //         }

        //         // Else we have merge, check for consistency and clean

        //         // Consistency checks - no empty changes are expected and
        //         // create>create, delete>delete, delete>update, update>create
        //         // are not rational. Checking in one direction is enough
        //         if (thisECT === ChangeTypes.EMPTY || otherECT === ChangeTypes.EMPTY) {
        //             throw Error('Empty delta found on delta merge for entityId ' + entityId)
        //         }
        //         if (thisECT === ChangeTypes.DELETE && otherECT !== ChangeTypes.CREATE) {
        //             throw Error('Irrational delta sequence DELETE -> !CREATE')
        //         }
        //         if (otherECT === ChangeTypes.CREATE && thisECT !== ChangeTypes.DELETE) {
        //             throw Error('Irrational delta sequence !DELETE -> CREATE')
        //         }

        //         // Merge
        //         let thisForward = entityDeltaThis[0]
        //         let thisReverse = entityDeltaThis[1]
        //         let otherForward = entityDeltaNext[0]
        //         let otherReverse = entityDeltaNext[1]

        //         // Merge forward (with priority for the second delta)
        //         if (otherForward) {
        //             thisForward = { ...thisForward, ...structuredClone(otherForward) }
        //         }
        //         // Merge reverse (with priority for the first delta)
        //         if (otherReverse) {
        //             thisReverse = { ...structuredClone(otherReverse), ...thisReverse }
        //         }

        //         // Update the entity delta or remove if empty
        //         if (thisForward || thisReverse) {
        //             this._data[entityId] = [thisForward, thisReverse]
        //         } else {
        //             delete this._data[entityId]
        //         }
        //     }
        // }

        // Iterate over all entity deltas in the next delta
        for (let entityDelta of next.data) {
            this.mergeEntityDelta(entityDelta)
        }
    }
}


export function squishDeltas(deltas: DeltaData[]) {
    let squishedDelta = new Delta([])

    for (let delta of deltas) {
        squishedDelta.mergeWith(new Delta(delta))
    }

    return squishedDelta
}

export function entityDeltaFromChange(change: Change): EntityDeltaData {
    const lastState = change.lastState
    const oldState: Record<string, any> = change.old_state ? dumpToDict(change.old_state) : {};
    const newState: Record<string, any> = change.new_state ? dumpToDict(change.new_state) : {};

    const reverseDelta: Record<string, any> = {};
    const forwardDelta: Record<string, any> = {};

    const allKeys = new Set([...Object.keys(oldState), ...Object.keys(newState)]);
    for (const key of allKeys) {
        const oldVal = oldState[key];
        const newVal = newState[key];

        if (!entityKeysAreEqual(oldVal, newVal)) {
            if (oldVal !== undefined) {
                reverseDelta[key] = oldVal;
            }
            if (newVal !== undefined) {
                forwardDelta[key] = newVal;
            }
        }
    }

    return [lastState.id, reverseDelta, forwardDelta]
}


function entityDeltaChangeType(entityDelta: EntityDeltaData, reverse: boolean = false): ChangeTypes {
    let reverseComponentIndex = 1
    let forwardComponentIndex = 2
    if (reverse) {
        reverseComponentIndex = 2
        forwardComponentIndex = 1
    }

    let forwardPresent = Object.keys(entityDelta[forwardComponentIndex]).length > 0
    let reversePresent = Object.keys(entityDelta[reverseComponentIndex]).length > 0

    if (forwardPresent && reversePresent) {
        return ChangeTypes.UPDATE
    } else if (forwardPresent) {
        return ChangeTypes.CREATE
    } else if (reversePresent) {
        return ChangeTypes.DELETE
    } else {
        return ChangeTypes.EMPTY
    }
}
