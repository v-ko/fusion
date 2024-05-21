import { Entity, EntityData, SerializedEntity, dumpToDict, loadFromDict } from "./libs/Entity"
import { currentTime, get_new_id, timestamp } from "./util"

export enum ChangeTypes {
    EMPTY = 0,
    CREATE = 1,
    UPDATE = 2,
    DELETE = 3
}


interface ChangeData {
    id: string
    old_state?: Entity<EntityData>
    new_state?: Entity<EntityData>
    timestamp: string;
}

export interface SerializedChangeData {
    id: string
    old_state?: SerializedEntity
    new_state?: SerializedEntity
    delta?: Partial<EntityData>
    timestamp: string;
}


export class Change implements ChangeData {
    public id: string
    public old_state?: Entity<EntityData>
    public new_state?: Entity<EntityData>
    public timestamp: string

    constructor(data: ChangeData) {
        this.id = data.id
        this.old_state = data.old_state
        this.new_state = data.new_state
        this.timestamp = data.timestamp
    }

    static new(old_state?: Entity<EntityData>, new_state?: Entity<EntityData>): Change {
        let change = new Change({
            id: get_new_id(),
            old_state: old_state,
            new_state: new_state,
            timestamp: timestamp(currentTime())
        })

        return change;
    }

    get time(): Date {
        return new Date(this.timestamp)
    }

    changeType() {
        if (this.old_state && this.new_state) {
            return ChangeTypes.UPDATE
        } else if (this.old_state) {
            return ChangeTypes.DELETE
        } else if (this.new_state) {
            return ChangeTypes.CREATE
        } else {
            return ChangeTypes.EMPTY
        }
    }

    // def asdict(self) -> dict:
    //     if self.old_state:
    //         old_state = dump_to_dict(self.old_state)
    //     else:
    //         old_state = None

    //     if self.new_state:
    //         new_state = dump_to_dict(self.new_state)
    //     else:
    //         new_state = None

    //     return dict(old_state=old_state,
    //                 new_state=new_state,
    //                 id=self.id,
    //                 timestamp=timestamp(self.time, microseconds=True))

    asdict(): SerializedChangeData {
        let oldState: SerializedEntity | undefined
        let newState: SerializedEntity | undefined

        if (this.old_state) {
            oldState = dumpToDict(this.old_state)
        }
        if (this.new_state) {
            newState = dumpToDict(this.new_state)
        }

        return {
            old_state: oldState,
            new_state: newState,
            id: this.id,
            timestamp: timestamp(this.time, true)
        }
    }

    static fromSafeDeltaDict(serializedChange: SerializedChangeData): Change {
        let changeData: ChangeData = {
            id: serializedChange.id,
            timestamp: serializedChange.timestamp
        }
        let old_state_dict = serializedChange.old_state
        let new_state_dict = serializedChange.new_state as Record<string, any>

        // Get the delta and use it to generate the new_state
        let delta = serializedChange.delta as Record<string, any>
        if (delta !== undefined) {
            if (!old_state_dict) {
                throw new Error("Cannot apply delta to empty state")
            }
            new_state_dict = { ...old_state_dict };

            for (let key in delta.keys()) {
                if (delta[key] !== undefined) {
                    new_state_dict[key] = delta[key];
                }
            }
        }

        if (old_state_dict) {
            changeData.old_state = loadFromDict(old_state_dict)
        }
        if (new_state_dict) {
            changeData.new_state = loadFromDict(new_state_dict as SerializedEntity)
        }

        return new Change(changeData)
    }

    delta(): Partial<EntityData> {
        let delta_dict: Record<string, any> = {}
        let old_state = this.old_state as Record<string, any>
        let new_state = this.new_state as Record<string, any>

        if (old_state === undefined || new_state === undefined) {
            throw new Error("Cannot get delta from empty state")
        }

        for (let key in old_state) {
            let old_val = old_state[key]
            let new_val = new_state[key]

            if (old_val !== new_val) {
                delta_dict[key] = new_val
            }
        }

        return delta_dict
    }

    asSafeDeltaDict(): SerializedChangeData {
        let changeDict = this.asdict()

        if (this.isUpdate()) {
            changeDict.delta = this.delta()
            delete changeDict.new_state
        }

        return changeDict
    }

    static create(entity: Entity<EntityData>): Change {
        return Change.new(undefined, entity)
    }

    static delete(entity: Entity<EntityData>): Change {
        return Change.new(entity, undefined)
    }

    static update(old_state: Entity<EntityData>, new_state: Entity<EntityData>): Change {
        return Change.new(old_state, new_state)
    }

    isCreate(): boolean {
        return this.changeType() === ChangeTypes.CREATE
    }

    isDelete(): boolean {
        return this.changeType() === ChangeTypes.DELETE
    }

    isUpdate(): boolean {
        return this.changeType() === ChangeTypes.UPDATE
    }

    isEmpty(): boolean {
        return this.changeType() === ChangeTypes.EMPTY
    }

    get lastState(): Entity<EntityData> | undefined {
        /**
         * Return the latest available state.
         */
        if (this.new_state !== undefined) {
            return this.new_state
        }
        return this.old_state
    }

    reversed(): Change {
        /**
         * Return a reversed change.
         */
        if (this.isCreate()) {
            return Change.delete(this.new_state!)
        } else if (this.isDelete()) {
            return Change.create(this.old_state!)
        } else if (this.isUpdate()) {
            return Change.update(this.new_state!, this.old_state!)
        } else {
            // raise exeption
            throw new Error("Cannot reverse empty change")
        }
    }
}
