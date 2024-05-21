import { fusion } from "../index";
import { getLogger } from "../logging";
import { get_new_id as getNewId } from "../util";

const log = getLogger('entity.ts');

// Library related variables and functions
let entityLibrary: { [key: string]: typeof Entity<any> } = {};
let _lastEntityId: number = 0


export function getEntityId(): string {
    if (fusion.reproducibleIds) {
        _lastEntityId += 1;
        return _lastEntityId.toString().padStart(8, '0');
    } else {
        return getNewId();
    }
}


export function resetEntityIdCounter() {
    _lastEntityId = 0
}


export function entityType<T extends typeof Entity<S>, S extends EntityData>(name: string): Function {
    // If 'name' is not a string - throw error
    if (typeof name !== 'string') {
        throw new Error('Entity type name must be a string');
    }

    return function(entity_class: T): T {
        let entityClassName = name; // Use the explicitly provided name instead of inferring
        // log.info(`Registering entity class ${entityClassName}`);
        entityLibrary[entityClassName] = entity_class;
        return entity_class;
    }
}

export function getEntityClassByName<T extends typeof Entity>(entity_class_name: string): T {
    return entityLibrary[entity_class_name] as T
}


export interface SerializedEntity extends EntityData {
    type_name: string;
}

// Serialization related functions
export function dumpToDict<T extends Entity<EntityData>>(entity: T): SerializedEntity {
    const entityDict = entity.toObject() as SerializedEntity
    entityDict.type_name = entity.constructor.name

    return entityDict
}


export function loadFromDict<T extends SerializedEntity>(entityDict: T): Entity<EntityData> {
    const typeName = entityDict.type_name
    const cls = getEntityClassByName(typeName)
    let instance: any

    if (cls === undefined) {
        throw new Error(`Entity class ${typeName} not found.`)
    } else {
        instance = new (cls as new (data: T) => Entity<T>)(entityDict)
    }
    return instance
}

// Entity definition
export interface EntityData {
    id: string;  // Allows for composite ids
    // parentId: string;
}


export abstract class Entity<T extends EntityData> {
    _data: T;

    constructor(data: T) {
        this._data = data;

        // // Assign an id if it is not provided
        // if (this._data.id === undefined) {
        //     this._data.id = getEntityId()
        // }
    }

    get id(): string {
        return this._data.id;
    }

    // get parentId(): string {
    //     return this._data.parentId;
    // }

    abstract get parentId(): string;
    get parent_id(): string {
        log.warning('parent_id is deprecated. Use parentId instead.');
        return this.parentId;
    }

    // withId<S extends typeof this>(new_id: string): S {
    //     const newData = { ...this._data, id: new_id };
    //     return new (this.constructor as { new(data: T): S })(newData);
    // }
    // copy<S extends typeof this>(): S {
    //     // We're copying the data object in the constructor, so no need to do it here
    //     return new (this.constructor as { new(data: T): S })(this._data);
    // }
    withId(new_id: string): this {
        const newData = { ...this._data, id: new_id };
        return new (<any>this.constructor)(newData);
    }
    copy(): this {
        // We're copying the data object in the constructor, so no need to do it here
        return new (<any>this.constructor)(this.data());
    }
    copyWithNewId(): this {
        return this.withId(getEntityId());
    }
    data(): T {
        // console.log(this._data)
        return {...this._data};   // structuredClone(this._data);
    }
    toObject(): T {
        return this.data();
    }
    asdict(): T {
        return this.data();
    }

    replace(new_data: Partial<T>) {
        if (new_data.id !== undefined) {
            throw new Error(
                'The id of an entity is immutable. Use the withId method to create an object with the new id.');
        }
        const newData = { ...this._data, ...new_data };
        this._data = newData;
    }

    replace_silent(new_data: Record<string, any>): Record<string, any> {
        const newData: Partial<T> = {};
        const leftovers: Record<string, any> = {};

        for (const key in new_data) {
            if ((key as keyof T) in this._data || typeof (this._data as any)[key] !== 'undefined') {
                newData[key as keyof T] = new_data[key];
            } else {
                leftovers[key] = new_data[key];
            }
        }

        this.replace(newData);

        return leftovers;
    }
}

