import { Store, SearchFilter } from "./BaseStore"
import { Entity, EntityData } from "../libs/Entity"
import { Change } from "../Change"
import { getLogger } from "../logging"

const log = getLogger('InMemoryRepository')


export class InMemoryStore extends Store {
    private perEntityId: Map<string, Entity<EntityData>>;
    private perEntityParentId: Map<string, Entity<EntityData>[]>;
    private perEntityType: Map<string, Entity<EntityData>[]>;

    constructor() {
        super()
        this.perEntityId = new Map();
        this.perEntityParentId = new Map();
        this.perEntityType = new Map();
    }

    private upsertToCache(entity: Entity<EntityData>, isNew: boolean): void {
        // Copy the entity to protect the repo from untracked changes
        entity = entity.copy();

        // Update perEntityId map
        this.perEntityId.set(entity.id, entity);

        // Update perEntityParentId map
        if (entity.parentId !== null) {
            const siblings = this.perEntityParentId.get(entity.parentId) || [];
            const existingIndex = siblings.findIndex(sibling => sibling.id === entity.id);
            if (existingIndex !== -1 && isNew) {
                throw new Error(`Entity with ID ${entity.id} already exists in parentId mapping.`);
            }
            if (existingIndex === -1) {
                siblings.push(entity);
            } else {
                siblings[existingIndex] = entity; // Update existing entity in case of update
            }
            this.perEntityParentId.set(entity.parentId, siblings);
        }

        // Update perEntityType map
        const typeName = entity.constructor.name;
        const typeEntities = this.perEntityType.get(typeName) || [];
        const typeIndex = typeEntities.findIndex(e => e.id === entity.id);
        if (typeIndex === -1 && isNew) {
            typeEntities.push(entity);
        } else if (typeIndex !== -1 && !isNew) {
            typeEntities[typeIndex] = entity; // Update existing entity in case of update
        } else if (isNew) {
            throw new Error(`Entity with ID ${entity.id} already exists in type mapping.`);
        }
        this.perEntityType.set(typeName, typeEntities);
    }

    private removeFromCache(entity: Entity<EntityData>): void {
        // Remove from perEntityId map
        this.perEntityId.delete(entity.id);

        // Remove from perEntityParentId map
        if (entity.parentId !== null) {
            const siblings = this.perEntityParentId.get(entity.parentId) || [];
            const index = siblings.findIndex(sibling => sibling.id === entity.id);
            if (index !== -1) {
                siblings.splice(index, 1);
                if (siblings.length > 0) {
                    this.perEntityParentId.set(entity.parentId, siblings);
                } else {
                    this.perEntityParentId.delete(entity.parentId);
                }
            }
        }

        // Remove from perEntityType map
        const typeName = entity.constructor.name;
        const typeEntities = this.perEntityType.get(typeName) || [];
        const typeIndex = typeEntities.findIndex(e => e.id === entity.id);
        if (typeIndex !== -1) {
            typeEntities.splice(typeIndex, 1);
            if (typeEntities.length > 0) {
                this.perEntityType.set(typeName, typeEntities);
            } else {
                this.perEntityType.delete(typeName);
            }
        }
    }

    insertOne(entity: Entity<EntityData>): Change {
        // console.log('Inserting entity', entity.id, entity.parentId, entity.constructor.name)
        if (this.perEntityId.has(entity.id)) {
            throw new Error(`Entity with ID ${entity.id} already exists.`);
        }
        this.upsertToCache(entity, true);
        return Change.create(entity);
    }

    updateOne(entity: Entity<EntityData>): Change {
        let oldState = this.perEntityId.get(entity.id);
        if (!oldState) {
            throw new Error(`Entity with ID ${entity.id} does not exist.`);
        }
        this.upsertToCache(entity, false);
        return Change.update(oldState, entity);
    }

    removeOne(entity: Entity<EntityData>): Change {
        if (!this.perEntityId.has(entity.id)) {
            throw new Error(`Entity with ID ${entity.id} does not exist.`);
        }
        let oldEntity = this.perEntityId.get(entity.id);
        if (oldEntity) {
            this.removeFromCache(oldEntity);
        }
        return Change.delete(entity);
    }


    *find<T extends Entity<EntityData>>(filter: SearchFilter = {}): Generator<T> {
        const { id, type, parentId, ...otherFilters } = filter;

        let candidates: Entity<EntityData>[] = [];

        // Filter by ID
        if (id !== undefined) {
            const entity = this.perEntityId.get(id);
            if (entity && (!type || entity instanceof type)) {
                candidates.push(entity);
            }
        }
        // Filter by Parent ID
        else if (parentId !== undefined) {
            const entities = this.perEntityParentId.get(parentId) || [];
            if (type) {
                candidates = entities.filter(entity => entity instanceof type);
            } else {
                candidates = entities;
            }
        }
        // Filter by Type
        else if (type !== undefined) {
            const typeName = type.name;
            candidates = this.perEntityType.get(typeName) || [];
        } else {
            // If no specific ID, type, or Parent ID is provided, consider all entities
            candidates = Array.from(this.perEntityId.values());
        }

        // Apply otherFilters
        candidatesLoop: for (const candidate of candidates as Record<string, any>[]) {
            for (const [key, value] of Object.entries(otherFilters)) {
                if (candidate._data[key] !== value) {
                    continue candidatesLoop; // Skip entities that do not match other filters
                }
            }
            yield candidate.copy() as T;
        }
    }

    findOne<T extends Entity<EntityData>>(filter: SearchFilter): T | undefined {
        for (const entity of this.find<T>(filter)) {
            return entity; // Return the first entity that matches the filter
        }
        return undefined; // If no entity matches the filter
    }

}
