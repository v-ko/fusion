import { ChangeType } from "../Change";
import { Store } from "./BaseStore";
import { dumpToDict } from '../libs/Entity';
import { cryptoModule } from "../base-util";
import { getLogger } from "../logging";
import { Delta } from "./Delta";

const log = getLogger('HashTree')

const subtleCrypto = cryptoModule();

function sortObjectProperties(obj: any, depth: number = 1): any {
    if (depth > 3) {
        throw new Error("Depth exceeded: This function supports sorting up to 3 levels deep only.");
    }

    if (Array.isArray(obj)) {
        return obj.map(item =>
            typeof item === 'object' && item !== null ?
                sortObjectProperties(item, depth + 1) :
                item
        );
    }

    if (typeof obj !== 'object' || obj === null) {
        return obj;
    }

    const sorted: { [key: string]: any } = {};
    Object.keys(obj).sort().forEach(key => {
        const value = obj[key];
        sorted[key] = typeof value === 'object' && value !== null ?
            sortObjectProperties(value, depth + 1) :
            value;
    });

    return sorted;
}

function getEntityDataString(entity: any): string {
    const data = dumpToDict(entity);
    const sortedData = sortObjectProperties(data);
    return JSON.stringify(sortedData);
}

enum NodeType {  // The hash struct needs to index multiple trees in the store
    SUPER_ROOT,  // This is the store root
    ROOT,        // Each entity without a parent is a root
    NON_ROOT     // All other entities are non-roots
}

export class HashTreeNode {
    // The store itsel is not an entity, so the root has null id
    tree: HashTree;
    type: NodeType;
    entityId: string; // The super-root has an empty id
    parentId: string = ''; //  root nodes have an empty parent id

    children: { [key: string]: HashTreeNode } = {}; // by entity id
    childrenSorted: HashTreeNode[] = []; // sorted by entity id

    entityDataHash: string = ''; // '' is possible at init or for the super root node (all others are serializable and should be specified on instantiation or at data updates)
    _hash: string = '';

    _hashOutdated: boolean = true;
    _removed: boolean = false;
    _childrenNotSorted: boolean = false;

    constructor(tree: HashTree, entityId: string, parentId: string, entityDataHash: string, type: NodeType) {
        this.tree = tree;
        this.type = type;
        this.entityId = entityId;
        this.parentId = parentId;
        this.entityDataHash = entityDataHash;
    }

    data(): any {
        return {
            entityId: this.entityId,
            parentId: this.parentId,
            entityDataHash: this.entityDataHash,
            hash: this.hash,
            hashOutdated: this.hashOutdated,
            removed: this.removed,
            childrenSortOutdated: this.childrenSortOutdated,
            children: this.childrenSorted.map(child => child.data())
        }
    }

    get hash(): string {
        if (this._hashOutdated) {
            throw Error("Hash requested, but it's outdated.");
        }
        return this._hash;
    }

    get hashOutdated(): boolean {
        return this._hashOutdated;
    }
    setHashOutdated() {
        /**
         * Set hash outdated for this node and all its parents
         */
        this._hashOutdated = true;

        // Set for all parents
        let parent = this.tree.nodes[this.parentId];
        if (parent && !parent.hashOutdated) {
            parent.setHashOutdated();
        }
    }
    get removed(): boolean {
        return this._removed;
    }
    markAsRemoved() {
        this._removed = true;
        this.setHashOutdated();
    }
    get childrenSortOutdated(): boolean {
        return this._childrenNotSorted;
    }
    setChildrenSortOutdated() {
        this._childrenNotSorted = true;
        this.setHashOutdated();
    }
    sortChildren() {
        this.childrenSorted.sort((a, b) => {
            if (a.entityId < b.entityId) {
                return -1;
            }
            if (a.entityId > b.entityId) {
                return 1;
            }
            return 0;
        });
        this._childrenNotSorted = false;
        this.setHashOutdated();
    }

    addChild(child: HashTreeNode) {
        if (child.entityId === ''){
            throw Error("Cannot add a child with empty entity id. This is reserved for the super-root");
        }

        this.children[child.entityId] = child;
        this.childrenSorted.push(child);
        this.setChildrenSortOutdated();
        // this.sortChildren();
    }
    removeChild(child: HashTreeNode) {
        delete this.children[child.entityId];
        let index = this.childrenSorted.indexOf(child);
        if (index === -1) {
            throw Error("Child not found");
        }
        this.childrenSorted.splice(index, 1);
        this.setChildrenSortOutdated(); // not needed if sort is on every insert
    }
    safeForSubtreeRemoval(): boolean {
        if (this.removed && this.childrenSorted.length === 0) {
            return true;
        }
        for (let child of this.childrenSorted) {
            if (!child.removed || !child.safeForSubtreeRemoval()) {
                return false;
            }
        }
        return true;
    }
    async updateHash() {
        if (this.removed) {
            throw Error("Cannot update hash of a removed node");
        }

        if (this.childrenSortOutdated) {
            throw Error("Cannot update hash: children not sorted");
        }

        // Check for any removed children - they should have been cleaned up
        // And update hashes for all children
        for (let child of this.childrenSorted) {
            if (child.removed) {
                throw Error("Cannot update hash: child node marked for removal must be cleaned up first");
            }
            if (child.hashOutdated) {
                await child.updateHash();
            }
        }

        const childHashes = this.childrenSorted.map((child) => child.hash);
        this._hash = await hash(this.entityDataHash, childHashes);
        this._hashOutdated = false;
        this.hash
    }
}

export class HashTree {
    private _tmpSubtrees: { [key: string]: HashTreeNode[] } = {}; // by parent id
    superRoot: HashTreeNode | null = null;
    nodes: { [key: string]: HashTreeNode } = {} // by entity id
    _removedNodeCleanupNeeded: boolean = false; // Handle nodes marked for removal

    constructor() {
        this.createSuperRoot();
    }

    get removedNodeCleanupNeeded(): boolean {
        return this._removedNodeCleanupNeeded;
    }
    cleanUpRemovedNodesLater() {
        this._removedNodeCleanupNeeded = true;
    }

    createSuperRoot() {
        if (!!this.superRoot) {
            throw Error("Cannot have multiple super-root nodes");
        }
        let superRoot = new HashTreeNode(this, '', '', '', NodeType.SUPER_ROOT);
        this.insertNode(superRoot);
    }

    createRoot(entityId: string, entityDataHash: string) {
        if (entityId === '') {
            throw Error("Root nodes must have an entity id");
        }
        let root = new HashTreeNode(this, entityId, '', entityDataHash, NodeType.ROOT);
        this.insertNode(root);
        return root;
    }

    createNonRoot(entityId: string, parentId: string, entityDataHash: string) {
        if (parentId === '') {
            throw Error("Non-root nodes must have a parent id");
        }
        if (entityId === '') {
            throw Error("Non-root nodes must have an entity id");
        }
        let nonRoot = new HashTreeNode(this, entityId, parentId, entityDataHash, NodeType.NON_ROOT);
        this.insertNode(nonRoot);
        return nonRoot;
    }

    insertNode(node: HashTreeNode) {
        if (this.nodes[node.entityId]) {
            throw Error("Node with this entity id already exists");
        }

        let parent = this.nodes[node.parentId];

        if (node.type === NodeType.SUPER_ROOT) { // For adding the super-root
            if (!!this.superRoot){
                throw Error("Cannot have multiple super-root nodes");
            }
            if (parent) {
                throw Error("Wtf. Super-root node cannot have a parent");
            }
            this.superRoot = node;
            this.nodes[node.entityId] = node;

        // Else if the parent is not in the tree, add it to the temporary subtrees
        } else if (!parent) {
            log.info('Parent not found, adding to tmp subtrees', node.parentId)
            if (!this._tmpSubtrees[node.parentId]) {  // Create list if not already present
                this._tmpSubtrees[node.parentId] = [];
            }
            this._tmpSubtrees[node.parentId].push(node);
            return;

        } else {  // If the parent is in the tree, add the node to it
            parent.addChild(node);
            this.nodes[node.entityId] = node;
        }

        // If some of the tmp subtree roots have this node as a parent
        // reattach them properly
        if (this._tmpSubtrees[node.entityId]) {
            log.info('Reattaching tmp subtrees', node.entityId)
            let subtree = this._tmpSubtrees[node.entityId];
            delete this._tmpSubtrees[node.entityId];

            for (let child of subtree) {
                this.insertNode(child); // This will recursively re-check
            }

        }
    }
    removeNode(node: HashTreeNode) {
        log.info('Marking node for removal', node.entityId)
        node.markAsRemoved();
        this.cleanUpRemovedNodesLater();
    }
    parent(node: HashTreeNode): HashTreeNode | null {
        return this.nodes[node.parentId] || null;
    }

    deleteNodesMarkedForRemoval() {
        /** Remove nodes marked for removal */
        log.info('Cleaning up hash tree')
        // Remove nodes marked for removal. First detach from parents, then delete
        let forDeletion: HashTreeNode[] = [];
        for (let node of Object.values(this.nodes)) {
            if (!node.removed){
                continue;
            }

            if(node.safeForSubtreeRemoval()) {
                forDeletion.push(node); // Mark for deletion from the index
                let parent = this.parent(node)
                if (!parent) {
                    throw Error("Unexpected missing parent");
                }
                parent.removeChild(node); // Remove from parent
            } else {
                throw Error("Cannot remove node with children");
            }
        }

        // After all have been removed from the parents - actually delete them
        for (let node of forDeletion) {
            delete this.nodes[node.entityId];
        }

        this._removedNodeCleanupNeeded = false;
    }

    async updateRootHash() {
        if (!this.superRoot) {
            throw Error("Cannot update hash of an empty tree");
        }

        if (this.removedNodeCleanupNeeded) {
            this.deleteNodesMarkedForRemoval();
        }

        // Check for hanging subtrees
        if (Object.keys(this._tmpSubtrees).length > 0) {
            throw Error("Cannot update hash: hanging tmp subtrees");
        }

        // Sort children where needed
        for (let node of Object.values(this.nodes)) {
            if (node.childrenSortOutdated) {
                node.sortChildren();
            }
        }

        await this.superRoot.updateHash();
    }

    rootHash(): string {
        if (!this.superRoot) {
            throw Error("Cannot get hash of an empty tree");
        }
        if (this.removedNodeCleanupNeeded) {
            throw Error("Tree needs cleanup");
        }
        if (this.superRoot.hashOutdated) {
            throw Error("Super-root hash is outdated");
        }
        return this.superRoot.hash;
    }
}


async function hash(data: string, dataForConcat: string[] = []): Promise<string> {
    // Concatenate (child hashes should be sorted by entityId)
    let concatenatedData = data + dataForConcat.join('');

    const encoder = new TextEncoder();
    const dataEncoded = encoder.encode(concatenatedData);
    const hashBuffer: ArrayBuffer = await subtleCrypto.digest("SHA-256", dataEncoded);
    const hashArray = Array.from(new Uint8Array(hashBuffer));
    const hashHex = hashArray.map(byte => byte.toString(16).padStart(2, '0')).join('');

    return hashHex;
}

async function buildSubtree(store: Store, tree: HashTree, startNode: HashTreeNode) {
    const children = Array.from(store.find({ parentId: startNode.entityId }));

    // Add children
    for (let child of children) {
        const data = getEntityDataString(child);
        let childNode = tree.createNonRoot(child.id, child.parentId, await hash(data));
        await buildSubtree(store, tree, childNode)
    }
}

export async function buildHashTree(store: Store): Promise<HashTree> {
    let tree: HashTree = new HashTree();

    // For every root node - add it, and build its subtree
    let rootEntities = Array.from(store.find({ parentId: '' }));
    for (let rootEntity of rootEntities) {
        const data = getEntityDataString(rootEntity);
        let rootNode = tree.createRoot(rootEntity.id, await hash(data));
        await buildSubtree(store, tree, rootNode)
    }
    await tree.updateRootHash();
    return tree
}

export async function updateHashTree(tree: HashTree, store: Store, delta: Delta) {
    // For deletions - remove the node
    for (let change of delta.changes()) {
        let changeType = change.type();
        let entity = store.findOne({ id: change.entityId });

        if (changeType === ChangeType.DELETE) {
            let node = tree.nodes[change.entityId];
            if (!node) {
                throw Error("Cannot delete a node that doesn't exist");
            }
            tree.removeNode(node);

        } else if (changeType === ChangeType.UPDATE) {
            let node = tree.nodes[change.entityId];
            if (!node){
                throw Error("Cannot update a node that doesn't exist");
            }
            if (entity === undefined) {
                throw Error(`Entity not found for the given change ${change.entityId}`);
            }
            const data = getEntityDataString(entity);
            node.entityDataHash = await hash(data);
            node.setHashOutdated();

        } else if (changeType === ChangeType.CREATE) {
            if (entity === undefined) {
                throw Error(`Entity not found for the given change ${change.entityId}`);
            }
            const data = getEntityDataString(entity);
            let hashString = await hash(data);
            let nodeType = entity.parentId === '' ? NodeType.ROOT : NodeType.NON_ROOT;
            let node = new HashTreeNode(tree, change.entityId, entity.parentId, hashString, nodeType);
            tree.insertNode(node);
        } else {
            throw Error("Unexpected change type");
        }
    }
    await tree.updateRootHash();
}
