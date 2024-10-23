import { Change, ChangeTypes } from "../Change";
import { Store } from "./BaseStore";
import { dumpToDict } from '../libs/Entity';
import { cryptoModule } from "../util";
import { getLogger } from "../logging";

const log = getLogger('HashTree')

const subtleCrypto = cryptoModule();
const SUPER_ROOT_ID = ''

export class HashTreeNode {
    // The store itsel is not an entity, so the root has null id
    entityId: string = SUPER_ROOT_ID; // '' means the main/super root node
    parentId: string = ''; // '' means no parent entity (those are under the main/super root)
    entityDataHash: string = ''; // '' is possible at init or for the super root node (all others are serializable and should be specified on instantiation or at data updates)
    hash: string = ''; // '' means outdated
    children: { [key: string]: HashTreeNode } = {}; // by entity id
    childrenSorted: HashTreeNode[] = []; // sorted by entity id
    removed: boolean = false;
    childrenSortOutdated: boolean = false;
    // dataHashOutdated: boolean = false;

    constructor(entityId: string, parentId: string, entityDataHash: string, isRoot: boolean = false) {
        this.entityId = entityId;
        this.parentId = parentId;
        this.entityDataHash = entityDataHash;

        if (!isRoot && entityId === '') {
            throw Error("Non-root nodes must have an entity id");
        }
        // if (isRoot) {
        //     this.dataHashOutdated = false;
        // }
    }

    get hashOutdated(): boolean {
        return this.hash === '' || this.childrenSortOutdated //|| this.dataHashOutdated;
    }
    setHashOutdated() {
        this.hash = '';
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
        this.childrenSortOutdated = false;
    }
    async updateHash() {
        if (this.removed) {
            throw Error("Cannot update hash of a removed node");
        }

        if (this.childrenSortOutdated) {
            this.sortChildren();
        }

        for (let child of this.childrenSorted) {
            if (child.hashOutdated) {
                await child.updateHash();
            }
        }

        this.hash = await hash(this.entityDataHash, this.childrenSorted.map((child) => child.hash));
        log.info('Updated node hash to', this.hash)
    }
    addChild(child: HashTreeNode) {
        if (child.entityId === SUPER_ROOT_ID){
            throw Error("Cannot add a child with empty entity id. This is reserved for the super-root");
        }

        this.children[child.entityId] = child;
        this.childrenSorted.push(child);
        this.childrenSortOutdated = true;
    }
    removeChild(child: HashTreeNode) {
        delete this.children[child.entityId];
        let index = this.childrenSorted.indexOf(child);
        if (index === -1) {
            throw Error("Child not found");
        }
        this.childrenSorted.splice(index, 1);
        this.childrenSortOutdated = true;
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
}

export class HashTree {
    private _tmpSubtrees: { [key: string]: HashTreeNode[] } = {}; // by parent id
    root: HashTreeNode | null = null;
    nodes: { [key: string]: HashTreeNode } = {} // by entity id
    cleanupNeeded: boolean = false; // Handle nodes marked for removal and assert no tmp subtrees

    setHashOutdated(node: HashTreeNode) {
        log.info('Setting hash outdated', node.entityId)

        if (node.hashOutdated) {
            return;
        }
        node.setHashOutdated();  // Set for the node itself

        // Set for all parents
        let parent = this.nodes[node.parentId];
        if (parent) {
            this.setHashOutdated(parent);
        }
    }

    insertNode(node: HashTreeNode) {
        // log.info('Inserting node', node.entityId, node.parentId)
        let parent = this.nodes[node.parentId];

        if (node.entityId === SUPER_ROOT_ID) { // For adding the super-root
            if (!!this.root){
                throw Error("Cannot have multiple super-root nodes");
            }
            if (parent) {
                throw Error("Wtf. Super-root node cannot have a parent");
            }
            this.root = node;
            this.nodes[node.entityId] = node;

        // Else if the parent is not in the tree, add it to the temporary subtrees
        } else if (!parent) {
            log.info('Parent not found, adding to tmp subtrees', node.parentId)
            this.cleanupNeeded = true; // To check for hanging tmp subtrees later
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
        node.removed = true;
        this.cleanupNeeded = true;
    }
    parent(node: HashTreeNode): HashTreeNode | null {
        return this.nodes[node.parentId] || null;
    }

    cleanUp() {
        /** Remove nodes marked for removal and assert no hanging tmp subtrees */
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

        // Assert no tmp subtrees
        if (Object.keys(this._tmpSubtrees).length > 0) {
            throw Error("Temporary subtrees not empty");
        }

        this.cleanupNeeded = false;
    }

    async updateHash() {
        if (!this.root) {
            throw Error("Cannot update hash of an empty tree");
        }

        if (this.cleanupNeeded) {
            this.cleanUp();
        }

        await this.root.updateHash();
    }

    rootHash(): string {
        if (!this.root) {
            throw Error("Cannot get hash of an empty tree");
        }
        if (this.cleanupNeeded) {
            throw Error("Tree needs cleanup");
        }
        if (this.root.hashOutdated) {
            throw Error("Root hash is outdated");
        }
        return this.root.hash;
    }
}


async function hash(entityDataString: string, childHashes: string[] = []): Promise<string> {
    // Concatenate (child hashes should be sorted by entityId)
    let data = entityDataString + childHashes.join('');

    const encoder = new TextEncoder();
    const dataEncoded = encoder.encode(data);
    let hashBuffer: ArrayBuffer = new ArrayBuffer(0);
    // try{
        hashBuffer = await subtleCrypto.digest("SHA-256", dataEncoded);
    // } catch (e) {
    //     log.info('Error hashing', e)
    //     return 'error'
    // }
    const hashArray = Array.from(new Uint8Array(hashBuffer));
    const hashHex = hashArray.map(byte => byte.toString(16).padStart(2, '0')).join('');

    return hashHex;
    // return cryptoModule().createHash('sha256').update(data).digest('hex');
}


export async function buildHashTree(store: Store): Promise<HashTree> {
    let rootNode = new HashTreeNode('', '', '', true);
    let tree: HashTree = new HashTree();

    async function buildSubtree(tree: HashTree, startNode: HashTreeNode) {
        const children = Array.from(store.find({ parentId: startNode.entityId }));

        // Add children
        for (let child of children) {
            let data = JSON.stringify(dumpToDict(child));
            let childNode: HashTreeNode = new HashTreeNode(child.id, child.parentId, await hash(data));
            tree.insertNode(childNode);
            buildSubtree(tree, childNode)
        }
    }

    // Build the subtrees for all entities with no parent
    tree.insertNode(rootNode);
    await buildSubtree(tree, rootNode)
    await tree.updateHash();
    return tree
}

export async function updateHashTree(tree: HashTree, store: Store, changes: Change[]) {
    // For deletions - remove the node
    let changedEntitiesIds = new Map<string, boolean>();
    for (let change of changes) {
        let changeType = change.changeType();
        let entity = change.lastState;

        // The function expects aggregated changes
        if (changedEntitiesIds.has(entity.id)) {
            throw Error("Multiple changes for the same entity id");
        }

        if (changeType === ChangeTypes.DELETE) {
            let node = tree.nodes[entity.id];
            if (node) {
                tree.removeNode(node);
            } else {
                throw Error("Entity not found in the hash tree");
            }

        } else if (changeType === ChangeTypes.UPDATE) {
            let node = tree.nodes[entity.id];
            if (node) {
                let data = JSON.stringify(dumpToDict(entity));
                node.entityDataHash = await hash(data);
                tree.setHashOutdated(node);
            } else {
                throw Error("Entity not found in the hash tree");
            }

        } else if (changeType === ChangeTypes.CREATE) {
            let data = JSON.stringify(dumpToDict(entity));
            let hashString = await hash(data);
            let node = new HashTreeNode(entity.id, entity.parentId, hashString);
            tree.insertNode(node);
        } else {
            throw Error("Unexpected change type");
        }
    }
    await tree.updateHash();
}
