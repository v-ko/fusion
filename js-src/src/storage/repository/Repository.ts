import { getLogger } from "../../logging";
import { Commit, CommitData } from "../version-control/Commit";
import { CommitGraph, CommitGraphData } from "../version-control/CommitGraph";
import { StorageAdapter, InternalRepoUpdate } from "./StorageAdapter";
import { inferRepoChangesFromGraphs, sanityCheckAndHydrateInternalRepoUpdate } from "../management/SyncUtils";
import { Delta, DeltaData, squishDeltas } from "../../model/Delta";
import { createId } from "../../util/base";
import { HashTree, buildHashTree, updateHashTree } from "../version-control/HashTree";
import { InMemoryStore, IndexConfig } from "../domain-store/InMemoryStore";
import { InMemoryStorageAdapter } from "./InMemoryStorageAdapter";
import { IndexedDBStorageAdapter } from "./IndexedDB_storageAdapter";
let log = getLogger('Repository')

export type StorageAdapterNames = "InMemory" | "IndexedDB" | "InMemorySingletonForTesting";

// For running tests only
let _inmemadapter_instances_by_id: Map<string, InMemoryStorageAdapter> = new Map();

export function clearInMemoryAdapterInstances() {
    _inmemadapter_instances_by_id.clear();
}

export interface StorageAdapterArgs {
    projectId: string;
    localBranchName: string;
    indexConfig?: any; // For InMemory, can be used to pass index configuration
}

export interface StorageAdapterConfig {
    name: StorageAdapterNames
    args: StorageAdapterArgs;
}

export interface ResetFilter {
    relativeToHead: number;
}

export interface RepoUpdateData {
    commitGraph: CommitGraphData;
    newCommits: CommitData[];
}

export class Repository {
    private _storageAdapter: StorageAdapter;
    private _adapterConfig: StorageAdapterConfig;
    private _isCaching: boolean;
    private _indexConfigs: readonly IndexConfig[];

    private _currentBranch: string;
    private _headStore: InMemoryStore | null = null;
    _commitGraph: CommitGraph = new CommitGraph();  // Public only for testing purposes
    private _commitById: Map<string, Commit> = new Map();
    private _hashTree: HashTree | null = null;

    private constructor(
        storageAdapter: StorageAdapter,
        adapterConfig: StorageAdapterConfig,
        enableCaching: boolean,
        indexConfigs: readonly IndexConfig[]) {

        this._storageAdapter = storageAdapter;
        this._isCaching = enableCaching;
        this._adapterConfig = adapterConfig;
        this._indexConfigs = indexConfigs;
        this._currentBranch = adapterConfig.args.localBranchName;

        // Initialize the commit graph and head store
        if (enableCaching) {
            log.info(`Repository constructor: caching enabled. Using storage adapter: ${adapterConfig.name}`);
            this._commitGraph = new CommitGraph();
            this._headStore = new InMemoryStore(indexConfigs);
        } else {
            log.info(`Repository constructor: caching disabled. Using storage adapter: ${adapterConfig.name}`);
        }
    }

    private static async _getStorageAdapter(config: StorageAdapterConfig): Promise<StorageAdapter> {
        if (config.name === 'InMemory') {
            return new InMemoryStorageAdapter();
        } else if (config.name === 'IndexedDB') {
            const { projectId } = config.args;
            const storageAdapter = new IndexedDBStorageAdapter(projectId);
            await storageAdapter.initialize();
            return storageAdapter;
        } else if (config.name === 'InMemorySingletonForTesting') {
            const { projectId } = config.args;
            if (!_inmemadapter_instances_by_id.has(projectId)) {
                const inMemAdapter = new InMemoryStorageAdapter();
                _inmemadapter_instances_by_id.set(projectId, inMemAdapter);
            }
            return _inmemadapter_instances_by_id.get(projectId)!;
        } else {
            throw new Error(`Unknown storage adapter type: ${config.name}`);
        }
    }

    private static async init(config: StorageAdapterConfig, enableCaching: boolean, indexConfigs: readonly IndexConfig[]): Promise<Repository> {
        const storageAdapter = await this._getStorageAdapter(config);
        const repo = new Repository(storageAdapter, config, enableCaching, indexConfigs);
        // Always init with defualt current branch. There's no repo without
        // a head. And then we'll pull from wherever
        repo._commitGraph.createBranch(repo._currentBranch);
        return repo
    }

    static async open(config: StorageAdapterConfig, enableCaching: boolean, indexConfigs: readonly IndexConfig[]): Promise<Repository> {
        let repo = await Repository.init(config, enableCaching, indexConfigs);

        log.info(`Opening repository with caching: ${enableCaching}, storage adapter: ${config.name}`);

        //
        const commitGraph = await repo._storageAdapter.getCommitGraph();
        const branch = commitGraph.branch(repo._currentBranch);
        if (branch === undefined) {
            throw new Error(`Branch ${repo._currentBranch} not found in the commit graph. Branches: ${commitGraph.branches().map(b => b.name).join(', ')}`);
        }

        if (!repo._isCaching) {
            log.info(`Repository opened without caching. Using storage adapter: ${config.name}`);
            return repo;
        }

        repo._hashTree = await buildHashTree(repo.headStore);
        await repo.pull(repo._storageAdapter);
        log.info(`Repository opened with caching enabled. Using storage adapter: ${config.name}`);
        return repo;
    }

    static async create(config: StorageAdapterConfig, enableCaching: boolean, indexConfigs: readonly IndexConfig[]): Promise<Repository> {
        const repo = await Repository.init(config, enableCaching, indexConfigs);

        // Create the default branch on the adappters
        // Locally the default is created in the constructor (to use for pull from adapetr)
        await repo._storageAdapter.applyUpdate({
            addedCommits: [],
            removedCommits: [],
            addedBranches: [{ name: repo._currentBranch, headCommitId: null }],
            updatedBranches: [],
            removedBranches: []
        });

        log.info(`Created repository with caching: ${enableCaching}, current branch: ${repo._currentBranch}`);
        if (repo._isCaching) {
            repo._hashTree = await buildHashTree(repo.headStore);
        }
        return repo;
    }

    get headStore(): InMemoryStore {
        if (!this._headStore) {
            throw new Error("Head store is not available. Caching might be disabled.");
        }
        return this._headStore;
    }

    get hashTree(): HashTree {
        if (!this._hashTree) {
            throw new Error("Hash tree is not initialized");
        }
        return this._hashTree;
    }
    
    async getCommitGraph(): Promise<CommitGraph> {
        if (this._isCaching) {
            // We don't clone here for performance reasons, but it means the
            // caller should not mutate the returned graph.
            return this._commitGraph;
        }
        return await this._storageAdapter.getCommitGraph();
    }

    async getCommits(ids: string[]): Promise<Commit[]> {
        if (this._isCaching) {
            const commits: Commit[] = [];
            for (const id of ids) {
                const commit = this._commitById.get(id);
                if (!commit) {
                    throw new Error(`Commit ${id} not found in cache`);
                }
                commits.push(commit);
            }
            return commits;
        }
        return this._storageAdapter.getCommits(ids);
    }

    async commit(delta: Delta, message: string): Promise<Commit> {
        if (!this._currentBranch) {
            throw new Error("Current branch is not set");
        }
        if (!this._isCaching) {
            throw new Error("Cannot create a commit without head state caching enabled");
        }

        log.info('Committing', delta, message)
        // Apply to the head store
        this.headStore.applyDelta(delta)

        // Get snapshotHash
        try {
            await updateHashTree(this.hashTree, this.headStore, delta)
        } catch (e) {
            throw Error('Error updating hash tree: ' + e)
        }
        let snapshotHash = this.hashTree.rootHash()

        // Create commit
        const commitGraph = this._commitGraph!;
        let commits = commitGraph.branchCommits(this._currentBranch);

        let parentId = ''
        if (commits.length > 0) {
            // Initial commit
            parentId = commits.at(-1)!.id
        }
        let commit = new Commit({
            id: createId(),
            parentId: parentId,
            snapshotHash: snapshotHash,
            deltaData: delta.data,
            message: message,
            timestamp: Date.now()
        })

        // Add commit to sync graph
        commitGraph.addCommit(commit)
        this._commitById.set(commit.id, commit)
        commitGraph.setBranch(this._currentBranch, commit.id)

        const branch = commitGraph.branch(this._currentBranch)!;

        const internalUpdate: InternalRepoUpdate = {
            addedCommits: [commit],
            removedCommits: [],
            addedBranches: [],
            updatedBranches: [branch],
            removedBranches: []
        };
        await this._storageAdapter.applyUpdate(internalUpdate);

        return commit;
    }

    async createBranch(branchName: string): Promise<void> {
        if (!this._isCaching) {
            // In non-caching mode, this should probably be an atomic operation
            // on the storage adapter, which is not implemented.
            // For now, we only support this in caching mode.
            throw new Error("Cannot create a branch without head state caching enabled");
        }
        const commitGraph = this._commitGraph!;
        commitGraph.createBranch(branchName);
        const branch = commitGraph.branch(branchName)!;

        const update: InternalRepoUpdate = {
            addedCommits: [],
            removedCommits: [],
            addedBranches: [branch],
            updatedBranches: [],
            removedBranches: []
        };
        await this._storageAdapter.applyUpdate(update);
        // throw Error(`Created branch ${JSON.stringify(await this._storageAdapter.getCommitGraph())} for name ${branchName}`)
    }

    async pull(repository: Repository | StorageAdapter) {
        let remoteGraph = await repository.getCommitGraph()
        const ownGraph = await this.getCommitGraph();

        let repoUpdateSlim = inferRepoChangesFromGraphs(ownGraph, remoteGraph);

        // Get new commits in full (with deltas)
        let newCommits = await repository.getCommits(
            repoUpdateSlim.addedCommits.map((data) => data.id));

        // Sanity check and hydrate addedCommits with the newCommits (with deltas included)
        let repoUpdate = sanityCheckAndHydrateInternalRepoUpdate(repoUpdateSlim, newCommits);

        // Persist the changes to the underlying storage first
        await this._storageAdapter.applyUpdate(repoUpdate);

        if (this._isCaching) {
            await this._applyInternalUpdateToCache(repoUpdate, remoteGraph);
        }
    }

    async hydrateCacheFromStorageAdater() {
        if (!this._isCaching) {
            throw new Error("Cannot hydrate cache without caching enabled");
        }

        log.info('Hydrating cache from storage adapter');
        // Get the commit graph from the storage adapter
        const remoteGraph = await this._storageAdapter.getCommitGraph();
        const ownGraph = await this.getCommitGraph();

        let repoUpdateSlim = inferRepoChangesFromGraphs(ownGraph, remoteGraph);

        // Get new commits in full (with deltas)
        let newCommits = await this._storageAdapter.getCommits(
            repoUpdateSlim.addedCommits.map((data) => data.id));

        let repoUpdate = sanityCheckAndHydrateInternalRepoUpdate(repoUpdateSlim, newCommits);

        await this._applyInternalUpdateToCache(repoUpdate, remoteGraph);
    }

    async applyRepoUpdate(updateInfo: RepoUpdateData): Promise<void> {
        let remoteGraph = CommitGraph.fromData(updateInfo.commitGraph);
        let newCommits = updateInfo.newCommits.map((data) => new Commit(data));

        // Form the internalRepoUpdate object
        let repoUpdateSlim = inferRepoChangesFromGraphs(this._commitGraph, remoteGraph);
        // Sanity check and hydrate addedCommits with the newCommits (with deltas included)
        let repoUpdate = sanityCheckAndHydrateInternalRepoUpdate(repoUpdateSlim, newCommits);

        // Persist the changes to the underlying storage first
        await this._storageAdapter.applyUpdate(repoUpdate);

        if (this._isCaching) {
            await this._applyInternalUpdateToCache(repoUpdate, remoteGraph);
        }
    }

    async reset(filter: ResetFilter): Promise<void> {
        if (!this._currentBranch) {
            throw new Error("Current branch is not set");
        }
        if (!this._isCaching) {
            throw new Error("Cannot reset without head state caching enabled");
        }
        const commitGraph = this._commitGraph!;

        // Update the head store state to reflect the requested branch/head pos
        let { relativeToHead } = filter

        if (relativeToHead === 0) {
            return
        } else if (relativeToHead > 0) {
            throw new Error("Reset forward not supported")
        }

        let headCommit = commitGraph.headCommit(this._currentBranch)
        if (!headCommit) {
            throw new Error("No head commit found")
        }

        let branchCommits = commitGraph.branchCommits(this._currentBranch)
        let index = branchCommits.indexOf(headCommit)
        if (index === -1) {
            throw new Error("Head commit not found in branch commits")
        }

        let targetIndex = index + relativeToHead
        if (targetIndex < 0) {
            throw new Error("Resetting too far back")
        }

        let targetCommit = branchCommits.at(targetIndex)
        if (!targetCommit) {
            throw new Error("Target commit not found")
        }

        // Revert the head store state by applying the deltas for the removed
        // commits in reverse
        let commitsToRevert = branchCommits.slice(targetIndex + 1)
        const reversedDeltas = commitsToRevert.map(c => new Delta(c.deltaData as DeltaData).reversed().data);
        const squishedDelta = squishDeltas(reversedDeltas);
        this.headStore.applyDelta(squishedDelta);


        // Remove from commit graph and local commits
        commitsToRevert.forEach((commit) => {
            this._commitById.delete(commit.id)
            commitGraph.removeCommit(commit.id)
        })

        // Update hash tree
        await updateHashTree(this.hashTree, this.headStore, squishedDelta)
        commitGraph.setBranch(this._currentBranch, targetCommit.id)

        // Assert that the hash is correct
        let snapshotHash = this.hashTree.rootHash()
        if (snapshotHash !== targetCommit.snapshotHash) {
            throw new Error("Snapshot hash of the head store state does not match the one of the applied commit (on reset)")
        }

        // Persist changes
        const branch = commitGraph.branch(this._currentBranch)!;
        const update: InternalRepoUpdate = {
            addedCommits: [],
            removedCommits: commitsToRevert,
            addedBranches: [],
            updatedBranches: [branch],
            removedBranches: []
        };
        await this._storageAdapter.applyUpdate(update);
    }

    async _applyInternalUpdateToCache(repoChanges: InternalRepoUpdate, remoteGraph: CommitGraph): Promise<void> {
        // If caching, apply the now-persisted changes to the in-memory cache
        const cacheGraph = this._commitGraph!;

        let {
            addedCommits,
            removedCommits,
            addedBranches,
            updatedBranches,
            removedBranches
        } = repoChanges

        // Do the commit removal
        removedCommits.forEach((commit) => {
            this._commitById.delete(commit.id)
            cacheGraph.removeCommit(commit.id)
        })

        // Add new commits to the local map for further processing
        addedCommits.forEach((commit) => {
            this._commitById.set(commit.id, commit)
            cacheGraph.addCommit(commit)
        })

        let commitsBehind: string[] = [] // ids
        let remoteHeadCommit = remoteGraph.headCommit(this._currentBranch!)
        let localHeadCommit = cacheGraph.headCommit(this._currentBranch!)

        if (remoteHeadCommit) {
            let remoteHeadId = remoteHeadCommit.id
            let localHeadId = localHeadCommit ? localHeadCommit.id : null
            if (remoteHeadId !== localHeadId) {
                // Get the commits behind
                let behind = remoteGraph.commitsBetween(localHeadId, remoteHeadId)
                commitsBehind = behind.map((c) => c.id)
            }
        } else {
            if (localHeadCommit) {
                throw new Error("Irrational changes - remote branch empty, while local is not")
            }
        }

        if (commitsBehind.length === 0) {
            log.info('No new commits to apply to headStore.')
        } else {
            // Squish deltas and apply the update to the head store
            let deltas: DeltaData[] = []

            for (let commitId of commitsBehind) {
                let commit = this._commitById.get(commitId)
                if (commit && commit.deltaData) {
                    deltas.push(commit.deltaData)
                } else {
                    throw new Error(`Critical: Missing commit or deltaData for ${commitId}`)
                }
            }

            // Apply to head store
            let squishedDelta = squishDeltas(deltas)
            this.headStore.applyDelta(squishedDelta)

            // Update the hash tree
            await updateHashTree(this.hashTree, this.headStore, squishedDelta)

            // Assert hash is correct
            let snapshotHash = this.hashTree.rootHash()
            if (snapshotHash !== remoteHeadCommit!.snapshotHash) {
                console.log(remoteHeadCommit, this.hashTree)
                throw new Error("Snapshot hash mismatch after pull")
            }
        }

        // Apply branch changes to the cache
        addedBranches.forEach((branch) => {
            cacheGraph.createBranch(branch.name)
        })
        updatedBranches.forEach((branch) => {
            cacheGraph.setBranch(branch.name, branch.headCommitId)
        })
        removedBranches.forEach((branch) => {
            if (branch.name === this._currentBranch) {
                throw new Error("Cannot remove current branch")
            }
            cacheGraph.removeBranch(branch.name)
        })
    }

    async eraseStorage(): Promise<void> {
        return this._storageAdapter.eraseStorage();
    }

    close() {
        log.info('Repo close called. Closing storage adapter.');
        this._storageAdapter.close();
    }
}
