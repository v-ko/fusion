import { Commit } from "./Commit";
import { HashTree, buildHashTree, updateHashTree } from "./HashTree";
import { createId } from "../util";
import { CommitGraph } from "./CommitGraph";
import { InMemoryStore } from "./InMemoryStore";
import { BaseAsyncRepository, ResetFilter } from "./BaseRepository";
import { Delta, DeltaData, squishDeltas } from "./Delta";
import { getLogger } from "../logging";
import { inferRepoChangesFromGraphUpdate } from "./SyncUtils";

const log = getLogger('InMemoryRepository')


export class AsyncInMemoryRepository extends BaseAsyncRepository {
    private _commitGraph: CommitGraph = new CommitGraph();
    private _commitById: Map<string, Commit> = new Map();
    private _headStore: InMemoryStore = new InMemoryStore()
    private _hashTree: HashTree | null = null;
    _currentBranch: string | null = null;

    async init(defaultBranchName: string) {
        this._commitGraph.createBranch(defaultBranchName)
        this._currentBranch = defaultBranchName
        this._hashTree = await buildHashTree(this.headStore)
    }

    static async initFromRemote(repository: BaseAsyncRepository, localBranchName: string = 'main'): Promise<AsyncInMemoryRepository> {
        let repo = new AsyncInMemoryRepository()
        await repo.init(localBranchName)
        await repo.pull(repository)
        return repo
    }

    get currentBranch() {
        if (!this._currentBranch) {
            throw new Error("Current branch is null. Repo not initialized")
        }
        return this._currentBranch
    }

    get commitGraph() {
        if (!this._commitGraph) {
            throw new Error("Sync graph is null. Have you called init?")
        }
        return this._commitGraph
    }
    get headStore() {
        if (!this._headStore) {
            throw new Error("Head state is null. Have you called init?")
        }
        return this._headStore
    }

    get hashTree() {
        if (!this._hashTree) {
            throw new Error("Hash tree is null. Have you called init?")
        }
        return this._hashTree
    }

    async createBranch(branchName: string): Promise<void> {
        this.commitGraph.createBranch(branchName)
    }

    async getCommitGraph(): Promise<CommitGraph> {
        // if (!this._commitGraph) {
        //     throw new Error("Commit graph is null. Have you called init?")
        // }
        return CommitGraph.fromData(this._commitGraph.data())
    }

    async getCommits(ids: string[]): Promise<Commit[]> {
        return ids.map((id) => {
            let commit = this._commitById.get(id)
            if (!commit) {
                throw new Error(`Commit ${id} not found`)
            }
            return commit
        })
    }

    async commit(delta: Delta, message: string): Promise<Commit> {
        log.info('Committing', delta, message)
        // Apply to the head store
        this.headStore.applyDelta(delta)

        // Get snapshotHash
        try{
            await updateHashTree(this.hashTree, this.headStore, delta)
        } catch (e) {
            console.error('Error updating hash tree', e)
            throw Error('Error updating hash tree: ' + e)
        }
        let snapshotHash = this.hashTree.rootHash()

        // Create commit
        let commits = this.commitGraph.branchCommits(this.currentBranch)

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
        this.commitGraph.addCommit(commit)
        this._commitById.set(commit.id, commit)
        this.commitGraph.setBranch(this.currentBranch, commit.id)

        return commit
    }

    async reset(filter: ResetFilter): Promise<void> {
        // Update the head store state to reflect the requested branch/head pos
        let { relativeToHead } = filter

        if (relativeToHead === 0) {
            return
        } else if (relativeToHead > 0) {
            throw new Error("Reset forward not supported")
        }

        let currentBranch = this.currentBranch
        let headCommit = this.commitGraph.headCommit(currentBranch)
        if (!headCommit) {
            throw new Error("No head commit found")
        }

        let branchCommits = this.commitGraph.branchCommits(currentBranch)
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
        let commits = branchCommits.slice(0, targetIndex + 1)
        let deltas = commits.map((commit) => commit.deltaData)
        let squishedDelta = squishDeltas(deltas as DeltaData[]).reversed()
        this.headStore.applyDelta(squishedDelta)

        // Remove from commit graph and local commits
        let removedCommits = branchCommits.slice(targetIndex + 1)
        removedCommits.forEach((commit) => {
            this._commitById.delete(commit.id)
            this.commitGraph.removeCommit(commit.id)
        })

        // Update hash tree
        await updateHashTree(this.hashTree, this.headStore, squishedDelta)
        this.commitGraph.setBranch(currentBranch, targetCommit.id)

        // Assert that the hash is correct
        let snapshotHash = this.hashTree.rootHash()
        if (snapshotHash !== targetCommit.snapshotHash) {
            throw new Error("Snapshot hash of the head store state does not match the one of the applied commit (on reset)")
        }
    }

    async _checkAndApplyUpdate(remoteGraph: CommitGraph, newCommits: Commit[]) {
        // Since we'll update the commit graph to the received one - we need to
        // ensure that the changes are rational, remove unneeded commits, and
        // fetch the new ones
        const localGraph = this.commitGraph

        let repoChanges = inferRepoChangesFromGraphUpdate(localGraph, remoteGraph, newCommits)
        // log.info('[_checkAndApplyUpdate] Repo changes', repoChanges)
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
            localGraph.removeCommit(commit.id)
        })

        // If the current branch head has moved (in remote) - prep the changes
        // and the local head state update
        // Get branch info
        let commitsBehind: string[] = [] // ids

        let remoteHeadCommit = remoteGraph.headCommit(this.currentBranch)
        let localHeadCommit = localGraph.headCommit(this.currentBranch)
        // log.info('remoteHeadCommit', remoteHeadCommit)
        // log.info('localHeadCommit', localHeadCommit)

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

        // log.info('[_checkAndApplyUpdate] Commits behind', commitsBehind)

        // Add commits
        addedCommits.forEach((commit) => {
            this._commitById.set(commit.id, commit)
            localGraph.addCommit(commit)
        })

        if (commitsBehind.length === 0) {
            // No need to update
            log.info('No need to update')
            return
        }

        // Squish deltas and apply the update to the head store
        let deltas: DeltaData[] = []

        for (let commitId of commitsBehind) {
            let commit = this._commitById.get(commitId)
            if (commit) {
                if (!commit.deltaData){
                    throw new Error("Delta data missing")
                }
                deltas.push(commit.deltaData)
            } else {
                throw new Error("Critical: Missing commit")
            }
        }

        let squishedDelta = squishDeltas(deltas as DeltaData[])
        this.headStore.applyDelta(squishedDelta)

        // Update the hash tree
        await updateHashTree(this.hashTree, this.headStore, squishedDelta)

        // Assert hash is correct
        let snapshotHash = this.hashTree.rootHash()
        if (snapshotHash !== remoteHeadCommit!.snapshotHash) {
            console.log(remoteHeadCommit, this.hashTree)
            throw new Error("Snapshot hash mismatch")
        }
        // log.info('[_checkAndApplyUpdate] Updated hash tree', snapshotHash)

        // Apply branch changes
        addedBranches.forEach((branch) => {
            localGraph.createBranch(branch.name)
        })
        updatedBranches.forEach((branch) => {
            localGraph.setBranch(branch.name, branch.headCommitId)
        })
        removedBranches.forEach((branch) => {
            if (branch.name === this.currentBranch) {
                throw new Error("Cannot remove current branch")
            }
            localGraph.removeBranch(branch.name)
        })
    }
    async eraseStorage(): Promise<void> {
        this._commitGraph = new CommitGraph()
        this._commitById = new Map()
        this._headStore = new InMemoryStore()
        // Pretty much no need to do anything. This method is for removing persisted
        // data when the user deletes a project
    }
}
