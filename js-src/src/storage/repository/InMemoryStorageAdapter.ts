import { Commit } from "../version-control/Commit";
import { CommitGraph } from "../version-control/CommitGraph";
import { getLogger } from "../../logging";
import { StorageAdapter, InternalRepoUpdate } from "./StorageAdapter";

const log = getLogger('InMemoryRepository')


export class InMemoryStorageAdapter implements StorageAdapter {
    private _commitGraph: CommitGraph = new CommitGraph();
    private _commitById: Map<string, Commit> = new Map();

    async getCommitGraph(): Promise<CommitGraph> {
        return CommitGraph.fromData(this._commitGraph.data());
    }

    async getCommits(ids: string[]): Promise<Commit[]> {
        return ids.map((id) => {
            const commit = this._commitById.get(id);
            if (!commit) {
                throw new Error(`Commit ${id} not found`);
            }
            return commit;
        });
    }

    async applyUpdate(update: InternalRepoUpdate): Promise<void> {
        // Add new commits
        for (const commit of update.addedCommits) {
            this._commitById.set(commit.id, commit);
            this._commitGraph.addCommit(commit);
        }

        // Remove old commits
        for (const commit of update.removedCommits) {
            this._commitById.delete(commit.id);
            this._commitGraph.removeCommit(commit.id);
        }

        // Add new branches
        for (const branch of update.addedBranches) {
            this._commitGraph.createBranch(branch.name);
            if (branch.headCommitId) {
                this._commitGraph.setBranch(branch.name, branch.headCommitId);
            }
        }

        // Update branches
        for (const branch of update.updatedBranches) {
            this._commitGraph.setBranch(branch.name, branch.headCommitId);
        }

        // Remove branches
        for (const branch of update.removedBranches) {
            this._commitGraph.removeBranch(branch.name);
        }
    }

    close(): void {
        // Nothing to do for in-memory storage
    }

    async eraseStorage(): Promise<void> {
        this._commitGraph = new CommitGraph();
        this._commitById = new Map();
    }
}
