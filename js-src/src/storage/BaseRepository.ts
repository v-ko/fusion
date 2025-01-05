import { getLogger } from "../logging";
import { Commit, CommitData } from "./Commit";
import { CommitGraph, CommitGraphData } from "./CommitGraph";

let log = getLogger('BaseRepository')

export interface ResetFilter {
    relativeToHead: number;
}

export interface BranchMetadata {
    name: string;
    headCommitId: string | null;
}

export interface RepoUpdateData {
    commitGraph: CommitGraphData; // Removed commits are inferred from the graph
    newCommits: CommitData[] // Added commits with included deltas
}

export interface InternalRepoUpdate {
    addedCommits: Commit[];
    removedCommits: Commit[];
    addedBranches: BranchMetadata[];
    updatedBranches: BranchMetadata[];
    removedBranches: BranchMetadata[];
}

export abstract class BaseAsyncRepository {
    /**
     * Defines a base interface, that a Repository class should i mplement.
     * Should be compatible with repo wrappers working over some network
     */
    abstract getCommitGraph(): Promise<CommitGraph>;
    abstract getCommits(ids?: string[]): Promise<Commit[]>;
    abstract commit(deltaData: any, message: string): Promise<Commit>;

    abstract createBranch(branchName: string): Promise<void>;
    // abstract pull(repo: BaseAsyncRepository): Promise<void>;
    abstract reset(filter: ResetFilter): Promise<void>;
    // abstract applyRepoUpdate(updateInfo: RepoUpdate): Promise<void>;

    abstract _checkAndApplyUpdate(remoteGraph: CommitGraph, newCommits: Commit[]): Promise<void>;

    async pull(repository: BaseAsyncRepository) {

        // Get the update info from the remote
        let localGraph = await this.getCommitGraph()
        let remoteGraph = await repository.getCommitGraph()
        log.info('Pulling from remote', repository, localGraph, remoteGraph)

        // Since we'll update the commit graph to the received one - we need to
        // ensure that the changes are rational, remove unneeded commits, and
        // fetch the new ones
        let localSet = new Set(localGraph.commits().map((c) => c.id))

        // Infer missing commits from the commit graph
        let missingCommits = remoteGraph.commits().filter((c) => !localSet.has(c.id))

        // Get full commits (with delta info)
        if (missingCommits.length > 0) {
            missingCommits = await repository.getCommits(missingCommits.map((c) => c.id))
        }

        await this._checkAndApplyUpdate(remoteGraph, missingCommits)

    }

    async applyRepoUpdate(updateInfo: RepoUpdateData): Promise<void> {
        // Remote commit graph
        let remoteGraph = CommitGraph.fromData(updateInfo.commitGraph)
        let newCommits = updateInfo.newCommits.map((data) => new Commit(data))

        await this._checkAndApplyUpdate(remoteGraph, newCommits)
    }


}
