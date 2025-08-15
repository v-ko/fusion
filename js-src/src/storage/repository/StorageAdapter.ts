import { Commit, CommitMetadata } from "../version-control/Commit";
import { CommitGraph } from "../version-control/CommitGraph";

export interface BranchMetadata {
    name: string;
    headCommitId: string | null;
}

export interface InternalRepoUpdateNoDeltas {
    addedCommits: CommitMetadata[];
    removedCommits: CommitMetadata[];
    addedBranches: BranchMetadata[];
    updatedBranches: BranchMetadata[];
    removedBranches: BranchMetadata[];
}

export interface InternalRepoUpdate extends InternalRepoUpdateNoDeltas {
    addedCommits: Commit[];
}

export interface StorageAdapter {
    getCommitGraph(): Promise<CommitGraph>;
    getCommits(ids: string[]): Promise<Commit[]>;
    applyUpdate(update: InternalRepoUpdate): Promise<void>;
    close(): void;
    eraseStorage(): Promise<void>;
}
