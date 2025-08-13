import { Commit } from "../version-control/Commit";
import { CommitGraph } from "../version-control/CommitGraph";

export interface BranchMetadata {
    name: string;
    headCommitId: string | null;
}

export interface InternalRepoUpdate {
    addedCommits: Commit[];
    removedCommits: Commit[];
    addedBranches: BranchMetadata[];
    updatedBranches: BranchMetadata[];
    removedBranches: BranchMetadata[];
}

export interface StorageAdapter {
    getCommitGraph(): Promise<CommitGraph>;
    getCommits(ids: string[]): Promise<Commit[]>;
    applyUpdate(update: InternalRepoUpdate): Promise<void>;
    close(): void;
    eraseStorage(): Promise<void>;
}
