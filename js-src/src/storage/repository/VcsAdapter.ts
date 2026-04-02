import { Commit, CommitMetadata } from "../version-control/Commit";
import { CommitGraph } from "../version-control/CommitGraph";

export interface BranchMetadata {
    name: string;
    headCommitId: string | null;
}

export interface InternalRepoUpdateNoDeltas {
    addedCommits: CommitMetadata[];
    removedCommits: CommitMetadata[];
    updatedCommits: CommitMetadata[];
    addedBranches: BranchMetadata[];
    updatedBranches: BranchMetadata[];
    removedBranches: BranchMetadata[];
}

export interface InternalRepoUpdate extends Omit<InternalRepoUpdateNoDeltas, 'addedCommits' | 'updatedCommits'> {
    addedCommits: Commit[];
    updatedCommits: Commit[];
}

export interface VcsAdapter {
    getCommitGraph(): Promise<CommitGraph>;
    getCommits(ids: string[]): Promise<Commit[]>;
    applyUpdate(update: InternalRepoUpdate): Promise<void>;
    close(): void;
    eraseStorage(): Promise<void>;

    // Project metadata - we consider it part of the .pamet repo (comparable to git metadata)
    getProjectProperties(): Promise<object | null>;
    setProjectProperties(properties: object): Promise<void>;
}
