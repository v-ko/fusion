import { Delta, DeltaData } from "../../model/Delta";

export interface CommitMetadataData {
    id: string;
    parentId: string;
    message: string;
    timestamp: number;
    snapshotHash: string;
    deltaData?: never;  // Annotation trick to not allow passing Commit objects where CommitMetadata is expected
}

export interface CommitData extends Omit<CommitMetadataData, 'deltaData'> {
    deltaData: DeltaData;
}

export class CommitMetadata {
    id: string;
    parentId: string;
    snapshotHash: string;
    timestamp: number;
    message: string;

    constructor(data: CommitMetadataData) {
        this.id = data.id;
        this.parentId = data.parentId;
        this.snapshotHash = data.snapshotHash;
        this.timestamp = data.timestamp;
        this.message = data.message;
    }

    data(): CommitMetadataData {
        return {
            id: this.id,
            parentId: this.parentId,
            snapshotHash: this.snapshotHash,
            timestamp: this.timestamp,
            message: this.message,
        };
    }
}

export class Commit {
    id: string;
    parentId: string;
    snapshotHash: string;
    timestamp: number;
    message: string;
    deltaData: DeltaData;

    constructor(data: CommitData) {
        this.id = data.id;
        this.parentId = data.parentId;
        this.snapshotHash = data.snapshotHash;
        this.timestamp = data.timestamp;
        this.message = data.message;
        this.deltaData = data.deltaData;
    }

    data(): CommitData {
        return {
            id: this.id,
            parentId: this.parentId,
            snapshotHash: this.snapshotHash,
            timestamp: this.timestamp,
            message: this.message,
            deltaData: this.deltaData,
        };
    }

    get delta(): Delta {
        return new Delta(this.deltaData);
    }

    metadata(): CommitMetadata {
        let { deltaData, ...metadata } = this.data();
        return new CommitMetadata(metadata);
    }
}
