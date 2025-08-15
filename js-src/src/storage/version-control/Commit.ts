import { Delta, DeltaData } from "../../model/Delta";
import { getLogger } from "../../logging";

const log = getLogger("Commit");


export interface CommitMetadataData {
    id: string;
    parentId: string;
    message: string;
    timestamp: number;
    snapshotHash: string;
}

export interface CommitData extends CommitMetadataData {
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

export class Commit extends CommitMetadata {
    deltaData: DeltaData;

    constructor(data: CommitData) {
        super(data);
        this.deltaData = data.deltaData;
    }

    data(): CommitData {
        return {
            ...super.data(),
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
