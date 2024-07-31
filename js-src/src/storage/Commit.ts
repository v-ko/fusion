import { Delta, DeltaData } from "./Delta";
import { getLogger } from "../logging";

const log = getLogger("Commit");


export interface CommitData {
    id: string;
    parentId: string;
    deltaData?: DeltaData;
    message: string;
    timestamp: number;
    snapshotHash: string;
}

export class Commit {
    id: string;
    parentId: string;
    deltaData?: DeltaData; // it's assumed to return a mutable object in some instances!
    snapshotHash: string;
    timestamp: number;
    message: string;

    constructor(data: CommitData) {
        this.id = data.id;
        this.parentId = data.parentId;
        this.deltaData = data.deltaData;
        this.snapshotHash = data.snapshotHash;
        this.timestamp = data.timestamp;
        this.message = data.message;
    }

    data(withDelta: boolean = true): CommitData {
        return {
            id: this.id,
            parentId: this.parentId,
            deltaData: withDelta ? structuredClone(this.deltaData) : undefined,
            snapshotHash: this.snapshotHash,
            timestamp: this.timestamp,
            message: this.message,
        };
    }

    get delta(): Delta {
        let data = this.deltaData || {} as DeltaData;
        return new Delta(data);
    }
}
