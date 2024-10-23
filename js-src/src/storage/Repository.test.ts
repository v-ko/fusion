import { AsyncInMemoryRepository } from "./AsyncInMemoryRepo";
import { InMemoryStore } from "./InMemoryStore";
import { Entity, entityType } from "../libs/Entity";
import type { EntityData } from "../libs/Entity";
import { Change } from "../Change";
import { deltaFromChanges } from "./BaseStore";
import { Commit } from "./Commit";

@entityType("DummyPage")
class DummyPage extends Entity<EntityData> {
    constructor(data: EntityData) {
        super(data);
    }
    get parentId(): string {
        return '';
    }
}

interface DummyEntityData extends EntityData {
    name: string;
    pageId: string;
}

@entityType("DummyEntity")
class DummyEntity extends Entity<EntityData> {
    _data: DummyEntityData;

    constructor(data: DummyEntityData) {
        super(data);
        this._data = data;
    }
    get name(): string {
        return this._data.name;
    }
    set name(name: string) {
        this._data.name = name;
    }
    get parentId(): string {
        return this._data.pageId;
    }
}

describe("Repository base functionality", () => {
    let repo: AsyncInMemoryRepository;

    // beforeEach(() => {
    //     repo = new AsyncInMemoryRepository();
    //     repo.init();
    // });

    test("Commit", async () => {
        // Create repo,
        repo = new AsyncInMemoryRepository()
        await repo.init('dev1')
        let initialHash = repo.hashTree.rootHash()
        // console.log('Initial hash:', initialHash)

        let sourceStore = new InMemoryStore()

        let changes: Change[] = [
            sourceStore.insertOne(new DummyPage({ id: 'page1' })),
            sourceStore.insertOne(new DummyEntity({ id: 'entity1', name: 'entity1', pageId: 'page1' })),
        ]
        let delta = deltaFromChanges(changes)
        let commit: Commit
        try{
            commit = await repo.commit(delta, 'Initial commit')
        } catch (e) {
            throw Error ('Error committing' + e)
        }


        let hashAfterCommit = repo.hashTree.rootHash()
        console.log('Hash after commit:', hashAfterCommit)

        expect(commit.snapshotHash).toEqual(hashAfterCommit)
        expect(hashAfterCommit).not.toEqual(initialHash)

        // Reverse
        let reverseDelta = delta.reversed()
        let reverseCommit: Commit
        try {
            reverseCommit = await repo.commit(reverseDelta, 'Reverse commit')
        } catch (e) {
            console.log(e)
            throw e
        }
        let hashAfterReverseCommit = repo.hashTree.rootHash()

        expect(reverseCommit.snapshotHash).toEqual(hashAfterReverseCommit)
        expect(hashAfterReverseCommit).toEqual(initialHash)
    });

    test("Pull same branch", async () => {
        let repo1 = new AsyncInMemoryRepository()
        await repo1.init('dev1')
        let repo2 = new AsyncInMemoryRepository()
        await repo2.init('dev1')

        // Add some data to repo1
        let sourceStore = new InMemoryStore()

        let changes: Change[] = [
            sourceStore.insertOne(new DummyPage({ id: 'page1' })),
            sourceStore.insertOne(new DummyEntity({ id: 'entity1', name: 'entity1', pageId: 'page1' })),
        ]

        let delta = deltaFromChanges(changes)
        await repo1.commit(delta, 'Initial commit')

        // Pull from repo1 to repo2
        await repo2.pull(repo1)

        let hash1 = repo1.hashTree.rootHash()
        let hash2 = repo2.hashTree.rootHash()
        expect(hash1).toEqual(hash2)

        // Make a change in repo2 (generate the change outside of it, since
        // the async repo does not have a way to directly alter entities in the
        // head store)
        let changes2: Change[] = [
            sourceStore.insertOne(new DummyEntity({ id: 'entity2', name: 'entity2', pageId: 'page1' })),
        ]
        let delta2 = deltaFromChanges(changes2)
        await repo2.commit(delta2, 'Second commit')

        // Pull from repo2 to repo1
        await repo1.pull(repo2)

        let hashAfterChange = repo2.hashTree.rootHash()
        let hashAfterPull = repo1.hashTree.rootHash()
        expect(hashAfterChange).toEqual(hashAfterPull)
    });

    test("Sync (pull, automerge, push)", async () => {

    });

    test("Sync with conflict", async () => {
    });
});


describe("Repository pull and sync tests", () => {

});
