import { Entity, EntityData, entityType } from "../libs/Entity";
import { Delta } from "./Delta";
import { InMemoryStore as InMemoryStore } from "./InMemoryStore";

interface PageData extends EntityData {
    name: string;
}

// mock Page entity subclass
@entityType("Page")
class Page extends Entity<PageData> {
    name: string;

    constructor(data: any) {
        super(data);
        this.name = data.name;
    }
    get parentId(): string {
        return "";
    }
}

interface NoteData extends EntityData {
    name: string;
    pageId: string;
}

// mock Note entity subclass
@entityType("Note")
class Note extends Entity<NoteData> {
    constructor(data: any) {
        super(data);
    }
    get parentId(): string {
        return this._data.pageId;
    }
}


describe("InMemoryStore", () => {
    let repo = new InMemoryStore();

    beforeEach(() => {
        repo = new InMemoryStore();
    });

    test("Entity CRUD operations", () => {
        let entity = new Page({
            id: "123",
            name: "Test Page",
        })

        // Test insert
        let changeCreate = repo.insertOne(entity);
        expect(changeCreate).toBeDefined();

        let all_entities = [...repo.find()];
        expect(all_entities.length).toBe(1);

        // Test update
        entity.name = "456";
        let changeUpdate = repo.updateOne(entity);
        expect(changeUpdate).toBeDefined();
        expect([...repo.find()].length).toBe(1);

        // Test delete
        let changeDelete = repo.removeOne(entity);
        expect(changeDelete).toBeDefined();
        expect([...repo.find()].length).toBe(0);
    });

    // Test find by id, parent-id, type and prop
    // Add a page and a note to the store and test the find method
    test("Find by id, parent-id, type and prop", () => {
        let page = new Page({
            id: "123",
            name: "Test Page",
        })
        let note = new Note({
            id: "456",
            name: "Test Note",
            pageId: "123"
        });

        repo.insertOne(page);
        repo.insertOne(note);

        // Test find by id
        let foundPage = repo.findOne({ id: "123" });
        expect(foundPage).toEqual(page);

        // Test find by parent-id
        let foundNote = repo.findOne({ parentId: "123" });
        expect(foundNote).toEqual(note);

        // Test find by type
        let foundPage2 = repo.findOne({ type: Page });
        expect(foundPage2).toEqual(page);

        let foundNote2 = repo.findOne({ type: Note });
        expect(foundNote2).toEqual(note);

        // Test find by prop
        let foundNote3 = repo.findOne({ name: "Test Note" });
        expect(foundNote3).toEqual(note);
    });

    // Test delta operations
    // Add a page, add a couple of notes, update the second note, remove the
    // first note. Then infer the delta from the changes, reverse it and
    // infer that the repo is empty
    test("Delta operations", () => {
        let page = new Page({
            id: "123",
            name: "Test Page",
        })
        let note1 = new Note({
            id: "456",
            name: "Test Note 1",
            pageId: "123"
        });
        let note2 = new Note({
            id: "789",
            name: "Test Note 2",
            pageId: "123"
        });

        let changes = [
            repo.insertOne(page),
            repo.insertOne(note1),
            repo.insertOne(note2),
        ]

        note2._data.name = "Updated Note 2";

        // Check that the repo has copied the entity (so that alterations cannot
        // leak over the repo interface)
        let repoNote2 = repo.findOne({ id: "789" });
        expect((repoNote2 as Note)._data.name).toBe("Test Note 2");

        let delta = Delta.fromChanges(changes);

        let changeDelete = repo.removeOne(note1);
        changes.push(changeDelete);

        delta = Delta.fromChanges(changes);

        let changeUpdate = repo.updateOne(note2);
        changes.push(changeUpdate);

        delta = Delta.fromChanges(changes);
        delta = delta.reversed()

        let reverseChanges = delta.changes();
        for (let change of reverseChanges) {
            repo.applyChange(change);
        }

        let all_entities = [...repo.find()];
        expect(all_entities.length).toBe(0);
    });
});
