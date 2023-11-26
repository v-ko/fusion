import { Entity, EntityData } from "../libs/Entity";
import { InMemoryRepository } from "./InMemoryRepository";

interface PageData extends EntityData{
    name: string;
}

// mock Page entity subclass
class Page extends Entity<PageData> implements PageData{
    name: string;

    constructor(data: any) {
        super(data);
        this.name = data.name;
    }
    get parentId(): string {
        return "123";
    }
}


test("InMemoryRepository", () => {
    let repo = new InMemoryRepository();
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
