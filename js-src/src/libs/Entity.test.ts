import { Entity, EntityData, dumpToDict, entityType, loadFromDict } from './Entity'

interface IDummyEntity extends EntityData {
    testProp: string;
}

@entityType('DummyEntity')
class DummyEntity extends Entity<IDummyEntity> implements IDummyEntity {
    get parentId(): string {
        return '';
    }
    get testProp(): string {
        return this._data.testProp;
    }
    set testProp(newVal: string) {
        this._data.testProp = newVal;
    }
}

test('Entity methods', () => {
    let e = new DummyEntity({ id: '123', testProp: 'test' });
    e = e.withId('456');
    // e.replace({ id: 'Note2' });
    let leftovers = e.replace_silent({ s: 'Note3', testProp: 'test2' });

    let outDict = e.toObject();

    expect(e.id).toBe('456');
    expect(outDict).toEqual({ id: '456', testProp: 'test2' });
    expect(leftovers).toEqual({ s: 'Note3' });
});

test('Entity copy', () => {
    let e = new DummyEntity({ id: '123', testProp: 'test' });
    let e2 = e.copy();

    e2.testProp = 'changed';

    expect(e.testProp).toBe('test');
    expect(e2.testProp).toBe('changed');
});

test('Entity serialization and deserialization', () => {
    let e = new DummyEntity({ id: '123', testProp: 'test' });
    let eDict = dumpToDict(e);
    let e2 = loadFromDict(eDict) as DummyEntity;

    expect(e2._data).toEqual(e._data);
});
