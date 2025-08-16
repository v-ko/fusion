import { DummyNote } from 'fusion/storage/test-utils';
import { Entity, EntityData, dumpToDict, entityType, loadFromDict } from './Entity'


test('Entity methods', () => {
    let e = new DummyNote({ id: '456', parent_id: '', testProp: 'test' });
    let leftovers = e.replace_silent({ s: 'Note3', testProp: 'test2' });

    let outDict = e.data();

    expect(outDict).toEqual({ id: '456', testProp: 'test2' });
    expect(leftovers).toEqual({ s: 'Note3' });
});

test('Entity copy', () => {
    let e = new DummyNote({ id: '123', parent_id: '', testProp: 'test' });
    let e2 = e.copy();

    e2.testProp = 'changed';

    expect(e.testProp).toBe('test');
    expect(e2.testProp).toBe('changed');
});

test('Entity serialization and deserialization', () => {
    let e = new DummyNote({ id: '123', parent_id: '', testProp: 'test' });
    let eDict = dumpToDict(e);
    let e2 = loadFromDict(eDict) as DummyNote;

    expect(e2._data).toEqual(e._data);
});
