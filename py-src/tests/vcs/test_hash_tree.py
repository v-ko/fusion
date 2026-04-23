"""Tests for HashTree."""

from fusion.storage.delta import Delta
from fusion.storage.hash_tree import (
    HangingSubtreesError,
    HashTree,
    build_hash_tree,
    update_hash_tree,
)
from fusion.storage.in_memory_store import InMemoryStore


def test_empty_tree_deterministic_hash():
    tree1 = HashTree()
    tree1.update_root_hash()

    tree2 = HashTree()
    tree2.update_root_hash()

    assert tree1.root_hash() == tree2.root_hash()


def test_adding_nodes_changes_hash():
    tree = HashTree()
    tree.update_root_hash()
    empty_hash = tree.root_hash()

    tree.create_node("root1", "", "data1")
    tree.update_root_hash()
    one_root_hash = tree.root_hash()
    assert one_root_hash != empty_hash

    tree.create_node("child1", "root1", "cdata1")
    tree.update_root_hash()
    with_child_hash = tree.root_hash()
    assert with_child_hash != one_root_hash


def test_insertion_order_doesnt_affect_hash():
    tree_a = HashTree()
    tree_a.create_node("root", "", "rdata")
    tree_a.create_node("a", "root", "da")
    tree_a.create_node("b", "root", "db")
    tree_a.create_node("c", "root", "dc")
    tree_a.update_root_hash()

    tree_b = HashTree()
    tree_b.create_node("root", "", "rdata")
    tree_b.create_node("c", "root", "dc")
    tree_b.create_node("a", "root", "da")
    tree_b.create_node("b", "root", "db")
    tree_b.update_root_hash()

    assert tree_a.root_hash() == tree_b.root_hash()


def test_out_of_order_insertion_buffers_correctly():
    ref = HashTree()
    ref.create_node("root", "", "rd")
    ref.create_node("child", "root", "cd")
    ref.create_node("grandchild", "child", "gd")
    ref.update_root_hash()

    tree = HashTree()
    tree.create_node("grandchild", "child", "gd")
    tree.create_node("child", "root", "cd")
    tree.create_node("root", "", "rd")
    tree.update_root_hash()

    assert tree.root_hash() == ref.root_hash()
    assert len(tree.nodes) == 4  # super-root + 3


def test_hanging_subtrees_raise():
    tree = HashTree()
    tree.create_node("orphan", "nonexistent-parent", "data")
    try:
        tree.update_root_hash()
        assert False, "Should have raised"
    except HangingSubtreesError:
        pass


def test_remove_leaf():
    tree = HashTree()
    tree.create_node("root", "", "rd")
    tree.create_node("childA", "root", "da")
    tree.create_node("childB", "root", "db")
    tree.update_root_hash()
    full_hash = tree.root_hash()

    # Remove childA
    node_a = tree.nodes["childA"]
    tree.remove_node(node_a)
    tree.update_root_hash()
    after_remove = tree.root_hash()
    assert after_remove != full_hash

    # The node is gone
    assert "childA" not in tree.nodes


def test_build_hash_tree_from_store(DummyPage, DummyNote):
    store = InMemoryStore()
    page = DummyPage(id="page1", parent_id="", name="Test Page")
    note = DummyNote(id="note1", parent_id="page1", test_prop="hello")
    store.insert_one(page)
    store.insert_one(note)

    tree = build_hash_tree(store)
    h = tree.root_hash()
    assert h  # non-empty

    # Rebuilding gives the same hash
    tree2 = build_hash_tree(store)
    assert tree2.root_hash() == h


def test_update_hash_tree_incremental(DummyPage, DummyNote):
    store = InMemoryStore()
    page = DummyPage(id="page1", parent_id="", name="Test Page")
    note1 = DummyNote(id="note1", parent_id="page1", test_prop="hello")
    store.insert_one(page)
    store.insert_one(note1)

    tree = build_hash_tree(store)
    hash_before = tree.root_hash()

    # Insert a new note
    note2 = DummyNote(id="note2", parent_id="page1", test_prop="world")
    change = store.insert_one(note2)
    delta = Delta.from_changes([change])
    update_hash_tree(tree, store, delta)

    hash_after = tree.root_hash()
    assert hash_after != hash_before

    # Full rebuild should match
    tree_full = build_hash_tree(store)
    assert tree_full.root_hash() == hash_after
