"""Tests for the VCS primitives: HashTree, Commit, CommitGraph, Repository."""

from fusion import Entity, entity_type
from fusion.storage.change import Change
from fusion.storage.commit import Commit, CommitMetadata
from fusion.storage.commit_graph import BranchMetadata, CommitGraph, CommitGraphData
from fusion.storage.delta import Delta
from fusion.storage.hash_tree import (
    HangingSubtreesError,
    HashTree,
    build_hash_tree,
    update_hash_tree,
)
from fusion.storage.in_memory_store import InMemoryStore
from fusion.storage.repository import Repository, RepositoryIntegrityError


# ---------------------------------------------------------------------------
# Test entity types
# ---------------------------------------------------------------------------
@entity_type
class DummyPage(Entity):
    name: str = ""


@entity_type
class DummyNote(Entity):
    test_prop: str = ""


# ===========================================================================
# CommitGraph tests
# ===========================================================================
class TestCommitGraph:
    def test_create_and_query_branch(self):
        g = CommitGraph()
        g.create_branch("main")
        assert g.branch("main") is not None
        assert g.branch("main").head_commit_id is None

    def test_add_commit_and_head(self):
        g = CommitGraph()
        g.create_branch("main")
        cm = CommitMetadata(
            id="c1", parent_id="", snapshot_hash="h1", timestamp=1.0, message="init"
        )
        g.add_commit(cm)
        g.set_branch("main", "c1")
        head = g.head_commit("main")
        assert head is not None
        assert head.id == "c1"

    def test_branch_commits_order(self):
        g = CommitGraph()
        g.create_branch("main")
        c1 = CommitMetadata(
            id="c1", parent_id="", snapshot_hash="h1", timestamp=1.0, message="first"
        )
        c2 = CommitMetadata(
            id="c2", parent_id="c1", snapshot_hash="h2", timestamp=2.0, message="second"
        )
        c3 = CommitMetadata(
            id="c3", parent_id="c2", snapshot_hash="h3", timestamp=3.0, message="third"
        )
        g.add_commit(c1)
        g.add_commit(c2)
        g.add_commit(c3)
        g.set_branch("main", "c3")

        branch_commits = g.branch_commits("main")
        assert [c.id for c in branch_commits] == ["c1", "c2", "c3"]

    def test_serialization_roundtrip(self):
        g = CommitGraph()
        g.create_branch("main")
        cm = CommitMetadata(
            id="c1", parent_id="", snapshot_hash="h1", timestamp=1.0, message="init"
        )
        g.add_commit(cm)
        g.set_branch("main", "c1")

        data = g.data()
        g2 = CommitGraph.from_data(data)
        assert g2.branch("main").head_commit_id == "c1"
        assert g2.commit("c1").message == "init"

    def test_remove_branch(self):
        g = CommitGraph()
        g.create_branch("main")
        g.remove_branch("main")
        assert g.branch("main") is None

    def test_commits_between(self):
        g = CommitGraph()
        g.create_branch("main")
        for i in range(1, 6):
            parent = f"c{i-1}" if i > 1 else ""
            g.add_commit(
                CommitMetadata(
                    id=f"c{i}",
                    parent_id=parent,
                    snapshot_hash=f"h{i}",
                    timestamp=float(i),
                    message=f"m{i}",
                )
            )
        g.set_branch("main", "c5")

        between = g.commits_between("c2", "c5")
        # Should return c3, c4, c5 (exclusive of start)
        assert [c.id for c in between] == ["c3", "c4", "c5"]


# ===========================================================================
# HashTree tests
# ===========================================================================
class TestHashTree:
    def test_empty_tree_deterministic_hash(self):
        tree1 = HashTree()
        tree1.update_root_hash()

        tree2 = HashTree()
        tree2.update_root_hash()

        assert tree1.root_hash() == tree2.root_hash()

    def test_adding_nodes_changes_hash(self):
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

    def test_insertion_order_doesnt_affect_hash(self):
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

    def test_out_of_order_insertion_buffers_correctly(self):
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

    def test_hanging_subtrees_raise(self):
        tree = HashTree()
        tree.create_node("orphan", "nonexistent-parent", "data")
        try:
            tree.update_root_hash()
            assert False, "Should have raised"
        except HangingSubtreesError:
            pass

    def test_remove_leaf(self):
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

    def test_build_hash_tree_from_store(self):
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

    def test_update_hash_tree_incremental(self):
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


# ===========================================================================
# Repository tests
# ===========================================================================
class TestRepository:
    def test_commit_and_hash_integrity(self):
        repo = Repository.create()
        initial_hash = repo.hash_tree.root_hash()

        # Build a delta outside the repo
        aux_store = InMemoryStore()
        changes = [
            aux_store.insert_one(DummyPage(id="page1", parent_id="", name="Page 1")),
            aux_store.insert_one(
                DummyNote(id="note1", parent_id="page1", test_prop="e1")
            ),
        ]
        delta = Delta.from_changes(changes)

        commit = repo.commit(delta, "Initial commit")
        hash_after = repo.hash_tree.root_hash()

        assert commit.snapshot_hash == hash_after
        assert hash_after != initial_hash

        # Reverse commit restores hash
        reverse_delta = delta.reversed()
        reverse_commit = repo.commit(reverse_delta, "Reverse commit")
        assert reverse_commit.snapshot_hash == repo.hash_tree.root_hash()
        assert repo.hash_tree.root_hash() == initial_hash

    def test_commit_graph_tracks_commits(self):
        repo = Repository.create()

        aux = InMemoryStore()
        changes = [
            aux.insert_one(DummyPage(id="p1", parent_id="", name="P1")),
        ]
        repo.commit(Delta.from_changes(changes), "first")
        repo.commit(
            Delta.from_changes(
                [aux.insert_one(DummyNote(id="n1", parent_id="p1", test_prop="x"))]
            ),
            "second",
        )

        graph = repo.get_commit_graph()
        branch_commits = graph.branch_commits("main")
        assert len(branch_commits) == 2

    def test_reset(self):
        repo = Repository.create()

        aux = InMemoryStore()
        c1_delta = Delta.from_changes(
            [
                aux.insert_one(DummyPage(id="p1", parent_id="", name="P1")),
            ]
        )
        commit1 = repo.commit(c1_delta, "first")
        hash_after_c1 = repo.hash_tree.root_hash()

        c2_delta = Delta.from_changes(
            [
                aux.insert_one(DummyNote(id="n1", parent_id="p1", test_prop="x")),
            ]
        )
        repo.commit(c2_delta, "second")

        # Reset back one commit
        repo.reset(-1)

        assert repo.hash_tree.root_hash() == hash_after_c1
        graph = repo.get_commit_graph()
        assert len(graph.branch_commits("main")) == 1

    def test_remove_page_with_child_and_verify(self):
        repo = Repository.create()

        aux = InMemoryStore()
        changes = [
            aux.insert_one(DummyPage(id="page1", parent_id="", name="P1")),
            aux.insert_one(DummyNote(id="note1", parent_id="page1", test_prop="n1")),
            aux.insert_one(DummyPage(id="page2", parent_id="", name="P2")),
            aux.insert_one(DummyNote(id="note2", parent_id="page2", test_prop="n2")),
        ]
        repo.commit(Delta.from_changes(changes), "initial")

        # Remove page1 and its note
        page1 = aux.find_one(id="page1")
        note1 = aux.find_one(id="note1")
        remove_changes = [
            aux.remove_one(note1),
            aux.remove_one(page1),
        ]
        commit = repo.commit(Delta.from_changes(remove_changes), "remove page1")

        # Rebuild hash tree from scratch and compare
        rebuilt = build_hash_tree(repo.head_store)
        assert rebuilt.root_hash() == commit.snapshot_hash

    def test_head_store_reflects_commits(self):
        repo = Repository.create()

        aux = InMemoryStore()
        page = DummyPage(id="p1", parent_id="", name="My Page")
        changes = [aux.insert_one(page)]
        repo.commit(Delta.from_changes(changes), "add page")

        found = repo.head_store.find_one(id="p1")
        assert found is not None
        assert found.name == "My Page"
