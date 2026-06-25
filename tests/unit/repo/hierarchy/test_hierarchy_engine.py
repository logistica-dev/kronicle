# tests/unit/repo/hierarchy/test_hierarchy_engine.py

from kronicle.repo.hierarchy.hierarchy_engine import HierarchyEngine


class Node:
    def __init__(self, uid: str) -> None:
        self.uid = uid
        self.id = uid

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Node):
            return NotImplemented
        return self.uid == other.id

    def __hash__(self) -> int:
        return hash(self.uid)

    def __repr__(self) -> str:
        return f"Node({self.uid!r})"


def _make_engine(children_map: dict[str, list[str]]) -> HierarchyEngine:
    all_nodes = {}
    for nid in children_map:
        if nid not in all_nodes:
            all_nodes[nid] = Node(nid)
        for child_id in children_map[nid]:
            if child_id not in all_nodes:
                all_nodes[child_id] = Node(child_id)

    parent_map: dict[str, list[str]] = {nid: [] for nid in all_nodes}
    for parent_id, child_ids in children_map.items():
        for child_id in child_ids:
            parent_map[child_id].append(parent_id)

    def parents_of(node: Node) -> list[Node]:
        return [all_nodes[pid] for pid in parent_map.get(node.uid, [])]

    def children_of(node: Node) -> list[Node]:
        return [all_nodes[cid] for cid in children_map.get(node.uid, [])]

    return HierarchyEngine(parents_of=parents_of, children_of=children_of)  # type: ignore[arg-type]


def test_single_node_has_no_ancestors():
    engine = _make_engine({"a": []})
    a = Node("a")
    assert list(engine.ancestors(a)) == []
    assert list(engine.descendants(a)) == []


def test_single_node_collectors_return_empty():
    engine = _make_engine({"a": []})
    a = Node("a")
    assert engine.ancestors_list(a) == []
    assert engine.descendants_list(a) == []
    assert engine.ancestors_ids(a) == set()
    assert engine.descendants_ids(a) == set()


def test_single_node_predicates():
    engine = _make_engine({"a": []})
    a = Node("a")
    b = Node("b")
    assert not engine.is_ancestor(a, b)
    assert not engine.is_descendant(a, b)


def test_single_node_would_create_cycle_false():
    engine = _make_engine({"a": []})
    a = Node("a")
    assert not engine.would_create_cycle(a, a)


def test_chain_ancestors():
    children_map = {"a": ["b"], "b": ["c"]}
    engine = _make_engine(children_map)
    a, c = Node("a"), Node("c")
    assert list(engine.ancestors(a)) == []
    assert list(engine.ancestors(Node("b"))) == [a]
    assert list(engine.ancestors(c)) == [Node("b"), a]


def test_chain_descendants():
    children_map = {"a": ["b"], "b": ["c"]}
    engine = _make_engine(children_map)
    b, c = Node("b"), Node("c")
    assert list(engine.descendants(Node("a"))) == [b, c]
    assert list(engine.descendants(b)) == [c]
    assert list(engine.descendants(c)) == []


def test_chain_collectors():
    children_map = {"a": ["b"], "b": ["c"]}
    engine = _make_engine(children_map)
    a = Node("a")
    assert engine.ancestors_list(a) == []
    assert engine.descendants_list(a) == [Node("b"), Node("c")]
    assert engine.ancestors_ids(a) == set()
    assert engine.descendants_ids(a) == {"b", "c"}


def test_chain_predicates():
    children_map = {"a": ["b"], "b": ["c"]}
    engine = _make_engine(children_map)
    a, b, c = Node("a"), Node("b"), Node("c")
    assert engine.is_ancestor(c, a)
    assert engine.is_ancestor(c, b)
    assert not engine.is_ancestor(a, b)
    assert not engine.is_ancestor(a, c)
    assert engine.is_descendant(a, c)
    assert engine.is_descendant(a, b)
    assert not engine.is_descendant(c, a)
    assert not engine.is_descendant(c, b)


def test_tree_ancestors():
    children_map = {"a": ["b", "c"], "b": ["d"]}
    engine = _make_engine(children_map)
    a, b, c, d = Node("a"), Node("b"), Node("c"), Node("d")
    assert list(engine.ancestors(a)) == []
    assert list(engine.ancestors(b)) == [a]
    assert list(engine.ancestors(c)) == [a]
    assert list(engine.ancestors(d)) == [b, a]


def test_tree_descendants():
    children_map = {"a": ["b", "c"], "b": ["d"]}
    engine = _make_engine(children_map)
    a, b, c, d = Node("a"), Node("b"), Node("c"), Node("d")
    assert list(engine.descendants(a)) == [b, c, d]
    assert list(engine.descendants(b)) == [d]
    assert list(engine.descendants(c)) == []
    assert list(engine.descendants(d)) == []


def test_tree_collectors():
    children_map = {"a": ["b", "c"], "b": ["d"]}
    engine = _make_engine(children_map)
    a = Node("a")
    assert {n.uid for n in engine.ancestors_list(a)} == set()
    assert {n.uid for n in engine.descendants_list(a)} == {"b", "c", "d"}
    assert engine.ancestors_ids(a) == set()
    assert engine.descendants_ids(a) == {"b", "c", "d"}


def test_tree_predicates():
    children_map = {"a": ["b", "c"], "b": ["d"]}
    engine = _make_engine(children_map)
    a, b, c, d = Node("a"), Node("b"), Node("c"), Node("d")
    assert engine.is_ancestor(b, a)
    assert engine.is_ancestor(d, a)
    assert engine.is_ancestor(d, b)
    assert not engine.is_ancestor(a, b)
    assert engine.is_ancestor(c, a)
    assert engine.is_descendant(a, b)
    assert engine.is_descendant(a, d)
    assert engine.is_descendant(b, d)
    assert not engine.is_descendant(b, a)
    assert not engine.is_descendant(c, a)


def test_multiple_roots():
    children_map = {"a": ["c"], "b": ["d"]}
    engine = _make_engine(children_map)
    a, b = Node("a"), Node("b")
    assert list(engine.ancestors(a)) == []
    assert list(engine.ancestors(b)) == []
    assert list(engine.ancestors(Node("c"))) == [a]
    assert list(engine.ancestors(Node("d"))) == [b]
    assert list(engine.descendants(a)) == [Node("c")]
    assert list(engine.descendants(b)) == [Node("d")]


def test_would_create_cycle_true():
    children_map = {"a": ["b"], "b": ["c"]}
    engine = _make_engine(children_map)
    assert engine.would_create_cycle(parent=Node("b"), child=Node("a"))
    assert engine.would_create_cycle(parent=Node("c"), child=Node("b"))
    assert engine.would_create_cycle(parent=Node("c"), child=Node("a"))


def test_would_create_cycle_false():
    children_map = {"a": ["b"]}
    engine = _make_engine(children_map)
    assert not engine.would_create_cycle(parent=Node("a"), child=Node("c"))
    assert not engine.would_create_cycle(parent=Node("c"), child=Node("a"))
    assert not engine.would_create_cycle(parent=Node("c"), child=Node("b"))


def test_would_create_cycle_self_loop_is_not_detected():
    children_map = {}
    engine = _make_engine(children_map)
    a = Node("a")
    assert not engine.would_create_cycle(parent=a, child=a)


def test_ancestors_cycle_safe():
    children_map = {"a": ["b"], "b": ["a"]}
    engine = _make_engine(children_map)
    a, b = Node("a"), Node("b")
    result = list(engine.ancestors(a))
    assert b in result
    assert len(result) == 1


def test_descendants_cycle_safe():
    children_map = {"a": ["b"], "b": ["a"]}
    engine = _make_engine(children_map)
    a, b = Node("a"), Node("b")
    result = list(engine.descendants(a))
    assert b in result
    assert len(result) == 1
