package storage

import "fmt"

type namespacedGraphTransaction struct {
	namespace *NamespacedEngine
	tx        GraphTransaction
}

func (t *namespacedGraphTransaction) TransactionID() string { return t.tx.TransactionID() }
func (t *namespacedGraphTransaction) Commit() error         { return t.tx.Commit() }
func (t *namespacedGraphTransaction) Rollback() error       { return t.tx.Rollback() }
func (t *namespacedGraphTransaction) IsActive() bool        { return t.tx.IsActive() }
func (t *namespacedGraphTransaction) OperationCount() int   { return t.tx.OperationCount() }
func (t *namespacedGraphTransaction) Namespace() string     { return t.namespace.namespace }

func (t *namespacedGraphTransaction) SetNamespace(ns string) error {
	if ns != "" && ns != t.namespace.namespace {
		return fmt.Errorf("%w: pinned to %q, attempted %q", ErrCrossNamespaceTransaction, t.namespace.namespace, ns)
	}
	return t.tx.SetNamespace(t.namespace.namespace)
}

func (t *namespacedGraphTransaction) SetDeferredConstraintValidation(deferValidation bool) error {
	return t.tx.SetDeferredConstraintValidation(deferValidation)
}

func (t *namespacedGraphTransaction) SetSkipCreateExistenceCheck(skip bool) error {
	return t.tx.SetSkipCreateExistenceCheck(skip)
}

func (t *namespacedGraphTransaction) SetImplicit(implicit bool) error {
	return t.tx.SetImplicit(implicit)
}

func (t *namespacedGraphTransaction) HasPendingNodeMutations() bool {
	return t.tx.HasPendingNodeMutations()
}

func (t *namespacedGraphTransaction) SetMetadata(metadata map[string]interface{}) error {
	return t.tx.SetMetadata(metadata)
}

func (t *namespacedGraphTransaction) GetMetadata() map[string]interface{} {
	return t.tx.GetMetadata()
}

func (t *namespacedGraphTransaction) CreateNode(node *Node) (NodeID, error) {
	namespaced := copyNode(node)
	namespaced.ID = t.namespace.prefixNodeID(node.ID)
	actualID, err := t.tx.CreateNode(namespaced)
	if err != nil {
		return "", err
	}
	return t.namespace.unprefixNodeID(actualID), nil
}

func (t *namespacedGraphTransaction) UpdateNode(node *Node) error {
	namespaced := copyNode(node)
	namespaced.ID = t.namespace.prefixNodeID(node.ID)
	return t.tx.UpdateNode(namespaced)
}

func (t *namespacedGraphTransaction) DeleteNode(id NodeID) error {
	return t.tx.DeleteNode(t.namespace.prefixNodeID(id))
}

func (t *namespacedGraphTransaction) CreateEdge(edge *Edge) error {
	namespaced := copyEdge(edge)
	namespaced.ID = t.namespace.prefixEdgeID(edge.ID)
	namespaced.StartNode = t.namespace.prefixNodeID(edge.StartNode)
	namespaced.EndNode = t.namespace.prefixNodeID(edge.EndNode)
	return t.tx.CreateEdge(namespaced)
}

func (t *namespacedGraphTransaction) UpdateEdge(edge *Edge) error {
	namespaced := copyEdge(edge)
	namespaced.ID = t.namespace.prefixEdgeID(edge.ID)
	namespaced.StartNode = t.namespace.prefixNodeID(edge.StartNode)
	namespaced.EndNode = t.namespace.prefixNodeID(edge.EndNode)
	return t.tx.UpdateEdge(namespaced)
}

func (t *namespacedGraphTransaction) DeleteEdge(id EdgeID) error {
	return t.tx.DeleteEdge(t.namespace.prefixEdgeID(id))
}

func (t *namespacedGraphTransaction) BulkCreateEdges(edges []*Edge) error {
	namespaced := make([]*Edge, 0, len(edges))
	for _, edge := range edges {
		next := copyEdge(edge)
		next.ID = t.namespace.prefixEdgeID(edge.ID)
		next.StartNode = t.namespace.prefixNodeID(edge.StartNode)
		next.EndNode = t.namespace.prefixNodeID(edge.EndNode)
		namespaced = append(namespaced, next)
	}
	return t.tx.BulkCreateEdges(namespaced)
}

func (t *namespacedGraphTransaction) GetNode(id NodeID) (*Node, error) {
	node, err := t.tx.GetNode(t.namespace.prefixNodeID(id))
	if err != nil {
		return nil, err
	}
	return t.namespace.toUserNode(node), nil
}

func (t *namespacedGraphTransaction) GetEdge(id EdgeID) (*Edge, error) {
	edge, err := t.tx.GetEdge(t.namespace.prefixEdgeID(id))
	if err != nil {
		return nil, err
	}
	return t.namespace.toUserEdge(edge), nil
}

func (t *namespacedGraphTransaction) GetNodesByLabel(label string) ([]*Node, error) {
	nodes, err := t.tx.GetNodesByLabel(label)
	if err != nil {
		return nil, err
	}
	return t.toUserNodes(nodes), nil
}

func (t *namespacedGraphTransaction) GetFirstNodeByLabel(label string) (*Node, error) {
	node, err := t.tx.GetFirstNodeByLabel(label)
	if err != nil {
		return nil, err
	}
	return t.namespace.toUserNode(node), nil
}

func (t *namespacedGraphTransaction) GetOutgoingEdges(nodeID NodeID) ([]*Edge, error) {
	edges, err := t.tx.GetOutgoingEdges(t.namespace.prefixNodeID(nodeID))
	if err != nil {
		return nil, err
	}
	return t.toUserEdges(edges), nil
}

func (t *namespacedGraphTransaction) GetIncomingEdges(nodeID NodeID) ([]*Edge, error) {
	edges, err := t.tx.GetIncomingEdges(t.namespace.prefixNodeID(nodeID))
	if err != nil {
		return nil, err
	}
	return t.toUserEdges(edges), nil
}

func (t *namespacedGraphTransaction) GetEdgesBetween(startID, endID NodeID) ([]*Edge, error) {
	edges, err := t.tx.GetEdgesBetween(t.namespace.prefixNodeID(startID), t.namespace.prefixNodeID(endID))
	if err != nil {
		return nil, err
	}
	return t.toUserEdges(edges), nil
}

func (t *namespacedGraphTransaction) GetEdgeBetween(startID, endID NodeID, edgeType string) *Edge {
	edge := t.tx.GetEdgeBetween(t.namespace.prefixNodeID(startID), t.namespace.prefixNodeID(endID), edgeType)
	if edge == nil {
		return nil
	}
	return t.namespace.toUserEdge(edge)
}

func (t *namespacedGraphTransaction) GetEdgesByType(edgeType string) ([]*Edge, error) {
	edges, err := t.tx.GetEdgesByType(edgeType)
	if err != nil {
		return nil, err
	}
	return t.toUserEdges(edges), nil
}

func (t *namespacedGraphTransaction) AllNodes() ([]*Node, error) {
	nodes, err := t.tx.AllNodes()
	if err != nil {
		return nil, err
	}
	return t.toUserNodes(nodes), nil
}

func (t *namespacedGraphTransaction) GetAllNodes() []*Node {
	return t.toUserNodes(t.tx.GetAllNodes())
}

func (t *namespacedGraphTransaction) toUserNodes(nodes []*Node) []*Node {
	out := make([]*Node, 0, len(nodes))
	for _, node := range nodes {
		if node != nil && t.namespace.hasNodePrefix(node.ID) {
			out = append(out, t.namespace.toUserNode(node))
		}
	}
	return out
}

func (t *namespacedGraphTransaction) toUserEdges(edges []*Edge) []*Edge {
	out := make([]*Edge, 0, len(edges))
	for _, edge := range edges {
		if edge != nil && t.namespace.hasEdgePrefix(edge.ID) {
			out = append(out, t.namespace.toUserEdge(edge))
		}
	}
	return out
}
