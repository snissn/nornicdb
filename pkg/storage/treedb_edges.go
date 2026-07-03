package storage

import "errors"

// GetOutgoingEdges returns all outgoing edges for nodeID.
// Returned edges may share cached storage and must not be mutated.
func (e *TreeDBEngine) GetOutgoingEdges(nodeID NodeID) ([]*Edge, error) {
	if nodeID == "" {
		return nil, ErrInvalidID
	}
	if err := e.ensureOpen(); err != nil {
		return nil, err
	}
	if ids, ok := e.adjCacheLoadOutgoing(nodeID); ok {
		return e.materializeAdjEdges(ids, treeDBAdjOutgoing, nodeID)
	}
	cacheGuard := e.guardSeq.Load()
	edges, ids, err := e.collectEdgesAndIDsByIndexPrefix(treeDBOutgoingIndexPrefix(nodeID), treeDBAdjOutgoing, nodeID)
	if err != nil {
		return nil, err
	}
	e.adjCacheStoreOutgoingIfGuard(nodeID, ids, cacheGuard)
	return edges, nil
}

// GetIncomingEdges returns all incoming edges for nodeID.
// Returned edges may share cached storage and must not be mutated.
func (e *TreeDBEngine) GetIncomingEdges(nodeID NodeID) ([]*Edge, error) {
	if nodeID == "" {
		return nil, ErrInvalidID
	}
	if err := e.ensureOpen(); err != nil {
		return nil, err
	}
	if ids, ok := e.adjCacheLoadIncoming(nodeID); ok {
		return e.materializeAdjEdges(ids, treeDBAdjIncoming, nodeID)
	}
	cacheGuard := e.guardSeq.Load()
	edges, ids, err := e.collectEdgesAndIDsByIndexPrefix(treeDBIncomingIndexPrefix(nodeID), treeDBAdjIncoming, nodeID)
	if err != nil {
		return nil, err
	}
	e.adjCacheStoreIncomingIfGuard(nodeID, ids, cacheGuard)
	return edges, nil
}

// GetAdjacentEdges returns both outgoing and incoming edges for nodeID.
// Returned edges may share cached storage and must not be mutated.
func (e *TreeDBEngine) GetAdjacentEdges(nodeID NodeID) ([]*Edge, []*Edge, error) {
	if nodeID == "" {
		return nil, nil, ErrInvalidID
	}
	if err := e.ensureOpen(); err != nil {
		return nil, nil, err
	}
	cachedOutIDs, outHit := e.adjCacheLoadOutgoing(nodeID)
	cachedInIDs, inHit := e.adjCacheLoadIncoming(nodeID)
	if outHit && inHit {
		outgoing, err := e.materializeAdjEdges(cachedOutIDs, treeDBAdjOutgoing, nodeID)
		if err != nil {
			return nil, nil, err
		}
		incoming, err := e.materializeAdjEdges(cachedInIDs, treeDBAdjIncoming, nodeID)
		if err != nil {
			return nil, nil, err
		}
		return outgoing, incoming, nil
	}

	cacheGuard := e.guardSeq.Load()
	var outgoing, incoming []*Edge
	if !outHit {
		var outIDs []EdgeID
		var err error
		outgoing, outIDs, err = e.collectEdgesAndIDsByIndexPrefix(treeDBOutgoingIndexPrefix(nodeID), treeDBAdjOutgoing, nodeID)
		if err != nil {
			return nil, nil, err
		}
		e.adjCacheStoreOutgoingIfGuard(nodeID, outIDs, cacheGuard)
	} else {
		var err error
		outgoing, err = e.materializeAdjEdges(cachedOutIDs, treeDBAdjOutgoing, nodeID)
		if err != nil {
			return nil, nil, err
		}
	}

	if !inHit {
		var inIDs []EdgeID
		var err error
		incoming, inIDs, err = e.collectEdgesAndIDsByIndexPrefix(treeDBIncomingIndexPrefix(nodeID), treeDBAdjIncoming, nodeID)
		if err != nil {
			return nil, nil, err
		}
		e.adjCacheStoreIncomingIfGuard(nodeID, inIDs, cacheGuard)
	} else {
		var err error
		incoming, err = e.materializeAdjEdges(cachedInIDs, treeDBAdjIncoming, nodeID)
		if err != nil {
			return nil, nil, err
		}
	}

	return outgoing, incoming, nil
}

// GetEdgesByType returns all edges with edgeType.
func (e *TreeDBEngine) GetEdgesByType(edgeType string) ([]*Edge, error) {
	return e.collectEdgesByIndexPrefix(treeDBEdgeTypeIndexPrefix(edgeType))
}

func (e *TreeDBEngine) collectEdgesByIndexPrefix(prefix []byte) ([]*Edge, error) {
	ids, err := e.collectEdgeIDsByIndexPrefix(prefix)
	if err != nil {
		return nil, err
	}
	return e.materializeEdges(ids)
}

func (e *TreeDBEngine) collectEdgesAndIDsByIndexPrefix(prefix []byte, direction treeDBAdjDirection, nodeID NodeID) ([]*Edge, []EdgeID, error) {
	ids, err := e.collectEdgeIDsByIndexPrefix(prefix)
	if err != nil {
		return nil, nil, err
	}
	edges, err := e.materializeAdjEdges(ids, direction, nodeID)
	if err != nil {
		return nil, nil, err
	}
	if len(edges) != len(ids) {
		ids = make([]EdgeID, 0, len(edges))
		for _, edge := range edges {
			ids = append(ids, edge.ID)
		}
	}
	return edges, ids, nil
}

func (e *TreeDBEngine) collectEdgeIDsByIndexPrefix(prefix []byte) ([]EdgeID, error) {
	if err := e.ensureOpen(); err != nil {
		return nil, err
	}
	it, err := e.db.Iterator(prefix, treeDBRangeEnd(prefix))
	if err != nil {
		return nil, mapTreeDBError(err)
	}
	defer it.Close()
	var ids []EdgeID
	for ; it.Valid(); it.Next() {
		ids = append(ids, EdgeID(string(it.Key()[len(prefix):])))
	}
	return ids, mapTreeDBError(it.Error())
}

type treeDBAdjDirection uint8

const (
	treeDBAdjOutgoing treeDBAdjDirection = iota + 1
	treeDBAdjIncoming
)

func treeDBEdgeMatchesAdj(edge *Edge, direction treeDBAdjDirection, nodeID NodeID) bool {
	if edge == nil {
		return false
	}
	switch direction {
	case treeDBAdjOutgoing:
		return edge.StartNode == nodeID
	case treeDBAdjIncoming:
		return edge.EndNode == nodeID
	default:
		return true
	}
}

func (e *TreeDBEngine) materializeAdjEdges(ids []EdgeID, direction treeDBAdjDirection, nodeID NodeID) ([]*Edge, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	edges := make([]*Edge, 0, len(ids))
	for _, id := range ids {
		if cached, ok := e.cacheLoadEdgeReadOnly(id); ok {
			if treeDBEdgeMatchesAdj(cached, direction, nodeID) {
				edges = append(edges, cached)
			}
			continue
		}
		edge, err := e.GetEdge(id)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if treeDBEdgeMatchesAdj(edge, direction, nodeID) {
			edges = append(edges, edge)
		}
	}
	return edges, nil
}

func (e *TreeDBEngine) materializeEdges(ids []EdgeID) ([]*Edge, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	edges := make([]*Edge, 0, len(ids))
	for _, id := range ids {
		edge, err := e.GetEdge(id)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		edges = append(edges, edge)
	}
	return edges, nil
}

// GetEdgesBetween returns all edges from startID to endID.
func (e *TreeDBEngine) GetEdgesBetween(startID, endID NodeID) ([]*Edge, error) {
	if startID == "" || endID == "" {
		return nil, ErrInvalidID
	}
	return e.collectEdgesByBetweenPrefix(treeDBEdgeBetweenIndexPrefix(startID, endID))
}

func (e *TreeDBEngine) collectEdgesByBetweenPrefix(prefix []byte) ([]*Edge, error) {
	if err := e.ensureOpen(); err != nil {
		return nil, err
	}
	it, err := e.db.Iterator(prefix, treeDBRangeEnd(prefix))
	if err != nil {
		return nil, mapTreeDBError(err)
	}
	defer it.Close()
	var edges []*Edge
	for ; it.Valid(); it.Next() {
		edgeID := treeDBEdgeIDFromBetweenKey(it.Key())
		if edgeID == "" {
			continue
		}
		edge, err := e.GetEdge(edgeID)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		edges = append(edges, edge)
	}
	return edges, mapTreeDBError(it.Error())
}

// GetEdgeBetween returns one edge from startID to endID with edgeType.
func (e *TreeDBEngine) GetEdgeBetween(startID, endID NodeID, edgeType string) *Edge {
	if startID == "" || endID == "" || edgeType == "" {
		return nil
	}
	if err := e.ensureOpen(); err != nil {
		return nil
	}
	data, err := e.db.GetAppend(treeDBEdgeBetweenHeadKey(startID, endID, edgeType), nil)
	if err == nil && len(data) > 0 {
		edge, err := e.GetEdge(EdgeID(string(data)))
		if err == nil && treeDBEdgeMatchesBetween(edge, startID, endID, edgeType) {
			return edge
		}
	}
	edges, err := e.collectEdgesByBetweenPrefix(treeDBTypedEdgeBetweenIndexPrefix(startID, endID, edgeType))
	if err != nil || len(edges) == 0 {
		return nil
	}
	return edges[0]
}

// GetInDegree returns the number of incoming edges.
func (e *TreeDBEngine) GetInDegree(nodeID NodeID) int {
	edges, err := e.GetIncomingEdges(nodeID)
	if err != nil {
		return 0
	}
	return len(edges)
}

// GetOutDegree returns the number of outgoing edges.
func (e *TreeDBEngine) GetOutDegree(nodeID NodeID) int {
	edges, err := e.GetOutgoingEdges(nodeID)
	if err != nil {
		return 0
	}
	return len(edges)
}
