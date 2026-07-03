package storage

func (e *TreeDBEngine) cacheLoadNode(id NodeID) (*Node, bool) {
	if e == nil || id == "" {
		return nil, false
	}
	e.nodeCacheMu.RLock()
	cached, ok := e.nodeCache[id]
	e.nodeCacheMu.RUnlock()
	if !ok || cached == nil {
		return nil, false
	}
	return copyNode(cached), true
}

func (e *TreeDBEngine) cacheStoreNode(node *Node) {
	if e == nil || node == nil || node.ID == "" {
		return
	}
	cached := copyNode(node)
	normalizePropertyMapShapes(cached.Properties)
	e.nodeCacheMu.Lock()
	if e.nodeCacheMaxEntries > 0 && len(e.nodeCache) > e.nodeCacheMaxEntries {
		e.nodeCache = make(map[NodeID]*Node, e.nodeCacheMaxEntries)
	}
	e.nodeCache[node.ID] = cached
	e.nodeCacheMu.Unlock()
}

func (e *TreeDBEngine) cacheStoreNodeIfGuard(node *Node, guard uint64) bool {
	if e == nil || node == nil || node.ID == "" {
		return false
	}
	if e.guardSeq.Load() != guard {
		return false
	}
	cached := copyNode(node)
	normalizePropertyMapShapes(cached.Properties)
	e.nodeCacheMu.Lock()
	defer e.nodeCacheMu.Unlock()
	if e.guardSeq.Load() != guard {
		return false
	}
	if e.nodeCacheMaxEntries > 0 && len(e.nodeCache) > e.nodeCacheMaxEntries {
		e.nodeCache = make(map[NodeID]*Node, e.nodeCacheMaxEntries)
	}
	e.nodeCache[node.ID] = cached
	return true
}

func (e *TreeDBEngine) cacheDeleteNode(id NodeID) {
	if e == nil || id == "" {
		return
	}
	e.nodeCacheMu.Lock()
	delete(e.nodeCache, id)
	e.nodeCacheMu.Unlock()
}

func (e *TreeDBEngine) cacheLoadEdge(id EdgeID) (*Edge, bool) {
	if e == nil || id == "" {
		return nil, false
	}
	e.edgeCacheMu.RLock()
	cached, ok := e.edgeCache[id]
	e.edgeCacheMu.RUnlock()
	if !ok || cached == nil {
		return nil, false
	}
	return copyEdge(cached), true
}

// cacheLoadEdgeReadOnly returns the cached edge body without copying.
// Callers must treat the returned edge as immutable. This is intentionally
// private to traversal materialization; GetEdge keeps returning a defensive
// copy through cacheLoadEdge.
func (e *TreeDBEngine) cacheLoadEdgeReadOnly(id EdgeID) (*Edge, bool) {
	if e == nil || id == "" {
		return nil, false
	}
	e.edgeCacheMu.RLock()
	cached, ok := e.edgeCache[id]
	e.edgeCacheMu.RUnlock()
	if !ok || cached == nil {
		return nil, false
	}
	return cached, true
}

func (e *TreeDBEngine) cacheStoreEdge(edge *Edge) {
	if e == nil || edge == nil || edge.ID == "" {
		return
	}
	cached := copyEdge(edge)
	normalizePropertyMapShapes(cached.Properties)
	e.edgeCacheMu.Lock()
	e.cacheStoreEdgeLocked(edge.ID, cached)
	e.edgeCacheMu.Unlock()
}

func (e *TreeDBEngine) cacheStoreEdges(edges []*Edge) {
	if e == nil || len(edges) == 0 {
		return
	}
	e.edgeCacheMu.Lock()
	for _, edge := range edges {
		if edge == nil || edge.ID == "" {
			continue
		}
		cached := copyEdge(edge)
		normalizePropertyMapShapes(cached.Properties)
		e.cacheStoreEdgeLocked(edge.ID, cached)
	}
	e.edgeCacheMu.Unlock()
}

func (e *TreeDBEngine) cacheStoreEdgeLocked(id EdgeID, cached *Edge) {
	if cached == nil {
		return
	}
	if e.edgeCacheMaxItems > 0 && len(e.edgeCache) > e.edgeCacheMaxItems {
		e.edgeCache = make(map[EdgeID]*Edge, e.edgeCacheMaxItems)
		e.edgeCacheByPtr = make(map[*Edge]EdgeID, e.edgeCacheMaxItems)
	}
	if e.edgeCacheByPtr == nil {
		e.edgeCacheByPtr = make(map[*Edge]EdgeID, e.edgeCacheMaxItems)
	}
	if prev := e.edgeCache[id]; prev != nil {
		delete(e.edgeCacheByPtr, prev)
	}
	e.edgeCache[id] = cached
	e.edgeCacheByPtr[cached] = id
}

func (e *TreeDBEngine) cacheDeleteEdge(id EdgeID) {
	if e == nil || id == "" {
		return
	}
	e.edgeCacheMu.Lock()
	if cached := e.edgeCache[id]; cached != nil {
		delete(e.edgeCacheByPtr, cached)
	}
	delete(e.edgeCache, id)
	e.edgeCacheMu.Unlock()
}

func (e *TreeDBEngine) cacheDeleteEdgeCandidate(edge *Edge) {
	if e == nil || edge == nil {
		return
	}
	e.edgeCacheMu.Lock()
	if id, ok := e.edgeCacheByPtr[edge]; ok {
		delete(e.edgeCache, id)
		delete(e.edgeCacheByPtr, edge)
	}
	if edge.ID != "" {
		if cached := e.edgeCache[edge.ID]; cached != nil {
			delete(e.edgeCacheByPtr, cached)
		}
		delete(e.edgeCache, edge.ID)
	}
	e.edgeCacheMu.Unlock()
}

// adjCacheLoadOutgoing returns the cached outgoing EdgeIDs for nodeID.
// The returned slice is shared with the cache and must be treated as read-only.
func (e *TreeDBEngine) adjCacheLoadOutgoing(nodeID NodeID) ([]EdgeID, bool) {
	if e == nil || nodeID == "" {
		return nil, false
	}
	e.adjCacheMu.RLock()
	ids, ok := e.outgoingAdjCache[nodeID]
	e.adjCacheMu.RUnlock()
	return ids, ok
}

// adjCacheLoadIncoming returns the cached incoming EdgeIDs for nodeID.
// The returned slice is shared with the cache and must be treated as read-only.
func (e *TreeDBEngine) adjCacheLoadIncoming(nodeID NodeID) ([]EdgeID, bool) {
	if e == nil || nodeID == "" {
		return nil, false
	}
	e.adjCacheMu.RLock()
	ids, ok := e.incomingAdjCache[nodeID]
	e.adjCacheMu.RUnlock()
	return ids, ok
}

func (e *TreeDBEngine) adjCacheStoreOutgoing(nodeID NodeID, ids []EdgeID) {
	if e == nil || nodeID == "" {
		return
	}
	cached := make([]EdgeID, len(ids))
	copy(cached, ids)
	e.adjCacheMu.Lock()
	if e.adjCacheMaxNodes > 0 && len(e.outgoingAdjCache) > e.adjCacheMaxNodes {
		e.outgoingAdjCache = make(map[NodeID][]EdgeID, e.adjCacheMaxNodes)
	}
	e.outgoingAdjCache[nodeID] = cached
	e.adjCacheMu.Unlock()
}

func (e *TreeDBEngine) adjCacheStoreOutgoingIfGuard(nodeID NodeID, ids []EdgeID, guard uint64) bool {
	if e == nil || nodeID == "" {
		return false
	}
	if e.guardSeq.Load() != guard {
		return false
	}
	cached := make([]EdgeID, len(ids))
	copy(cached, ids)
	e.adjCacheMu.Lock()
	defer e.adjCacheMu.Unlock()
	if e.guardSeq.Load() != guard {
		return false
	}
	if e.adjCacheMaxNodes > 0 && len(e.outgoingAdjCache) > e.adjCacheMaxNodes {
		e.outgoingAdjCache = make(map[NodeID][]EdgeID, e.adjCacheMaxNodes)
	}
	e.outgoingAdjCache[nodeID] = cached
	return true
}

func (e *TreeDBEngine) adjCacheStoreIncoming(nodeID NodeID, ids []EdgeID) {
	if e == nil || nodeID == "" {
		return
	}
	cached := make([]EdgeID, len(ids))
	copy(cached, ids)
	e.adjCacheMu.Lock()
	if e.adjCacheMaxNodes > 0 && len(e.incomingAdjCache) > e.adjCacheMaxNodes {
		e.incomingAdjCache = make(map[NodeID][]EdgeID, e.adjCacheMaxNodes)
	}
	e.incomingAdjCache[nodeID] = cached
	e.adjCacheMu.Unlock()
}

func (e *TreeDBEngine) adjCacheStoreIncomingIfGuard(nodeID NodeID, ids []EdgeID, guard uint64) bool {
	if e == nil || nodeID == "" {
		return false
	}
	if e.guardSeq.Load() != guard {
		return false
	}
	cached := make([]EdgeID, len(ids))
	copy(cached, ids)
	e.adjCacheMu.Lock()
	defer e.adjCacheMu.Unlock()
	if e.guardSeq.Load() != guard {
		return false
	}
	if e.adjCacheMaxNodes > 0 && len(e.incomingAdjCache) > e.adjCacheMaxNodes {
		e.incomingAdjCache = make(map[NodeID][]EdgeID, e.adjCacheMaxNodes)
	}
	e.incomingAdjCache[nodeID] = cached
	return true
}

func (e *TreeDBEngine) adjCacheInvalidateForEdge(edge *Edge) {
	if e == nil || edge == nil {
		return
	}
	e.adjCacheMu.Lock()
	delete(e.outgoingAdjCache, edge.StartNode)
	delete(e.incomingAdjCache, edge.EndNode)
	e.adjCacheMu.Unlock()
}

func (e *TreeDBEngine) adjCacheInvalidateForEdges(edges []*Edge) {
	if e == nil || len(edges) == 0 {
		return
	}
	e.adjCacheMu.Lock()
	for _, edge := range edges {
		if edge == nil {
			continue
		}
		delete(e.outgoingAdjCache, edge.StartNode)
		delete(e.incomingAdjCache, edge.EndNode)
	}
	e.adjCacheMu.Unlock()
}

func (e *TreeDBEngine) adjCacheInvalidateAll() {
	if e == nil {
		return
	}
	e.adjCacheMu.Lock()
	e.outgoingAdjCache = make(map[NodeID][]EdgeID, e.adjCacheMaxNodes)
	e.incomingAdjCache = make(map[NodeID][]EdgeID, e.adjCacheMaxNodes)
	e.adjCacheMu.Unlock()
}

func (e *TreeDBEngine) applyBodyCache(
	createdNodes []*Node,
	updatedNodes []*Node,
	deletedNodeIDs []NodeID,
	createdEdges []*Edge,
	updatedEdges []*Edge,
	oldUpdatedEdges []*Edge,
	deletedEdgeIDs []EdgeID,
	deletedEdges []*Edge,
) {
	for _, node := range createdNodes {
		e.cacheStoreNode(node)
	}
	for _, node := range updatedNodes {
		e.cacheStoreNode(node)
	}
	e.cacheStoreEdges(createdEdges)
	e.adjCacheInvalidateForEdges(createdEdges)
	for i, edge := range updatedEdges {
		e.cacheStoreEdge(edge)
		e.adjCacheInvalidateForEdge(edge)
		if i < len(oldUpdatedEdges) && !sameTreeDBEdgeEndpoints(edge, oldUpdatedEdges[i]) {
			e.adjCacheInvalidateForEdge(oldUpdatedEdges[i])
		}
	}
	for i := len(updatedEdges); i < len(oldUpdatedEdges); i++ {
		e.adjCacheInvalidateForEdge(oldUpdatedEdges[i])
	}
	for _, id := range deletedNodeIDs {
		e.cacheDeleteNode(id)
	}
	for _, id := range deletedEdgeIDs {
		e.cacheDeleteEdge(id)
	}
	if len(deletedEdges) == len(deletedEdgeIDs) {
		for _, edge := range deletedEdges {
			e.adjCacheInvalidateForEdge(edge)
		}
	} else if len(deletedEdgeIDs) > 0 {
		// Future delete paths may only know edge IDs. Fall back to a full clear
		// rather than risk stale endpoint adjacency entries.
		e.adjCacheInvalidateAll()
	}
}

func sameTreeDBEdgeEndpoints(a, b *Edge) bool {
	if a == nil || b == nil {
		return false
	}
	return a.StartNode == b.StartNode && a.EndNode == b.EndNode
}
