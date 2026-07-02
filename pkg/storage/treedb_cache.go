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

func (e *TreeDBEngine) cacheStoreEdge(edge *Edge) {
	if e == nil || edge == nil || edge.ID == "" {
		return
	}
	cached := copyEdge(edge)
	normalizePropertyMapShapes(cached.Properties)
	e.edgeCacheMu.Lock()
	if e.edgeCacheMaxItems > 0 && len(e.edgeCache) > e.edgeCacheMaxItems {
		e.edgeCache = make(map[EdgeID]*Edge, e.edgeCacheMaxItems)
	}
	e.edgeCache[edge.ID] = cached
	e.edgeCacheMu.Unlock()
}

func (e *TreeDBEngine) cacheDeleteEdge(id EdgeID) {
	if e == nil || id == "" {
		return
	}
	e.edgeCacheMu.Lock()
	delete(e.edgeCache, id)
	e.edgeCacheMu.Unlock()
}

func (e *TreeDBEngine) applyBodyCache(
	createdNodes []*Node,
	updatedNodes []*Node,
	deletedNodeIDs []NodeID,
	createdEdges []*Edge,
	updatedEdges []*Edge,
	deletedEdgeIDs []EdgeID,
) {
	for _, id := range deletedNodeIDs {
		e.cacheDeleteNode(id)
	}
	for _, id := range deletedEdgeIDs {
		e.cacheDeleteEdge(id)
	}
	for _, node := range createdNodes {
		e.cacheStoreNode(node)
	}
	for _, node := range updatedNodes {
		e.cacheStoreNode(node)
	}
	for _, edge := range createdEdges {
		e.cacheStoreEdge(edge)
	}
	for _, edge := range updatedEdges {
		e.cacheStoreEdge(edge)
	}
}
