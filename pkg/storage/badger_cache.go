package storage

// =============================================================================
// BADGER ENGINE CACHE INVARIANTS + INVALIDATION
// =============================================================================
//
// This module centralizes cache writes/invalidations for BadgerEngine.
//
// Invariants:
//   - Node cache stores deep copies (see copyNode) and GetNode returns a deep
//     copy, so callers cannot mutate cached state.
//   - Edge type cache is an acceleration structure for GetEdgesByType and must
//     be invalidated whenever edges of a type are created/deleted OR when an
//     edge changes its Type.
//   - Cached node/edge counts are maintained on successful mutations to keep
//     Stats() O(1).
//
// Entry points:
//   - cacheOnNodeCreated / cacheOnNodeUpdated / cacheOnNodeDeleted
//   - cacheOnEdgeCreated / cacheOnEdgeUpdated / cacheOnEdgeDeleted
//
// These functions should be the only places that mutate/invalidate the caches
// in response to successful storage mutations.

func (b *BadgerEngine) cacheStoreNode(node *Node) {
	if node == nil {
		return
	}

	b.nodeCacheMu.Lock()
	// Simple eviction: if cache is too large, clear it.
	// Keeps behavior consistent with existing code paths.
	if b.nodeCacheMaxEntries > 0 && len(b.nodeCache) > b.nodeCacheMaxEntries {
		b.nodeCache = make(map[NodeID]*Node, b.nodeCacheMaxEntries)
	}
	b.nodeCache[node.ID] = copyNode(node)
	b.nodeCacheMu.Unlock()
}

func (b *BadgerEngine) labelCacheGetFirst(label string) (NodeID, bool) {
	if label == "" {
		return "", false
	}
	normalized := normalizeLabel(label)
	b.labelFirstNodeCacheMu.RLock()
	id, ok := b.labelFirstNodeCache[normalized]
	b.labelFirstNodeCacheMu.RUnlock()
	return id, ok
}

func (b *BadgerEngine) labelCacheSetFirst(label string, id NodeID) {
	if label == "" || id == "" {
		return
	}
	normalized := normalizeLabel(label)
	b.labelFirstNodeCacheMu.Lock()
	if b.labelFirstCacheMax > 0 && len(b.labelFirstNodeCache) > b.labelFirstCacheMax {
		b.labelFirstNodeCache = make(map[string]NodeID, b.labelFirstCacheMax)
	}
	b.labelFirstNodeCache[normalized] = id
	b.labelFirstNodeCacheMu.Unlock()
}

func (b *BadgerEngine) labelCacheInvalidateForNodeLabels(labels []string, nodeID NodeID) {
	if len(labels) == 0 || nodeID == "" {
		return
	}
	b.labelFirstNodeCacheMu.Lock()
	for _, label := range labels {
		normalized := normalizeLabel(label)
		if cached, ok := b.labelFirstNodeCache[normalized]; ok && cached == nodeID {
			delete(b.labelFirstNodeCache, normalized)
		}
	}
	b.labelFirstNodeCacheMu.Unlock()
}

func (b *BadgerEngine) labelCacheInvalidateForRemovedLabels(oldLabels, newLabels []string, nodeID NodeID) {
	if len(oldLabels) == 0 || nodeID == "" {
		return
	}
	if len(newLabels) == 0 {
		b.labelCacheInvalidateForNodeLabels(oldLabels, nodeID)
		return
	}

	newSet := make(map[string]struct{}, len(newLabels))
	for _, label := range newLabels {
		newSet[normalizeLabel(label)] = struct{}{}
	}

	b.labelFirstNodeCacheMu.Lock()
	for _, label := range oldLabels {
		normalized := normalizeLabel(label)
		if _, ok := newSet[normalized]; ok {
			continue
		}
		if cached, ok := b.labelFirstNodeCache[normalized]; ok && cached == nodeID {
			delete(b.labelFirstNodeCache, normalized)
		}
	}
	b.labelFirstNodeCacheMu.Unlock()
}

func (b *BadgerEngine) cacheDeleteNode(id NodeID) {
	if id == "" {
		return
	}
	b.nodeCacheMu.Lock()
	delete(b.nodeCache, id)
	b.nodeCacheMu.Unlock()
}

func (b *BadgerEngine) cacheOnNodeCreated(node *Node) {
	b.cacheStoreNode(node)
	b.nodeCount.Add(1)
	b.addNamespaceNodeCount(node.ID, 1)
	b.maintainPropertyIndexesOnNodeCreated(node)
}

func (b *BadgerEngine) cacheOnNodeUpdated(node *Node) {
	b.cacheStoreNode(node)
	// Updates are handled by cacheOnNodeUpdatedWithOldNode which has the
	// old-property context needed to remove stale index entries. The
	// `oldNode == nil` path is legitimately "we don't know the diff" —
	// fall back to treating it as a create so the new values at least
	// land in the indexes (no stale-removal possible without oldNode).
	b.maintainPropertyIndexesOnNodeCreated(node)
}

func (b *BadgerEngine) cacheOnNodeUpdatedWithOldNode(node *Node, oldNode *Node) {
	b.cacheStoreNode(node)
	if oldNode != nil {
		b.labelCacheInvalidateForRemovedLabels(oldNode.Labels, node.Labels, node.ID)
	}
	b.maintainPropertyIndexesOnNodeUpdated(node, oldNode)
}

func (b *BadgerEngine) cacheOnNodesCreated(nodes []*Node) {
	if len(nodes) == 0 {
		return
	}

	var created int64
	for _, node := range nodes {
		if node == nil {
			continue
		}
		b.cacheStoreNode(node)
		created++
	}

	if created > 0 {
		b.nodeCount.Add(created)
	}

	for _, node := range nodes {
		if node == nil {
			continue
		}
		b.addNamespaceNodeCount(node.ID, 1)
		b.maintainPropertyIndexesOnNodeCreated(node)
	}
}

// cacheOnNodeDeleted invalidates node cache and updates cached counts.
// edgesDeleted is the number of edges removed as part of deleting this node.
func (b *BadgerEngine) cacheOnNodeDeleted(id NodeID, edgesDeleted int64) {
	b.cacheDeleteNode(id)

	// Decrement cached node count for O(1) stats.
	b.nodeCount.Add(-1)
	b.addNamespaceNodeCount(id, -1)

	// Decrement cached edge count for edges deleted with this node.
	if edgesDeleted > 0 {
		b.edgeCount.Add(-edgesDeleted)
		b.addNamespaceEdgeCountFromNode(id, -edgesDeleted)
		// We don't know which types were removed cheaply; invalidate whole type cache.
		b.InvalidateEdgeTypeCache()
	}
}

func (b *BadgerEngine) cacheOnNodeDeletedWithLabels(nodeID NodeID, labels []string, edgesDeleted int64) {
	b.cacheDeleteNode(nodeID)
	b.labelCacheInvalidateForNodeLabels(labels, nodeID)

	// Decrement cached node count for O(1) stats.
	b.nodeCount.Add(-1)
	b.addNamespaceNodeCount(nodeID, -1)

	// Decrement cached edge count for edges deleted with this node.
	if edgesDeleted > 0 {
		b.edgeCount.Add(-edgesDeleted)
		b.addNamespaceEdgeCountFromNode(nodeID, -edgesDeleted)
		// We don't know which types were removed cheaply; invalidate whole type cache.
		b.InvalidateEdgeTypeCache()
	}

	// Remove stale property-index entries. Without labels + properties of
	// the deleted node we can't do targeted removal, so the narrower
	// cacheOnNodeDeleted (without labels) leaves the index potentially
	// dangling — callers MUST use this variant when property indexes exist.
	b.maintainPropertyIndexesOnNodeDeletedWithLabels(nodeID, labels)
}

func (b *BadgerEngine) cacheOnEdgeCreated(edge *Edge) {
	if edge == nil {
		return
	}
	b.InvalidateEdgeTypeCacheForType(edge.Type)
	b.edgeCount.Add(1)
	b.addNamespaceEdgeCount(edge.ID, 1)
}

// cacheOnEdgeUpdated invalidates relevant edge type cache entries when an edge changes.
func (b *BadgerEngine) cacheOnEdgeUpdated(oldType string, newEdge *Edge) {
	if newEdge == nil {
		return
	}
	// If type changed, invalidate both old and new (old cache would still contain this edge).
	if oldType != "" && oldType != newEdge.Type {
		b.InvalidateEdgeTypeCacheForType(oldType)
	}
	b.InvalidateEdgeTypeCacheForType(newEdge.Type)
}

func (b *BadgerEngine) cacheOnEdgeDeleted(id EdgeID, edgeType string) {
	if edgeType != "" {
		b.InvalidateEdgeTypeCacheForType(edgeType)
	} else {
		b.InvalidateEdgeTypeCache()
	}
	b.edgeCount.Add(-1)
	b.addNamespaceEdgeCount(id, -1)
}

func (b *BadgerEngine) cacheOnEdgesCreated(edges []*Edge) {
	if len(edges) == 0 {
		return
	}
	// Bulk inserts can include many types; invalidate once.
	b.InvalidateEdgeTypeCache()
	b.edgeCount.Add(int64(len(edges)))

	for _, edge := range edges {
		if edge == nil {
			continue
		}
		b.addNamespaceEdgeCount(edge.ID, 1)
	}
}

func (b *BadgerEngine) cacheOnEdgesDeleted(deletedIDs []EdgeID) {
	if len(deletedIDs) == 0 {
		return
	}
	// Bulk delete may cover many types; invalidate once.
	b.InvalidateEdgeTypeCache()
	b.edgeCount.Add(-int64(len(deletedIDs)))

	// Batch namespace updates under a single lock.
	deltas := make(map[string]int64)
	for _, id := range deletedIDs {
		prefix, ok := namespacePrefixFromID(string(id))
		if !ok {
			continue
		}
		deltas[prefix]--
	}
	if len(deltas) == 0 {
		return
	}
	b.namespaceCountsMu.Lock()
	for prefix, delta := range deltas {
		b.namespaceEdgeCounts[prefix] += delta
	}
	b.namespaceCountsMu.Unlock()
}

func (b *BadgerEngine) cacheOnNodesDeleted(deletedNodeIDs []NodeID, deletedNodeCount, totalEdgesDeleted int64) {
	if deletedNodeCount <= 0 {
		return
	}

	for _, nodeID := range deletedNodeIDs {
		b.cacheDeleteNode(nodeID)
	}
	b.nodeCount.Add(-deletedNodeCount)

	// Update per-namespace node counts.
	namespaces := make(map[string]int64)
	for _, nodeID := range deletedNodeIDs {
		prefix, ok := namespacePrefixFromID(string(nodeID))
		if !ok {
			continue
		}
		namespaces[prefix]--
	}
	if len(namespaces) > 0 {
		b.namespaceCountsMu.Lock()
		for prefix, delta := range namespaces {
			b.namespaceNodeCounts[prefix] += delta
		}
		b.namespaceCountsMu.Unlock()
	}

	if totalEdgesDeleted > 0 {
		b.edgeCount.Add(-totalEdgesDeleted)

		// We only have an aggregate edge delete count. If the deleted nodes span
		// multiple namespaces, we can't attribute edges precisely.
		if len(namespaces) == 1 {
			for prefix := range namespaces {
				b.namespaceCountsMu.Lock()
				b.namespaceEdgeCounts[prefix] -= totalEdgesDeleted
				b.namespaceCountsMu.Unlock()
				break
			}
		}

		b.InvalidateEdgeTypeCache()
	}
}

func (b *BadgerEngine) cacheOnNodesDeletedWithLabels(deletedNodes []*Node, deletedNodeCount, totalEdgesDeleted int64) {
	if deletedNodeCount <= 0 {
		return
	}

	for _, node := range deletedNodes {
		if node == nil {
			continue
		}
		b.cacheDeleteNode(node.ID)
		b.labelCacheInvalidateForNodeLabels(node.Labels, node.ID)
	}
	b.nodeCount.Add(-deletedNodeCount)

	// Update per-namespace node counts.
	namespaces := make(map[string]int64)
	for _, node := range deletedNodes {
		if node == nil {
			continue
		}
		prefix, ok := namespacePrefixFromID(string(node.ID))
		if !ok {
			continue
		}
		namespaces[prefix]--
	}
	if len(namespaces) > 0 {
		b.namespaceCountsMu.Lock()
		for prefix, delta := range namespaces {
			b.namespaceNodeCounts[prefix] += delta
		}
		b.namespaceCountsMu.Unlock()
	}

	if totalEdgesDeleted > 0 {
		b.edgeCount.Add(-totalEdgesDeleted)

		// We only have an aggregate edge delete count. If the deleted nodes span
		// multiple namespaces, we can't attribute edges precisely.
		if len(namespaces) == 1 {
			for prefix := range namespaces {
				b.namespaceCountsMu.Lock()
				b.namespaceEdgeCounts[prefix] -= totalEdgesDeleted
				b.namespaceCountsMu.Unlock()
				break
			}
		}

		b.InvalidateEdgeTypeCache()
	}
}

func (b *BadgerEngine) addNamespaceNodeCount(id NodeID, delta int64) {
	prefix, ok := namespacePrefixFromID(string(id))
	if !ok {
		return
	}
	b.namespaceCountsMu.Lock()
	b.namespaceNodeCounts[prefix] += delta
	b.namespaceCountsMu.Unlock()
}

func (b *BadgerEngine) addNamespaceEdgeCount(id EdgeID, delta int64) {
	prefix, ok := namespacePrefixFromID(string(id))
	if !ok {
		return
	}
	b.namespaceCountsMu.Lock()
	b.namespaceEdgeCounts[prefix] += delta
	b.namespaceCountsMu.Unlock()
}

func (b *BadgerEngine) addNamespaceEdgeCountFromNode(nodeID NodeID, delta int64) {
	prefix, ok := namespacePrefixFromID(string(nodeID))
	if !ok {
		return
	}
	b.namespaceCountsMu.Lock()
	b.namespaceEdgeCounts[prefix] += delta
	b.namespaceCountsMu.Unlock()
}
