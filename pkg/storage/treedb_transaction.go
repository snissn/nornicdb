package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
)

var treeDBTxIDSeq atomic.Uint64

// TreeDBTransaction implements GraphTransaction with TreeDB conditional writes.
type TreeDBTransaction struct {
	engine   *TreeDBEngine
	tx       *treedb.ConditionalTxn
	snapshot treedb.Snapshot
	id       string

	active bool

	namespace string
	metadata  map[string]interface{}

	deferConstraintValidation bool
	skipCreateExistenceCheck  bool
	implicit                  bool

	opCount int

	pendingNodes map[NodeID]*Node
	pendingEdges map[EdgeID]*Edge
	deletedNodes map[NodeID]struct{}
	deletedEdges map[EdgeID]struct{}

	createdNodeIDs map[NodeID]struct{}
	originalNodes  map[NodeID]*Node

	createdNodes      []*Node
	updatedNodes      []*Node
	deletedNodeIDs    []NodeID
	createdEdges      []*Edge
	updatedEdges      []*Edge
	oldUpdatedEdges   []*Edge
	deletedEdgeIDs    []EdgeID
	deletedEdgeBodies []*Edge

	nodeDelta        int64
	edgeDelta        int64
	nodePrefixDeltas map[string]int64
	edgePrefixDeltas map[string]int64

	guardValue []byte
	held       [][]byte
}

var _ GraphTransaction = (*TreeDBTransaction)(nil)

func newTreeDBTransaction(engine *TreeDBEngine, tx *treedb.ConditionalTxn) *TreeDBTransaction {
	return &TreeDBTransaction{
		engine: engine,
		tx:     tx,
		active: true,
	}
}

func (t *TreeDBTransaction) TransactionID() string {
	if t == nil {
		return ""
	}
	if t.id == "" {
		t.id = "treedb-tx-" + strconv.FormatUint(treeDBTxIDSeq.Add(1), 10)
	}
	return t.id
}
func (t *TreeDBTransaction) IsActive() bool      { return t != nil && t.active }
func (t *TreeDBTransaction) OperationCount() int { return t.opCount }
func (t *TreeDBTransaction) Namespace() string   { return t.namespace }

func (t *TreeDBTransaction) ensureActive() error {
	if t == nil || !t.active || t.tx == nil {
		return ErrTransactionClosed
	}
	if err := t.engine.ensureOpen(); err != nil {
		return err
	}
	return nil
}

func (t *TreeDBTransaction) ensureScanSnapshot() (treedb.Snapshot, error) {
	if err := t.ensureActive(); err != nil {
		return nil, err
	}
	if t.snapshot != nil {
		return t.snapshot, nil
	}
	return nil, fmt.Errorf("%w: TreeDB scan snapshot unavailable", ErrNotImplemented)
}

func (t *TreeDBTransaction) closeScanSnapshot() error {
	if t == nil || t.snapshot == nil {
		return nil
	}
	err := mapTreeDBError(t.snapshot.Close())
	t.snapshot = nil
	return err
}

func (t *TreeDBTransaction) closeTransactionHandles() error {
	if t == nil {
		return nil
	}
	var err error
	if t.tx != nil {
		err = mapTreeDBError(t.tx.Close())
		t.tx = nil
	}
	if closeErr := t.closeScanSnapshot(); err == nil {
		err = closeErr
	}
	return err
}

func (t *TreeDBTransaction) getVersionedAppendForRead(key []byte) ([]byte, error) {
	data, _, err := t.tx.GetVersionedAppend(key, nil)
	return data, mapTreeDBError(err)
}

func (t *TreeDBTransaction) readPredicateGuard(key []byte) error {
	_, _, err := t.tx.GetVersionedAppend(key, nil)
	mapped := mapTreeDBError(err)
	if mapped != nil {
		if errors.Is(mapped, ErrNotFound) {
			return nil
		}
		return mapped
	}
	return nil
}

func (t *TreeDBTransaction) guardBytes() []byte {
	if t.guardValue == nil {
		t.guardValue = make([]byte, 8)
		binary.LittleEndian.PutUint64(t.guardValue, t.engine.guardSeq.Add(1))
	}
	return t.guardValue
}

func (t *TreeDBTransaction) bumpGuardKey(key []byte) error {
	return t.setKey(key, t.guardBytes())
}

func (t *TreeDBTransaction) SetNamespace(ns string) error {
	if err := t.ensureActive(); err != nil {
		return err
	}
	if ns == "" {
		return fmt.Errorf("namespace must be non-empty")
	}
	if t.namespace != "" && t.namespace != ns {
		return fmt.Errorf("%w: pinned to %q, attempted %q", ErrCrossNamespaceTransaction, t.namespace, ns)
	}
	t.namespace = ns
	return nil
}

func (t *TreeDBTransaction) pinNamespaceFromID(id string) error {
	ns, err := treeDBNamespaceFromID(id)
	if err != nil {
		return err
	}
	if t.namespace == "" {
		t.namespace = ns
		return nil
	}
	if t.namespace != ns {
		return fmt.Errorf("%w: pinned to %q, attempted %q", ErrCrossNamespaceTransaction, t.namespace, ns)
	}
	return nil
}

func (t *TreeDBTransaction) pinEdgeNamespace(edge *Edge) error {
	if err := t.pinNamespaceFromID(string(edge.ID)); err != nil {
		return err
	}
	if err := t.pinNamespaceFromID(string(edge.StartNode)); err != nil {
		return err
	}
	return t.pinNamespaceFromID(string(edge.EndNode))
}

func (t *TreeDBTransaction) SetDeferredConstraintValidation(deferValidation bool) error {
	if err := t.ensureActive(); err != nil {
		return err
	}
	t.deferConstraintValidation = deferValidation
	return nil
}

func (t *TreeDBTransaction) SetSkipCreateExistenceCheck(skip bool) error {
	if err := t.ensureActive(); err != nil {
		return err
	}
	t.skipCreateExistenceCheck = skip
	return nil
}

func (t *TreeDBTransaction) SetImplicit(implicit bool) error {
	if err := t.ensureActive(); err != nil {
		return err
	}
	t.implicit = implicit
	return nil
}

func (t *TreeDBTransaction) HasPendingNodeMutations() bool {
	return len(t.pendingNodes) > 0 || len(t.deletedNodes) > 0
}

func (t *TreeDBTransaction) ensurePendingNodes() map[NodeID]*Node {
	if t.pendingNodes == nil {
		t.pendingNodes = make(map[NodeID]*Node, 1)
	}
	return t.pendingNodes
}

func (t *TreeDBTransaction) ensurePendingEdges() map[EdgeID]*Edge {
	if t.pendingEdges == nil {
		t.pendingEdges = make(map[EdgeID]*Edge, 1)
	}
	return t.pendingEdges
}

func (t *TreeDBTransaction) ensureDeletedNodes() map[NodeID]struct{} {
	if t.deletedNodes == nil {
		t.deletedNodes = make(map[NodeID]struct{}, 1)
	}
	return t.deletedNodes
}

func (t *TreeDBTransaction) ensureDeletedEdges() map[EdgeID]struct{} {
	if t.deletedEdges == nil {
		t.deletedEdges = make(map[EdgeID]struct{}, 1)
	}
	return t.deletedEdges
}

func (t *TreeDBTransaction) ensureCreatedNodeIDs() map[NodeID]struct{} {
	if t.createdNodeIDs == nil {
		t.createdNodeIDs = make(map[NodeID]struct{}, 1)
	}
	return t.createdNodeIDs
}

func (t *TreeDBTransaction) ensureOriginalNodes() map[NodeID]*Node {
	if t.originalNodes == nil {
		t.originalNodes = make(map[NodeID]*Node, 1)
	}
	return t.originalNodes
}

func (t *TreeDBTransaction) rememberOriginalNode(node *Node) {
	if node == nil {
		return
	}
	if _, ok := t.originalNodes[node.ID]; ok {
		return
	}
	if _, created := t.createdNodeIDs[node.ID]; created {
		return
	}
	t.ensureOriginalNodes()[node.ID] = copyNode(node)
}

func (t *TreeDBTransaction) SetMetadata(metadata map[string]interface{}) error {
	if err := t.ensureActive(); err != nil {
		return err
	}
	if metadata == nil {
		t.metadata = nil
		return nil
	}
	t.metadata = make(map[string]interface{}, len(metadata))
	for k, v := range metadata {
		t.metadata[k] = v
	}
	return nil
}

func (t *TreeDBTransaction) GetMetadata() map[string]interface{} {
	if t == nil || t.metadata == nil {
		return nil
	}
	out := make(map[string]interface{}, len(t.metadata))
	for k, v := range t.metadata {
		out[k] = v
	}
	return out
}

func (t *TreeDBTransaction) Commit() error {
	if err := t.ensureActive(); err != nil {
		return err
	}
	releaseCommitLocks := t.acquireUniqueConstraintCommitLocks()
	defer releaseCommitLocks()

	if err := t.validatePendingConstraints(); err != nil {
		_ = t.closeTransactionHandles()
		t.active = false
		return fmt.Errorf("constraint violation: %w", err)
	}
	bodyMutation := t.hasBodyMutation()
	edgeMutation := t.hasEdgeBodyMutation()
	if edgeMutation {
		t.engine.adjSeq.Add(1)
	}
	if bodyMutation {
		t.engine.guardSeq.Add(1)
	}
	var err error
	if t.engine.syncWrites {
		err = t.tx.CommitSync()
	} else {
		err = t.tx.Commit()
	}
	if err != nil {
		if edgeMutation {
			t.engine.adjSeq.Add(1)
		}
		_ = t.closeTransactionHandles()
		t.active = false
		return mapTreeDBError(err)
	}
	t.engine.applyCountDeltas(t.nodeDelta, t.edgeDelta, t.nodePrefixDeltas, t.edgePrefixDeltas)
	t.engine.applyBodyCache(
		t.createdNodes,
		t.updatedNodes,
		t.deletedNodeIDs,
		t.createdEdges,
		t.updatedEdges,
		t.oldUpdatedEdges,
		t.deletedEdgeIDs,
		t.deletedEdgeBodies,
	)
	if bodyMutation {
		t.engine.guardSeq.Add(1)
	}
	if edgeMutation {
		t.engine.adjSeq.Add(1)
	}
	t.applySchemaState()
	t.emitEvents()
	if err := t.closeScanSnapshot(); err != nil {
		t.tx = nil
		t.active = false
		return err
	}
	t.tx = nil
	t.active = false
	return nil
}

func (t *TreeDBTransaction) hasBodyMutation() bool {
	return len(t.createdNodes) > 0 ||
		len(t.updatedNodes) > 0 ||
		len(t.deletedNodeIDs) > 0 ||
		len(t.createdEdges) > 0 ||
		len(t.updatedEdges) > 0 ||
		len(t.deletedEdgeIDs) > 0
}

func (t *TreeDBTransaction) hasEdgeBodyMutation() bool {
	return len(t.createdEdges) > 0 ||
		len(t.updatedEdges) > 0 ||
		len(t.deletedEdgeIDs) > 0
}

func (t *TreeDBTransaction) Rollback() error {
	if t == nil || !t.active {
		return nil
	}
	t.active = false
	return t.closeTransactionHandles()
}

func (t *TreeDBTransaction) CreateNode(node *Node) (NodeID, error) {
	if err := t.ensureActive(); err != nil {
		return "", err
	}
	if node == nil {
		return "", ErrInvalidData
	}
	if err := treeDBValidPrefixedID("node", string(node.ID)); err != nil {
		return "", err
	}
	if err := t.pinNamespaceFromID(string(node.ID)); err != nil {
		return "", err
	}
	if existing, ok := t.pendingNodes[node.ID]; ok && existing != nil {
		return "", ErrAlreadyExists
	}
	skipExistenceCheck := t.skipCreateExistenceCheck && shouldSkipCreateExistenceCheck(node.ID)
	if !skipExistenceCheck {
		exists, err := t.tx.Has(nodeKey(node.ID))
		if err != nil {
			return "", mapTreeDBError(err)
		}
		if exists {
			return "", ErrAlreadyExists
		}
	}
	next := copyNode(node)
	now := time.Now()
	if next.CreatedAt.IsZero() {
		next.CreatedAt = now
	}
	if next.UpdatedAt.IsZero() {
		next.UpdatedAt = next.CreatedAt
	}
	if !t.deferConstraintValidation {
		if err := t.validateNodeConstraints(next); err != nil {
			return "", err
		}
	}
	if err := t.stageNodeCreate(next); err != nil {
		return "", err
	}
	t.ensureCreatedNodeIDs()[next.ID] = struct{}{}
	t.opCount++
	t.nodeDelta++
	t.addPrefixDelta(&t.nodePrefixDeltas, string(next.ID), 1)
	t.createdNodes = append(t.createdNodes, next)
	return next.ID, nil
}

func (t *TreeDBTransaction) UpdateNode(node *Node) error {
	if err := t.ensureActive(); err != nil {
		return err
	}
	if node == nil {
		return ErrInvalidData
	}
	if err := treeDBValidPrefixedID("node", string(node.ID)); err != nil {
		return err
	}
	if err := t.pinNamespaceFromID(string(node.ID)); err != nil {
		return err
	}
	oldNode, err := t.GetNode(node.ID)
	if err != nil {
		return err
	}
	t.rememberOriginalNode(oldNode)
	next := copyNode(node)
	if next.CreatedAt.IsZero() {
		next.CreatedAt = oldNode.CreatedAt
	}
	if next.UpdatedAt.IsZero() {
		next.UpdatedAt = time.Now()
	}
	if !t.deferConstraintValidation {
		if err := t.validateNodeConstraints(next); err != nil {
			return err
		}
	}
	if err := t.stageNodeUpdate(oldNode, next); err != nil {
		return err
	}
	t.opCount++
	t.updatedNodes = append(t.updatedNodes, next)
	return nil
}

func (t *TreeDBTransaction) DeleteNode(id NodeID) error {
	if err := t.ensureActive(); err != nil {
		return err
	}
	if err := treeDBValidPrefixedID("node", string(id)); err != nil {
		return err
	}
	if err := t.pinNamespaceFromID(string(id)); err != nil {
		return err
	}
	node, err := t.GetNode(id)
	if err != nil {
		return err
	}
	if err := t.readNodeEdgeGuard(id); err != nil {
		return err
	}
	t.rememberOriginalNode(node)
	edges, err := t.GetOutgoingEdges(id)
	if err != nil {
		return err
	}
	incoming, err := t.GetIncomingEdges(id)
	if err != nil {
		return err
	}
	seen := make(map[EdgeID]struct{}, len(edges)+len(incoming))
	for _, edge := range edges {
		seen[edge.ID] = struct{}{}
		if err := t.deleteEdgeLocked(edge.ID); err != nil {
			return err
		}
	}
	for _, edge := range incoming {
		if _, ok := seen[edge.ID]; ok {
			continue
		}
		if err := t.deleteEdgeLocked(edge.ID); err != nil {
			return err
		}
	}
	for _, label := range node.Labels {
		if err := t.deleteKey(treeDBLabelIndexKey(label, id)); err != nil {
			return err
		}
	}
	if err := t.bumpNodeMembershipGuards(node); err != nil {
		return err
	}
	if err := t.deleteKey(pendingEmbedKey(id)); err != nil {
		return err
	}
	if err := t.deleteKey(nodeKey(id)); err != nil {
		return err
	}
	delete(t.pendingNodes, id)
	t.ensureDeletedNodes()[id] = struct{}{}
	t.opCount++
	t.nodeDelta--
	t.addPrefixDelta(&t.nodePrefixDeltas, string(id), -1)
	t.deletedNodeIDs = append(t.deletedNodeIDs, id)
	return nil
}

func (t *TreeDBTransaction) CreateEdge(edge *Edge) error {
	if err := t.ensureActive(); err != nil {
		return err
	}
	if edge == nil {
		return ErrInvalidData
	}
	if err := t.validateEdgeIDs(edge); err != nil {
		return err
	}
	if err := t.pinEdgeNamespace(edge); err != nil {
		return err
	}
	if existing, ok := t.pendingEdges[edge.ID]; ok && existing != nil {
		return ErrAlreadyExists
	}
	exists, err := t.tx.Has(edgeKey(edge.ID))
	if err != nil {
		return mapTreeDBError(err)
	}
	if exists {
		return ErrAlreadyExists
	}
	if err := t.requireEdgeEndpointExists(edge.StartNode); err != nil {
		return err
	}
	if err := t.requireEdgeEndpointExists(edge.EndNode); err != nil {
		return err
	}
	next := copyEdge(edge)
	now := time.Now()
	if next.CreatedAt.IsZero() {
		next.CreatedAt = now
	}
	if next.UpdatedAt.IsZero() {
		next.UpdatedAt = next.CreatedAt
	}
	if !t.deferConstraintValidation {
		if err := t.validateEdgeConstraints(next); err != nil {
			return err
		}
	}
	if err := t.stageEdgeCreate(next); err != nil {
		return err
	}
	t.opCount++
	t.edgeDelta++
	t.addPrefixDelta(&t.edgePrefixDeltas, string(next.ID), 1)
	t.createdEdges = append(t.createdEdges, next)
	return nil
}

func (t *TreeDBTransaction) UpdateEdge(edge *Edge) error {
	if err := t.ensureActive(); err != nil {
		return err
	}
	if edge == nil {
		return ErrInvalidData
	}
	// Traversal cache hits expose read-only shared edges; evict before validation
	// so caller-mutated IDs or endpoints cannot keep dirty cached bodies visible.
	t.engine.cacheDeleteEdgeCandidate(edge)
	if err := t.validateEdgeIDs(edge); err != nil {
		return err
	}
	if err := t.pinEdgeNamespace(edge); err != nil {
		return err
	}
	oldEdge, err := t.GetEdge(edge.ID)
	if err != nil {
		return err
	}
	if err := t.requireEdgeEndpointExists(edge.StartNode); err != nil {
		return err
	}
	if err := t.requireEdgeEndpointExists(edge.EndNode); err != nil {
		return err
	}
	next := copyEdge(edge)
	if next.CreatedAt.IsZero() {
		next.CreatedAt = oldEdge.CreatedAt
	}
	if next.UpdatedAt.IsZero() {
		next.UpdatedAt = time.Now()
	}
	if !t.deferConstraintValidation {
		if err := t.validateEdgeConstraints(next); err != nil {
			return err
		}
	}
	if err := t.stageEdgeUpdate(oldEdge, next); err != nil {
		return err
	}
	t.opCount++
	t.oldUpdatedEdges = append(t.oldUpdatedEdges, copyEdge(oldEdge))
	t.updatedEdges = append(t.updatedEdges, next)
	return nil
}

func (t *TreeDBTransaction) DeleteEdge(id EdgeID) error {
	if err := t.ensureActive(); err != nil {
		return err
	}
	return t.deleteEdgeLocked(id)
}

func (t *TreeDBTransaction) BulkCreateEdges(edges []*Edge) error {
	if len(edges) == 0 {
		return nil
	}
	if err := t.ensureActive(); err != nil {
		return err
	}
	t.reserveEdgeCreateBatch(edges)
	endpointExists := make(map[NodeID]bool, len(edges)*2)
	seenEdgeIDs := make(map[EdgeID]struct{}, len(edges))
	prepared := make([]treeDBPreparedEdgeCreate, 0, len(edges))
	now := time.Now()
	for _, edge := range edges {
		if edge == nil {
			return ErrInvalidData
		}
		if err := t.validateEdgeIDs(edge); err != nil {
			return err
		}
		if err := t.pinEdgeNamespace(edge); err != nil {
			return err
		}
		if _, seen := seenEdgeIDs[edge.ID]; seen {
			return ErrAlreadyExists
		}
		if existing, ok := t.pendingEdges[edge.ID]; ok && existing != nil {
			return ErrAlreadyExists
		}
		exists, err := t.tx.Has(edgeKey(edge.ID))
		if err != nil {
			return mapTreeDBError(err)
		}
		if exists {
			return ErrAlreadyExists
		}
		if err := t.requireCachedEdgeEndpointExists(edge.StartNode, endpointExists); err != nil {
			return err
		}
		if err := t.requireCachedEdgeEndpointExists(edge.EndNode, endpointExists); err != nil {
			return err
		}
		next := copyEdge(edge)
		if next.CreatedAt.IsZero() {
			next.CreatedAt = now
		}
		if next.UpdatedAt.IsZero() {
			next.UpdatedAt = next.CreatedAt
		}
		if !t.deferConstraintValidation {
			if err := t.validateEdgeConstraints(next); err != nil {
				return err
			}
		}
		data, err := serializeEdge(next)
		if err != nil {
			return err
		}
		seenEdgeIDs[next.ID] = struct{}{}
		prepared = append(prepared, treeDBPreparedEdgeCreate{edge: next, data: data})
	}
	for _, item := range prepared {
		next := item.edge
		if err := t.stagePreparedEdgeCreate(next, item.data); err != nil {
			return err
		}
		t.opCount++
		t.edgeDelta++
		t.addPrefixDelta(&t.edgePrefixDeltas, string(next.ID), 1)
		t.createdEdges = append(t.createdEdges, next)
	}
	return nil
}

type treeDBPreparedEdgeCreate struct {
	edge *Edge
	data []byte
}

func (t *TreeDBTransaction) requireCachedEdgeEndpointExists(id NodeID, endpointExists map[NodeID]bool) error {
	exists, err := t.cachedNodeExistsForEdgeEndpoint(id, endpointExists)
	if err != nil {
		return err
	}
	if !exists {
		return ErrInvalidEdge
	}
	return nil
}

func (t *TreeDBTransaction) cachedNodeExistsForEdgeEndpoint(id NodeID, endpointExists map[NodeID]bool) (bool, error) {
	if exists, ok := endpointExists[id]; ok {
		return exists, nil
	}
	exists, err := t.nodeExistsForEdgeEndpoint(id)
	if err != nil {
		return false, err
	}
	endpointExists[id] = exists
	return exists, nil
}

func (t *TreeDBTransaction) deleteEdgeLocked(id EdgeID) error {
	if err := treeDBValidPrefixedID("edge", string(id)); err != nil {
		return err
	}
	if err := t.pinNamespaceFromID(string(id)); err != nil {
		return err
	}
	edge, err := t.GetEdge(id)
	if err != nil {
		return err
	}
	if err := t.deleteEdgeIndexes(edge); err != nil {
		return err
	}
	if err := t.bumpEdgeMembershipGuards(edge); err != nil {
		return err
	}
	if err := t.deleteKey(edgeKey(id)); err != nil {
		return err
	}
	delete(t.pendingEdges, id)
	t.ensureDeletedEdges()[id] = struct{}{}
	t.opCount++
	t.edgeDelta--
	t.addPrefixDelta(&t.edgePrefixDeltas, string(id), -1)
	t.deletedEdgeBodies = append(t.deletedEdgeBodies, copyEdge(edge))
	t.deletedEdgeIDs = append(t.deletedEdgeIDs, id)
	return nil
}

func (t *TreeDBTransaction) GetNode(id NodeID) (*Node, error) {
	if err := t.ensureActive(); err != nil {
		return nil, err
	}
	if id == "" {
		return nil, ErrInvalidID
	}
	visibility, err := t.nodeReadVisibility(id)
	if err != nil {
		return nil, err
	}
	if visibility.deleted {
		return nil, ErrNotFound
	}
	if visibility.pendingFound {
		return copyNode(visibility.pending), nil
	}
	data, err := t.getVersionedAppendForRead(nodeKey(id))
	if err != nil {
		return nil, err
	}
	node, err := deserializeNode(data)
	if err != nil {
		return nil, err
	}
	return node, nil
}

type treeDBNodeReadVisibility struct {
	pending      *Node
	pendingFound bool
	deleted      bool
}

func (t *TreeDBTransaction) nodeReadVisibility(id NodeID) (treeDBNodeReadVisibility, error) {
	if t.namespace != "" && !t.nodeInPinnedNamespace(id) {
		return treeDBNodeReadVisibility{}, fmt.Errorf("%w: attempted to read node %q from pinned namespace %q", ErrCrossNamespaceTransaction, id, t.namespace)
	}
	if _, deleted := t.deletedNodes[id]; deleted {
		return treeDBNodeReadVisibility{deleted: true}, nil
	}
	if node, ok := t.pendingNodes[id]; ok {
		return treeDBNodeReadVisibility{pending: node, pendingFound: true}, nil
	}
	return treeDBNodeReadVisibility{}, nil
}

func (t *TreeDBTransaction) requireEdgeEndpointExists(id NodeID) error {
	exists, err := t.nodeExistsForEdgeEndpoint(id)
	if err != nil {
		return err
	}
	if !exists {
		return ErrInvalidEdge
	}
	return nil
}

func (t *TreeDBTransaction) nodeExistsForEdgeEndpoint(id NodeID) (bool, error) {
	if id == "" {
		return false, ErrInvalidEdge
	}
	visibility, err := t.nodeReadVisibility(id)
	if err != nil {
		return false, err
	}
	if visibility.deleted {
		return false, nil
	}
	if visibility.pendingFound && visibility.pending != nil {
		return true, nil
	}
	exists, err := t.tx.Has(nodeKey(id))
	return exists, mapTreeDBError(err)
}

func (t *TreeDBTransaction) GetEdge(id EdgeID) (*Edge, error) {
	if err := t.ensureActive(); err != nil {
		return nil, err
	}
	if id == "" {
		return nil, ErrInvalidID
	}
	if t.namespace != "" && !t.edgeInPinnedNamespace(id) {
		return nil, fmt.Errorf("%w: attempted to read edge %q from pinned namespace %q", ErrCrossNamespaceTransaction, id, t.namespace)
	}
	if _, deleted := t.deletedEdges[id]; deleted {
		return nil, ErrNotFound
	}
	if edge, ok := t.pendingEdges[id]; ok {
		return copyEdge(edge), nil
	}
	data, err := t.getVersionedAppendForRead(edgeKey(id))
	if err != nil {
		return nil, err
	}
	edge, err := deserializeEdge(data)
	if err != nil {
		return nil, err
	}
	return edge, nil
}

func (t *TreeDBTransaction) GetNodesByLabel(label string) ([]*Node, error) {
	basePrefix := treeDBLabelIndexPrefix(label)
	prefix := append(append([]byte(nil), basePrefix...), treeDBNamespaceIDPrefix(t.namespace)...)
	ids, err := t.collectNodeIDs(prefix, treeDBNodeNamespaceReadGuardKey(t.namespace), len(basePrefix))
	if err != nil {
		return nil, err
	}
	for id, node := range t.pendingNodes {
		if _, deleted := t.deletedNodes[id]; deleted {
			continue
		}
		if t.nodeInPinnedNamespace(id) && treeDBLabelContains(node.Labels, label) {
			ids[id] = struct{}{}
		}
	}
	nodes := make([]*Node, 0, len(ids))
	for id := range ids {
		node, err := t.GetNode(id)
		if err == ErrNotFound {
			continue
		}
		if err != nil {
			return nil, err
		}
		if treeDBLabelContains(node.Labels, label) {
			nodes = append(nodes, node)
		}
	}
	return nodes, nil
}

func (t *TreeDBTransaction) GetFirstNodeByLabel(label string) (*Node, error) {
	nodes, err := t.GetNodesByLabel(label)
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, ErrNotFound
	}
	return nodes[0], nil
}

func (t *TreeDBTransaction) GetOutgoingEdges(nodeID NodeID) ([]*Edge, error) {
	prefix := treeDBOutgoingIndexPrefix(nodeID)
	return t.collectEdgesByIndexPrefix(prefix, treeDBNodeEdgeGuardKey(nodeID), len(prefix), func(edge *Edge) bool {
		return edge.StartNode == nodeID
	})
}

func (t *TreeDBTransaction) GetIncomingEdges(nodeID NodeID) ([]*Edge, error) {
	prefix := treeDBIncomingIndexPrefix(nodeID)
	return t.collectEdgesByIndexPrefix(prefix, treeDBNodeEdgeGuardKey(nodeID), len(prefix), func(edge *Edge) bool {
		return edge.EndNode == nodeID
	})
}

func (t *TreeDBTransaction) GetEdgesBetween(startID, endID NodeID) ([]*Edge, error) {
	return t.collectEdgesByBetweenPrefix(treeDBEdgeBetweenIndexPrefix(startID, endID), treeDBNodeEdgeGuardKey(startID), func(edge *Edge) bool {
		return edge.StartNode == startID && edge.EndNode == endID
	})
}

func (t *TreeDBTransaction) GetEdgeBetween(startID, endID NodeID, edgeType string) *Edge {
	edges, err := t.collectEdgesByBetweenPrefix(treeDBTypedEdgeBetweenIndexPrefix(startID, endID, edgeType), treeDBNodeEdgeGuardKey(startID), func(edge *Edge) bool {
		return edge.StartNode == startID && edge.EndNode == endID && strings.EqualFold(edge.Type, edgeType)
	})
	if err != nil || len(edges) == 0 {
		return nil
	}
	return edges[0]
}

func (t *TreeDBTransaction) GetEdgesByType(edgeType string) ([]*Edge, error) {
	basePrefix := treeDBEdgeTypeIndexPrefix(edgeType)
	prefix := append(append([]byte(nil), basePrefix...), treeDBNamespaceIDPrefix(t.namespace)...)
	return t.collectEdgesByIndexPrefix(prefix, treeDBEdgeNamespaceReadGuardKey(t.namespace), len(basePrefix), func(edge *Edge) bool {
		return t.edgeInPinnedNamespace(edge.ID) && strings.EqualFold(edge.Type, edgeType)
	})
}

func (t *TreeDBTransaction) AllNodes() ([]*Node, error) {
	if err := t.ensureActive(); err != nil {
		return nil, err
	}
	prefix := treeDBNodePrefixForNamespace(t.namespace)
	ids, err := t.collectNodeIDs(prefix, treeDBNodeNamespaceReadGuardKey(t.namespace), 1)
	if err != nil {
		return nil, err
	}
	out := make([]*Node, 0, len(ids)+len(t.pendingNodes))
	seen := make(map[NodeID]struct{}, len(ids)+len(t.pendingNodes))
	for id := range ids {
		node, err := t.GetNode(id)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		out = append(out, node)
		seen[id] = struct{}{}
	}
	for id, node := range t.pendingNodes {
		if _, ok := seen[id]; ok {
			continue
		}
		if _, deleted := t.deletedNodes[id]; deleted {
			continue
		}
		if !t.nodeInPinnedNamespace(id) {
			continue
		}
		out = append(out, copyNode(node))
	}
	return out, nil
}

func (t *TreeDBTransaction) GetAllNodes() []*Node {
	nodes, _ := t.AllNodes()
	return nodes
}

func (t *TreeDBTransaction) collectNodeIDs(prefix, guardKey []byte, idOffset int) (map[NodeID]struct{}, error) {
	if len(guardKey) == 0 {
		return nil, fmt.Errorf("%w: TreeDB transaction node range scan requires a namespace guard", ErrNotImplemented)
	}
	snap, err := t.ensureScanSnapshot()
	if err != nil {
		return nil, err
	}
	if idOffset <= 0 {
		idOffset = len(prefix)
	}
	if err := t.readPredicateGuard(guardKey); err != nil {
		return nil, err
	}
	ids := make(map[NodeID]struct{})
	err = snap.Iterate(prefix, treeDBRangeEnd(prefix), func(key, _ []byte) error {
		id := NodeID(string(key[idOffset:]))
		if _, deleted := t.deletedNodes[id]; !deleted {
			ids[id] = struct{}{}
		}
		return nil
	})
	return ids, mapTreeDBError(err)
}

func (t *TreeDBTransaction) collectEdgeIDs(prefix, guardKey []byte, idOffset int) (map[EdgeID]struct{}, error) {
	if len(guardKey) == 0 {
		return nil, fmt.Errorf("%w: TreeDB transaction edge range scan requires a namespace guard", ErrNotImplemented)
	}
	snap, err := t.ensureScanSnapshot()
	if err != nil {
		return nil, err
	}
	if idOffset <= 0 {
		idOffset = len(prefix)
	}
	if err := t.readPredicateGuard(guardKey); err != nil {
		return nil, err
	}
	ids := make(map[EdgeID]struct{})
	err = snap.Iterate(prefix, treeDBRangeEnd(prefix), func(key, _ []byte) error {
		id := EdgeID(string(key[idOffset:]))
		if _, deleted := t.deletedEdges[id]; !deleted {
			ids[id] = struct{}{}
		}
		return nil
	})
	return ids, mapTreeDBError(err)
}

func (t *TreeDBTransaction) collectEdgesByIndexPrefix(prefix, guardKey []byte, idOffset int, match func(*Edge) bool) ([]*Edge, error) {
	if len(guardKey) == 0 {
		return nil, fmt.Errorf("%w: TreeDB transaction edge range scan requires a namespace or node guard", ErrNotImplemented)
	}
	snap, err := t.ensureScanSnapshot()
	if err != nil {
		return nil, err
	}
	if idOffset <= 0 {
		idOffset = len(prefix)
	}
	if err := t.readPredicateGuard(guardKey); err != nil {
		return nil, err
	}
	ids := make(map[EdgeID]struct{})
	err = snap.Iterate(prefix, treeDBRangeEnd(prefix), func(key, _ []byte) error {
		id := EdgeID(string(key[idOffset:]))
		if _, deleted := t.deletedEdges[id]; !deleted {
			ids[id] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return nil, mapTreeDBError(err)
	}
	for id, edge := range t.pendingEdges {
		if _, deleted := t.deletedEdges[id]; deleted {
			continue
		}
		if match(edge) {
			ids[id] = struct{}{}
		}
	}
	out := make([]*Edge, 0, len(ids))
	for id := range ids {
		edge, err := t.GetEdge(id)
		if err == ErrNotFound {
			continue
		}
		if err != nil {
			return nil, err
		}
		if match(edge) {
			out = append(out, edge)
		}
	}
	return out, nil
}

func (t *TreeDBTransaction) collectEdgesByBetweenPrefix(prefix, guardKey []byte, match func(*Edge) bool) ([]*Edge, error) {
	if len(guardKey) == 0 {
		return nil, fmt.Errorf("%w: TreeDB transaction edge range scan requires a node guard", ErrNotImplemented)
	}
	snap, err := t.ensureScanSnapshot()
	if err != nil {
		return nil, err
	}
	if err := t.readPredicateGuard(guardKey); err != nil {
		return nil, err
	}
	ids := make(map[EdgeID]struct{})
	err = snap.Iterate(prefix, treeDBRangeEnd(prefix), func(key, _ []byte) error {
		id := treeDBEdgeIDFromBetweenKey(key)
		if id == "" {
			return nil
		}
		if _, deleted := t.deletedEdges[id]; !deleted {
			ids[id] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return nil, mapTreeDBError(err)
	}
	for id, edge := range t.pendingEdges {
		if _, deleted := t.deletedEdges[id]; deleted {
			continue
		}
		if match(edge) {
			ids[id] = struct{}{}
		}
	}
	out := make([]*Edge, 0, len(ids))
	for id := range ids {
		edge, err := t.GetEdge(id)
		if err == ErrNotFound {
			continue
		}
		if err != nil {
			return nil, err
		}
		if match(edge) {
			out = append(out, edge)
		}
	}
	return out, nil
}

func (t *TreeDBTransaction) stageNodeCreate(node *Node) error {
	t.reserveHeld(2 * (2 + len(node.Labels)))
	data, err := serializeNode(node)
	if err != nil {
		return err
	}
	if err := t.setKey(nodeKey(node.ID), data); err != nil {
		return err
	}
	for _, label := range node.Labels {
		if err := t.setKey(treeDBLabelIndexKey(label, node.ID), treeDBEmptyValue); err != nil {
			return err
		}
	}
	if t.engine.shouldIndexPendingEmbed(node) {
		if err := t.setKey(pendingEmbedKey(node.ID), treeDBEmptyValue); err != nil {
			return err
		}
	}
	if err := t.bumpNodeMembershipGuards(node); err != nil {
		return err
	}
	delete(t.deletedNodes, node.ID)
	t.ensurePendingNodes()[node.ID] = node
	return nil
}

func (t *TreeDBTransaction) stageNodeUpdate(oldNode, node *Node) error {
	t.reserveHeld(2 * (3 + len(oldNode.Labels) + len(node.Labels)))
	data, err := serializeNode(node)
	if err != nil {
		return err
	}
	if err := t.setKey(nodeKey(node.ID), data); err != nil {
		return err
	}
	oldLabels := make(map[string]struct{}, len(oldNode.Labels))
	for _, label := range oldNode.Labels {
		oldLabels[normalizeLabel(label)] = struct{}{}
	}
	newLabels := make(map[string]struct{}, len(node.Labels))
	for _, label := range node.Labels {
		normalized := normalizeLabel(label)
		newLabels[normalized] = struct{}{}
		if _, ok := oldLabels[normalized]; !ok {
			if err := t.setKey(treeDBLabelIndexKey(label, node.ID), treeDBEmptyValue); err != nil {
				return err
			}
		}
	}
	for _, label := range oldNode.Labels {
		if _, ok := newLabels[normalizeLabel(label)]; !ok {
			if err := t.deleteKey(treeDBLabelIndexKey(label, node.ID)); err != nil {
				return err
			}
		}
	}
	labelsChanged := false
	for _, label := range node.Labels {
		if _, ok := oldLabels[normalizeLabel(label)]; !ok {
			labelsChanged = true
			break
		}
	}
	if !labelsChanged {
		for _, label := range oldNode.Labels {
			if _, ok := newLabels[normalizeLabel(label)]; !ok {
				labelsChanged = true
				break
			}
		}
	}
	if labelsChanged {
		if err := t.bumpNodeNamespaceGuards(node.ID); err != nil {
			return err
		}
	}
	if t.engine.shouldIndexPendingEmbed(node) {
		if err := t.setKey(pendingEmbedKey(node.ID), treeDBEmptyValue); err != nil {
			return err
		}
	} else if err := t.deleteKey(pendingEmbedKey(node.ID)); err != nil {
		return err
	}
	t.ensurePendingNodes()[node.ID] = node
	return nil
}

func (t *TreeDBTransaction) stageEdgeCreate(edge *Edge) error {
	data, err := serializeEdge(edge)
	if err != nil {
		return err
	}
	return t.stagePreparedEdgeCreate(edge, data)
}

func (t *TreeDBTransaction) stagePreparedEdgeCreate(edge *Edge, data []byte) error {
	t.reserveHeld(18)
	if err := t.setKey(edgeKey(edge.ID), data); err != nil {
		return err
	}
	if err := t.writeEdgeIndexes(edge); err != nil {
		return err
	}
	if err := t.bumpEdgeMembershipGuards(edge); err != nil {
		return err
	}
	delete(t.deletedEdges, edge.ID)
	t.ensurePendingEdges()[edge.ID] = edge
	return nil
}

func (t *TreeDBTransaction) stageEdgeUpdate(oldEdge, edge *Edge) error {
	t.reserveHeld(24)
	data, err := serializeEdge(edge)
	if err != nil {
		return err
	}
	if err := t.setKey(edgeKey(edge.ID), data); err != nil {
		return err
	}
	membershipChanged := oldEdge.StartNode != edge.StartNode || oldEdge.EndNode != edge.EndNode || !strings.EqualFold(oldEdge.Type, edge.Type)
	if membershipChanged {
		if err := t.deleteEdgeIndexes(oldEdge); err != nil {
			return err
		}
		if err := t.writeEdgeIndexes(edge); err != nil {
			return err
		}
		if err := t.bumpEdgeMembershipGuards(oldEdge); err != nil {
			return err
		}
		if err := t.bumpEdgeMembershipGuards(edge); err != nil {
			return err
		}
	}
	t.ensurePendingEdges()[edge.ID] = edge
	return nil
}

func (t *TreeDBTransaction) writeEdgeIndexes(edge *Edge) error {
	if err := t.setKey(treeDBOutgoingIndexKey(edge.StartNode, edge.ID), treeDBEmptyValue); err != nil {
		return err
	}
	if err := t.setKey(treeDBIncomingIndexKey(edge.EndNode, edge.ID), treeDBEmptyValue); err != nil {
		return err
	}
	if err := t.setKey(treeDBEdgeTypeIndexKey(edge.Type, edge.ID), treeDBEmptyValue); err != nil {
		return err
	}
	if err := t.setKey(treeDBEdgeBetweenIndexKey(edge.StartNode, edge.EndNode, edge.Type, edge.ID), treeDBEmptyValue); err != nil {
		return err
	}
	return t.setKey(treeDBEdgeBetweenHeadKey(edge.StartNode, edge.EndNode, edge.Type), []byte(edge.ID))
}

func (t *TreeDBTransaction) deleteEdgeIndexes(edge *Edge) error {
	if err := t.deleteKey(treeDBOutgoingIndexKey(edge.StartNode, edge.ID)); err != nil {
		return err
	}
	if err := t.deleteKey(treeDBIncomingIndexKey(edge.EndNode, edge.ID)); err != nil {
		return err
	}
	if err := t.deleteKey(treeDBEdgeTypeIndexKey(edge.Type, edge.ID)); err != nil {
		return err
	}
	if err := t.deleteKey(treeDBEdgeBetweenIndexKey(edge.StartNode, edge.EndNode, edge.Type, edge.ID)); err != nil {
		return err
	}
	headKey := treeDBEdgeBetweenHeadKey(edge.StartNode, edge.EndNode, edge.Type)
	data, _, err := t.tx.GetVersionedAppend(headKey, nil)
	if err == nil && EdgeID(string(data)) == edge.ID {
		replacement, err := t.replacementEdgeBetweenHead(edge)
		if err != nil {
			return err
		}
		if replacement == "" {
			return t.deleteKey(headKey)
		}
		return t.setKey(headKey, []byte(replacement))
	}
	if err != nil && mapTreeDBError(err) != ErrNotFound {
		return mapTreeDBError(err)
	}
	return nil
}

func (t *TreeDBTransaction) replacementEdgeBetweenHead(deleted *Edge) (EdgeID, error) {
	if deleted == nil {
		return "", nil
	}
	prefix := treeDBTypedEdgeBetweenIndexPrefix(deleted.StartNode, deleted.EndNode, deleted.Type)
	if err := t.readPredicateGuard(treeDBNodeEdgeGuardKey(deleted.StartNode)); err != nil {
		return "", err
	}
	snap, err := t.ensureScanSnapshot()
	if err != nil {
		return "", err
	}
	var replacement EdgeID
	err = snap.Iterate(prefix, treeDBRangeEnd(prefix), func(key, _ []byte) error {
		id := treeDBEdgeIDFromBetweenKey(key)
		if id == "" || id == deleted.ID {
			return nil
		}
		if _, removed := t.deletedEdges[id]; removed {
			return nil
		}
		if pending, ok := t.pendingEdges[id]; ok && !treeDBEdgeMatchesBetween(pending, deleted.StartNode, deleted.EndNode, deleted.Type) {
			return nil
		}
		replacement = id
		return nil
	})
	if err != nil {
		return "", mapTreeDBError(err)
	}
	if replacement != "" {
		return replacement, nil
	}
	for id, pending := range t.pendingEdges {
		if id == "" || id == deleted.ID {
			continue
		}
		if _, removed := t.deletedEdges[id]; removed {
			continue
		}
		if treeDBEdgeMatchesBetween(pending, deleted.StartNode, deleted.EndNode, deleted.Type) {
			return id, nil
		}
	}
	return "", nil
}

func (t *TreeDBTransaction) readNodeEdgeGuard(nodeID NodeID) error {
	_, err := t.tx.Has(treeDBNodeEdgeGuardKey(nodeID))
	return mapTreeDBError(err)
}

func (t *TreeDBTransaction) bumpNodeEdgeGuard(nodeID NodeID) error {
	return t.bumpGuardKey(treeDBNodeEdgeGuardKey(nodeID))
}

func (t *TreeDBTransaction) bumpNodeMembershipGuards(node *Node) error {
	if node == nil {
		return nil
	}
	return t.bumpNodeNamespaceGuards(node.ID)
}

func (t *TreeDBTransaction) bumpNodeNamespaceGuards(id NodeID) error {
	ns, err := treeDBNamespaceFromID(string(id))
	if err != nil {
		return err
	}
	return t.bumpGuardKey(treeDBNodeNamespaceGuardKey(ns))
}

func (t *TreeDBTransaction) bumpEdgeMembershipGuards(edge *Edge) error {
	if edge == nil {
		return nil
	}
	ns, err := treeDBNamespaceFromID(string(edge.ID))
	if err != nil {
		return err
	}
	if err := t.bumpNodeEdgeGuard(edge.StartNode); err != nil {
		return err
	}
	if edge.EndNode != edge.StartNode {
		if err := t.bumpNodeEdgeGuard(edge.EndNode); err != nil {
			return err
		}
	}
	return t.bumpGuardKey(treeDBEdgeNamespaceGuardKey(ns))
}

func (t *TreeDBTransaction) nodeInPinnedNamespace(id NodeID) bool {
	return t.namespace == "" || strings.HasPrefix(string(id), t.namespace+":")
}

func (t *TreeDBTransaction) edgeInPinnedNamespace(id EdgeID) bool {
	return t.namespace == "" || strings.HasPrefix(string(id), t.namespace+":")
}

func (t *TreeDBTransaction) reserveHeld(extra int) {
	if extra <= 0 {
		return
	}
	needed := len(t.held) + extra
	if cap(t.held) >= needed {
		return
	}
	t.growHeld(needed)
}

func (t *TreeDBTransaction) reserveCreatedEdges(extra int) {
	if extra <= 0 {
		return
	}
	needed := len(t.createdEdges) + extra
	if cap(t.createdEdges) >= needed {
		return
	}
	next := make([]*Edge, len(t.createdEdges), needed)
	copy(next, t.createdEdges)
	t.createdEdges = next
}

func (t *TreeDBTransaction) growHeld(needed int) {
	nextCap := cap(t.held)
	if nextCap == 0 {
		nextCap = needed
	}
	for nextCap < needed {
		if nextCap < 1024 {
			nextCap *= 2
		} else {
			nextCap += nextCap / 2
		}
	}
	next := make([][]byte, len(t.held), nextCap)
	copy(next, t.held)
	t.held = next
}

func (t *TreeDBTransaction) reserveNodeCreateBatch(nodes []*Node) {
	if len(nodes) == 0 {
		return
	}
	writes := 0
	held := 0
	for _, node := range nodes {
		if node == nil {
			continue
		}
		nodeWrites := treeDBNodeRecordWriteCount + len(node.Labels)*treeDBNodeLabelIndexWriteCount + treeDBNodeNamespaceGuardWriteCount
		if t.engine.shouldIndexPendingEmbed(node) {
			nodeWrites += treeDBNodePendingEmbedWriteCount
		}
		writes += nodeWrites
		held += nodeWrites * 2
	}
	if writes > 0 {
		t.tx.ReserveWrites(writes)
		t.reserveHeld(held)
	}
}

func (t *TreeDBTransaction) reserveEdgeCreateBatch(edges []*Edge) {
	if len(edges) == 0 {
		return
	}
	edgeCount := 0
	writes := 0
	for _, edge := range edges {
		if edge == nil {
			continue
		}
		edgeCount++
		writes += treeDBEdgeCreateWriteCount(edge)
	}
	if edgeCount > 0 {
		if t.pendingEdges == nil {
			t.pendingEdges = make(map[EdgeID]*Edge, edgeCount)
		}
		if t.edgePrefixDeltas == nil {
			t.edgePrefixDeltas = make(map[string]int64, 1)
		}
		t.reserveCreatedEdges(edgeCount)
	}
	if writes > 0 {
		t.tx.ReserveWrites(writes)
		t.reserveHeld(writes * 2)
	}
}

const (
	treeDBNodeRecordWriteCount         = 1 // node body record.
	treeDBNodeLabelIndexWriteCount     = 1 // one label index entry per label.
	treeDBNodePendingEmbedWriteCount   = 1 // pending-embedding guard when embeddings are enabled.
	treeDBNodeNamespaceGuardWriteCount = 1 // namespace membership guard.

	treeDBEdgeRecordWriteCount         = 1 // edge body record.
	treeDBEdgeIndexWriteCount          = 5 // outgoing, incoming, type, between, and between-head indexes.
	treeDBEdgeBaseMembershipGuardCount = 2 // start-node edge guard plus edge namespace guard.
	treeDBEdgeEndMembershipGuardCount  = 1 // end-node edge guard for non-self edges.
)

func treeDBEdgeCreateWriteCount(edge *Edge) int {
	if edge == nil {
		return 0
	}
	writes := treeDBEdgeRecordWriteCount + treeDBEdgeIndexWriteCount + treeDBEdgeBaseMembershipGuardCount
	if edge.StartNode != edge.EndNode {
		writes += treeDBEdgeEndMembershipGuardCount
	}
	return writes
}

func (t *TreeDBTransaction) setKey(key, value []byte) error {
	t.held = append(t.held, key, value)
	return mapTreeDBError(t.tx.SetView(key, value))
}

func (t *TreeDBTransaction) deleteKey(key []byte) error {
	t.held = append(t.held, key)
	return mapTreeDBError(t.tx.DeleteView(key))
}

func (t *TreeDBTransaction) validateEdgeIDs(edge *Edge) error {
	if edge == nil {
		return ErrInvalidData
	}
	if err := treeDBValidPrefixedID("edge", string(edge.ID)); err != nil {
		return err
	}
	if edge.StartNode == "" || edge.EndNode == "" {
		return ErrInvalidEdge
	}
	if err := treeDBValidPrefixedID("node", string(edge.StartNode)); err != nil {
		return err
	}
	if err := treeDBValidPrefixedID("node", string(edge.EndNode)); err != nil {
		return err
	}
	return nil
}

func (t *TreeDBTransaction) validateNodeConstraints(node *Node) error {
	ns, err := treeDBNamespaceFromID(string(node.ID))
	if err != nil {
		return err
	}
	schema := t.engine.lookupSchemaForNamespace(ns)
	if schema == nil {
		return nil
	}
	if !treeDBSchemaHasValidationState(schema) {
		return nil
	}
	for _, constraint := range schema.GetConstraintsForLabels(node.Labels) {
		if constraint.EffectiveEntityType() != ConstraintEntityNode {
			continue
		}
		switch constraint.Type {
		case ConstraintUnique:
			if err := t.validateNodeUniqueConstraint(node, schema, constraint, ns); err != nil {
				return err
			}
		case ConstraintPropertyType:
		case ConstraintExists, ConstraintNodeKey, ConstraintTemporal, ConstraintDomain, ConstraintCardinality, ConstraintPolicy, ConstraintRelationshipKey:
			return fmt.Errorf("%w: treedb does not yet enforce %s node constraints", ErrNotImplemented, constraint.Type)
		default:
			return fmt.Errorf("%w: treedb does not yet enforce %s node constraints", ErrNotImplemented, constraint.Type)
		}
	}
	for _, ptc := range schema.GetPropertyTypeConstraintsForLabels(node.Labels) {
		if ptc.EntityType != "" && ptc.EntityType != ConstraintEntityNode {
			continue
		}
		value, ok := node.Properties[ptc.Property]
		if !ok || value == nil {
			continue
		}
		if err := ValidatePropertyType(value, ptc.ExpectedType); err != nil {
			return fmt.Errorf("property type constraint %s violated: %w", ptc.Name, err)
		}
	}
	return nil
}

func (t *TreeDBTransaction) validateNodeUniqueConstraint(node *Node, schema *SchemaManager, constraint Constraint, namespace string) error {
	if node == nil || schema == nil || len(constraint.Properties) != 1 {
		return nil
	}
	prop := constraint.Properties[0]
	value, ok := node.Properties[prop]
	if !ok || value == nil {
		return nil
	}
	for id, pending := range t.pendingNodes {
		if id == node.ID {
			continue
		}
		if _, deleted := t.deletedNodes[id]; deleted {
			continue
		}
		pendingValue, ok := pending.Properties[prop]
		if !ok || pendingValue == nil {
			continue
		}
		if treeDBLabelContains(pending.Labels, constraint.Label) && compareValues(pendingValue, value) {
			return &ConstraintViolationError{
				Type:       ConstraintUnique,
				Label:      constraint.Label,
				Properties: []string{prop},
				Message:    fmt.Sprintf("Node with %s=%v already exists in transaction", prop, value),
			}
		}
	}

	existingNode, found, cacheComplete, constrained := schema.lookupUniqueConstraintValueForValidation(constraint.Label, prop, value)
	if constrained {
		if found && existingNode != node.ID {
			if _, deleted := t.deletedNodes[existingNode]; !deleted {
				return uniqueConstraintViolation(constraint.Label, prop, value, existingNode)
			}
		}
		if cacheComplete {
			return nil
		}
	}
	return t.scanForUniqueViolation(namespace, constraint.Label, prop, value, node.ID)
}

func (t *TreeDBTransaction) scanForUniqueViolation(namespace, label, property string, value interface{}, excludeNodeID NodeID) error {
	if hook := getUniqueConstraintScanHook(); hook != nil {
		hook()
	}
	nodes, err := t.nodesByLabelForConstraintScan(label)
	if err != nil {
		return err
	}
	nsPrefix := namespace + ":"
	for _, node := range nodes {
		if node == nil || node.ID == "" || node.ID == excludeNodeID {
			continue
		}
		if _, deleted := t.deletedNodes[node.ID]; deleted {
			continue
		}
		if namespace != "" && !strings.HasPrefix(string(node.ID), nsPrefix) {
			continue
		}
		if !treeDBLabelContains(node.Labels, label) {
			continue
		}
		existingValue, ok := node.Properties[property]
		if ok && compareValues(existingValue, value) {
			return uniqueConstraintViolation(label, property, value, node.ID)
		}
	}
	return nil
}

func (t *TreeDBTransaction) nodesByLabelForConstraintScan(label string) ([]*Node, error) {
	if t.snapshot != nil {
		return t.GetNodesByLabel(label)
	}
	return t.engine.GetNodesByLabel(label)
}

func (t *TreeDBTransaction) validateEdgeConstraints(edge *Edge) error {
	if edge == nil || edge.Type == "" || t.namespace == "" {
		return nil
	}
	schema := t.engine.lookupSchemaForNamespace(t.namespace)
	if schema == nil {
		return nil
	}
	if !treeDBSchemaHasValidationState(schema) {
		return nil
	}
	for _, constraint := range schema.GetConstraintsForLabels([]string{edge.Type}) {
		if constraint.EffectiveEntityType() != ConstraintEntityRelationship {
			continue
		}
		return fmt.Errorf("%w: treedb does not yet enforce %s relationship constraints", ErrNotImplemented, constraint.Type)
	}
	for _, ptc := range schema.GetPropertyTypeConstraintsForLabels([]string{edge.Type}) {
		if ptc.EffectiveEntityType() != ConstraintEntityRelationship {
			continue
		}
		return fmt.Errorf("%w: treedb does not yet enforce relationship property type constraints", ErrNotImplemented)
	}
	return nil
}

func (t *TreeDBTransaction) validatePendingConstraints() error {
	if t.namespace != "" {
		schema := t.engine.lookupSchemaForNamespace(t.namespace)
		if schema == nil || !treeDBSchemaHasValidationState(schema) {
			return nil
		}
	}
	for _, node := range t.pendingNodes {
		if _, deleted := t.deletedNodes[node.ID]; deleted {
			continue
		}
		if err := t.validateNodeConstraints(node); err != nil {
			return err
		}
	}
	for _, edge := range t.pendingEdges {
		if _, deleted := t.deletedEdges[edge.ID]; deleted {
			continue
		}
		if err := t.validateEdgeConstraints(edge); err != nil {
			return err
		}
	}
	return nil
}

func treeDBSchemaHasValidationState(schema *SchemaManager) bool {
	if schema == nil {
		return false
	}
	schema.mu.RLock()
	hasState := len(schema.constraints) > 0 || len(schema.propertyTypeConstraints) > 0
	schema.mu.RUnlock()
	return hasState
}

func treeDBSchemaHasMaintenanceState(schema *SchemaManager) bool {
	if schema == nil {
		return false
	}
	schema.mu.RLock()
	hasState := len(schema.uniqueConstraints) > 0 || len(schema.propertyIndexes) > 0 || len(schema.compositeIndexes) > 0
	schema.mu.RUnlock()
	return hasState
}

func (t *TreeDBTransaction) acquireUniqueConstraintCommitLocks() func() {
	if len(t.pendingNodes) == 0 || t.namespace == "" {
		return func() {}
	}
	schema := t.engine.lookupSchemaForNamespace(t.namespace)
	if schema == nil {
		return func() {}
	}
	seen := make(map[uniqueConstraintLockKey]struct{}, len(t.pendingNodes))
	for _, node := range t.pendingNodes {
		if node == nil {
			continue
		}
		for _, constraint := range schema.GetConstraintsForLabels(node.Labels) {
			if constraint.Type != ConstraintUnique || constraint.EffectiveEntityType() != ConstraintEntityNode || len(constraint.Properties) != 1 {
				continue
			}
			prop := constraint.Properties[0]
			rawValue, has := node.Properties[prop]
			if !has {
				continue
			}
			canonicalValue, ok := uniqueConstraintValueKey(rawValue)
			if !ok {
				continue
			}
			seen[uniqueConstraintLockKey{
				label:    constraint.Label,
				property: prop,
				value:    canonicalValue,
			}] = struct{}{}
		}
	}
	if len(seen) == 0 {
		return func() {}
	}
	keys := make([]uniqueConstraintLockKey, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	return schema.acquireUniqueConstraintCommitLocks(keys)
}

func (t *TreeDBTransaction) applySchemaState() {
	if len(t.pendingNodes) == 0 && len(t.originalNodes) == 0 && len(t.deletedNodes) == 0 {
		return
	}
	if !treeDBSchemaHasMaintenanceState(t.engine.lookupSchemaForNamespace(t.namespace)) {
		return
	}

	affected := make(map[NodeID]struct{}, len(t.originalNodes)+len(t.pendingNodes)+len(t.deletedNodes))
	for id := range t.originalNodes {
		affected[id] = struct{}{}
	}
	for id := range t.pendingNodes {
		affected[id] = struct{}{}
	}
	for id := range t.deletedNodes {
		affected[id] = struct{}{}
	}

	for id := range affected {
		if node := t.originalNodes[id]; node != nil {
			t.unregisterNodeSchemaValues(node)
		}
	}
	for id := range affected {
		if _, deleted := t.deletedNodes[id]; deleted {
			continue
		}
		node := t.pendingNodes[id]
		if node == nil {
			continue
		}
		t.registerNodeSchemaValues(node)
	}
}

func (t *TreeDBTransaction) unregisterNodeSchemaValues(node *Node) {
	if node == nil {
		return
	}
	ns, err := treeDBNamespaceFromID(string(node.ID))
	if err != nil {
		return
	}
	schema := t.engine.lookupSchemaForNamespace(ns)
	if schema == nil {
		return
	}
	for _, label := range node.Labels {
		for propName, propValue := range node.Properties {
			schema.UnregisterUniqueValue(label, propName, propValue)
			_ = schema.PropertyIndexDelete(label, propName, node.ID, propValue)
		}
	}
	for _, label := range node.Labels {
		for _, idx := range schema.GetCompositeIndexesForLabel(label) {
			if idx != nil {
				idx.RemoveNode(node.ID, node.Properties)
			}
		}
	}
}

func (t *TreeDBTransaction) registerNodeSchemaValues(node *Node) {
	if node == nil {
		return
	}
	ns, err := treeDBNamespaceFromID(string(node.ID))
	if err != nil {
		return
	}
	schema := t.engine.lookupSchemaForNamespace(ns)
	if schema == nil {
		return
	}
	for _, label := range node.Labels {
		for propName, propValue := range node.Properties {
			schema.RegisterUniqueValue(label, propName, propValue, node.ID)
			_ = schema.PropertyIndexInsert(label, propName, node.ID, propValue)
		}
	}
	for _, label := range node.Labels {
		for _, idx := range schema.GetCompositeIndexesForLabel(label) {
			if idx != nil {
				_ = idx.IndexNode(node.ID, node.Properties)
			}
		}
	}
}

func (t *TreeDBTransaction) emitEvents() {
	for _, node := range t.createdNodes {
		t.engine.notifyNodeCreated(node)
	}
	for _, node := range t.updatedNodes {
		t.engine.notifyNodeUpdated(node)
	}
	for _, id := range t.deletedNodeIDs {
		t.engine.notifyNodeDeleted(id)
	}
	for _, edge := range t.createdEdges {
		t.engine.notifyEdgeCreated(edge)
	}
	for _, edge := range t.updatedEdges {
		t.engine.notifyEdgeUpdated(edge)
	}
	for _, id := range t.deletedEdgeIDs {
		t.engine.notifyEdgeDeleted(id)
	}
}

func (t *TreeDBTransaction) addPrefixDelta(dst *map[string]int64, id string, delta int64) {
	if prefix, ok := namespacePrefixFromID(id); ok {
		if *dst == nil {
			*dst = make(map[string]int64, 1)
		}
		(*dst)[prefix] += delta
	}
}
