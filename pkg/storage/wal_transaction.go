package storage

import (
	"fmt"
	"sync"

	"github.com/orneryd/nornicdb/pkg/config"
)

type walGraphTransaction struct {
	walEngine *WALEngine
	tx        GraphTransaction

	mu      sync.Mutex
	entries []walGraphTransactionEntry
}

type walGraphTransactionEntry struct {
	op       OperationType
	data     interface{}
	database string
}

func (t *walGraphTransaction) TransactionID() string { return t.tx.TransactionID() }
func (t *walGraphTransaction) IsActive() bool        { return t.tx.IsActive() }
func (t *walGraphTransaction) OperationCount() int   { return t.tx.OperationCount() }
func (t *walGraphTransaction) Namespace() string     { return t.tx.Namespace() }

func (t *walGraphTransaction) SetNamespace(ns string) error {
	return t.tx.SetNamespace(ns)
}

func (t *walGraphTransaction) SetDeferredConstraintValidation(deferValidation bool) error {
	return t.tx.SetDeferredConstraintValidation(deferValidation)
}

func (t *walGraphTransaction) SetSkipCreateExistenceCheck(skip bool) error {
	return t.tx.SetSkipCreateExistenceCheck(skip)
}

func (t *walGraphTransaction) SetImplicit(implicit bool) error {
	return t.tx.SetImplicit(implicit)
}

func (t *walGraphTransaction) HasPendingNodeMutations() bool {
	return t.tx.HasPendingNodeMutations()
}

func (t *walGraphTransaction) SetMetadata(metadata map[string]interface{}) error {
	return t.tx.SetMetadata(metadata)
}

func (t *walGraphTransaction) GetMetadata() map[string]interface{} {
	return t.tx.GetMetadata()
}

func (t *walGraphTransaction) CreateNode(node *Node) (NodeID, error) {
	id, err := t.tx.CreateNode(node)
	if err != nil {
		return "", err
	}
	recordNode := copyNode(node)
	if recordNode != nil {
		recordNode.ID = id
	}
	dbName := t.walEngine.databaseFromNode(recordNode)
	t.record(OpCreateNode, WALNodeData{Node: cloneNodeForWAL(dbName, recordNode), TxID: t.TransactionID()}, dbName)
	return id, nil
}

func (t *walGraphTransaction) UpdateNode(node *Node) error {
	oldNode, oldErr := t.tx.GetNode(node.ID)
	if err := t.tx.UpdateNode(node); err != nil {
		return err
	}
	dbName := t.walEngine.databaseFromNode(node)
	data := WALNodeData{Node: cloneNodeForWAL(dbName, node), TxID: t.TransactionID()}
	if oldErr == nil {
		data.OldNode = cloneNodeForWAL(dbName, oldNode)
	}
	t.record(OpUpdateNode, data, dbName)
	return nil
}

func (t *walGraphTransaction) DeleteNode(id NodeID) error {
	oldNode, oldErr := t.tx.GetNode(id)
	if err := t.tx.DeleteNode(id); err != nil {
		return err
	}
	dbName, unprefixedID := t.walEngine.databaseFromNodeID(id)
	data := WALDeleteData{ID: unprefixedID, TxID: t.TransactionID()}
	if oldErr == nil {
		data.OldNode = cloneNodeForWAL(dbName, oldNode)
	}
	t.record(OpDeleteNode, data, dbName)
	return nil
}

func (t *walGraphTransaction) CreateEdge(edge *Edge) error {
	dbName, err := t.walEngine.databaseFromEdge(edge)
	if err != nil {
		return err
	}
	if err := t.tx.CreateEdge(edge); err != nil {
		return err
	}
	t.record(OpCreateEdge, WALEdgeData{Edge: cloneEdgeForWAL(dbName, edge), TxID: t.TransactionID()}, dbName)
	return nil
}

func (t *walGraphTransaction) UpdateEdge(edge *Edge) error {
	dbName, err := t.walEngine.databaseFromEdge(edge)
	if err != nil {
		return err
	}
	oldEdge, oldErr := t.tx.GetEdge(edge.ID)
	if err := t.tx.UpdateEdge(edge); err != nil {
		return err
	}
	data := WALEdgeData{Edge: cloneEdgeForWAL(dbName, edge), TxID: t.TransactionID()}
	if oldErr == nil {
		data.OldEdge = cloneEdgeForWAL(dbName, oldEdge)
	}
	t.record(OpUpdateEdge, data, dbName)
	return nil
}

func (t *walGraphTransaction) DeleteEdge(id EdgeID) error {
	oldEdge, oldErr := t.tx.GetEdge(id)
	if err := t.tx.DeleteEdge(id); err != nil {
		return err
	}
	dbName, unprefixedID := t.walEngine.databaseFromEdgeID(id)
	data := WALDeleteData{ID: unprefixedID, TxID: t.TransactionID()}
	if oldErr == nil {
		data.OldEdge = cloneEdgeForWAL(dbName, oldEdge)
	}
	t.record(OpDeleteEdge, data, dbName)
	return nil
}

func (t *walGraphTransaction) BulkCreateEdges(edges []*Edge) error {
	dbName := t.walEngine.getDatabaseName()
	cloned := make([]*Edge, 0, len(edges))
	for _, edge := range edges {
		if edge == nil {
			cloned = append(cloned, nil)
			continue
		}
		edgeDB, err := t.walEngine.databaseFromEdge(edge)
		if err != nil {
			return err
		}
		if dbName == "" || dbName == "nornic" {
			dbName = edgeDB
		} else if edgeDB != dbName {
			return fmt.Errorf("wal: bulk transaction edges contain multiple databases: %q vs %q", dbName, edgeDB)
		}
		cloned = append(cloned, cloneEdgeForWAL(dbName, edge))
	}
	if err := t.tx.BulkCreateEdges(edges); err != nil {
		return err
	}
	t.record(OpBulkEdges, WALBulkEdgesData{Edges: cloned, TxID: t.TransactionID()}, dbName)
	return nil
}

func (t *walGraphTransaction) GetNode(id NodeID) (*Node, error) {
	return t.tx.GetNode(id)
}

func (t *walGraphTransaction) GetEdge(id EdgeID) (*Edge, error) {
	return t.tx.GetEdge(id)
}

func (t *walGraphTransaction) GetNodesByLabel(label string) ([]*Node, error) {
	return t.tx.GetNodesByLabel(label)
}

func (t *walGraphTransaction) GetFirstNodeByLabel(label string) (*Node, error) {
	return t.tx.GetFirstNodeByLabel(label)
}

func (t *walGraphTransaction) GetOutgoingEdges(nodeID NodeID) ([]*Edge, error) {
	return t.tx.GetOutgoingEdges(nodeID)
}

func (t *walGraphTransaction) GetIncomingEdges(nodeID NodeID) ([]*Edge, error) {
	return t.tx.GetIncomingEdges(nodeID)
}

func (t *walGraphTransaction) GetEdgesBetween(startID, endID NodeID) ([]*Edge, error) {
	return t.tx.GetEdgesBetween(startID, endID)
}

func (t *walGraphTransaction) GetEdgeBetween(startID, endID NodeID, edgeType string) *Edge {
	return t.tx.GetEdgeBetween(startID, endID, edgeType)
}

func (t *walGraphTransaction) GetEdgesByType(edgeType string) ([]*Edge, error) {
	return t.tx.GetEdgesByType(edgeType)
}

func (t *walGraphTransaction) AllNodes() ([]*Node, error) {
	return t.tx.AllNodes()
}

func (t *walGraphTransaction) GetAllNodes() []*Node {
	return t.tx.GetAllNodes()
}

func (t *walGraphTransaction) Commit() error {
	entries := t.snapshotEntries()
	if len(entries) == 0 || !t.walEnabled() {
		return t.tx.Commit()
	}

	dbName, err := t.databaseForEntries(entries)
	if err != nil {
		_ = t.tx.Rollback()
		return err
	}

	t.walEngine.mutationMu.RLock()
	defer t.walEngine.mutationMu.RUnlock()

	if _, err := t.walEngine.wal.AppendTxBegin(dbName, t.TransactionID(), nil); err != nil {
		_ = t.tx.Rollback()
		return fmt.Errorf("wal: failed to log transaction begin: %w", err)
	}
	for _, entry := range entries {
		if err := t.walEngine.wal.AppendWithDatabase(entry.op, entry.data, entry.database); err != nil {
			_ = t.tx.Rollback()
			return fmt.Errorf("wal: failed to log transaction %s: %w", entry.op, err)
		}
	}
	if err := t.tx.Commit(); err != nil {
		_, _ = t.walEngine.wal.AppendTxAbort(dbName, t.TransactionID(), err.Error())
		return err
	}
	if _, err := t.walEngine.wal.AppendTxCommit(dbName, t.TransactionID(), len(entries)); err != nil {
		return fmt.Errorf("wal: failed to log transaction commit: %w", err)
	}
	return nil
}

func (t *walGraphTransaction) Rollback() error {
	return t.tx.Rollback()
}

func (t *walGraphTransaction) record(op OperationType, data interface{}, database string) {
	if !t.walEnabled() {
		return
	}
	t.mu.Lock()
	t.entries = append(t.entries, walGraphTransactionEntry{op: op, data: data, database: database})
	t.mu.Unlock()
}

func (t *walGraphTransaction) snapshotEntries() []walGraphTransactionEntry {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.entries) == 0 {
		return nil
	}
	entries := make([]walGraphTransactionEntry, len(t.entries))
	copy(entries, t.entries)
	return entries
}

func (t *walGraphTransaction) walEnabled() bool {
	return t.walEngine != nil && t.walEngine.wal != nil && config.IsWALEnabled()
}

func (t *walGraphTransaction) databaseForEntries(entries []walGraphTransactionEntry) (string, error) {
	dbName := ""
	if namespace := t.Namespace(); namespace != "" {
		dbName = namespace
	}
	for _, entry := range entries {
		if entry.database == "" {
			continue
		}
		if dbName == "" || dbName == "nornic" {
			dbName = entry.database
			continue
		}
		if entry.database != dbName {
			return "", fmt.Errorf("wal: transaction spans multiple databases: %q vs %q", dbName, entry.database)
		}
	}
	if dbName == "" {
		dbName = t.walEngine.getDatabaseName()
	}
	return dbName, nil
}

var _ GraphTransaction = (*walGraphTransaction)(nil)
