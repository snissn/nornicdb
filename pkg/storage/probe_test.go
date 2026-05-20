package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestProbe_Isolation(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	tx1, _ := engine.BeginTransaction()
	tx2, _ := engine.BeginTransaction()

	t.Logf("tx1.StartTime=%v tx2.StartTime=%v", tx1.StartTime.UTC().UnixNano(), tx2.StartTime.UTC().UnixNano())
	t.Logf("tx1.beginSnapshot=%v", tx1.beginSnapshot)
	t.Logf("tx2.beginSnapshot=%v", tx2.beginSnapshot)

	node := &Node{ID: NodeID(prefixTestID("isolated-node")), Labels: []string{"Isolated"}}
	require.NoError(t, tx1.CreateNodeAndReturn(node))

	t.Logf("after tx1.CreateNode, tx2.beginSnapshot=%v tx2.readTS=%v", tx2.beginSnapshot, tx2.readTS)

	_, err := tx2.GetNode(NodeID(prefixTestID("isolated-node")))
	t.Logf("tx2.GetNode err=%v tx2.readTS=%v", err, tx2.readTS)
	t.Logf("tx2 namespace=%q", tx2.namespace)

	require.NoError(t, tx1.Commit())
	t.Logf("after tx1.Commit, namespaceMVCC[test].seq=%d", engine.namespaceMVCCSeqLoadForTest("test"))
	head, _ := engine.GetNodeCurrentHead(NodeID(prefixTestID("isolated-node")))
	t.Logf("head version ts=%d seq=%d", head.Version.CommitTimestamp.UTC().UnixNano(), head.Version.CommitSequence)
	t.Logf("head FloorVersion ts=%d seq=%d", head.FloorVersion.CommitTimestamp.UTC().UnixNano(), head.FloorVersion.CommitSequence)

	_, err = tx2.GetNode(NodeID(prefixTestID("isolated-node")))
	t.Logf("post-commit tx2.GetNode err=%v (readTS ts=%d seq=%d)", err, tx2.readTS.CommitTimestamp.UTC().UnixNano(), tx2.readTS.CommitSequence)
	require.NoError(t, tx2.Rollback())

	_ = time.Now
}

func (tx *BadgerTransaction) CreateNodeAndReturn(node *Node) error {
	_, err := tx.CreateNode(node)
	return err
}

func (b *BadgerEngine) namespaceMVCCSeqLoadForTest(ns string) uint64 {
	state, err := b.namespaceMVCC(ns)
	if err != nil {
		return 0
	}
	return state.seq.Load()
}
