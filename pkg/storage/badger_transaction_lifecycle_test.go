package storage

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type txLifecycleControllerStub struct {
	mu             sync.Mutex
	enabled        bool
	err            error
	gracefulExpire bool
	hardExpire     bool
	acquireCount   int
	releaseCount   int
	registerCount  int
	evaluateCount  int
	pruneCount     int
	pauseCount     int
	resumeCount    int
	registeredInfo []SnapshotReaderInfo
}

func (s *txLifecycleControllerStub) RegisterSnapshotReader(info SnapshotReaderInfo) func() {
	s.mu.Lock()
	s.registerCount++
	s.registeredInfo = append(s.registeredInfo, info)
	s.mu.Unlock()
	return func() {}
}

func (s *txLifecycleControllerStub) LifecycleStatus() map[string]interface{} {
	return map[string]interface{}{"enabled": s.enabled}
}

func (s *txLifecycleControllerStub) TriggerPruneNow(ctx context.Context) error {
	_ = ctx
	s.mu.Lock()
	s.pruneCount++
	s.mu.Unlock()
	return nil
}

func (s *txLifecycleControllerStub) PauseLifecycle() {
	s.mu.Lock()
	s.pauseCount++
	s.mu.Unlock()
}

func (s *txLifecycleControllerStub) ResumeLifecycle() {
	s.mu.Lock()
	s.resumeCount++
	s.mu.Unlock()
}

func (s *txLifecycleControllerStub) AcquireSnapshotReader(info SnapshotReaderInfo) (func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	s.acquireCount++
	s.registeredInfo = append(s.registeredInfo, info)
	return func() {
		s.mu.Lock()
		s.releaseCount++
		s.mu.Unlock()
	}, nil
}

func (s *txLifecycleControllerStub) EvaluateSnapshotReader(info SnapshotReaderInfo) (bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.evaluateCount++
	s.registeredInfo = append(s.registeredInfo, info)
	return s.gracefulExpire, s.hardExpire
}

func (s *txLifecycleControllerStub) RunPruneNow(ctx context.Context, opts MVCCPruneOptions) (int64, error) {
	_ = ctx
	_ = opts
	return 0, nil
}

func (s *txLifecycleControllerStub) StartLifecycle(ctx context.Context) {
	_ = ctx
}

func (s *txLifecycleControllerStub) StopLifecycle() {}

func (s *txLifecycleControllerStub) IsLifecycleEnabled() bool {
	return s.enabled
}

func (s *txLifecycleControllerStub) IsLifecycleRunning() bool {
	return false
}

func (s *txLifecycleControllerStub) ReaderRegistry() SnapshotReaderRegistry {
	return nil
}

func TestBeginTransaction_WithLifecycleAdmissionDoesNotDeadlock(t *testing.T) {
	// Reader admission is registered the first time the transaction's
	// namespace is pinned (on first prefixed write or SetNamespace),
	// not at BeginTransaction itself, because the namespace — and the
	// per-namespace MVCC counter to register against — is unknown at
	// begin time.
	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	controller := &txLifecycleControllerStub{enabled: true}
	engine.SetLifecycleController(controller)

	done := make(chan struct{})
	var tx *BadgerTransaction
	var err error
	go func() {
		tx, err = engine.BeginTransaction()
		if err == nil && tx != nil {
			err = tx.SetNamespace("test")
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("BeginTransaction blocked with lifecycle admission enabled")
	}

	require.NoError(t, err)
	require.NotNil(t, tx)
	require.Equal(t, 1, controller.acquireCount)
	require.Len(t, controller.registeredInfo, 1)
	require.False(t, controller.registeredInfo[0].SnapshotVersion.IsZero())
	require.NoError(t, tx.Rollback())
	require.Equal(t, 1, controller.releaseCount)
}

func TestBeginTransaction_LifecycleAdmissionFailureReturnsError(t *testing.T) {
	// Admission failure now surfaces on the first pin attempt, since
	// admission cannot run until a namespace is bound.
	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	controller := &txLifecycleControllerStub{enabled: true, err: ErrMVCCResourcePressure}
	engine.SetLifecycleController(controller)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NotNil(t, tx)
	pinErr := tx.SetNamespace("test")
	require.ErrorIs(t, pinErr, ErrMVCCResourcePressure)
	require.Equal(t, 0, controller.releaseCount)
	require.NoError(t, tx.Rollback())
}

func TestTransaction_GracefulSnapshotExpirationCancelsWorkAndReleasesReader(t *testing.T) {
	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	controller := &txLifecycleControllerStub{enabled: true}
	engine.SetLifecycleController(controller)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NotNil(t, tx)
	// Pin the transaction so the snapshot reader is registered with the
	// lifecycle controller; expiration paths only fire for registered readers.
	require.NoError(t, tx.SetNamespace("nornic"))

	controller.mu.Lock()
	controller.gracefulExpire = true
	controller.mu.Unlock()

	_, err = tx.GetNode(NodeID("nornic:missing"))
	require.ErrorIs(t, err, ErrMVCCSnapshotGracefulCancel)
	require.Equal(t, TxStatusRolledBack, tx.Status)
	require.Equal(t, 1, controller.releaseCount)
	require.Equal(t, 1, controller.evaluateCount)
}

func TestTransaction_HardSnapshotExpirationFailsCommitAndReleasesReader(t *testing.T) {
	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	controller := &txLifecycleControllerStub{enabled: true}
	engine.SetLifecycleController(controller)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NotNil(t, tx)
	require.NoError(t, tx.SetNamespace("nornic"))

	controller.mu.Lock()
	controller.hardExpire = true
	controller.mu.Unlock()

	err = tx.Commit()
	require.ErrorIs(t, err, ErrMVCCSnapshotHardExpired)
	require.Equal(t, TxStatusRolledBack, tx.Status)
	require.Equal(t, 1, controller.releaseCount)
	require.Equal(t, 1, controller.evaluateCount)
}

func TestLifecycleWrappers_DelegateLifecycleControls(t *testing.T) {
	base := NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	controller := &txLifecycleControllerStub{enabled: true}
	base.SetLifecycleController(controller)

	async := NewAsyncEngine(base, &AsyncEngineConfig{FlushInterval: time.Hour})
	t.Cleanup(func() { _ = async.Close() })
	wal := NewWALEngine(base, nil)
	namespaced := NewNamespacedEngine(base, "tenant_a")

	async.RegisterSnapshotReader(SnapshotReaderInfo{ReaderID: "async", Namespace: "override"})
	wal.RegisterSnapshotReader(SnapshotReaderInfo{ReaderID: "wal"})
	namespaced.RegisterSnapshotReader(SnapshotReaderInfo{ReaderID: "ns"})

	require.Equal(t, true, async.LifecycleStatus()["enabled"])
	require.Equal(t, true, wal.LifecycleStatus()["enabled"])
	require.Equal(t, "tenant_a", namespaced.LifecycleStatus()["namespace"])

	require.NoError(t, async.TriggerPruneNow(context.Background()))
	require.NoError(t, wal.TriggerPruneNow(context.Background()))
	require.NoError(t, namespaced.TriggerPruneNow(context.Background()))

	async.PauseLifecycle()
	wal.PauseLifecycle()
	namespaced.PauseLifecycle()
	async.ResumeLifecycle()
	wal.ResumeLifecycle()
	namespaced.ResumeLifecycle()

	controller.mu.Lock()
	defer controller.mu.Unlock()
	require.Equal(t, 3, controller.registerCount)
	require.Len(t, controller.registeredInfo, 3)
	require.Equal(t, "override", controller.registeredInfo[0].Namespace)
	require.Equal(t, "", controller.registeredInfo[1].Namespace)
	require.Equal(t, "tenant_a", controller.registeredInfo[2].Namespace)
	require.Equal(t, 3, controller.pruneCount)
	require.Equal(t, 3, controller.pauseCount)
	require.Equal(t, 3, controller.resumeCount)
}
