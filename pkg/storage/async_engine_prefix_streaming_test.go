package storage

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type asyncPrefixStreamingInner struct {
	*MemoryEngine
	prefixCalls int
	lastPrefix  string
}

type asyncStreamingOnlyInner struct {
	Engine
	streamCalls int
}

type asyncAllNodesErrorInner struct {
	Engine
	allNodesErr error
}

func (e *asyncPrefixStreamingInner) StreamNodes(ctx context.Context, fn func(node *Node) error) error {
	nodes, err := e.AllNodes()
	if err != nil {
		return err
	}
	for _, node := range nodes {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := fn(node); err != nil {
			return err
		}
	}
	return nil
}

func (e *asyncStreamingOnlyInner) StreamNodes(ctx context.Context, fn func(node *Node) error) error {
	e.streamCalls++
	nodes, err := e.AllNodes()
	if err != nil {
		return err
	}
	for _, node := range nodes {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := fn(node); err != nil {
			return err
		}
	}
	return nil
}

func (e *asyncStreamingOnlyInner) StreamEdges(ctx context.Context, fn func(edge *Edge) error) error {
	edges, err := e.AllEdges()
	if err != nil {
		return err
	}
	for _, edge := range edges {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := fn(edge); err != nil {
			return err
		}
	}
	return nil
}

func (e *asyncStreamingOnlyInner) StreamNodeChunks(ctx context.Context, chunkSize int, fn func(nodes []*Node) error) error {
	nodes, err := e.AllNodes()
	if err != nil {
		return err
	}
	if chunkSize <= 0 {
		chunkSize = 1
	}
	for i := 0; i < len(nodes); i += chunkSize {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		end := i + chunkSize
		if end > len(nodes) {
			end = len(nodes)
		}
		if err := fn(nodes[i:end]); err != nil {
			return err
		}
	}
	return nil
}

func (e *asyncAllNodesErrorInner) AllNodes() ([]*Node, error) {
	if e.allNodesErr != nil {
		return nil, e.allNodesErr
	}
	return e.Engine.AllNodes()
}

func (e *asyncPrefixStreamingInner) StreamEdges(ctx context.Context, fn func(edge *Edge) error) error {
	edges, err := e.AllEdges()
	if err != nil {
		return err
	}
	for _, edge := range edges {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := fn(edge); err != nil {
			return err
		}
	}
	return nil
}

func (e *asyncPrefixStreamingInner) StreamNodeChunks(ctx context.Context, chunkSize int, fn func(nodes []*Node) error) error {
	nodes, err := e.AllNodes()
	if err != nil {
		return err
	}
	if chunkSize <= 0 {
		chunkSize = 1
	}
	for i := 0; i < len(nodes); i += chunkSize {
		end := i + chunkSize
		if end > len(nodes) {
			end = len(nodes)
		}
		if err := fn(nodes[i:end]); err != nil {
			return err
		}
	}
	return nil
}

func (e *asyncPrefixStreamingInner) StreamNodesByPrefix(ctx context.Context, prefix string, fn func(node *Node) error) error {
	e.prefixCalls++
	e.lastPrefix = prefix
	nodes, err := e.AllNodes()
	if err != nil {
		return err
	}
	for _, node := range nodes {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if prefix != "" && !strings.HasPrefix(string(node.ID), prefix) {
			continue
		}
		if err := fn(node); err != nil {
			return err
		}
	}
	return nil
}

func TestAsyncEngine_StreamNodesByPrefix_DelegatesAndMergesCache(t *testing.T) {
	inner := &asyncPrefixStreamingInner{MemoryEngine: NewMemoryEngine()}
	defer inner.Close()

	_, err := inner.CreateNode(&Node{ID: NodeID("tenant_a:base1"), Labels: []string{"L"}})
	require.NoError(t, err)
	_, err = inner.CreateNode(&Node{ID: NodeID("tenant_b:base2"), Labels: []string{"L"}})
	require.NoError(t, err)

	ae := NewAsyncEngine(inner, nil)
	defer ae.Close()

	_, err = ae.CreateNode(&Node{ID: NodeID("tenant_a:cached1"), Labels: []string{"L"}})
	require.NoError(t, err)
	_, err = ae.CreateNode(&Node{ID: NodeID("tenant_b:cached2"), Labels: []string{"L"}})
	require.NoError(t, err)

	seen := make(map[NodeID]bool)
	err = ae.StreamNodesByPrefix(context.Background(), "tenant_a:", func(node *Node) error {
		seen[node.ID] = true
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, inner.prefixCalls)
	require.Equal(t, "tenant_a:", inner.lastPrefix)
	require.True(t, seen[NodeID("tenant_a:base1")])
	require.True(t, seen[NodeID("tenant_a:cached1")])
	require.False(t, seen[NodeID("tenant_b:base2")])
	require.False(t, seen[NodeID("tenant_b:cached2")])
}

func TestAsyncEngine_StreamNodesByPrefix_AllNodesFallbackBranches(t *testing.T) {
	base := NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	_, err := base.CreateNode(&Node{ID: "tenant_a:base", Labels: []string{"L"}})
	require.NoError(t, err)
	_, err = base.CreateNode(&Node{ID: "tenant_b:base", Labels: []string{"L"}})
	require.NoError(t, err)

	ae := NewAsyncEngine(base, &AsyncEngineConfig{FlushInterval: time.Hour, MinFlushInterval: time.Hour, MaxFlushInterval: time.Hour})
	t.Cleanup(func() { _ = ae.Close() })
	_, err = ae.CreateNode(&Node{ID: "tenant_a:cached", Labels: []string{"L"}})
	require.NoError(t, err)
	require.NoError(t, ae.DeleteNode("tenant_a:base"))

	var seen []NodeID
	err = ae.StreamNodesByPrefix(context.Background(), "tenant_a:", func(node *Node) error {
		seen = append(seen, node.ID)
		return ErrIterationStopped
	})
	require.NoError(t, err)
	require.Equal(t, []NodeID{"tenant_a:cached"}, seen)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = ae.StreamNodesByPrefix(ctx, "tenant_a:", func(node *Node) error { return nil })
	require.ErrorIs(t, err, context.Canceled)
}

func TestAsyncEngine_StreamNodesByPrefix_AdditionalFallbackErrors(t *testing.T) {
	t.Run("cached callback error is returned", func(t *testing.T) {
		base := NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })
		ae := NewAsyncEngine(base, &AsyncEngineConfig{FlushInterval: time.Hour, MinFlushInterval: time.Hour, MaxFlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae.Close() })
		_, err := ae.CreateNode(&Node{ID: "tenant_a:cached", Labels: []string{"L"}})
		require.NoError(t, err)

		wantErr := errors.New("visitor failed")
		err = ae.StreamNodesByPrefix(context.Background(), "tenant_a:", func(node *Node) error {
			return wantErr
		})
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("streaming fallback filters and propagates visitor errors", func(t *testing.T) {
		base := NewMemoryEngine()
		inner := &asyncStreamingOnlyInner{Engine: base}
		t.Cleanup(func() { _ = inner.Close() })
		_, err := inner.CreateNode(&Node{ID: "tenant_a:base", Labels: []string{"L"}})
		require.NoError(t, err)
		_, err = inner.CreateNode(&Node{ID: "tenant_b:base", Labels: []string{"L"}})
		require.NoError(t, err)

		ae := NewAsyncEngine(inner, &AsyncEngineConfig{FlushInterval: time.Hour, MinFlushInterval: time.Hour, MaxFlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae.Close() })
		_, err = ae.CreateNode(&Node{ID: "tenant_a:cached", Labels: []string{"L"}})
		require.NoError(t, err)
		require.NoError(t, ae.DeleteNode("tenant_a:base"))

		var seen []NodeID
		err = ae.StreamNodesByPrefix(context.Background(), "tenant_a:", func(node *Node) error {
			seen = append(seen, node.ID)
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 1, inner.streamCalls)
		require.Equal(t, []NodeID{"tenant_a:cached"}, seen)

		wantErr := errors.New("stream visitor failed")
		err = ae.StreamNodesByPrefix(context.Background(), "tenant_b:", func(node *Node) error {
			return wantErr
		})
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("all nodes fallback handles errors and iteration stop", func(t *testing.T) {
		base := &asyncAllNodesErrorInner{Engine: NewMemoryEngine(), allNodesErr: errors.New("all nodes failed")}
		t.Cleanup(func() { _ = base.Close() })
		ae := NewAsyncEngine(base, &AsyncEngineConfig{FlushInterval: time.Hour, MinFlushInterval: time.Hour, MaxFlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae.Close() })

		err := ae.StreamNodesByPrefix(context.Background(), "tenant_a:", func(node *Node) error { return nil })
		require.ErrorIs(t, err, base.allNodesErr)

		base.allNodesErr = nil
		_, err = base.CreateNode(&Node{ID: "tenant_a:base", Labels: []string{"L"}})
		require.NoError(t, err)
		_, err = base.CreateNode(&Node{ID: "tenant_b:base", Labels: []string{"L"}})
		require.NoError(t, err)

		var seen []NodeID
		err = ae.StreamNodesByPrefix(context.Background(), "tenant_a:", func(node *Node) error {
			seen = append(seen, node.ID)
			return ErrIterationStopped
		})
		require.NoError(t, err)
		require.Equal(t, []NodeID{"tenant_a:base"}, seen)

		wantErr := errors.New("all nodes visitor failed")
		err = ae.StreamNodesByPrefix(context.Background(), "tenant_a:", func(node *Node) error { return wantErr })
		require.ErrorIs(t, err, wantErr)
	})
}
