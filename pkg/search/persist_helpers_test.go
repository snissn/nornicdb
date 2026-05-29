package search

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteMsgpackSnapshots(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "bundle")
	err := writeMsgpackSnapshots(target, map[string]any{
		"a.msgpack": map[string]any{"a": 1},
		"b.msgpack": map[string]any{"b": 2},
	})
	require.NoError(t, err)

	_, err = os.Stat(filepath.Join(target, "a.msgpack"))
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(target, "b.msgpack"))
	require.NoError(t, err)
}

func TestWriteMsgpackSnapshotsAtomic(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "bundle")

	require.NoError(t, writeMsgpackSnapshotsAtomic(target, map[string]any{
		"state.msgpack": map[string]any{"v": 1},
	}))
	require.NoError(t, writeMsgpackSnapshotsAtomic(target, map[string]any{
		"state.msgpack": map[string]any{"v": 2},
		"meta.msgpack":  map[string]any{"m": 1},
	}))

	_, err := os.Stat(filepath.Join(target, "state.msgpack"))
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(target, "meta.msgpack"))
	require.NoError(t, err)
}

func TestWriteMsgpackSnapshot_ErrorOnInvalidParent(t *testing.T) {
	dir := t.TempDir()
	parentFile := filepath.Join(dir, "not-a-dir")
	require.NoError(t, os.WriteFile(parentFile, []byte("x"), 0o644))

	err := writeMsgpackSnapshot(filepath.Join(parentFile, "snapshot.msgpack"), map[string]any{"x": 1})
	require.Error(t, err)
}

func TestWriteMsgpackSnapshotsAndAtomic_ErrorPaths(t *testing.T) {
	t.Run("write snapshots fails on invalid parent path", func(t *testing.T) {
		dir := t.TempDir()
		parentFile := filepath.Join(dir, "not-a-dir")
		require.NoError(t, os.WriteFile(parentFile, []byte("x"), 0o644))

		err := writeMsgpackSnapshots(filepath.Join(parentFile, "bundle"), map[string]any{"a.msgpack": map[string]any{"a": 1}})
		require.Error(t, err)
	})

	t.Run("atomic write propagates encode errors and does not leave target", func(t *testing.T) {
		target := filepath.Join(t.TempDir(), "bundle")
		err := writeMsgpackSnapshotsAtomic(target, map[string]any{
			"bad.msgpack": make(chan int), // msgpack cannot encode channels
		})
		require.Error(t, err)
		_, statErr := os.Stat(target)
		require.True(t, os.IsNotExist(statErr))
	})

	t.Run("atomic write fails when parent is a file", func(t *testing.T) {
		dir := t.TempDir()
		parentFile := filepath.Join(dir, "not-a-dir")
		require.NoError(t, os.WriteFile(parentFile, []byte("x"), 0o644))

		err := writeMsgpackSnapshotsAtomic(filepath.Join(parentFile, "bundle"), map[string]any{
			"state.msgpack": map[string]any{"v": 1},
		})
		require.Error(t, err)
	})
}
