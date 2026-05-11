package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBadgerEngineDataDirFreeSpace(t *testing.T) {
	engine, _ := createTestBadgerEngineOnDisk(t)
	t.Cleanup(func() {
		require.NoError(t, engine.Close())
	})

	free, err := engine.DataDirFreeSpace()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, free, int64(0))
}
