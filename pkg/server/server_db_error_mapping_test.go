package server

import (
	stderrors "errors"
	"fmt"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
)

// TestMapTransientTransactionError verifies that HTTP transaction endpoints use
// error identity, not localized message text, to decide retryable failures.
func TestMapTransientTransactionError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want string
		ok   bool
	}{
		{
			name: "conflict changed after start",
			err:  fmt.Errorf("commit failed: %w: node x changed after transaction start", nornicerrors.ErrTransactionConflict),
			want: "Neo.TransientError.Transaction.Outdated",
			ok:   true,
		},
		{
			name: "deadlock",
			err:  fmt.Errorf("%w: waiting for lock", nornicerrors.ErrTransactionDeadlock),
			want: "Neo.TransientError.Transaction.DeadlockDetected",
			ok:   true,
		},
		{
			name: "graceful snapshot expiration",
			err:  fmt.Errorf("failed to create node: %w", nornicerrors.ErrMVCCSnapshotGracefulCancel),
			want: "Neo.TransientError.Transaction.Outdated",
			ok:   true,
		},
		{
			name: "hard snapshot expiration",
			err:  fmt.Errorf("begin read: %w", nornicerrors.ErrMVCCSnapshotHardExpired),
			want: "Neo.TransientError.Transaction.Outdated",
			ok:   true,
		},
		{
			name: "syntax error passthrough",
			err:  stderrors.New("invalid input 'RETURNN'"),
			want: "",
			ok:   false,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := mapTransientTransactionError(tc.err)
			if ok != tc.ok {
				t.Fatalf("ok mismatch: got %v want %v", ok, tc.ok)
			}
			if got != tc.want {
				t.Fatalf("code mismatch: got %q want %q", got, tc.want)
			}
		})
	}
}
