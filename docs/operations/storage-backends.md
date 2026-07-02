# Storage Backends

NornicDB supports three storage backend selectors. Badger remains the
production default; TreeDB is an opt-in persistent backend for operators who
want the native TreeDB transaction and durability path; memory is for ephemeral
or test runs.

## Selection

Use one of these equivalent selectors:

```bash
nornicdb serve --storage-backend treedb --data-dir /var/lib/nornicdb
```

```bash
NORNICDB_STORAGE_BACKEND=treedb
NORNICDB_DATA_DIR=/var/lib/nornicdb
```

```yaml
database:
  storage_backend: treedb
  data_dir: /var/lib/nornicdb
```

The legacy YAML shape also works:

```yaml
storage:
  backend: treedb
  path: /var/lib/nornicdb
```

Backend names are case-insensitive and canonicalized to `badger`, `treedb`, or
`memory`. An empty selector uses `badger`.

## Backend Matrix

| Backend | Production role | Data directory | Notes |
| --- | --- | --- | --- |
| `badger` | Default persistent backend | Required for persistent runs | Supports the existing Badger, legacy WAL, async write-behind, and encryption paths. |
| `treedb` | Opt-in persistent backend | Required | Uses TreeDB native conditional transactions and TreeDB `SyncWrites` for strict durability. |
| `memory` | Ephemeral/testing backend | Must be empty/implicit | Keeps the historical in-memory mode when no data directory is supplied. |

## TreeDB Capability Gates

TreeDB fails closed for feature combinations that are not production-ready yet:

| Requested feature | TreeDB behavior |
| --- | --- |
| In-memory mode | Hard error: TreeDB requires a persistent data directory. |
| Database encryption | Hard error: encryption is not supported by TreeDB yet. Use Badger for encrypted stores. |
| Memory decay | Hard error: memory decay is not wired to TreeDB yet. |
| Non-standalone cluster or raft mode | Hard error: TreeDB replication integration is reserved for the WAL/replication lane. |
| Legacy async write-behind | Disabled: TreeDB commits through its native transaction boundary. |
| Legacy `NORNICDB_WAL_*` replay/retention knobs | Not applied to TreeDB graph writes; TreeDB owns the local durable write boundary. |

These are startup errors, not warnings. Operators should prefer an explicit
failure over silently running with weaker durability, encryption, or replication
semantics than requested.

## Durability And Performance Evidence

- Strict durability is documented in [Durability](durability.md). With TreeDB,
  `NORNICDB_STRICT_DURABILITY=true` maps to TreeDB `SyncWrites`.
- The storage benchmark matrix is documented in
  [Storage Backend Benchmark Matrix](../performance/storage-backend-benchmarks.md).
  Storage or backend-selection PRs should use that matrix before claiming
  performance or regression results.
- TreeDB close/reopen, schema/index persistence, pending embedding markers,
  and fail-closed unsupported feature behavior are covered by storage backend
  and durability tests.
