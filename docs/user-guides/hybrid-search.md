# Hybrid Search

**Complete guide to combined vector + keyword search in NornicDB**

Last Updated: December 2025

---

## Overview

NornicDB provides a **hybrid search system** that combines three retrieval strategies for best-of-both-worlds results:

1. **Vector Search** — Cosine similarity on high-dimensional embeddings for semantic understanding
2. **BM25 Full-Text Search** — Keyword search with TF-IDF scoring for exact term matching
3. **RRF (Reciprocal Rank Fusion)** — Industry-standard algorithm to merge rankings from both strategies

By default, **both sides see all node properties**:

- **Embedding text / vector search**: embeddings are built from **all node properties plus node labels** by default, excluding built-in metadata fields like `embedding`, `has_embedding`, timestamps, and similar internal keys. This is configurable through embedding include/exclude settings if you want to restrict which properties contribute to embeddings.
- **BM25 full-text search**: BM25 also indexes **all node properties**. A small set of common text fields such as `content`, `text`, `title`, and `name` are added first for better ranking, but searchability is not limited to those fields.

This is the same approach used by Azure AI Search, Elasticsearch, Weaviate, and Google Cloud Search.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Search Service                          │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────┐ │
│  │  Vector Index   │  │  Fulltext Index │  │   Storage   │ │
│  │  (Cosine Sim)   │  │    (BM25)       │  │   Engine    │ │
│  └────────┬────────┘  └────────┬────────┘  └──────┬──────┘ │
│           │                    │                   │        │
│           └────────────┬───────┘                   │        │
│                        │                           │        │
│              ┌─────────▼─────────┐                 │        │
│              │   RRF Fusion      │─────────────────┘        │
│              │ score = Σ(w/(k+r))│                          │
│              └─────────┬─────────┘                          │
│                        │                                    │
│              ┌─────────▼─────────┐                          │
│              │   Ranked Results  │                          │
│              └───────────────────┘                          │
└─────────────────────────────────────────────────────────────┘
```

---

## Full-Text Search Properties

BM25 indexes **all node properties**, but these properties are treated as **priority fields** for ranking:

| Property       | Description                |
| -------------- | -------------------------- |
| `content`      | Main content field         |
| `text`         | Text content (file chunks) |
| `title`        | Node titles                |
| `name`         | Node names                 |
| `description`  | Node descriptions          |
| `path`         | File paths                 |
| `workerRole`   | Agent worker roles         |
| `requirements` | Task requirements          |

After those priority fields, **all remaining properties are also indexed**. Property names are included alongside values, so searches can match both general text and field-oriented content.

Example: a search for `docker configuration` can match a node through `content`, `title`, or any other indexed property that contains those terms.

## Embedding Text Inputs

The vector side of hybrid search depends on whatever text was embedded for the node.

By default, managed embeddings are generated from:

- node labels
- all node properties
- excluding built-in metadata/internal fields

That means hybrid search is not limited to a single `content` field unless you configure it that way.

If you set embedding include/exclude options, the vector side follows those rules, while the BM25 side still indexes all properties.

---

## RRF Algorithm

**Formula**: `RRF_score(doc) = Σ (weight_i / (k + rank_i))`

Where:

- `k` = constant (default: 60)
- `rank_i` = rank of document in result set i (1-indexed)
- `weight_i` = importance weight for result set i

### Adaptive Weights

The system automatically adjusts weights based on query characteristics:

| Query Type | Words | Vector Weight | BM25 Weight | Rationale              |
| ---------- | ----- | ------------- | ----------- | ---------------------- |
| Short      | 1-2   | 0.5           | 1.5         | Exact keyword matching |
| Medium     | 3-5   | 1.0           | 1.0         | Balanced               |
| Long       | 6+    | 1.5           | 0.5         | Semantic understanding |

---

## Usage

### REST API

The primary way to run hybrid searches is the REST search endpoint:

```bash
curl -X POST http://localhost:7474/nornicdb/search \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "query": "machine learning algorithms",
    "limit": 10,
    "labels": ["Document"]
  }'
```

Results include RRF metadata so you can see how each ranking strategy contributed:

```json
{
  "results": [
    {
      "id": "node-42",
      "score": 0.0312,
      "rrf_score": 0.0312,
      "vector_rank": 2,
      "bm25_rank": 1,
      "labels": ["Document"],
      "properties": { "title": "Introduction to ML Algorithms", "..." : "..." }
    }
  ]
}
```

### HTTP Search Request Fields

| Field      | Default | Description                                            |
| ---------- | ------- | ------------------------------------------------------ |
| `query`    | —       | The search text (required)                             |
| `limit`    | 10      | Maximum number of results                              |
| `labels`   | all     | Restrict results to nodes with any of these labels     |
| `filters`  | none    | Property filters as `{"key": ["value1", "value2"]}`    |
| `database` | default | Logical database name to search                        |

RRF tuning constants (`k`, vector/BM25 weights) are applied internally by the search service and are not exposed as HTTP request fields. For programmatic control over fusion weights or k, use the embedded Go API or the MCP `discover` tool. Internal RRF defaults are `k = 60` with adaptive vector/BM25 weights driven by query length (see the table above).

### Cypher Procedure

You can also run vector search from Cypher using `db.index.vector.queryNodes`:

```cypher
CALL db.index.vector.queryNodes('embeddings', 10, $queryEmbedding)
YIELD node, score
RETURN node.title, score
ORDER BY score DESC
```

See the [Vector Search Guide](vector-search.md) for full details on Cypher-based search.

---

## Fallback Chain

The search automatically falls back when needed:

1. **RRF Hybrid** (if embedding provided)
2. **Vector Only** (if BM25 returns no results)
3. **Full-Text Only** (if no embedding or vector search fails)

## Caching

Search results are cached so that **repeated identical requests** (same query and options) return immediately from memory. The cache is shared by all entry points (HTTP `/nornicdb/search`, Cypher vector procedures, MCP, etc.).

- **Key:** Query text + options (limit, types/labels, rerank, MMR settings). Same inputs ⇒ cache hit.
- **Size:** Up to 1000 entries (LRU); entries expire after 5 minutes (TTL).
- **Invalidation:** The cache is cleared whenever the index changes (node created, updated, or removed), so results stay correct after mutations.

Use the same query and options for repeated calls to benefit from the cache (e.g. same search box query and limit).

---

## Performance (Apple M3 Max)

| Operation     | Scale          | Time   |
| ------------- | -------------- | ------ |
| Vector Search | 10K vectors    | ~8.5ms |
| BM25 Search   | 10K documents  | ~255µs |
| RRF Fusion    | 100 candidates | ~27µs  |
| Index Build   | 38K nodes      | ~5.4s  |

## Related Guides

- [Vector Search](vector-search.md) — dedicated vector search guide with Cypher index examples
- [Data Import/Export](data-import-export.md) — importing data for search indexing
