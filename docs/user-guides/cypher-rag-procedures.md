# Cypher RAG Procedures

NornicDB exposes seam-aligned Cypher procedures for in-query RAG orchestration:

- `CALL db.retrieve({query: '...', limit: 10, ...})`
- `CALL db.rretrieve({query: '...', limit: 10, ...})`
- `CALL db.rerank({query: '...', candidates: [...], rerankTopK: 50, rerankMinScore: 0.0})`
- `CALL db.index.vector.embed('...') YIELD embedding`
- `CALL db.infer({prompt: '...', max_tokens: 256, ...})`

These procedures are read-only and designed to map directly to internal contracts:

- Retrieval/rerank use existing `search.Service` + `SearchOptions`.
- Inference uses existing Heimdall manager `Generate`/`Chat` contracts.

## Procedure behavior

- `db.retrieve`
  - Uses existing hybrid search behavior.
  - Reranking is optional and follows request/config defaults.

- `db.rretrieve`
  - Shorthand retrieve path for simple usage.
  - Automatically enables rerank only when a reranker is configured and available.
  - Useful when you want one-call behavior while keeping `db.retrieve` + `db.rerank` available for explicit before/after comparisons.

- `db.rerank`
  - Matches Stage-2 rerank API directly (does not run retrieval).
  - Requires caller-provided candidate rows (for example from `db.retrieve`).
  - Becomes pass-through ranking when no reranker is configured/available.
  - Use `rerankTopK` / `rerankMinScore` to tune rerank behavior.

- `db.index.vector.embed`
  - Embeds a text string using the configured embedding service for the current database.
  - Returns a vector array via `YIELD embedding`.
  - This is useful for fully manual Cypher search pipelines.

- `db.infer` caching behavior
  - The procedure itself does not cache; each call invokes the configured inference manager. Caching, when applicable, is the responsibility of the inference manager / model provider.

## Example

```cypher
CALL db.retrieve({query: 'zero-trust architecture', limit: 5}) YIELD node, score
WITH node, score
CALL db.infer({
  prompt: 'Summarize this node briefly: ' + coalesce(node.content, toString(node)),
  max_tokens: 120,
  temperature: 0.0
}) YIELD text
RETURN node, score, text
```

```cypher
CALL db.retrieve({query: 'zero-trust architecture', limit: 20}) YIELD node, score
WITH collect({id: node.id, content: coalesce(node.content, toString(node)), score: score}) AS candidates
CALL db.rerank({query: 'zero-trust architecture', candidates: candidates, rerankTopK: 20}) YIELD id, final_score
RETURN id, final_score
```

```cypher
CALL db.index.vector.embed('zero-trust architecture') YIELD embedding
CALL db.index.vector.queryNodes('doc_idx', 10, embedding) YIELD node, score
RETURN node, score
```

If you use `db.index.vector.embed()`, pass the returned `embedding` array into
`db.index.vector.queryNodes(..., embedding)` (or an inline array equivalent) for explicit pipeline control.

```cypher
CALL db.infer({prompt: 'Summarize: ...', temperature: 0.0}) YIELD text
RETURN text
```

## Lightweight Risk Notes

- Using LLM output as data in your own explicit downstream Cypher is supported.
- If you intentionally combine model-generated query text with dynamic execution procedures (for example, dynamic APOC execution), treat that as a high-risk pattern and review it carefully.
- Prefer parameterized query authoring and explicit mutation logic for sensitive write paths.
