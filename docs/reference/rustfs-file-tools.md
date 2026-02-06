---
summary: "How OpenClaw file tools (RustFS) perform semantic search and safe sending"
read_when:
  - You are enabling RustFS + rustfs-worker
  - You want semantic file search that is LLM-driven and traceable
title: "RustFS file tools reference"
---

# RustFS file tools reference

This page documents the OpenClaw agent tools backed by **RustFS**:

- `file_ingest`: upload a workspace file into RustFS
- `file_search`: list/search candidate files (optionally with semantic evidence)
- `file_send`: create a short-lived public link for a chosen file

## Safety contract (must follow)

- `file_search` **must not** automatically send files.
- Always ask the user to confirm a specific candidate (by `n` or `fileId`) before calling `file_send`.

## `file_search` semantic mode (dual evidence)

When `includeSemantic=true`, `file_search` uses a two-phase approach:

1. **RustFS coarse search**: list candidates using tenant + metadata filters.
2. **Deep memory evidence** (Top-K only): for each candidate file session `rustfs:file:<file_id>`, it concurrently calls:
   - `POST /retrieve_context` (query-driven evidence): returns `context` and memory matches for the given `user_input`
   - `POST /session/inspect` (session-level semantics): returns aggregated `topics/entities` (and optional `summary`)

This produces a more stable and explainable ranking:

- **Session semantics** answers “what this file is about”.
- **Query evidence** answers “why it matches this request”.

## Output fields (candidates)

`file_search` returns:

- `candidates[]`: a numbered list intended for user confirmation.
- `clarify` (optional): suggested follow-up questions when disambiguation is needed.

Each candidate may include:

- **Basic**: `n`, `fileId`, `filename`, `mime`, `size`, `createdAtMs`, `extractStatus`
- **Ingest hints**: `kind`, `tags`, `hint` (from `openclaw_ingest` and/or `classification`)
- **Semantic evidence** (when enabled):
  - `semanticScore`: relevance score used for re-ranking
  - `semanticContext`: short context snippet returned by deep-memory retrieval
  - `semanticTopics`, `semanticEntities`: session-level aggregated semantics
  - `semanticSummary`: optional session summary (if available)

## Traceability: file_id to memory_ids

If configured, `rustfs-worker` can request `memory_ids` from `POST /update_memory_index` (sync mode) and store them in:

- `annotations.deep_memory.memory_ids`

This lets you trace a RustFS `file_id` to the deep-memory ids that were written/updated for the file session.
