"""The **PrecedentRetriever** — compile-time, read-only cross-run precedent lookup (AI-17).

Concursus is a *compiler*: ``AgentDAG -> assemble -> frozen ProvisioningPlan -> Supervisor.run``
as a static topological walk, and resume is replay. The memory loop (:mod:`concursus.distill`,
AI-15/16) is the offline *write* half — it folds each finished run into ONE durable precedent note
under ``<vault>/precedents/`` and projects the accumulated set into a cross-run hub. This module is
the matching *read* half.

:class:`PrecedentRetriever` reads the DURABLE precedent store (via
:class:`~concursus.runindex.PrecedentIndex.from_vault` over a ``vault_path``) and, given a query
(a goal string / a DAG-shape / a ticket-family key), returns the most relevant prior *resolved*
runs as precedent context. It runs a small retrieval LADDER that degrades gracefully:

1. **StructuredKey** — an exact ``trail_id`` / family-key match (the cheapest, most precise rung).
2. **Lexical** — a stdlib-only BM25-ish rank by token overlap over the precedent payloads
   (``trail_id`` + executed node names + result-field keys + failure reasons + status).
3. **Dense** — cosine over an INJECTED ``embed_fn`` (default ``None`` ⇒ skipped). No heavy deps.

Identity guard (non-negotiable): this is a PURE COMPILE-TIME READ. It runs *before* a plan is
frozen, reads the durable store only (never a run log, never a live plan), selects/queries but
starts and seeds no run, and mutates no topology. It feeds the plan AUTHOR context — it is never a
runtime replan. Deleting the precedent notes empties it and loses nothing else. Stdlib only.
"""

from __future__ import annotations

import math
import re
from collections import Counter
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Sequence

from .runindex import PrecedentIndex

# The retrieval rungs, in ladder order (a small, honest enum-of-strings for the ``method`` tag).
_METHOD_STRUCTURED = "structured"
_METHOD_LEXICAL = "lexical"
_METHOD_DENSE = "dense"

_TOKEN_RE = re.compile(r"[a-z0-9]+")


def _tokenize(text: str) -> List[str]:
    """Lowercase, split on non-alphanumerics — the shared tokenizer for query + documents."""
    return _TOKEN_RE.findall(str(text).lower())


def _doc_tokens(payload: Dict[str, object]) -> List[str]:
    """The lexical *document* for one precedent payload: the tokens that make a run retrievable.

    Folds the retrievable surface of a distilled run — its ``trail_id``, executed ``nodes``, the
    ``results`` field-keys, any ``failed`` node names + reasons, and the one-word ``status`` — into
    a flat token list. Pure; reads the payload only (never a live plan)."""
    parts: List[str] = [str(payload.get("trail_id") or "")]
    parts.extend(str(n) for n in (payload.get("nodes") or []))
    results = payload.get("results")
    if isinstance(results, dict):
        parts.extend(str(k) for k in results.keys())
    outcome = payload.get("outcome") or {}
    failed = outcome.get("failed") if isinstance(outcome, dict) else None
    if isinstance(failed, dict):
        parts.extend(str(k) for k in failed.keys())
        parts.extend(str(v) for v in failed.values())
    parts.append(str(payload.get("status") or ""))

    tokens: List[str] = []
    for p in parts:
        tokens.extend(_tokenize(p))
    return tokens


def _bm25_scores(
    query_tokens: Sequence[str],
    docs: Sequence[List[str]],
    *,
    k1: float = 1.5,
    b: float = 0.75,
) -> List[float]:
    """A compact BM25 over pre-tokenized documents — stdlib only, no third-party deps.

    Returns one relevance score per document (same order). Zero for a document sharing no query
    token, so the caller can drop non-matches. Robust to an empty corpus / empty documents."""
    n = len(docs)
    if n == 0:
        return []
    total_len = sum(len(d) for d in docs)
    avgdl = (total_len / n) if total_len else 1.0

    df: Counter = Counter()
    for d in docs:
        for t in set(d):
            df[t] += 1

    q_terms = set(query_tokens)
    scores: List[float] = []
    for d in docs:
        tf = Counter(d)
        dl = len(d)
        s = 0.0
        for q in q_terms:
            f = tf.get(q, 0)
            if not f:
                continue
            idf = math.log(1 + (n - df[q] + 0.5) / (df[q] + 0.5))
            s += idf * (f * (k1 + 1)) / (f + k1 * (1 - b + b * dl / avgdl))
        scores.append(s)
    return scores


def _cosine(a: Sequence[float], b: Sequence[float]) -> float:
    """Cosine similarity of two equal-length vectors (``0.0`` for a zero / mismatched vector)."""
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    if na == 0.0 or nb == 0.0:
        return 0.0
    return dot / (na * nb)


@dataclass
class RetrievedPrecedent:
    """One retrieved prior run: the matched precedent payload plus retrieval provenance.

    Attributes:
        trail_id: The matched run/family id.
        method: Which ladder rung matched (``structured`` / ``lexical`` / ``dense``).
        score: The rung's relevance score (``1.0`` for an exact structured key match).
        payload: The verbatim precedent payload (read-only context for the plan author).
    """

    trail_id: str
    method: str
    score: float
    payload: Dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict:
        """A JSON-serializable view (for :meth:`ProvisioningPlan.to_dict` / a plan preview)."""
        return {
            "trail_id": self.trail_id,
            "method": self.method,
            "score": self.score,
            "precedent": dict(self.payload),
        }


class PrecedentRetriever:
    """Read-only, compile-time retrieval over the durable cross-run precedent store (AI-17).

    Wraps a ``vault_path`` and, on each :meth:`retrieve`, reads the current precedent notes through
    :class:`~concursus.runindex.PrecedentIndex.from_vault` and ranks them for a query via the
    StructuredKey → Lexical → (optional) Dense ladder. An optional ``embed_fn`` (``str -> vector``)
    enables the dense rung; default ``None`` skips it, so importing/using concursus needs no model.

    This never mutates a plan, never starts a run, and never reads a run log — it feeds the plan
    author context only. It is safe to call any number of times at compile time; deleting the
    precedent notes simply empties the result.
    """

    def __init__(
        self,
        vault_path,
        *,
        embed_fn: Optional[Callable[[str], Sequence[float]]] = None,
        limit: int = 5,
    ) -> None:
        self.vault_path = vault_path
        self.embed_fn = embed_fn
        self.limit = limit

    def retrieve(
        self,
        text: str = "",
        *,
        key: Optional[str] = None,
        nodes: Optional[Sequence[str]] = None,
        status: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[RetrievedPrecedent]:
        """Return the most relevant prior runs for a query, ranked by the retrieval ladder.

        Args:
            text: A free-text goal / description to match lexically (or densely).
            key: A structured ``trail_id`` / family key for an EXACT match (rung 1). When it hits,
                that precedent is returned alone (the cheapest, most precise rung).
            nodes: The DAG-shape — executed/planned node names — folded into the lexical query so a
                topologically similar prior run surfaces.
            status: Restrict candidates to a run ``status`` (``completed`` / ``partial`` /
                ``failed``); ``None`` considers all.
            limit: Max results (defaults to the retriever's ``limit``).

        The ladder degrades gracefully: an exact key match short-circuits; else a BM25-ish lexical
        rank over the payloads; else — only if an ``embed_fn`` was injected — a dense cosine rank.
        Returns ``[]`` for an empty store or a query that matches nothing. Pure read; no mutation.
        """
        cap = self.limit if limit is None else limit
        index = PrecedentIndex.from_vault(self.vault_path)
        payloads = index.query(status=status)
        if not payloads:
            return []

        # -- rung 1: StructuredKey (exact trail_id / family key) ------------
        if key is not None:
            hit = index.get(key)
            if hit is not None and (status is None or hit.get("status") == status):
                return [RetrievedPrecedent(str(key), _METHOD_STRUCTURED, 1.0, hit)]

        # Build the lexical/dense query from the free text + the DAG shape.
        query_tokens: List[str] = _tokenize(text)
        for node in nodes or []:
            query_tokens.extend(_tokenize(str(node)))
        if not query_tokens:
            return []

        docs = [_doc_tokens(p) for p in payloads]

        # -- rung 2: Lexical (BM25-ish token overlap) -----------------------
        scores = _bm25_scores(query_tokens, docs)
        ranked = sorted(
            ((s, p) for s, p in zip(scores, payloads) if s > 0.0),
            key=lambda sp: (-sp[0], str(sp[1].get("trail_id") or "")),
        )
        if ranked:
            return [
                RetrievedPrecedent(str(p.get("trail_id") or ""), _METHOD_LEXICAL, s, p)
                for s, p in ranked[:cap]
            ]

        # -- rung 3: Dense (only if an embed_fn was injected) ---------------
        if self.embed_fn is not None:
            qv = self.embed_fn(" ".join(query_tokens))
            dense = sorted(
                (
                    (_cosine(qv, self.embed_fn(" ".join(d))), p)
                    for d, p in zip(docs, payloads)
                ),
                key=lambda sp: (-sp[0], str(sp[1].get("trail_id") or "")),
            )
            dense = [sp for sp in dense if sp[0] > 0.0]
            if dense:
                return [
                    RetrievedPrecedent(str(p.get("trail_id") or ""), _METHOD_DENSE, s, p)
                    for s, p in dense[:cap]
                ]

        return []
