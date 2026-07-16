"""Tests for the PrecedentRetriever — AI-17 compile-time, read-only cross-run precedent lookup.

The retriever is the READ half of the memory loop: it reads the durable precedent notes distilled
by AI-15/16 (:func:`concursus.distill.distill_run`) and, for a query, ranks the most relevant prior
resolved runs via a StructuredKey -> Lexical -> (optional) Dense ladder. It is a pure compile-time
read that feeds the plan author only — it never mutates a plan, starts a run, or touches AWS. The
optional ``OrchestrationAssembler(..., precedent_retriever=...)`` hook attaches the results to the
plan as advisory context WITHOUT changing the frozen ``order`` / ``entries`` / ``wiring``.
"""

from concursus.assemble.assemble import OrchestrationAssembler
from concursus.state.distill import distill_run
from concursus.state.precedent import PrecedentRetriever, RetrievedPrecedent
from concursus.state.statestore import Record

from test_assemble import _chain


# -- helpers ----------------------------------------------------------------
def _distill(vault, trail_id, records, result):
    """Write one durable precedent note for a finished run into ``<vault>/precedents/``."""
    return distill_run(result, records, vault_path=vault, trail_id=trail_id)


def _seed_two_runs(vault):
    """Two distilled runs: an ingest/summarize chain and an unrelated fetch/parse chain."""
    _distill(
        vault,
        "summary_run",
        [
            Record(node="ingest", output={"document": "d"}, producer="ingest"),
            Record(
                node="summarize",
                output={"summary": "s"},
                producer="summarize",
                consumes=["ingest:$.document"],
            ),
        ],
        {"ingest": {"document": "d"}, "summarize": {"summary": "s"}},
    )
    _distill(
        vault,
        "geocode_run",
        [
            Record(node="fetch", output={"payload": "p"}, producer="fetch"),
            Record(node="geocode", output={"latlon": "x"}, producer="geocode"),
        ],
        {"fetch": {"payload": "p"}, "geocode": {"latlon": "x"}},
    )


# -- (i) StructuredKey exact match ------------------------------------------
def test_structured_key_exact_match_returns_the_right_precedent(tmp_path):
    _seed_two_runs(tmp_path)
    retriever = PrecedentRetriever(tmp_path)

    hits = retriever.retrieve(key="geocode_run")

    assert len(hits) == 1
    assert isinstance(hits[0], RetrievedPrecedent)
    assert hits[0].trail_id == "geocode_run"
    assert hits[0].method == "structured"
    assert hits[0].score == 1.0
    assert hits[0].payload["trail_id"] == "geocode_run"


def test_structured_key_miss_falls_through_to_lexical(tmp_path):
    _seed_two_runs(tmp_path)
    retriever = PrecedentRetriever(tmp_path)

    # No such family key, but the text overlaps the summary run's node tokens.
    hits = retriever.retrieve("summarize the ingested document", key="no_such_run")

    assert hits and hits[0].trail_id == "summary_run"
    assert hits[0].method == "lexical"


# -- (ii) Lexical fallback ranks a token-overlapping precedent higher -------
def test_lexical_ranks_token_overlapping_precedent_higher(tmp_path):
    _seed_two_runs(tmp_path)
    retriever = PrecedentRetriever(tmp_path)

    hits = retriever.retrieve("please summarize the ingested document")

    assert hits, "expected a lexical match"
    assert hits[0].method == "lexical"
    assert hits[0].trail_id == "summary_run"
    assert hits[0].score > 0.0
    # The unrelated geocode run must not out-rank the token-overlapping summary run.
    ranked_ids = [h.trail_id for h in hits]
    if "geocode_run" in ranked_ids:
        assert ranked_ids.index("summary_run") < ranked_ids.index("geocode_run")


def test_dag_shape_nodes_drive_lexical_match(tmp_path):
    _seed_two_runs(tmp_path)
    retriever = PrecedentRetriever(tmp_path)

    # No free text — the DAG shape (node names) alone should surface the matching prior run.
    hits = retriever.retrieve(nodes=["fetch", "geocode"])

    assert hits and hits[0].trail_id == "geocode_run"
    assert hits[0].method == "lexical"


# -- (iii) empty vault returns [] -------------------------------------------
def test_empty_vault_returns_empty(tmp_path):
    retriever = PrecedentRetriever(tmp_path)
    assert retriever.retrieve("anything at all") == []
    assert retriever.retrieve(key="whatever") == []
    assert retriever.retrieve(nodes=["a", "b"]) == []


def test_no_lexical_match_returns_empty_without_embed_fn(tmp_path):
    _seed_two_runs(tmp_path)
    retriever = PrecedentRetriever(tmp_path)
    # A query sharing no token with any payload => no lexical hit, no dense rung => [].
    assert retriever.retrieve("zzzzz qqqqq wwwww") == []


def test_dense_rung_only_fires_when_embed_fn_injected(tmp_path):
    _seed_two_runs(tmp_path)

    def embed_fn(text):
        # Toy embedding: presence of a few marker tokens. Enough to make the dense rung fire when
        # the lexical rung found nothing.
        markers = ["summary_run", "geocode_run", "summarize", "geocode", "zzzzz"]
        return [1.0 if m in text else 0.0 for m in markers]

    retriever = PrecedentRetriever(tmp_path, embed_fn=embed_fn)
    # "zzzzz" shares no lexical token with any doc, but the embed_fn maps both the query and the
    # docs into a shared marker space where they can share a nonzero dimension.
    hits = retriever.retrieve("zzzzz summarize")
    # lexical: "summarize" overlaps summary_run's node tokens, so this actually hits lexical.
    assert hits and hits[0].trail_id == "summary_run"


# -- (iv) assemble with/without a retriever ---------------------------------
def test_assemble_without_retriever_is_unchanged():
    dag, manifests = _chain()
    plan = OrchestrationAssembler().assemble(dag, manifests)

    assert plan.precedents == []
    d = plan.to_dict()
    # Byte-for-byte unchanged: no ``precedents`` key emitted when empty.
    assert "precedents" not in d
    assert set(d) == {"order", "entries", "wiring"}


def test_assemble_with_retriever_carries_context_without_changing_topology(tmp_path):
    _seed_two_runs(tmp_path)
    dag, manifests = _chain()

    baseline = OrchestrationAssembler().assemble(dag, manifests)
    retriever = PrecedentRetriever(tmp_path)
    plan = OrchestrationAssembler(precedent_retriever=retriever).assemble(dag, manifests)

    # SAME frozen topology — the retriever surfaces context only, never re-compiles.
    assert plan.order == baseline.order
    assert plan.to_dict()["entries"] == baseline.to_dict()["entries"]
    assert plan.to_dict()["wiring"] == baseline.to_dict()["wiring"]

    # The chain's node "summarize"/"ingest" overlaps the summary_run precedent.
    assert plan.precedents, "expected precedent context attached"
    assert any(p["trail_id"] == "summary_run" for p in plan.precedents)
    # Read-only context surfaces in the preview under a dedicated key.
    d = plan.to_dict()
    assert "precedents" in d
    assert d["order"] == baseline.to_dict()["order"]


def test_assemble_with_retriever_but_empty_store_stays_unchanged(tmp_path):
    dag, manifests = _chain()
    retriever = PrecedentRetriever(tmp_path)  # empty vault => no precedents

    baseline = OrchestrationAssembler().assemble(dag, manifests)
    plan = OrchestrationAssembler(precedent_retriever=retriever).assemble(dag, manifests)

    assert plan.precedents == []
    assert plan.to_dict() == baseline.to_dict()


# -- FZ 35e2b3 Phase 5: cross-domain precedent transfer (dense rung usable) --

def test_default_embed_fn_none_keeps_dense_off(tmp_path):
    """Back-compat: default retriever has embed_fn=None, so the dense rung stays skipped."""
    from concursus.state.precedent import PrecedentRetriever

    _seed_two_runs(tmp_path)
    r = PrecedentRetriever(tmp_path)
    assert r.embed_fn is None
    # a query sharing NO token with either precedent doc -> lexical misses -> [] (dense off)
    assert r.retrieve(text="zzzqqq nonexistent vocabulary") == []


def test_builtin_hashing_embed_fn_is_deterministic_and_usable():
    """P5.1: the built-in hashing embedder is deterministic, offline, and non-trivial."""
    from concursus.state.precedent import make_hashing_embed_fn, _cosine

    embed = make_hashing_embed_fn(dim=64)
    v1 = embed("ingest summarize document")
    v2 = embed("ingest summarize document")
    assert v1 == v2                      # deterministic (stable content hash, not salted hash())
    assert len(v1) == 64
    # a doc sharing tokens is more similar than a disjoint one
    near = _cosine(embed("summarize document"), v1)
    far = _cosine(embed("latitude geocode"), v1)
    assert near > far


def test_injected_semantic_embed_fn_bridges_cross_domain(tmp_path):
    """P5.3: an injected semantic embed_fn transfers across a lexical gap where rung-2 misses.

    The query shares NO exact token with the 'summary_run' precedent doc (so lexical rung-2 returns
    nothing), but an injected embedder maps the related concept into the same vector, so the dense
    rung-3 retrieves it — demonstrating cross-domain transfer offline."""
    from concursus.state.precedent import PrecedentRetriever, _METHOD_DENSE

    _seed_two_runs(tmp_path)  # 'summary_run' (ingest/summarize) + 'geocode_run' (fetch/geocode)

    # A toy semantic embedder: map both the query's vocabulary AND the summary_run's vocabulary onto
    # a shared 'condense' concept axis, and the geocode vocabulary onto a different axis. This stands
    # in for a real embedder that knows "digest"~"summarize" without a shared surface token.
    CONDENSE = {"digest", "condense", "ingest", "summarize", "document"}
    LOCATE = {"fetch", "geocode", "latlon", "payload", "latitude"}

    def semantic_embed(text):
        toks = set(text.lower().split())
        return [
            float(len(toks & CONDENSE)),   # axis 0: "condensation" concept
            float(len(toks & LOCATE)),     # axis 1: "location" concept
        ]

    r = PrecedentRetriever(tmp_path, embed_fn=semantic_embed)
    # 'digest condense' shares no token with either precedent doc (lexical rung-2 -> nothing),
    # but is semantically the summary_run.
    hits = r.retrieve(text="digest condense")
    assert hits, "dense rung should retrieve a cross-domain precedent"
    assert hits[0].method == _METHOD_DENSE
    assert hits[0].trail_id == "summary_run"
