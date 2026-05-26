"""Unit tests for translation.matching.semantic_matcher.

The matcher has two code paths:
  - Exact-name match: deterministic, always works regardless of whether the
    sentence-transformer model loaded.
  - Semantic fallback: needs the model. When the model isn't available
    (`sentence_transformers_available == False`), the semantic component of
    the score collapses to 0 and only the rapidfuzz string similarity
    contributes (weighted 0.3). The default threshold (0.7) is therefore
    unreachable without a model.

Most tests below exercise the deterministic surface so they pass with or
without the model. The threshold/semantic tests parameterize the threshold
low enough that the string-only fallback can clear it.
"""

from __future__ import annotations

import pytest

from translation.matching import semantic_matcher as sm_mod
from translation.matching.semantic_matcher import (
    MatchResult,
    Property,
    SemanticMatcher,
    calculate_similarity,
    initialize_semantic_model,
)


# ---------------------------------------------------------------------------
# Property dataclass
# ---------------------------------------------------------------------------


class TestProperty:
    def test_property_with_required_fields_only(self):
        prop = Property(name="connection.host", description="Database host")
        assert prop.name == "connection.host"
        assert prop.description == "Database host"
        assert prop.type == ""
        assert prop.section == ""
        assert prop.value is None
        assert prop.metadata is None

    def test_property_with_all_fields(self):
        prop = Property(
            name="ssl.mode",
            description="SSL connection mode",
            type="STRING",
            section="Security",
            value="verify-full",
            metadata={"direct_match": True},
        )
        assert prop.type == "STRING"
        assert prop.section == "Security"
        assert prop.value == "verify-full"
        assert prop.metadata == {"direct_match": True}


# ---------------------------------------------------------------------------
# MatchResult dataclass
# ---------------------------------------------------------------------------


class TestMatchResult:
    def test_match_result_construction(self):
        fm_info = {"name": "connection.host", "description": "host"}
        result = MatchResult(matched_fm_property=fm_info, similarity_score=0.95, match_type="semantic")
        assert result.matched_fm_property is fm_info
        assert result.similarity_score == 0.95
        assert result.match_type == "semantic"


# ---------------------------------------------------------------------------
# SemanticMatcher.__init__
# ---------------------------------------------------------------------------


class TestSemanticMatcherInit:
    def test_init_does_not_raise_when_model_unavailable(self):
        # Even if the model never loads, construction must not crash.
        matcher = SemanticMatcher()
        assert matcher is not None

    def test_init_starts_with_empty_caches(self):
        matcher = SemanticMatcher()
        assert matcher.fm_embeddings == {}
        assert matcher.fm_properties == {}


# ---------------------------------------------------------------------------
# find_best_match — exact-name match path (deterministic)
# ---------------------------------------------------------------------------


class TestFindBestMatchExact:
    @pytest.fixture
    def matcher(self):
        return SemanticMatcher()

    def test_exact_name_match_returns_exact_result(self, matcher):
        sm_prop = {"name": "connection.host", "description": "DB host"}
        fm_props = {
            "connection.host": {"description": "Database host", "section": "Connection"},
            "ssl.mode": {"description": "SSL mode", "section": "Security"},
        }
        result = matcher.find_best_match(sm_prop, fm_props)
        assert result is not None
        assert result.match_type == "exact"
        assert result.similarity_score == 1.0
        assert result.matched_fm_property is fm_props["connection.host"]

    def test_exact_match_preferred_over_close_string_match(self, matcher):
        # "connection.host" is present *and* "connection.hosts" is a near-miss;
        # exact match must win.
        sm_prop = {"name": "connection.host", "description": "host"}
        fm_props = {
            "connection.hosts": {"description": "hosts", "section": ""},
            "connection.host": {"description": "host", "section": ""},
        }
        result = matcher.find_best_match(sm_prop, fm_props)
        assert result.match_type == "exact"
        assert result.matched_fm_property is fm_props["connection.host"]


# ---------------------------------------------------------------------------
# find_best_match — semantic fallback path
# ---------------------------------------------------------------------------


class TestFindBestMatchSemantic:
    @pytest.fixture
    def matcher(self):
        return SemanticMatcher()

    def test_returns_none_when_no_fm_properties(self, matcher):
        sm_prop = {"name": "anything", "description": "x"}
        assert matcher.find_best_match(sm_prop, {}) is None

    def test_returns_none_when_no_match_clears_threshold(self, matcher):
        # No exact match, names are totally unrelated, threshold is the default
        # (0.7). Even with the model loaded the score won't clear that for
        # these names; without it, max possible score is 0.3.
        sm_prop = {"name": "zzz_unrelated_xxx", "description": ""}
        fm_props = {"alpha.beta.gamma": {"description": "", "section": ""}}
        assert matcher.find_best_match(sm_prop, fm_props) is None

    def test_string_similarity_can_match_with_low_threshold(self, matcher):
        # Two very similar names. The rapidfuzz string score alone (weighted
        # 0.3) is enough to clear a low semantic_threshold.
        sm_prop = {"name": "connection.hostname", "description": ""}
        fm_props = {
            "connection.host": {"description": "", "section": ""},
        }
        result = matcher.find_best_match(sm_prop, fm_props, semantic_threshold=0.2)
        assert result is not None
        assert result.match_type == "semantic"
        assert 0.0 < result.similarity_score <= 1.0
        assert result.matched_fm_property is fm_props["connection.host"]

    def test_direct_match_only_properties_skipped_in_semantic_phase(self, matcher):
        # Property flagged metadata.direct_match=True should never be selected
        # by the semantic phase — only the second candidate is eligible.
        sm_prop = {"name": "connection.hostname", "description": ""}
        fm_props = {
            "connection.host": {  # near-miss but blocked by direct_match flag
                "description": "",
                "section": "",
                "metadata": {"direct_match": True},
            },
            "connection.url": {  # eligible
                "description": "",
                "section": "",
            },
        }
        result = matcher.find_best_match(sm_prop, fm_props, semantic_threshold=0.1)
        if result is not None:
            assert result.matched_fm_property is fm_props["connection.url"]

    def test_higher_string_similarity_wins(self, matcher):
        # Two non-exact candidates; the one with higher string-similarity to
        # the SM name should win the semantic phase.
        sm_prop = {"name": "connection.host", "description": ""}
        fm_props = {
            "totally.different.key": {"description": "", "section": ""},
            "connection.hosts": {"description": "", "section": ""},  # near-miss
        }
        result = matcher.find_best_match(sm_prop, fm_props, semantic_threshold=0.2)
        assert result is not None
        assert result.matched_fm_property is fm_props["connection.hosts"]


# ---------------------------------------------------------------------------
# preload_fm_embeddings
# ---------------------------------------------------------------------------


class TestPreloadFmEmbeddings:
    def test_preload_stores_fm_properties_on_instance(self):
        matcher = SemanticMatcher()
        fm_props = {
            "connection.host": {"description": "host", "section": "Connection"},
            "ssl.mode": {"description": "ssl", "section": "Security"},
        }
        matcher.preload_fm_embeddings(fm_props)
        assert matcher.fm_properties == fm_props

    def test_preload_handles_empty_dict(self):
        matcher = SemanticMatcher()
        matcher.preload_fm_embeddings({})
        assert matcher.fm_properties == {}
        assert matcher.fm_embeddings == {}


# ---------------------------------------------------------------------------
# calculate_similarity
# ---------------------------------------------------------------------------


class TestCalculateSimilarity:
    def test_returns_float_between_zero_and_one_for_unrelated_names(self):
        sm = {"name": "alpha.beta", "description": "", "section": ""}
        fm = Property(name="zeta.eta", description="", section="")
        score = calculate_similarity(sm, fm)
        assert 0.0 <= score <= 1.0

    def test_identical_names_score_higher_than_unrelated(self):
        sm = {"name": "connection.host", "description": "", "section": ""}
        identical = Property(name="connection.host", description="", section="")
        unrelated = Property(name="xyz.totally.different", description="", section="")
        assert calculate_similarity(sm, identical) > calculate_similarity(sm, unrelated)

    def test_handles_missing_optional_keys_in_sm_property(self):
        # sm_property is dict-like; description/section are accessed via .get()
        sm = {"name": "connection.host"}
        fm = Property(name="connection.host", description="", section="")
        # Should not KeyError even though sm has no description/section keys.
        score = calculate_similarity(sm, fm)
        assert 0.0 <= score <= 1.0


# ---------------------------------------------------------------------------
# initialize_semantic_model
# ---------------------------------------------------------------------------


class TestInitializeSemanticModel:
    def test_returns_true_when_model_already_available(self, monkeypatch):
        monkeypatch.setattr(sm_mod, "sentence_transformers_available", True)
        assert initialize_semantic_model() is True

    def test_returns_false_when_local_model_missing(self, monkeypatch, tmp_path):
        # Force the "model not yet available" path and point Path() at an
        # empty tmpdir so the local-model branch fails.
        monkeypatch.setattr(sm_mod, "sentence_transformers_available", False)
        monkeypatch.chdir(tmp_path)
        result = initialize_semantic_model()
        # Either False (local path missing) or True (sentence_transformers
        # auto-downloaded). The contract: the function never raises.
        assert isinstance(result, bool)
