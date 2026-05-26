"""Tests for src/data/sensitive_configs.py."""

import pytest

from data.sensitive_configs import SENSITIVE_PATTERNS, STATIC_SENSITIVE_CONFIGS


class TestStaticSensitiveConfigs:
    def test_is_a_list(self):
        assert isinstance(STATIC_SENSITIVE_CONFIGS, list)

    def test_has_many_entries(self):
        assert len(STATIC_SENSITIVE_CONFIGS) >= 100

    def test_all_entries_are_lowercase_strings(self):
        for entry in STATIC_SENSITIVE_CONFIGS:
            assert isinstance(entry, str)
            assert entry == entry.lower(), f"{entry!r} should be lowercased"

    def test_no_duplicate_entries(self):
        assert len(STATIC_SENSITIVE_CONFIGS) == len(set(STATIC_SENSITIVE_CONFIGS))

    def test_no_empty_strings(self):
        for entry in STATIC_SENSITIVE_CONFIGS:
            assert entry.strip() == entry
            assert entry

    @pytest.mark.parametrize(
        "key",
        [
            "password",
            "database.password",
            "ssl.keystore.password",
            "aws.secret.access.key",
            "bearer.token",
            "sasl.jaas.config",
            "tls.private.key",
            "salesforce.consumer.secret",
        ],
    )
    def test_well_known_keys_are_in_list(self, key):
        assert key in STATIC_SENSITIVE_CONFIGS


class TestSensitivePatterns:
    def test_is_a_list(self):
        assert isinstance(SENSITIVE_PATTERNS, list)

    def test_contains_expected_patterns(self):
        # These are checked by substring against config keys, so order/contents matter.
        assert set(SENSITIVE_PATTERNS) == {"password", "token", "secret", "credential"}

    def test_all_patterns_lowercase(self):
        for p in SENSITIVE_PATTERNS:
            assert p == p.lower()
