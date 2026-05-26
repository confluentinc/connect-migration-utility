"""Tests for src/data/connector_packs.py."""

import pytest

from data.connector_packs import COMMERCIAL_PACK_CONNECTORS, PREMIUM_PACK_CONNECTORS


class TestPremiumPack:
    def test_is_a_dict(self):
        assert isinstance(PREMIUM_PACK_CONNECTORS, dict)

    def test_has_five_entries(self):
        assert len(PREMIUM_PACK_CONNECTORS) == 5

    @pytest.mark.parametrize(
        "connector_class",
        [
            "io.confluent.connect.jms.IbmMqSinkConnector",
            "io.confluent.connect.ibm.mq.IbmMQSourceConnector",
            "io.confluent.connect.splunk.s2s.SplunkS2SSourceConnector",
            "io.confluent.connect.oracle.cdc.OracleCdcSourceConnector",
            "io.confluent.connect.oracle.xstream.cdc.OracleXStreamSourceConnector",
        ],
    )
    def test_premium_entry_is_present(self, connector_class):
        assert connector_class in PREMIUM_PACK_CONNECTORS

    def test_all_keys_are_strings(self):
        for key in PREMIUM_PACK_CONNECTORS:
            assert isinstance(key, str)
            assert key.startswith("io.confluent.connect.")

    def test_all_values_are_dicts(self):
        for value in PREMIUM_PACK_CONNECTORS.values():
            assert isinstance(value, dict)


class TestCommercialPack:
    def test_is_a_dict(self):
        assert isinstance(COMMERCIAL_PACK_CONNECTORS, dict)

    def test_has_many_entries(self):
        # Be lax — there are >50 entries; assert a floor in case future deletions go unnoticed.
        assert len(COMMERCIAL_PACK_CONNECTORS) >= 50

    @pytest.mark.parametrize(
        "connector_class",
        [
            # Spot-check from both halves of the dict.
            "io.confluent.connect.amps.AmpsSourceConnector",
            "io.confluent.connect.cassandra.CassandraSinkConnector",
            "io.confluent.salesforce.SalesforceCdcSourceConnector",
            "io.confluent.connect.aws.dynamodb.DynamoDbSinkConnector",
            "io.confluent.connect.gcs.GcsSinkConnector",
            "io.confluent.connect.zendesk.ZendeskSourceConnector",
        ],
    )
    def test_commercial_entry_is_present(self, connector_class):
        assert connector_class in COMMERCIAL_PACK_CONNECTORS

    def test_all_keys_are_strings(self):
        for key in COMMERCIAL_PACK_CONNECTORS:
            assert isinstance(key, str)
            assert key

    def test_all_values_are_dicts(self):
        for value in COMMERCIAL_PACK_CONNECTORS.values():
            assert isinstance(value, dict)


class TestPackOverlap:
    def test_ibm_mq_appears_in_both_packs(self):
        # Documented quirk of the original code: IbmMqSinkConnector is listed under
        # both premium and commercial. The test pins it so a future refactor that
        # tries to dedupe alerts the author.
        ibm = "io.confluent.connect.jms.IbmMqSinkConnector"
        assert ibm in PREMIUM_PACK_CONNECTORS
        assert ibm in COMMERCIAL_PACK_CONNECTORS

    def test_ibm_mq_source_appears_in_both_packs(self):
        ibm_source = "io.confluent.connect.ibm.mq.IbmMQSourceConnector"
        assert ibm_source in PREMIUM_PACK_CONNECTORS
        assert ibm_source in COMMERCIAL_PACK_CONNECTORS
