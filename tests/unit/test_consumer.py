"""Unit tests for Kafka consumer enrichment logic."""
import pytest
from datetime import datetime, timedelta

from consumer.kafka_consumer import (
    parse_glpi_datetime,
    enrich_ticket,
    PRIORITY_MAP,
    SLA_LIMITS_HOURS,
)


class TestParseGlpiDatetime:
    def test_valid_datetime_string(self):
        result = parse_glpi_datetime("2026-05-01 14:30:00")
        assert result == datetime(2026, 5, 1, 14, 30, 0)

    def test_iso_format(self):
        result = parse_glpi_datetime("2026-05-01T14:30:00")
        assert result == datetime(2026, 5, 1, 14, 30, 0)

    def test_null_string(self):
        assert parse_glpi_datetime("NULL") is None

    def test_none_value(self):
        assert parse_glpi_datetime(None) is None

    def test_empty_string(self):
        assert parse_glpi_datetime("") is None

    def test_invalid_format(self):
        assert parse_glpi_datetime("not-a-date") is None


class TestEnrichTicket:
    def _base_ticket(self, **overrides):
        t = {
            "id": 42,
            "name": "Test ticket",
            "content": "Description",
            "priority": 2,  # High
            "status": 2,
            "urgency": 3,
            "impact": 3,
            "date_creation": "2026-05-01 08:00:00",
            "solvedate": None,
            "closedate": None,
            "itilcategories_id": "network",
        }
        t.update(overrides)
        return t

    def test_mttr_calculated_when_resolved(self):
        ticket = self._base_ticket(
            date_creation="2026-05-01 08:00:00",
            solvedate="2026-05-01 14:00:00",
        )
        result = enrich_ticket(ticket)
        assert result["mttr_hours"] == 6.0

    def test_mttr_is_none_when_not_resolved(self):
        ticket = self._base_ticket(solvedate=None)
        result = enrich_ticket(ticket)
        assert result["mttr_hours"] is None

    def test_priority_mapping_high(self):
        ticket = self._base_ticket(priority=2)
        result = enrich_ticket(ticket)
        assert result["priority_label"] == "High"

    def test_priority_mapping_very_high(self):
        ticket = self._base_ticket(priority=1)
        result = enrich_ticket(ticket)
        assert result["priority_label"] == "Very High"

    def test_sla_respected_when_within_limit(self):
        # High priority SLA = 8h, resolved in 6h → respected
        ticket = self._base_ticket(
            priority=2,
            date_creation="2026-05-01 08:00:00",
            solvedate="2026-05-01 14:00:00",
        )
        result = enrich_ticket(ticket)
        assert result["sla_respected"] is True

    def test_sla_violated_when_over_limit(self):
        # High priority SLA = 8h, resolved in 10h → violated
        ticket = self._base_ticket(
            priority=2,
            date_creation="2026-05-01 08:00:00",
            solvedate="2026-05-01 18:00:00",
        )
        result = enrich_ticket(ticket)
        assert result["sla_respected"] is False

    def test_sla_none_when_unresolved(self):
        ticket = self._base_ticket(solvedate=None)
        result = enrich_ticket(ticket)
        assert result["sla_respected"] is None

    def test_required_fields_present(self):
        result = enrich_ticket(self._base_ticket())
        for field in ["glpi_ticket_id", "title", "priority_label", "urgency", "impact", "source"]:
            assert field in result

    def test_unknown_priority_defaults_to_medium(self):
        ticket = self._base_ticket(priority=99)
        result = enrich_ticket(ticket)
        assert result["priority_label"] == "Medium"


class TestSlaLimits:
    def test_very_high_sla_is_4h(self):
        assert SLA_LIMITS_HOURS["Very High"] == 4

    def test_high_sla_is_8h(self):
        assert SLA_LIMITS_HOURS["High"] == 8

    def test_medium_sla_is_24h(self):
        assert SLA_LIMITS_HOURS["Medium"] == 24

    def test_priority_map_covers_all_codes(self):
        for code in [1, 2, 3, 4, 5]:
            assert code in PRIORITY_MAP
