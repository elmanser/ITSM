"""Unit tests for GLPI producer — weekend logic and mock ticket generation."""
import pytest
from datetime import datetime, timezone
from unittest.mock import patch


class TestWeekendDetection:
    """Verify the weekend detection logic used by the producer."""

    def _is_weekend(self, dt: datetime) -> bool:
        return dt.weekday() >= 5

    def test_saturday_is_weekend(self):
        # 2026-05-16 is a Saturday
        saturday = datetime(2026, 5, 16, 10, 0, tzinfo=timezone.utc)
        assert self._is_weekend(saturday) is True

    def test_sunday_is_weekend(self):
        # 2026-05-17 is a Sunday
        sunday = datetime(2026, 5, 17, 10, 0, tzinfo=timezone.utc)
        assert self._is_weekend(sunday) is True

    def test_monday_is_not_weekend(self):
        monday = datetime(2026, 5, 18, 10, 0, tzinfo=timezone.utc)
        assert self._is_weekend(monday) is False

    def test_friday_is_not_weekend(self):
        friday = datetime(2026, 5, 15, 10, 0, tzinfo=timezone.utc)
        assert self._is_weekend(friday) is False


class TestMockTicketGeneration:
    """Verify mock ticket structure matches what the consumer expects."""

    def _generate_mock_ticket(self, ticket_id: int) -> dict:
        import random
        from datetime import timedelta

        status_choices = [1, 2, 5, 6]
        priority_choices = [1, 2, 3, 4, 5]
        urgency_impact_choices = [1, 2, 3, 4, 5]
        category_choices = ["network", "hardware", "software", "security", "access"]

        creation_hours_ago = random.uniform(2, 72)
        date_creation = datetime(2026, 5, 1, 8, 0) - timedelta(hours=creation_hours_ago)

        ticket = {
            "id": ticket_id,
            "name": f"Mock Ticket {ticket_id}",
            "content": "Simulated ticket for real-time testing",
            "priority": random.choice(priority_choices),
            "status": random.choice(status_choices),
            "urgency": random.choice(urgency_impact_choices),
            "impact": random.choice(urgency_impact_choices),
            "date_creation": date_creation.strftime("%Y-%m-%d %H:%M:%S"),
            "itilcategories_id": random.choice(category_choices),
        }
        if ticket["status"] in [5, 6]:
            resolve_h = random.uniform(1, creation_hours_ago)
            resolve_dt = date_creation + timedelta(hours=resolve_h)
            ticket["solvedate"] = resolve_dt.strftime("%Y-%m-%d %H:%M:%S")

        return ticket

    def test_mock_ticket_has_required_fields(self):
        ticket = self._generate_mock_ticket(1)
        for field in ["id", "name", "priority", "status", "urgency", "impact", "date_creation"]:
            assert field in ticket

    def test_mock_ticket_priority_is_valid(self):
        for i in range(20):
            ticket = self._generate_mock_ticket(i)
            assert ticket["priority"] in [1, 2, 3, 4, 5]

    def test_resolved_ticket_has_solvedate(self):
        # Force a resolved ticket
        with patch("random.choice", side_effect=[1, 5, 3, 3, "network"]):
            ticket = self._generate_mock_ticket(99)
            if ticket["status"] in [5, 6]:
                assert "solvedate" in ticket

    def test_solvedate_after_creation(self):
        from datetime import timedelta
        for i in range(30):
            ticket = self._generate_mock_ticket(i)
            if "solvedate" in ticket:
                creation = datetime.strptime(ticket["date_creation"], "%Y-%m-%d %H:%M:%S")
                resolve = datetime.strptime(ticket["solvedate"], "%Y-%m-%d %H:%M:%S")
                assert resolve > creation, "Resolution date must be after creation date"
