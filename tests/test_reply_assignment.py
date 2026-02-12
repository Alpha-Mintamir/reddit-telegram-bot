import unittest

from app.workflow.reply_assignment import build_team_members, get_next_member


class ReplyAssignmentTests(unittest.TestCase):
    def test_round_robin_two_members(self):
        rows = [
            {"team_id": "1", "member_name": "A", "is_active": "true"},
            {"team_id": "1", "member_name": "B", "is_active": "true"},
        ]
        tm = build_team_members(rows)
        state = {}
        first, state = get_next_member("1", tm, state)
        second, state = get_next_member("1", tm, state)
        third, state = get_next_member("1", tm, state)
        self.assertEqual(first["member_name"], "A")
        self.assertEqual(second["member_name"], "B")
        self.assertEqual(third["member_name"], "A")


if __name__ == "__main__":
    unittest.main()




