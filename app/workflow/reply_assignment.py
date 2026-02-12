from __future__ import annotations

from typing import Dict, List, Sequence, Tuple


def build_team_members(teams_rows: Sequence[Dict[str, str]]) -> Dict[str, List[Dict[str, str]]]:
    team_map: Dict[str, List[Dict[str, str]]] = {}
    for row in teams_rows:
        team_id = row.get("team_id", "").strip()
        if not team_id:
            continue
        is_active = row.get("is_active", "true").strip().lower() in {"1", "true", "yes", "y", "active"}
        if not is_active:
            continue
        team_map.setdefault(team_id, []).append(row)
    return team_map


def get_next_member(
    team_id: str,
    team_members: Dict[str, List[Dict[str, str]]],
    state: Dict[str, str],
) -> Tuple[Dict[str, str], Dict[str, str]]:
    members = team_members.get(team_id, [])
    if not members:
        raise ValueError(f"No active members found for team_id={team_id}")

    key = f"reply_cursor_team_{team_id}"
    current_index = int(state.get(key, "0") or "0")
    selected = members[current_index % len(members)]
    next_index = (current_index + 1) % len(members)

    new_state = dict(state)
    new_state[key] = str(next_index)
    return selected, new_state


def filter_unseen_comments(comments: Sequence[Dict[str, str]], known_comment_ids: set[str]) -> List[Dict[str, str]]:
    return [c for c in comments if c.get("comment_id") and c["comment_id"] not in known_comment_ids]




