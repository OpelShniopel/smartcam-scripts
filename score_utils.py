DEFAULT_SCORE_STATE = {
    "home_name": "HOME",
    "away_name": "AWAY",
    "home_points": 0,
    "away_points": 0,
    "home_fouls": 0,
    "away_fouls": 0,
    "home_timeouts": 3,
    "away_timeouts": 3,
    "quarter": 1,
    "clock": "10:00",
    "visible": False,
    "game_id": 0,
    "updated_at": 0,
    "milestone": None,
}

TEAM_NAME_MAX_LEN = 8
_warned_team_name_truncations: set[tuple[str, str, str]] = set()


def default_score_state() -> dict:
    return DEFAULT_SCORE_STATE.copy()


def truncate_team_name(field: str, value, *, log_prefix: str) -> str:
    text = str(value)
    if len(text) <= TEAM_NAME_MAX_LEN:
        return text

    truncated = text[:TEAM_NAME_MAX_LEN]
    warning_key = (log_prefix, field, text)
    if warning_key not in _warned_team_name_truncations:
        _warned_team_name_truncations.add(warning_key)
        print(
            f"{log_prefix} WARNING: {field} truncated to {TEAM_NAME_MAX_LEN} chars: "
            f"{text!r} -> {truncated!r}"
        )
    return truncated
