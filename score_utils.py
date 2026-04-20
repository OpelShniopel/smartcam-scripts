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

def default_score_state() -> dict:
    return DEFAULT_SCORE_STATE.copy()


def truncate_team_name(field: str, value, *, log_prefix: str) -> str:
    text = str(value)
    first_word = text.split()[0] if text.strip() else text
    return first_word
