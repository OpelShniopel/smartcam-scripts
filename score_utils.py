TEAM_NAME_MAX_LEN = 8
_warned_team_name_truncations: set[tuple[str, str, str]] = set()


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
