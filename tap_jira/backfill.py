"""Planning for a one-off, project-scoped issue backfill.

A backfill replays the ``issues`` stream (and the streams fetched per issue)
for a single Jira project over a bounded time range.  Unlike a regular
incremental sync it must not consume the tap's global bookmark: the backfill's
progress is tracked in an isolated state namespace, so the ordinary
``bookmarks`` handed to the incremental target are left exactly as they were.

The scope is configured with a ``backfill`` object::

    {
      "backfill": {
        "project": "ABC",
        "start_date": "2024-01-01T00:00:00Z",
        "end_date": "2024-02-01T00:00:00Z"
      }
    }

For convenience the same scope may be supplied as the flat settings
``backfill_project``, ``backfill_start_date`` and ``backfill_end_date``.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from singer_sdk.helpers.types import TapState

#: Jira project keys are letters/digits/underscores; numeric project IDs are also
#: valid JQL literals and need no quoting.
_SIMPLE_PROJECT_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*|[0-9]+")

#: The streams that participate in a backfill run: the ``issues`` stream itself
#: plus every stream that is fetched per issue.
BACKFILL_STREAM_NAMES: tuple[str, ...] = (
    "issues",
    "issue_changelog",
    "issue_comments",
    "issue_watchers",
    "issue_worklogs",
)

#: Top-level STATE key under which backfill progress is isolated.
BACKFILL_STATE_KEY = "backfill"

#: JQL only understands minute precision.
_JQL_DATETIME_FORMAT = "%Y-%m-%d %H:%M"


def parse_datetime(value: Any, *, field: str) -> datetime | None:  # noqa: ANN401
    """Parse a configured backfill boundary.

    Args:
        value: A ``datetime``, an ISO 8601 string, or ``None``.
        field: Setting name, used in error messages.

    Returns:
        The parsed datetime, or ``None`` when no value was supplied.

    Raises:
        TypeError: If the value is not a string or datetime.
        ValueError: If the value is not a parseable datetime.
    """
    if value is None or value == "":
        return None

    if isinstance(value, datetime):
        return value

    if not isinstance(value, str):
        msg = f"The 'backfill.{field}' setting must be an ISO 8601 datetime."
        raise TypeError(msg)

    text = value.strip()
    if not text:
        return None

    # ``datetime.fromisoformat`` only accepts a trailing ``Z`` from 3.11 onward.
    if text.endswith(("Z", "z")):
        text = f"{text[:-1]}+00:00"

    try:
        return datetime.fromisoformat(text)
    except ValueError as exc:
        msg = (
            f"The 'backfill.{field}' setting is not a valid ISO 8601 datetime: "
            f"{value!r}."
        )
        raise ValueError(msg) from exc


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def first_set(mapping: Mapping[str, Any], *keys: str) -> Any:  # noqa: ANN401
    """Return the first present, non-``None`` value among ``keys``.

    Unlike ``dict.get(key, default)``, an explicit ``null`` for an earlier key
    does not shadow a later alias.

    Args:
        mapping: The mapping to read.
        *keys: Candidate keys, in priority order.

    Returns:
        The first available value, or ``None``.
    """
    for key in keys:
        value = mapping.get(key)
        if value is not None:
            return value
    return None


def later_of(first: datetime | None, second: datetime | None) -> datetime | None:
    """Return the later of two datetimes, treating naive values as UTC.

    Args:
        first: The first candidate.
        second: The second candidate.

    Returns:
        The later candidate, or ``None`` when both are ``None``.
    """
    if first is None:
        return second
    if second is None:
        return first

    first_utc = _as_utc(first)
    second_utc = _as_utc(second)
    assert first_utc is not None  # noqa: S101
    assert second_utc is not None  # noqa: S101
    return first if first_utc >= second_utc else second


def format_jql_datetime(value: datetime) -> str:
    """Render a datetime the way the rest of the tap renders JQL literals."""
    return value.strftime(_JQL_DATETIME_FORMAT)


def _quote_project(project: str) -> str:
    """Render a project key or ID as a JQL literal.

    Plain Jira project keys (letters, digits and underscores) and numeric
    project IDs are emitted bare; anything else is quoted and escaped.
    """
    if _SIMPLE_PROJECT_RE.fullmatch(project):
        return project
    escaped = project.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


@dataclass(frozen=True)
class BackfillScope:
    """A single project and (optionally) bounded time window to backfill."""

    project: str
    start_date: datetime | None = None
    end_date: datetime | None = None

    @classmethod
    def from_config(cls, raw: Any) -> BackfillScope | None:  # noqa: ANN401
        """Build a scope from a ``backfill`` configuration object.

        Args:
            raw: The ``backfill`` setting, or ``None`` when disabled.

        Returns:
            The parsed scope, or ``None`` when no backfill is configured.

        Raises:
            TypeError: If the setting is not an object.
            ValueError: If the setting is otherwise malformed.
        """
        if not raw:
            return None

        if not isinstance(raw, Mapping):
            msg = "The 'backfill' setting must be an object."
            raise TypeError(msg)

        project = (
            raw.get("project")
            or raw.get("project_key")
            or raw.get("project_id")
            or raw.get("key")
        )
        if not isinstance(project, str) or not project.strip():
            msg = (
                "The 'backfill' setting requires a non-empty 'project' "
                "(or 'project_key')."
            )
            raise ValueError(msg)

        start_date = parse_datetime(
            first_set(raw, "start_date", "since"),
            field="start_date",
        )
        end_date = parse_datetime(
            first_set(raw, "end_date", "until"),
            field="end_date",
        )

        start_utc = _as_utc(start_date)
        end_utc = _as_utc(end_date)
        if start_utc is not None and end_utc is not None and start_utc > end_utc:
            msg = (
                "The 'backfill.start_date' setting must not be later than "
                "'backfill.end_date'."
            )
            raise ValueError(msg)

        return cls(
            project=project.strip(),
            start_date=start_date,
            end_date=end_date,
        )

    @property
    def project_clause(self) -> str:
        """Return the JQL clause that limits the backfill to one project."""
        return f"project = {_quote_project(self.project)}"

    def jql_bounds(self) -> list[str]:
        """Return JQL clauses for the configured time window."""
        clauses: list[str] = []

        if self.start_date is not None:
            start = format_jql_datetime(self.start_date)
            clauses.append(f"(created>='{start}' or updated>='{start}')")

        if self.end_date is not None:
            end = format_jql_datetime(self.end_date)
            clauses.append(f"(created<'{end}' or updated<'{end}')")

        return clauses

    def jql(self) -> str:
        """Return the scope as a JQL expression."""
        return " and ".join([self.project_clause, *self.jql_bounds()])

    def to_state(self) -> dict[str, Any]:
        """Return the JSON-compatible scope persisted in STATE messages."""
        state: dict[str, Any] = {"project": self.project}
        if self.start_date is not None:
            state["start_date"] = self.start_date.isoformat()
        if self.end_date is not None:
            state["end_date"] = self.end_date.isoformat()
        return state

    def matches(self, other: Mapping[str, Any]) -> bool:
        """Return whether a persisted state scope describes this backfill.

        Used to discard progress left behind by a differently scoped backfill.
        """
        try:
            candidate = type(self).from_config(other)
        except (TypeError, ValueError):
            return False

        if candidate is None:
            return False

        return (
            candidate.project == self.project
            and _as_utc(candidate.start_date) == _as_utc(self.start_date)
            and _as_utc(candidate.end_date) == _as_utc(self.end_date)
        )


@dataclass(frozen=True)
class BackfillPlan:
    """The resolved plan for a one-off backfill run."""

    scope: BackfillScope
    streams: tuple[str, ...] = BACKFILL_STREAM_NAMES

    @property
    def jql(self) -> str:
        """Return the scope filter as a JQL expression."""
        return self.scope.jql()

    def describe(self) -> str:
        """Return a human-readable summary of the plan."""
        return (
            f"project={self.scope.project!r} "
            f"start_date={self.scope.start_date!s} "
            f"end_date={self.scope.end_date!s} "
            f"streams={list(self.streams)} jql={self.jql!r}"
        )


def scope_from_config(config: Mapping[str, Any]) -> BackfillScope | None:
    """Resolve the backfill scope from a tap or stream configuration.

    Supports the nested ``backfill`` object as well as the flat
    ``backfill_project`` / ``backfill_start_date`` / ``backfill_end_date``
    settings.  When the backfill window omits a boundary, the tap-level
    ``start_date`` / ``end_date`` is used instead.

    Args:
        config: The tap or stream configuration.

    Returns:
        The configured scope, or ``None`` when no backfill is requested.

    Raises:
        TypeError: If the setting is not an object.
        ValueError: If the configured scope is malformed.
    """
    raw = config.get("backfill")
    if not raw:
        project = config.get("backfill_project")
        if not project:
            return None
        raw = {
            "project": project,
            "start_date": config.get("backfill_start_date"),
            "end_date": config.get("backfill_end_date"),
        }
    elif not isinstance(raw, Mapping):
        msg = "The 'backfill' setting must be an object."
        raise TypeError(msg)

    merged = dict(raw)
    if first_set(merged, "start_date", "since") is None and config.get("start_date"):
        merged["start_date"] = config["start_date"]
    if first_set(merged, "end_date", "until") is None and config.get("end_date"):
        merged["end_date"] = config["end_date"]

    return BackfillScope.from_config(merged)


def plan_from_config(config: Mapping[str, Any]) -> BackfillPlan | None:
    """Resolve the backfill plan from a tap or stream configuration.

    Args:
        config: The tap or stream configuration.

    Returns:
        The configured plan, or ``None`` when no backfill is requested.

    Raises:
        TypeError: If the setting is not an object.
        ValueError: If the configured scope is malformed.
    """
    scope = scope_from_config(config)
    return BackfillPlan(scope=scope) if scope else None


def backfill_state_namespace(tap_state: TapState, scope: BackfillScope) -> TapState:
    """Return the isolated state namespace for a backfill run.

    The namespace lives under ``state["backfill"]`` and holds the resolved
    ``scope`` plus a ``bookmarks`` subtree for the participating streams.  The
    ordinary ``state["bookmarks"]`` tree is never touched.

    Progress left behind by a differently scoped backfill is discarded.

    Args:
        tap_state: The full tap state dictionary.
        scope: The scope of the current backfill.

    Returns:
        A writable tap-state-shaped dictionary for backfill progress.
    """
    # ``TapState`` only declares ``bookmarks``; cast so the extra top-level
    # ``backfill`` key can be read and written without a TypedDict complaint.
    state = cast("dict[str, Any]", tap_state)

    namespace = state.get(BACKFILL_STATE_KEY)
    if not isinstance(namespace, dict):
        namespace = {}
        state[BACKFILL_STATE_KEY] = namespace

    stored_scope = namespace.get("scope")
    if not (
        isinstance(stored_scope, Mapping) and scope.matches(stored_scope)
    ):  # pragma: no branch
        namespace.clear()
        namespace["scope"] = scope.to_state()

    namespace.setdefault("bookmarks", {})
    return cast("TapState", namespace)
