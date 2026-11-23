"""Tests for the one-off, project-scoped backfill."""

from __future__ import annotations

import typing as t
from typing import cast

import pytest
from singer_sdk.io_base import SingerWriter
from singer_sdk.singerlib.encoding.simple import StateMessage

from tap_jira.backfill import (
    BackfillPlan,
    BackfillScope,
    plan_from_config,
    scope_from_config,
)
from tap_jira.streams import (
    IssueChangeLogStream,
    IssueComments,
    IssueStream,
    IssueWatchersStream,
    IssueWorklogs,
)
from tap_jira.tap import TapJira

PROJECT = "ABC"
START = "2024-01-01T00:00:00Z"
END = "2024-02-01T00:00:00Z"

BACKFILL_YEAR = 2024
BACKFILL_END_MONTH = 2
BOOKMARK_YEAR = 2030
BOOKMARK_MONTH = 6

GLOBAL_BOOKMARK = {
    "bookmarks": {
        "issues": {
            "replication_key": "updated",
            "replication_key_value": "2030-06-01T00:00:00+00:00",
        },
    },
}


class RecordingWriter(SingerWriter):
    """Singer writer that keeps every emitted message in memory."""

    def __init__(self) -> None:
        """Initialize the recorder."""
        super().__init__()
        self.messages: list[t.Any] = []

    def write_message(self, message: t.Any, *args: t.Any, **kwargs: t.Any) -> None:  # noqa: ANN401
        """Record and forward a message.

        Args:
            message: The Singer message.
            *args: Positional arguments for the base writer.
            **kwargs: Keyword arguments for the base writer.
        """
        self.messages.append(message)
        super().write_message(message, *args, **kwargs)

    @property
    def state_messages(self) -> list[t.Any]:
        """Return the emitted STATE messages.

        Returns:
            A list of emitted STATE message payloads.
        """
        return [
            message.value
            for message in self.messages
            if isinstance(message, StateMessage)
        ]


def _tap(
    *,
    backfill: dict[str, t.Any] | None = None,
    state: dict[str, t.Any] | None = None,
    **extra: t.Any,
) -> TapJira:
    config: dict[str, t.Any] = {
        "domain": "test.atlassian.net",
        "email": "test@example.com",
        "api_token": "test-token",
    }
    if backfill is not None:
        config["backfill"] = backfill
    config.update(extra)
    return TapJira(config=config, state=state)


def _fake_records(
    monkeypatch: pytest.MonkeyPatch,
) -> list[tuple[str, str | None, t.Any]]:
    """Stub every backfilled stream's HTTP records with one issue.

    Returns:
        A list which is populated with ``(stream_name, jql, context)`` tuples.
    """
    captured: list[tuple[str, str | None, t.Any]] = []

    def fake_get_records(
        self: IssueStream,
        context: t.Any,  # noqa: ANN401
    ) -> t.Iterator[dict[str, t.Any]]:
        params = self.get_url_params(context, None)
        captured.append((self.name, params.get("jql"), context))
        if self.name == "issues":
            yield {
                "id": "10001",
                "fields": {
                    "created": "2024-01-05T00:00:00+00:00",
                    "updated": "2024-01-05T00:00:00+00:00",
                },
            }

    for stream_class in (
        IssueStream,
        IssueChangeLogStream,
        IssueComments,
        IssueWatchersStream,
        IssueWorklogs,
    ):
        monkeypatch.setattr(stream_class, "get_records", fake_get_records)

    return captured


class TestScopeParsing:
    """The ``backfill`` setting must be parsed and validated."""

    def test_nested_scope(self) -> None:
        """A nested ``backfill`` object yields the expected scope."""
        scope = scope_from_config(
            {
                "backfill": {
                    "project": PROJECT,
                    "start_date": START,
                    "end_date": END,
                },
            },
        )

        assert scope is not None
        assert scope.project == PROJECT
        assert scope.start_date is not None
        assert scope.start_date.year == BACKFILL_YEAR
        assert scope.end_date is not None
        assert scope.end_date.month == BACKFILL_END_MONTH

    def test_flat_settings(self) -> None:
        """The flat shorthand settings are honored."""
        scope = scope_from_config(
            {
                "backfill_project": PROJECT,
                "backfill_start_date": START,
                "backfill_end_date": END,
            },
        )

        assert scope is not None
        assert scope.project == PROJECT

    def test_no_backfill(self) -> None:
        """Without any backfill setting no scope is planned."""
        assert scope_from_config({}) is None
        assert plan_from_config({"start_date": START}) is None

    def test_window_falls_back_to_top_level_dates(self) -> None:
        """Omitted window bounds fall back to the tap-level dates."""
        scope = scope_from_config(
            {
                "start_date": START,
                "end_date": END,
                "backfill": {"project": PROJECT},
            },
        )

        assert scope is not None
        assert scope.jql_bounds() == [
            "(created>='2024-01-01 00:00' or updated>='2024-01-01 00:00')",
            "(created<'2024-02-01 00:00' or updated<'2024-02-01 00:00')",
        ]

    def test_missing_project_is_rejected(self) -> None:
        """A backfill without a project is invalid."""
        with pytest.raises(ValueError, match="project"):
            scope_from_config({"backfill": {"start_date": START}})

    def test_bad_datetime_is_rejected(self) -> None:
        """Unparseable datetimes are rejected."""
        with pytest.raises(ValueError, match="ISO 8601"):
            scope_from_config(
                {"backfill": {"project": PROJECT, "start_date": "not-a-date"}},
            )

    def test_reversed_window_is_rejected(self) -> None:
        """A window that ends before it starts is rejected."""
        with pytest.raises(ValueError, match="must not be later"):
            scope_from_config(
                {
                    "backfill": {
                        "project": PROJECT,
                        "start_date": END,
                        "end_date": START,
                    },
                },
            )

    def test_project_key_is_bare(self) -> None:
        """A plain project key needs no quoting."""
        assert BackfillScope(project=PROJECT).project_clause == "project = ABC"

    def test_numeric_project_is_unquoted(self) -> None:
        """Numeric project IDs are emitted without quotes."""
        scope = BackfillScope(project="10001")
        assert scope.project_clause == "project = 10001"

    def test_unusual_project_is_quoted(self) -> None:
        """A project name with spaces is quoted and escaped."""
        scope = BackfillScope(project='My "Odd" Project')
        assert scope.project_clause == 'project = "My \\"Odd\\" Project"'

    def test_plan_describes_streams(self) -> None:
        """The plan names every participating stream."""
        plan = plan_from_config({"backfill": {"project": PROJECT}})
        assert isinstance(plan, BackfillPlan)
        assert "issues" in plan.streams
        assert "issue_comments" in plan.streams
        assert PROJECT in plan.describe()


class TestBackfillRequests:
    """Backfill requests must stay inside the project/time scope."""

    def test_jql_is_project_and_window_scoped(self) -> None:
        """The issues JQL is limited to the project and window."""
        tap = _tap(
            backfill={"project": PROJECT, "start_date": START, "end_date": END},
            state=GLOBAL_BOOKMARK,
        )
        params = IssueStream(tap).get_url_params(context=None, next_page_token=None)

        assert params["jql"] == (
            "project = ABC "
            "and (created>='2024-01-01 00:00' or updated>='2024-01-01 00:00') "
            "and (created<'2024-02-01 00:00' or updated<'2024-02-01 00:00') "
            "and (id != null) order by updated asc"
        )

    def test_global_bookmark_is_not_used_as_lower_bound(self) -> None:
        """The global bookmark must not leak into backfill requests."""
        tap = _tap(
            backfill={"project": PROJECT, "start_date": START, "end_date": END},
            state=GLOBAL_BOOKMARK,
        )
        jql = IssueStream(tap).get_url_params(context=None, next_page_token=None)["jql"]

        assert "2030" not in jql
        assert "2024-01-01 00:00" in jql

    def test_top_level_end_date_is_ignored(self) -> None:
        """The top-level ``end_date`` must not override the backfill window."""
        tap = _tap(
            backfill={"project": PROJECT, "start_date": START, "end_date": END},
            end_date="2024-03-01T00:00:00",
        )
        jql = IssueStream(tap).get_url_params(context=None, next_page_token=None)["jql"]

        assert "2024-02-01 00:00" in jql
        assert "2024-03-01 00:00" not in jql

    def test_child_streams_inherit_scope(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Child streams only run for in-scope issues fetched by the parent."""
        captured = _fake_records(monkeypatch)
        tap = _tap(
            backfill={"project": PROJECT, "start_date": START, "end_date": END},
            state=GLOBAL_BOOKMARK,
        )
        tap.sync_all()

        names = [name for name, _, _ in captured]
        assert "issues" in names
        assert set(names) == {
            "issues",
            "issue_changelog",
            "issue_comments",
            "issue_watchers",
            "issue_worklogs",
        }

        issue_contexts = [context for name, _, context in captured if name != "issues"]
        assert issue_contexts
        assert all(context == {"issue_id": "10001"} for context in issue_contexts)

    def test_paging_lower_bound_is_stable(self) -> None:
        """The lower bound must not advance between pages of one request."""
        tap = _tap(
            backfill={"project": PROJECT, "start_date": START, "end_date": END},
        )
        stream = IssueStream(tap)
        stream._write_starting_replication_value(None)  # noqa: SLF001

        # Simulate the isolated bookmark advancing mid-sync.
        state = stream.state_manager.get_context_state(None)
        state["replication_key"] = "updated"
        state["replication_key_value"] = "2024-01-20T00:00:00+00:00"

        next_page_token = "abc"
        jql = stream.get_url_params(
            context=None,
            next_page_token=next_page_token,
        )["jql"]
        assert "2024-01-01 00:00" in jql
        assert "2024-01-20 00:00" not in jql


class TestBackfillState:
    """Backfill STATE must be scoped and leave the global bookmark alone."""

    def test_global_bookmark_untouched_by_sync(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Running the backfill never advances the global issues bookmark."""
        _fake_records(monkeypatch)
        tap = _tap(
            backfill={"project": PROJECT, "start_date": START, "end_date": END},
            state=GLOBAL_BOOKMARK,
        )
        tap.sync_all()

        assert (
            tap.state["bookmarks"]["issues"] == GLOBAL_BOOKMARK["bookmarks"]["issues"]
        )

    def test_normal_state_unchanged_after_backfill(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The STATE an incremental target resumes from is unchanged."""
        _fake_records(monkeypatch)
        writer = RecordingWriter()
        original = {
            "bookmarks": {
                "issues": {
                    "replication_key": "updated",
                    "replication_key_value": "2030-06-01T00:00:00+00:00",
                },
                "issue_comments": {
                    "replication_key": "created",
                    "replication_key_value": "2031-01-01T00:00:00+00:00",
                },
            },
        }
        tap = TapJira(
            config={
                "domain": "test.atlassian.net",
                "email": "test@example.com",
                "api_token": "test-token",
                "backfill": {
                    "project": PROJECT,
                    "start_date": START,
                    "end_date": END,
                },
            },
            state=original,
            message_writer=writer,
        )
        tap.sync_all()

        # Hand the final STATE back to an ordinary incremental run: the issues
        # stream must resume from the same global bookmark as before.
        resumed = _tap(state=writer.state_messages[-1])
        stream = IssueStream(resumed)
        stream._write_starting_replication_value(None)  # noqa: SLF001

        starting = stream.get_starting_timestamp(None)
        assert starting is not None
        assert starting.year == BOOKMARK_YEAR
        assert starting.month == BOOKMARK_MONTH
        assert (
            resumed.state["bookmarks"]["issue_comments"]["replication_key_value"]
            == "2031-01-01T00:00:00+00:00"
        )

    def test_emitted_state_carries_backfill_scope(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Every emitted STATE reports the backfill scope and its progress."""
        _fake_records(monkeypatch)
        writer = RecordingWriter()
        tap = TapJira(
            config={
                "domain": "test.atlassian.net",
                "email": "test@example.com",
                "api_token": "test-token",
                "backfill": {
                    "project": PROJECT,
                    "start_date": START,
                    "end_date": END,
                },
            },
            state=GLOBAL_BOOKMARK,
            message_writer=writer,
        )
        tap.sync_all()

        states = writer.state_messages
        assert states
        for state in states:
            assert state["backfill"]["scope"]["project"] == PROJECT
            assert state["backfill"]["scope"]["start_date"] == (
                "2024-01-01T00:00:00+00:00"
            )
            # The ordinary bookmarks are untouched in every STATE message.
            assert state["bookmarks"] == GLOBAL_BOOKMARK["bookmarks"]

        final = states[-1]
        assert final["backfill"]["bookmarks"]["issues"]["replication_key_value"] == (
            "2024-01-05T00:00:00+00:00"
        )

    def test_plan_is_written_up_front(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """STATE announces the backfill scope before any records are read."""
        _fake_records(monkeypatch)
        writer = RecordingWriter()
        tap = TapJira(
            config={
                "domain": "test.atlassian.net",
                "email": "test@example.com",
                "api_token": "test-token",
                "backfill": {"project": PROJECT, "start_date": START},
            },
            state=GLOBAL_BOOKMARK,
            message_writer=writer,
        )
        tap.sync_all()

        first_state = writer.state_messages[0]
        assert first_state["backfill"]["scope"]["project"] == PROJECT
        assert "bookmarks" in first_state["backfill"]

    def test_no_backfill_keeps_ordinary_behavior(self) -> None:
        """Without a plan the stream uses the global bookmark as before."""
        tap = _tap(state=GLOBAL_BOOKMARK)
        stream = IssueStream(tap)

        assert stream.backfill_scope is None
        stream._write_starting_replication_value(None)  # noqa: SLF001

        starting = stream.get_starting_timestamp(None)
        assert starting is not None
        assert starting.year == BOOKMARK_YEAR
        # The starting marker was written to the ordinary global bookmarks.
        assert (
            tap.state["bookmarks"]["issues"]["starting_replication_value"]
            == "2030-06-01T00:00:00+00:00"
        )

    def test_stale_progress_from_other_scope_is_discarded(self) -> None:
        """Progress from a differently scoped backfill is not resumed."""
        tap = _tap(
            backfill={"project": PROJECT, "start_date": START, "end_date": END},
            state={
                "bookmarks": GLOBAL_BOOKMARK["bookmarks"],
                "backfill": {
                    "scope": {
                        "project": "OTHER",
                        "start_date": "2020-01-01T00:00:00+00:00",
                    },
                    "bookmarks": {
                        "issues": {
                            "replication_key": "updated",
                            "replication_key_value": "2020-01-02T00:00:00+00:00",
                        },
                    },
                },
            },
        )
        stream = IssueStream(tap)
        state = stream.backfill_state

        assert state is not None
        state_dict = cast("dict[str, t.Any]", state)
        assert state_dict["scope"]["project"] == PROJECT
        assert state_dict["bookmarks"] == {}

    def test_matching_progress_is_resumed(self) -> None:
        """A matching scope resumes from the isolated bookmark."""
        progress = "2024-01-15T00:00:00+00:00"
        tap = _tap(
            backfill={"project": PROJECT, "start_date": START, "end_date": END},
            state={
                "backfill": {
                    "scope": {
                        "project": PROJECT,
                        "start_date": START,
                        "end_date": END,
                    },
                    "bookmarks": {
                        "issues": {
                            "replication_key": "updated",
                            "replication_key_value": progress,
                        },
                    },
                },
            },
        )
        jql = IssueStream(tap).get_url_params(context=None, next_page_token=None)["jql"]

        assert "2024-01-15 00:00" in jql
        assert "2024-01-01 00:00" not in jql

    def test_only_backfill_streams_are_selected(self) -> None:
        """A backfill selects just the issues stream and its children."""
        tap = _tap(backfill={"project": PROJECT})
        plan = tap.backfill_plan

        assert plan is not None
        selected = set(plan.streams)
        assert "users" not in selected
        assert "projects" not in selected
