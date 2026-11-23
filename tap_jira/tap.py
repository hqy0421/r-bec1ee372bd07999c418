"""tap-jira tap class."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any, cast

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_jira import streams
from tap_jira.backfill import (
    BACKFILL_STATE_KEY,
    BackfillPlan,
    backfill_state_namespace,
    plan_from_config,
)

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Sequence

    from singer_sdk.streams.rest import RESTStream

    from tap_jira.client import JiraStream


class TapJira(Tap):
    """tap-jira tap class."""

    name = "tap-jira"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "start_date",
            th.DateTimeType,
            description="Earliest record date to sync",
        ),
        th.Property(
            "end_date",
            th.DateTimeType,
            description="Latest record date to sync",
        ),
        th.Property(
            "backfill",
            th.ObjectType(
                th.Property(
                    "project",
                    th.StringType,
                    description=(
                        "The Jira project key (or numeric project ID) whose issues "
                        "should be backfilled."
                    ),
                ),
                th.Property(
                    "start_date",
                    th.DateTimeType,
                    description=(
                        "Start of the backfill window. Defaults to the top-level "
                        "'start_date'."
                    ),
                ),
                th.Property(
                    "end_date",
                    th.DateTimeType,
                    description=(
                        "End of the backfill window. Defaults to the top-level "
                        "'end_date'."
                    ),
                ),
            ),
            description=(
                "Plan a one-off backfill of a single project's issues. While a "
                "backfill is configured only the issues stream and its child "
                "streams sync, request and STATE scopes are limited to this window, "
                "and the global incremental bookmark is left untouched."
            ),
        ),
        th.Property(
            "backfill_project",
            th.StringType,
            description="Shorthand for 'backfill.project'.",
        ),
        th.Property(
            "backfill_start_date",
            th.DateTimeType,
            description="Shorthand for 'backfill.start_date'.",
        ),
        th.Property(
            "backfill_end_date",
            th.DateTimeType,
            description="Shorthand for 'backfill.end_date'.",
        ),
        th.Property(
            "domain",
            th.StringType,
            description="The Domain for your Jira account, e.g. meltano.atlassian.net",
            required=True,
        ),
        th.Property(
            "api_token",
            th.StringType,
            description="Jira API Token.",
            required=True,
            secret=True,
            title="API Token",
        ),
        th.Property(
            "email",
            th.StringType,
            description="The user email for your Jira account.",
            required=True,
        ),
        th.Property(
            "cloud_id",
            th.StringType,
            description=(
                "The Cloud ID for your Jira account. Optional - if provided, "
                "uses cloud-based API URLs instead of domain-based URLs."
            ),
            required=False,
        ),
        th.Property(
            "page_size",
            th.ObjectType(
                th.Property(
                    "issues",
                    th.IntegerType,
                    description="Page size for issues stream",
                    default=100,
                ),
            ),
        ),
        th.Property(
            "stream_options",
            th.ObjectType(
                th.Property(
                    "issues",
                    th.ObjectType(
                        th.Property(
                            "jql",
                            th.StringType,
                            description="A JQL query to filter issues",
                            title="JQL Query",
                            default="id != null",
                        ),
                        th.Property(
                            "fields",
                            th.StringType,
                            description="A comma-separated list of fields to include",
                            title="Fields",
                            default="*all",
                        ),
                    ),
                    title="Issues Stream Options",
                    description="Options specific to the issues stream",
                ),
            ),
            description="Options for individual streams",
        ),
        th.Property(
            "include_audit_logs",
            th.BooleanType,
            description="Include the audit logs stream",
            default=False,
        ),
    ).to_dict()

    @override
    def discover_streams(self) -> Sequence[RESTStream[Any]]:
        """Return a list of discovered streams."""
        stream_list: list[JiraStream[Any]] = [
            streams.UsersStream(self),
            streams.FieldStream(self),
            streams.CustomFieldContextStream(self),
            streams.CustomFieldOptionStream(self),
            streams.ServerInfoStream(self),
            streams.IssueTypeStream(self),
            streams.ProjectStream(self),
            streams.ProjectVersionStream(self),
            streams.WorkflowStatusStream(self),
            streams.IssueStream(self),
            streams.PermissionStream(self),
            streams.ProjectRoleStream(self),
            streams.PriorityStream(self),
            streams.PermissionHolderStream(self),
            streams.SprintStream(self),
            streams.ProjectRoleActorStream(self),
            streams.DashboardStream(self),
            streams.FilterStream(self),
            streams.FilterDefaultShareScopeStream(self),
            streams.GroupsPickerStream(self),
            streams.LicenseStream(self),
            streams.ScreensStream(self),
            streams.ScreenSchemesStream(self),
            streams.StatusStream(self),
            streams.WorkflowStream(self),
            streams.Resolutions(self),
            streams.IssueChangeLogStream(self),
            streams.IssueComments(self),
            streams.BoardStream(self),
            streams.IssueWatchersStream(self),
            streams.IssueWorklogs(self),
            streams.ComponentStream(self),
        ]

        if self.config.get("include_audit_logs", False):
            stream_list.append(streams.AuditingStream(self))

        return stream_list

    @override
    def load_state(self, state: dict[str, Any]) -> None:
        """Load state, preserving the isolated backfill namespace.

        The SDK's default loader only understands ``bookmarks``.  A backfill
        tracks its progress under a separate top-level ``backfill`` key, so
        restore that namespace verbatim to allow an interrupted backfill to
        resume without disturbing the global bookmarks.

        Args:
            state: Initialize the tap's state with this value.
        """
        namespace = state.get(BACKFILL_STATE_KEY)
        super().load_state(state)

        if isinstance(namespace, dict):
            cast("dict[str, Any]", self.state)[BACKFILL_STATE_KEY] = namespace

    @property
    def backfill_plan(self) -> BackfillPlan | None:
        """Return the configured one-off backfill plan, if any.

        Returns:
            The resolved backfill plan, or ``None`` for an ordinary sync.

        Raises:
            TypeError: If the ``backfill`` setting is not an object.
            ValueError: If the ``backfill`` settings are malformed.
        """
        return plan_from_config(self.config)

    def _sync_backfill(self, plan: BackfillPlan) -> None:
        """Run a one-off, project-scoped backfill.

        Only the backfilled streams are synced.  Their STATE is tracked in an
        isolated ``backfill`` namespace, so the ordinary ``bookmarks`` handed to
        the incremental target are never modified.

        Args:
            plan: The backfill plan to execute.
        """
        self.logger.info("Planned backfill: %s", plan.describe())

        # Materialize the isolated state namespace up-front so every STATE
        # message emitted during the run carries the backfill scope.
        backfill_state_namespace(self.state, plan.scope)

        # The plan decides exactly which streams run: the issues stream and the
        # streams fetched per issue. Every other stream is deselected.
        for stream in self.streams.values():
            stream.selected = stream.name in plan.streams

        if self.state:
            self._state_writer.write_state(self.state)

        for stream in self.streams.values():
            if not stream.selected and not stream.has_selected_descendents:
                continue
            if stream.parent_stream_type:
                self.logger.debug(
                    "Backfill child stream '%s' is synced by its parent '%s'.",
                    stream.name,
                    stream.parent_stream_type.__name__,
                )
                continue

            stream.sync()
            stream.finalize_state_progress_markers()

        for stream in self.streams.values():
            stream.log_sync_costs()

    def sync_all(self) -> None:  # type: ignore[misc]  # ty: ignore[override-of-final-method]
        """Sync the tap, honoring a configured backfill plan.

        When a ``backfill`` scope is configured only that one-off backfill runs;
        otherwise this is the SDK's ordinary sync.
        """
        plan = self.backfill_plan
        if plan is None:
            super().sync_all()
            return

        self._sync_backfill(plan)
