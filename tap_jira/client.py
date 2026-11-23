"""REST client handling, including tap-jiraStream base class."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any, TypeVar

import requests.auth
from singer_sdk.streams import RESTStream
from singer_sdk.streams._state import StreamStateManager

from tap_jira.backfill import (
    BACKFILL_STREAM_NAMES,
    BackfillScope,
    backfill_state_namespace,
    later_of,
    scope_from_config,
)

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable
    from datetime import datetime

    from requests import Response
    from singer_sdk.helpers.types import Context, TapState

    _Auth = Callable[[requests.PreparedRequest], requests.PreparedRequest]


_TNextPageToken = TypeVar("_TNextPageToken")


class ResumableAPIError(Exception):
    """Exception raised for resumable API errors."""

    @override
    def __init__(self, message: str, response: requests.Response) -> None:
        super().__init__(message)
        self.response = response


class JiraStream(RESTStream[_TNextPageToken]):
    """tap-jira stream class."""

    next_page_token_jsonpath = "$.paging.start"  # noqa: S105
    records_jsonpath = "$[*]"  # Or override `parse_response`.
    instance_name: str

    @property
    def backfill_scope(self) -> BackfillScope | None:
        """Return the configured backfill scope for this stream, if any.

        Only streams that take part in a backfill run honor the ``backfill``
        setting; every other stream keeps its ordinary incremental behavior.

        Returns:
            The configured scope, or ``None`` when this stream is not part of a
            backfill.
        """
        if self.name not in BACKFILL_STREAM_NAMES:
            return None
        return scope_from_config(self.config)

    @property
    def backfill_state(self) -> TapState | None:
        """Return the isolated STATE namespace used for backfill progress.

        Returns:
            The backfill namespace, or ``None`` when no backfill is configured.
        """
        scope = self.backfill_scope
        if scope is None:
            return None
        return backfill_state_namespace(self.tap_state, scope)

    def backfill_starting_timestamp(
        self,
        context: Context | None,
    ) -> datetime | None:
        """Return the (page-stable) lower bound of a backfill request.

        During a sync the SDK records the run's starting value in the state
        ``starting_replication_value`` marker once, before paging, so every page
        of the same request uses the same bound.  Before the first marker is
        written (or when a caller asks outside a sync) fall back to the later of
        the window start and any bookmark already persisted for this backfill.

        Args:
            context: Stream partition or context dictionary.

        Returns:
            The effective lower bound, or ``None`` when the backfill is unbounded
            below.
        """
        scope = self.backfill_scope
        if scope is None:
            return None

        state = self.state_manager.get_context_state(context)

        marker = state.get("starting_replication_value")
        if marker is not None:
            return self._parse_datetime(marker)

        bookmark: datetime | None = None
        value = state.get("replication_key_value")
        if value and state.get("replication_key") == self.replication_key:
            bookmark = self._parse_datetime(value)

        return later_of(scope.start_date, bookmark)

    @override
    @property
    def state_manager(self) -> StreamStateManager:
        """Return the state manager, redirected during a backfill run.

        Backfill progress is written to an isolated namespace so that the
        global ``bookmarks`` handed to the incremental target never move.

        Returns:
            The state manager for this stream.
        """
        manager = getattr(self, "_backfill_state_manager", None)
        if manager is not None:
            return manager  # type: ignore[no-any-return]

        scope = self.backfill_scope
        if scope is None:
            return super().state_manager

        manager = StreamStateManager(
            tap_name=self.tap_name,
            stream_name=self.name,
            tap_state=backfill_state_namespace(self.tap_state, scope),
            state_partitioning_keys=self.state_partitioning_keys,
            is_sorted=self.is_sorted,
            check_sorted=self.check_sorted,
        )
        self._backfill_state_manager = manager
        return manager

    @override
    def _write_starting_replication_value(
        self,
        context: Context | None,
    ) -> None:
        """Write the starting replication value, scoped to the backfill.

        Args:
            context: Stream partition or context dictionary.
        """
        scope = self.backfill_scope
        if scope is None:
            super()._write_starting_replication_value(context)
            return

        config = dict(self.config)
        if scope.start_date is not None:
            config["start_date"] = scope.start_date.isoformat()
        else:
            config.pop("start_date", None)

        self.state_manager.write_starting_replication_value(
            context=context,
            replication_method=self.replication_method,
            replication_key=self.replication_key,
            config=config,
            compare_start_date_fn=self.compare_start_date,
        )

    @override
    @property
    def url_base(self) -> str:
        """Returns base url."""
        cloud_id = self.config.get("cloud_id")
        if cloud_id:
            return f"https://api.atlassian.com/ex/jira/{cloud_id}/rest/api/3"
        domain = self.config["domain"]
        return f"https://{domain}/rest/api/3"

    @override
    @property
    def authenticator(self) -> _Auth:
        """Stream authenticator."""
        return requests.auth.HTTPBasicAuth(
            password=self.config["api_token"],
            username=self.config["email"],
        )

    @override
    @property
    def http_headers(self) -> dict[str, str]:
        """Force English-language responses from the Jira Cloud API.

        Without this, error messages (e.g. "The board does not support
        sprints") are localized to the API user's profile language, which
        breaks string-matching in ``validate_response`` overrides.
        See https://developer.atlassian.com/cloud/jira/platform/rest/v3/intro/#special-request-headers
        """
        headers: dict[str, str] = super().http_headers
        headers["Accept-Language"] = "en"
        headers["X-Force-Accept-Language"] = "true"
        return headers

    @override
    def get_records(self, context: Context | None) -> Iterable[dict[str, Any]]:
        try:
            yield from super().get_records(context)
        except ResumableAPIError as e:
            self.logger.warning(e)


class JiraStartAtPaginatedStream(JiraStream[int]):
    """Jira stream that uses the startAt pagination parameter."""

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: int | None,
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict[str, Any] = {}
        if next_page_token:
            params["startAt"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key

        return params

    def get_next_page_token(
        self,
        response: Response,
        previous_token: int | None,
    ) -> int | None:
        """Return a token for identifying next page or None if no more pages."""
        # If pagination is required, return a token which can be used to get the
        #       next page. If this is the final page, return "None" to end the
        #       pagination loop.
        resp_json = response.json()

        if previous_token is None:
            previous_token = 0

        total = -1
        results = 0
        _value = None
        is_last = None

        if (
            isinstance(resp_json, dict)
            and resp_json.get(self.instance_name) is not None
        ):
            _value = resp_json.get(self.instance_name)
            total = resp_json.get("total", -1)
            is_last = resp_json.get("isLast")
            results = len(_value)  # type: ignore[arg-type]

        if isinstance(is_last, bool) and total == -1 and not is_last:
            return previous_token + results

        if _value is None:
            page = resp_json
            if len(page) == 0 or total <= previous_token + results:
                return None
        elif len(_value) == 0 or total <= previous_token + results:
            return None

        return previous_token + results
