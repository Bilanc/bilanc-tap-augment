import collections
import json
import os
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests
import singer
from singer import bookmarks, metadata, metrics

session = requests.Session()
logger = singer.get_logger()

# API and sync controls are fixed in code to match existing tap patterns.
BASE_URL: str = "https://api.augmentcode.com"
STREAM_NAME: str = "user_activity_daily"
REQUEST_TIMEOUT_SECONDS: int = 60
MAX_PAGE_SIZE: int = 100
AUGMENT_DAILY_READY_HOUR_UTC: int = 3

REQUIRED_CONFIG_KEYS: List[str] = ["start_date"]
KEY_PROPERTIES: Dict[str, List[str]] = {STREAM_NAME: ["date", "user_email"]}
SUB_STREAMS: Dict[str, List[str]] = {}


class DependencyException(Exception):
    pass


class AugmentException(Exception):
    pass


class BadCredentialsException(AugmentException):
    pass


@singer.utils.handle_top_exception(logger)
def main() -> None:
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    if args.discover:
        do_discover(args.config)
    else:
        catalog = args.properties if args.properties else get_catalog()
        do_sync(args.config, args.state, catalog)


def do_sync(
    config: Dict[str, Any], state: Dict[str, Any], catalog: Dict[str, Any]
) -> None:
    api_key: Optional[str] = config.get("api_key")
    if not api_key:
        raise BadCredentialsException("No API key provided in config.")
    start_date_value: Optional[str] = config.get("start_date")
    selected_stream_ids: List[str] = get_selected_streams(catalog)
    validate_dependencies(selected_stream_ids)

    state = translate_state(state, catalog)
    singer.write_state(state)

    # Execute sync for selected streams only.
    for stream in catalog["streams"]:
        stream_id: str = stream["tap_stream_id"]
        stream_schema: Dict[str, Any] = stream["schema"]
        mdata: List[Dict[str, Any]] = stream["metadata"]

        if not SYNC_FUNCTIONS.get(stream_id):
            continue

        if stream_id in selected_stream_ids:
            singer.write_schema(stream_id, stream_schema, stream["key_properties"])
            sync_func = SYNC_FUNCTIONS[stream_id]
            sub_stream_ids = SUB_STREAMS.get(stream_id, None)

            if not sub_stream_ids:
                state = sync_func(
                    stream_schema, state, mdata, start_date_value, api_key
                )
            else:
                # Generic sub-stream branch kept to stay consistent with shared Singer tap structure.
                stream_schemas: Dict[str, Dict[str, Any]] = {stream_id: stream_schema}
                stream_mdata: Dict[str, List[Dict[str, Any]]] = {stream_id: mdata}
                for sub_stream_id in sub_stream_ids:
                    if sub_stream_id in selected_stream_ids:
                        sub_stream = get_stream_from_catalog(sub_stream_id, catalog)
                        if not sub_stream:
                            continue
                        stream_schemas[sub_stream_id] = sub_stream["schema"]
                        stream_mdata[sub_stream_id] = sub_stream["metadata"]
                        singer.write_schema(
                            sub_stream_id,
                            sub_stream["schema"],
                            sub_stream["key_properties"],
                        )
                state = sync_func(
                    stream_schemas, state, stream_mdata, start_date_value, api_key
                )
            singer.write_state(state)


def do_discover(config: Dict[str, Any]) -> None:
    # Emit catalog JSON for discovery mode.
    _ = config
    catalog = get_catalog()
    print(json.dumps(catalog, indent=2))


def get_selected_streams(catalog: Dict[str, Any]) -> List[str]:
    # Preserve shared selection behavior used by other taps and runner scripts.
    selected_streams: List[str] = []
    for stream in catalog["streams"]:
        stream_metadata = stream["metadata"]
        if stream["schema"].get("selected", False):
            selected_streams.append(stream["tap_stream_id"])
        else:
            for entry in stream_metadata:
                if not entry["breadcrumb"] and entry["metadata"].get("selected", None):
                    selected_streams.append(stream["tap_stream_id"])

    return selected_streams


def translate_state(state: Dict[str, Any], catalog: Dict[str, Any]) -> Dict[str, Any]:
    # Normalize state into singer's expected nested bookmark shape.
    nested_dict = lambda: collections.defaultdict(nested_dict)
    new_state: Dict[str, Any] = nested_dict()

    for stream in catalog["streams"]:
        stream_name = stream["tap_stream_id"]
        if bookmarks.get_bookmark(state, stream_name, "since"):
            new_state["bookmarks"][stream_name]["since"] = bookmarks.get_bookmark(
                state, stream_name, "since"
            )

    return new_state


def validate_dependencies(selected_stream_ids: List[str]) -> None:
    # Kept for compatibility with shared tap scaffolding even though this tap has no sub-streams.
    errs: List[str] = []
    msg_tmpl = (
        "Unable to extract '{0}' data, "
        "to receive '{0}' data, you also need to select '{1}'."
    )

    for main_stream, sub_streams in SUB_STREAMS.items():
        if main_stream not in selected_stream_ids:
            for sub_stream in sub_streams:
                if sub_stream in selected_stream_ids:
                    errs.append(msg_tmpl.format(sub_stream, main_stream))

    if errs:
        raise DependencyException(" ".join(errs))


def get_stream_from_catalog(
    stream_id: str, catalog: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    for stream in catalog["streams"]:
        if stream["tap_stream_id"] == stream_id:
            return stream
    return None


def get_catalog() -> Dict[str, Any]:
    # Build Singer catalog entries from loaded schemas and metadata.
    raw_schemas = load_schemas()
    streams: List[Dict[str, Any]] = []

    for schema_name, schema in raw_schemas.items():
        mdata = populate_metadata(schema_name, schema)
        catalog_entry = {
            "stream": schema_name,
            "tap_stream_id": schema_name,
            "schema": schema,
            "metadata": metadata.to_list(mdata),
            "key_properties": KEY_PROPERTIES[schema_name],
        }
        streams.append(catalog_entry)

    return {"streams": streams}


def get_user_activity_daily(
    schema: Dict[str, Any],
    state: Dict[str, Any],
    mdata: List[Dict[str, Any]],
    start_date_value: str,
    api_key: str,
) -> Dict[str, Any]:
    # Sync daily per-user usage records from Augment.
    bookmark_value: str = get_bookmark(state, STREAM_NAME, "since", start_date_value)
    next_sync_date: date = singer.utils.strptime_to_utc(bookmark_value).date()

    # Augment documents that previous-day analytics become queryable around 02:00 UTC.
    # We use a 03:00 UTC cutoff to only query for data that is available.
    # Docs: https://docs.augmentcode.com/analytics/api-reference
    now_utc: datetime = datetime.now(timezone.utc)
    if now_utc.hour < AUGMENT_DAILY_READY_HOUR_UTC:
        latest_fully_finalized_analytics_date_utc: date = now_utc.date() - timedelta(
            days=2
        )
    else:
        latest_fully_finalized_analytics_date_utc = now_utc.date() - timedelta(days=1)

    # If the next bookmark day is beyond yesterday UTC, we have already synced through the latest day
    # we are confident is available from Augment's daily analytics.
    if next_sync_date > latest_fully_finalized_analytics_date_utc:
        logger.info(
            "No Augment data to sync. next_sync_date=%s latest_fully_finalized_analytics_date_utc=%s",
            next_sync_date,
            latest_fully_finalized_analytics_date_utc,
        )
        return state

    with metrics.record_counter(STREAM_NAME) as counter:
        # Iterate day-by-day through the latest finalized report day (yesterday UTC).
        while next_sync_date <= latest_fully_finalized_analytics_date_utc:
            sync_date_value: str = next_sync_date.isoformat()
            cursor: Optional[str] = None

            while True:
                # Page through all user activity results for this day.
                response_data: Dict[str, Any] = _request_user_activity_page(
                    api_key=api_key,
                    start_date_value=sync_date_value,
                    end_date_value=sync_date_value,
                    cursor=cursor,
                )
                users: List[Dict[str, Any]] = response_data.get("users", [])
                extraction_time: str = singer.utils.now()

                for user in users:
                    user_email: Optional[str] = user.get("user_email")

                    if not user_email:
                        continue

                    metrics_payload: Dict[str, Any] = user.get("metrics") or {}
                    # Flatten nested metrics from the API response.
                    record: Dict[str, Any] = {
                        "date": sync_date_value,
                        "user_email": user_email,
                        "active_days": user.get("active_days"),
                        "total_modified_lines_of_code": metrics_payload.get(
                            "total_modified_lines_of_code"
                        ),
                        "total_messages": metrics_payload.get("total_messages"),
                        "total_tool_calls": metrics_payload.get("total_tool_calls"),
                        "completions_count": metrics_payload.get("completions_count"),
                        "completions_accepted": metrics_payload.get(
                            "completions_accepted"
                        ),
                        "completions_lines_of_code": metrics_payload.get(
                            "completions_lines_of_code"
                        ),
                        "completions_suggested_lines_of_code": metrics_payload.get(
                            "completions_suggested_lines_of_code"
                        ),
                        "chat_messages": metrics_payload.get("chat_messages"),
                        "remote_agent_messages": metrics_payload.get(
                            "remote_agent_messages"
                        ),
                        "remote_agent_lines_of_code": metrics_payload.get(
                            "remote_agent_lines_of_code"
                        ),
                        "ide_agent_messages": metrics_payload.get("ide_agent_messages"),
                        "ide_agent_lines_of_code": metrics_payload.get(
                            "ide_agent_lines_of_code"
                        ),
                        "cli_agent_interactive_messages": metrics_payload.get(
                            "cli_agent_interactive_messages"
                        ),
                        "cli_agent_interactive_lines_of_code": metrics_payload.get(
                            "cli_agent_interactive_lines_of_code"
                        ),
                        "cli_agent_non_interactive_messages": metrics_payload.get(
                            "cli_agent_non_interactive_messages"
                        ),
                        "cli_agent_non_interactive_lines_of_code": metrics_payload.get(
                            "cli_agent_non_interactive_lines_of_code"
                        ),
                    }
                    record["inserted_at"] = singer.utils.strftime(extraction_time)

                    try:
                        # Apply schema-based coercion before writing Singer records.
                        with singer.Transformer() as transformer:
                            transformed_record: Dict[str, Any] = transformer.transform(
                                record,
                                schema,
                                metadata=metadata.to_map(mdata),
                            )
                    except Exception:
                        logger.exception("Failed to transform record [%s]", record)
                        raise

                    singer.write_record(
                        STREAM_NAME, transformed_record, time_extracted=extraction_time
                    )
                    counter.increment()

                pagination: Dict[str, Any] = response_data.get("pagination") or {}
                has_more: bool = bool(pagination.get("has_more"))
                cursor = pagination.get("next_cursor")

                if not has_more or not cursor:
                    break

            # Advance bookmark to the next day after a full day is completely paged.
            following_sync_date: date = next_sync_date + timedelta(days=1)
            singer.write_bookmark(
                state, STREAM_NAME, "since", following_sync_date.isoformat()
            )
            singer.write_state(state)
            next_sync_date = following_sync_date

    return state


def get_bookmark(
    state: Dict[str, Any], stream_name: str, bookmark_key: str, start_date: str
) -> str:
    # Start from persisted bookmark when present, otherwise use configured start date.
    stream_dict = bookmarks.get_bookmark(state, stream_name, bookmark_key)
    if stream_dict:
        return stream_dict
    return start_date


def _request_user_activity_page(
    api_key: str,
    start_date_value: str,
    end_date_value: str,
    cursor: Optional[str],
) -> Dict[str, Any]:
    # Request one page from Augment's user-activity endpoint for a single day.
    headers: Dict[str, str] = {"Authorization": f"Bearer {api_key}"}
    params: Dict[str, Any] = {
        "start_date": start_date_value,
        "end_date": end_date_value,
        "page_size": MAX_PAGE_SIZE,
    }
    if cursor:
        params["cursor"] = cursor

    url: str = f"{BASE_URL}/analytics/v0/user-activity"

    # V1 behavior: single request per page, fail fast on non-2xx.
    with metrics.http_request_timer(STREAM_NAME) as timer:
        response: requests.Response = session.get(
            url=url,
            headers=headers,
            params=params,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        timer.tags[metrics.Tag.http_status_code] = response.status_code

    response.raise_for_status()
    return response.json()


def load_schemas() -> Dict[str, Dict[str, Any]]:
    # Load all local JSON schemas so discover/sync stay schema-driven.
    schemas: Dict[str, Dict[str, Any]] = {}

    for filename in os.listdir(get_abs_path("schemas")):
        path = os.path.join(get_abs_path("schemas"), filename)
        file_raw = filename.replace(".json", "")
        with open(path, encoding="utf-8") as file:
            schemas[file_raw] = json.load(file)

    return schemas


def populate_metadata(schema_name: str, schema: Dict[str, Any]) -> Dict[str, Any]:
    # Mark key properties as automatic and all others as available for Singer discovery.
    mdata = metadata.new()
    mdata = metadata.write(
        mdata, (), "table-key-properties", KEY_PROPERTIES[schema_name]
    )

    for field_name in schema["properties"].keys():
        if field_name in KEY_PROPERTIES[schema_name]:
            mdata = metadata.write(
                mdata, ("properties", field_name), "inclusion", "automatic"
            )
        else:
            mdata = metadata.write(
                mdata, ("properties", field_name), "inclusion", "available"
            )

    return mdata


def get_abs_path(path: str) -> str:
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


SYNC_FUNCTIONS = {
    STREAM_NAME: get_user_activity_daily,
}


if __name__ == "__main__":
    main()
