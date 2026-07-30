# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import copy
import itertools
import logging
from typing import Any, Callable, TYPE_CHECKING

from flask import current_app
from flask_babel import _

from superset.common.chart_data import ChartDataResultFormat, ChartDataResultType
from superset.common.db_query_status import QueryStatus
from superset.connectors.sqla.models import BaseDatasource
from superset.exceptions import (
    QueryObjectValidationError,
    SupersetErrorException,
    SupersetErrorsException,
    SupersetParseError,
)
from superset.utils import csv
from superset.utils.core import (
    error_msg_from_exception,
    extract_column_dtype,
    extract_dataframe_dtypes,
    ExtraFiltersReasonType,
    get_column_name,
    get_time_filter_status,
)

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from superset.common.query_context import QueryContext
    from superset.common.query_object import QueryObject


def _get_datasource(
    query_context: QueryContext, query_obj: QueryObject
) -> BaseDatasource:
    return query_obj.datasource or query_context.datasource


def _get_columns(
    query_context: QueryContext, query_obj: QueryObject, _: bool
) -> dict[str, Any]:
    datasource = _get_datasource(query_context, query_obj)
    return {
        "data": [
            {
                "column_name": col.column_name,
                "verbose_name": col.verbose_name,
                "dtype": extract_column_dtype(col),
            }
            for col in datasource.columns
        ]
    }


def _get_timegrains(
    query_context: QueryContext, query_obj: QueryObject, _: bool
) -> dict[str, Any]:
    datasource = _get_datasource(query_context, query_obj)
    return {
        "data": [
            {
                "name": grain.name,
                "function": grain.function,
                "duration": grain.duration,
            }
            for grain in datasource.database.grains()
        ]
    }


def _get_query(
    query_context: QueryContext,
    query_obj: QueryObject,
    _: bool,
) -> dict[str, Any]:
    datasource = _get_datasource(query_context, query_obj)
    result = {"language": datasource.query_language}
    try:
        result["query"] = datasource.get_query_str(query_obj.to_dict())
    except QueryObjectValidationError as err:
        # Validation errors (missing required fields, invalid config)
        # No SQL was generated
        result["error"] = err.message
    except SupersetParseError as err:
        # Parsing errors (SQL optimization/parsing failed)
        # SQL was generated but couldn't be optimized - show both
        if err.error.extra and (sql := err.error.extra.get("sql")) is not None:
            result["query"] = sql
        result["error"] = err.error.message
    return result


_STREAMABLE_CSV_RESULT_TYPES = {
    ChartDataResultType.SAMPLES,
    ChartDataResultType.DRILL_DETAIL,
}


def _can_stream_csv(
    query_context: QueryContext,
    query_obj: QueryObject,
    result_type: ChartDataResultType,
) -> bool:
    datasource = _get_datasource(query_context, query_obj)
    is_streamable_type = result_type in _STREAMABLE_CSV_RESULT_TYPES
    is_csv_format = query_context.result_format == ChartDataResultFormat.CSV
    has_post_processing = bool(query_obj.post_processing)
    has_time_offsets = bool(query_obj.time_offsets)
    has_batches = hasattr(datasource, "get_df_batches")
    eligible = (
        is_streamable_type
        and is_csv_format
        and not has_post_processing
        and not has_time_offsets
        and has_batches
    )
    logger.info(
        "CSV streaming eligibility result_type=%s format=%s eligible=%s "
        "has_post_processing=%s has_time_offsets=%s has_batches=%s",
        result_type,
        query_context.result_format,
        eligible,
        has_post_processing,
        has_time_offsets,
        has_batches,
    )
    if not eligible:
        logger.info(
            "CSV streaming skipped datasource=%s streamable_type=%s",
            type(datasource).__name__,
            is_streamable_type,
        )
    return eligible


def _get_full_streaming_csv(
    query_context: QueryContext,
    query_obj: QueryObject,
    result_type: ChartDataResultType,
) -> dict[str, Any]:
    datasource = _get_datasource(query_context, query_obj)
    chunk_size = current_app.config["CSV_EXPORT"].get("chunksize")
    logger.info(
        "Starting streaming CSV export datasource=%s result_type=%s chunk_size=%s",
        type(datasource).__name__,
        result_type,
        chunk_size,
    )
    payload: dict[str, Any] = {
        "data": None,
        "result_format": query_context.result_format,
        "query": "",
        "status": QueryStatus.SUCCESS,
        "error": None,
        "stacktrace": None,
        "rowcount": None,
        "sql_rowcount": None,
        "colnames": [],
        "indexnames": [],
        "coltypes": [],
        "applied_filters": [],
        "rejected_filters": [],
        "from_dttm": query_obj.from_dttm,
        "to_dttm": query_obj.to_dttm,
        "cache_key": None,
        "cached_dttm": None,
        "cache_timeout": None,
        "is_cached": False,
    }
    try:
        batches, sql = datasource.get_df_batches(
            query_obj.to_dict(), chunk_size=chunk_size
        )
        payload["query"] = sql
        csv_chunks = csv.stream_escaped_csv(
            batches, index=False, **current_app.config["CSV_EXPORT"]
        )
        first_chunk = next(csv_chunks, "")
        payload["data"] = itertools.chain([first_chunk], csv_chunks)
    except (SupersetErrorException, SupersetErrorsException):
        raise
    except Exception as ex:  # pylint: disable=broad-except
        logger.warning("Streaming CSV export failed", exc_info=True)
        payload["status"] = QueryStatus.FAILED
        payload["error"] = error_msg_from_exception(ex)

    if (
        result_type == ChartDataResultType.RESULTS
        and payload["status"] != QueryStatus.FAILED
    ):
        return {
            "data": payload["data"],
            "colnames": payload["colnames"],
            "coltypes": payload["coltypes"],
            "rowcount": payload["rowcount"],
            "sql_rowcount": payload["sql_rowcount"],
        }
    return payload


def _get_full(
    query_context: QueryContext,
    query_obj: QueryObject,
    force_cached: bool | None = False,
) -> dict[str, Any]:
    datasource = _get_datasource(query_context, query_obj)
    result_type = query_obj.result_type or query_context.result_type

    if _can_stream_csv(query_context, query_obj, result_type):
        return _get_full_streaming_csv(query_context, query_obj, result_type)

    payload = query_context.get_df_payload(query_obj, force_cached=force_cached)
    df = payload["df"]
    status = payload["status"]
    if status != QueryStatus.FAILED:
        payload["colnames"] = list(df.columns)
        payload["indexnames"] = list(df.index)
        payload["coltypes"] = extract_dataframe_dtypes(df, datasource)
        payload["data"] = query_context.get_data(df, payload["coltypes"])
        payload["result_format"] = query_context.result_format
    del payload["df"]

    applied_time_columns, rejected_time_columns = get_time_filter_status(
        datasource, query_obj.applied_time_extras
    )

    applied_filter_columns = payload.get("applied_filter_columns", [])
    rejected_filter_columns = payload.get("rejected_filter_columns", [])
    del payload["applied_filter_columns"]
    del payload["rejected_filter_columns"]
    payload["applied_filters"] = [
        {"column": get_column_name(col)} for col in applied_filter_columns
    ] + applied_time_columns
    payload["rejected_filters"] = [
        {
            "reason": ExtraFiltersReasonType.COL_NOT_IN_DATASOURCE,
            "column": get_column_name(col),
        }
        for col in rejected_filter_columns
    ] + rejected_time_columns

    if result_type == ChartDataResultType.RESULTS and status != QueryStatus.FAILED:
        return {
            "data": payload.get("data"),
            "colnames": payload.get("colnames"),
            "coltypes": payload.get("coltypes"),
            "rowcount": payload.get("rowcount"),
            "sql_rowcount": payload.get("sql_rowcount"),
        }
    return payload


def _get_samples(
    query_context: QueryContext, query_obj: QueryObject, force_cached: bool = False
) -> dict[str, Any]:
    datasource = _get_datasource(query_context, query_obj)
    query_obj = copy.copy(query_obj)
    query_obj.is_timeseries = False
    query_obj.orderby = []
    query_obj.metrics = None
    query_obj.post_processing = []
    qry_obj_cols = []
    for o in datasource.columns:
        if isinstance(o, dict):
            qry_obj_cols.append(o.get("column_name"))
        else:
            qry_obj_cols.append(o.column_name)
    query_obj.columns = qry_obj_cols
    query_obj.from_dttm = None
    query_obj.to_dttm = None
    if query_context.result_format not in ChartDataResultFormat.table_like():
        query_obj.row_limit = current_app.config["SAMPLES_ROW_LIMIT"]
    return _get_full(query_context, query_obj, force_cached)


def _get_drill_detail(
    query_context: QueryContext, query_obj: QueryObject, force_cached: bool = False
) -> dict[str, Any]:
    # todo(yongjie): Remove this function,
    #  when determining whether samples should be applied to the time filter.
    datasource = _get_datasource(query_context, query_obj)
    query_obj = copy.copy(query_obj)
    query_obj.is_timeseries = False
    query_obj.metrics = None
    query_obj.post_processing = []
    qry_obj_cols = []
    for o in datasource.columns:
        if isinstance(o, dict):
            qry_obj_cols.append(o.get("column_name"))
        else:
            qry_obj_cols.append(o.column_name)
    query_obj.columns = qry_obj_cols
    query_obj.orderby = [(query_obj.columns[0], True)]
    return _get_full(query_context, query_obj, force_cached)


def _get_results(
    query_context: QueryContext, query_obj: QueryObject, force_cached: bool = False
) -> dict[str, Any]:
    payload = _get_full(query_context, query_obj, force_cached)
    return payload


_result_type_functions: dict[
    ChartDataResultType, Callable[[QueryContext, QueryObject, bool], dict[str, Any]]
] = {
    ChartDataResultType.COLUMNS: _get_columns,
    ChartDataResultType.TIMEGRAINS: _get_timegrains,
    ChartDataResultType.QUERY: _get_query,
    ChartDataResultType.SAMPLES: _get_samples,
    ChartDataResultType.FULL: _get_full,
    ChartDataResultType.RESULTS: _get_results,
    # for requests for post-processed data we return the full results,
    # and post-process it later where we have the chart context, since
    # post-processing is unique to each visualization type
    ChartDataResultType.POST_PROCESSED: _get_full,
    ChartDataResultType.DRILL_DETAIL: _get_drill_detail,
}


def get_query_results(
    result_type: ChartDataResultType,
    query_context: QueryContext,
    query_obj: QueryObject,
    force_cached: bool,
) -> dict[str, Any]:
    """
    Return result payload for a chart data request.

    :param result_type: the type of result to return
    :param query_context: query context to which the query object belongs
    :param query_obj: query object for which to retrieve the results
    :param force_cached: should results be forcefully retrieved from cache
    :raises QueryObjectValidationError: if an unsupported result type is requested
    :return: JSON serializable result payload
    """
    if result_func := _result_type_functions.get(result_type):
        return result_func(query_context, query_obj, force_cached)
    raise QueryObjectValidationError(
        _("Invalid result type: %(result_type)s", result_type=result_type)
    )
