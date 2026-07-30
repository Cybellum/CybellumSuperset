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

import logging
from typing import Any, cast, TypedDict

import pandas as pd
from flask import current_app as app
from flask_babel import gettext as __

from superset import db, results_backend, results_backend_use_msgpack
from superset.commands.base import BaseCommand
from superset.errors import ErrorLevel, SupersetError, SupersetErrorType
from superset.exceptions import SupersetErrorException, SupersetSecurityException
from superset.models.sql_lab import Query
from superset.sql.parse import SQLScript
from superset.sqllab.limiting_factor import LimitingFactor
from superset.utils import core as utils, csv
from superset.views.utils import _deserialize_results_payload

logger = logging.getLogger(__name__)


class SqlExportResult(TypedDict):
    query: Query
    count: int
    data: list[Any]


class SqlResultExportCommand(BaseCommand):
    _client_id: str
    _query: Query

    def __init__(
        self,
        client_id: str,
    ) -> None:
        self._client_id = client_id

    def validate(self) -> None:
        self._query = (
            db.session.query(Query).filter_by(client_id=self._client_id).one_or_none()
        )
        if self._query is None:
            raise SupersetErrorException(
                SupersetError(
                    message=__(
                        "The query associated with these results could not be found. "
                        "You need to re-run the original query."
                    ),
                    error_type=SupersetErrorType.RESULTS_BACKEND_ERROR,
                    level=ErrorLevel.ERROR,
                ),
                status=404,
            )

        try:
            self._query.raise_for_access()
        except SupersetSecurityException as ex:
            raise SupersetErrorException(
                SupersetError(
                    message=__("Cannot access the query"),
                    error_type=SupersetErrorType.QUERY_SECURITY_ACCESS_ERROR,
                    level=ErrorLevel.ERROR,
                ),
                status=403,
            ) from ex

    def run(
        self,
    ) -> SqlExportResult:
        stage = "validation"
        try:
            logger.info("Starting SQL Lab CSV export client_id=%s", self._client_id)
            self.validate()
            logger.info(
                "Validated SQL Lab CSV export client_id=%s query_id=%s "
                "database_id=%s results_key_present=%s",
                self._client_id,
                self._query.id,
                self._query.database_id,
                bool(self._query.results_key),
            )
            blob = None
            if results_backend and self._query.results_key:
                stage = "results_backend_fetch"
                logger.info(
                    "Fetching SQL Lab CSV source from results backend "
                    "client_id=%s query_id=%s results_key=%s",
                    self._client_id,
                    self._query.id,
                    self._query.results_key,
                )
                blob = results_backend.get(self._query.results_key)
                logger.info(
                    "Fetched SQL Lab CSV source from results backend "
                    "client_id=%s query_id=%s blob_bytes=%s",
                    self._client_id,
                    self._query.id,
                    len(blob) if blob else 0,
                )
            if blob:
                stage = "results_decompression"
                logger.info(
                    "Decompressing SQL Lab CSV source client_id=%s query_id=%s "
                    "blob_bytes=%s",
                    self._client_id,
                    self._query.id,
                    len(blob),
                )
                payload = utils.zlib_decompress(
                    blob, decode=not results_backend_use_msgpack
                )
                logger.info(
                    "Decompressed SQL Lab CSV source client_id=%s query_id=%s "
                    "payload_size=%s",
                    self._client_id,
                    self._query.id,
                    len(payload),
                )
                stage = "results_deserialization"
                obj = _deserialize_results_payload(
                    payload, self._query, cast(bool, results_backend_use_msgpack)
                )
                logger.info(
                    "Deserialized SQL Lab CSV source client_id=%s query_id=%s "
                    "rows=%s columns=%s",
                    self._client_id,
                    self._query.id,
                    len(obj["data"]),
                    len(obj["columns"]),
                )

                stage = "dataframe_creation"
                logger.info(
                    "Creating SQL Lab CSV dataframe client_id=%s query_id=%s",
                    self._client_id,
                    self._query.id,
                )
                df = pd.DataFrame(
                    data=obj["data"],
                    dtype=object,
                    columns=[c["name"] for c in obj["columns"]],
                )
                logger.info(
                    "Created SQL Lab CSV dataframe client_id=%s query_id=%s "
                    "rows=%s columns=%s dataframe_bytes=%s",
                    self._client_id,
                    self._query.id,
                    len(df.index),
                    len(df.columns),
                    int(df.memory_usage(index=True, deep=False).sum()),
                )

                stage = "csv_conversion"
                csv_options = app.config["CSV_EXPORT"]
                logger.info(
                    "Converting SQL Lab dataframe to CSV client_id=%s query_id=%s "
                    "rows=%s columns=%s chunksize=%s encoding=%s",
                    self._client_id,
                    self._query.id,
                    len(df.index),
                    len(df.columns),
                    csv_options.get("chunksize"),
                    csv_options.get("encoding", "utf-8"),
                )
                csv_string = csv.df_to_escaped_csv(df, index=False, **csv_options)
                total_rows = len(df.index)
                logger.info(
                    "Converted SQL Lab dataframe to CSV client_id=%s query_id=%s "
                    "csv_characters=%s",
                    self._client_id,
                    self._query.id,
                    len(csv_string),
                )
            else:
                stage = "sql_preparation"
                logger.info(
                    "Preparing SQL Lab CSV query client_id=%s query_id=%s",
                    self._client_id,
                    self._query.id,
                )
                if self._query.select_sql:
                    sql = self._query.select_sql
                    limit = None
                else:
                    sql = self._query.executed_sql
                    script = SQLScript(sql, self._query.database.db_engine_spec.engine)
                    limit = script.statements[-1].get_limit_value()
                if limit is not None and self._query.limiting_factor in {
                    LimitingFactor.QUERY,
                    LimitingFactor.DROPDOWN,
                    LimitingFactor.QUERY_AND_DROPDOWN,
                }:
                    limit -= 1
                stage = "database_query"
                csv_options = app.config["CSV_EXPORT"]
                sql_chunk_size = csv_options.get("chunksize")
                logger.info(
                    "Executing SQL Lab CSV query client_id=%s query_id=%s "
                    "database_id=%s catalog=%s schema=%s sql_length=%s limit=%s "
                    "chunk_size=%s",
                    self._client_id,
                    self._query.id,
                    self._query.database_id,
                    self._query.catalog,
                    self._query.schema,
                    len(sql),
                    limit,
                    sql_chunk_size,
                )

                batch_kwargs = dict(csv_options)
                batch_kwargs.pop("chunksize", None)
                batch_kwargs["index"] = False

                stage = "csv_conversion"
                csv_parts: list[str] = []
                total_rows = 0
                columns_count = 0
                first_batch = True
                for batch_df in self._query.database.stream_dataframe_batches(
                    sql,
                    self._query.catalog,
                    self._query.schema,
                    chunk_size=sql_chunk_size,
                ):
                    if limit is not None:
                        remaining = limit - total_rows
                        if remaining <= 0:
                            break
                        if len(batch_df.index) > remaining:
                            batch_df = batch_df.iloc[:remaining]

                    columns_count = len(batch_df.columns)
                    logger.info(
                        "Converting SQL Lab CSV batch to CSV client_id=%s "
                        "query_id=%s batch_rows=%s total_rows=%s",
                        self._client_id,
                        self._query.id,
                        len(batch_df.index),
                        total_rows + len(batch_df.index),
                    )
                    batch_kwargs["header"] = first_batch
                    csv_parts.append(csv.df_to_escaped_csv(batch_df, **batch_kwargs))
                    total_rows += len(batch_df.index)
                    first_batch = False

                    if limit is not None and total_rows >= limit:
                        break

                csv_string = "".join(csv_parts)
                logger.info(
                    "Converted SQL Lab query to CSV client_id=%s query_id=%s "
                    "rows=%s columns=%s csv_characters=%s",
                    self._client_id,
                    self._query.id,
                    total_rows,
                    columns_count,
                    len(csv_string),
                )

            stage = "csv_encoding"
            encoding = app.config["CSV_EXPORT"].get("encoding", "utf-8")
            csv_data = csv_string.encode(encoding)
            logger.info(
                "Completed SQL Lab CSV export client_id=%s query_id=%s rows=%s "
                "csv_bytes=%s",
                self._client_id,
                self._query.id,
                total_rows,
                len(csv_data),
            )

            return {
                "query": self._query,
                "count": total_rows,
                "data": csv_data,
            }
        except Exception:
            logger.exception(
                "SQL Lab CSV export failed client_id=%s query_id=%s stage=%s",
                self._client_id,
                getattr(getattr(self, "_query", None), "id", None),
                stage,
            )
            raise
