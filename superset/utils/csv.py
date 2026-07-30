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
import io
import logging
import re
import urllib.request
from typing import Any, Iterable, Iterator, Optional, Union
from urllib.error import URLError

import numpy as np
import pandas as pd

from superset.utils import json
from superset.utils.core import GenericDataType

logger = logging.getLogger(__name__)

negative_number_re = re.compile(r"^-[0-9.]+$")

# This regex will match if the string starts with:
#
#     1. one of -, @, +, |, =, %
#     2. two double quotes immediately followed by one of -, @, +, |, =, %
#     3. one or more spaces immediately followed by one of -, @, +, |, =, %
#
problematic_chars_re = re.compile(r'^(?:"{2}|\s{1,})(?=[\-@+|=%])|^[\-@+|=%]')


def escape_value(value: str) -> str:
    """
    Escapes a set of special characters.

    http://georgemauer.net/2017/10/07/csv-injection.html
    """
    needs_escaping = problematic_chars_re.match(value) is not None
    is_negative_number = negative_number_re.match(value) is not None

    if needs_escaping and not is_negative_number:
        # Escape pipe to be extra safe as this
        # can lead to remote code execution
        value = value.replace("|", "\\|")

        # Precede the line with a single quote. This prevents
        # evaluation of commands and some spreadsheet software
        # will hide this visually from the user. Many articles
        # claim a preceding space will work here too, however,
        # when uploading a csv file in Google sheets, a leading
        # space was ignored and code was still evaluated.
        value = "'" + value

    return value


def df_to_escaped_csv(df: pd.DataFrame, **kwargs: Any) -> Any:
    def escape_values(v: Any) -> Union[str, Any]:
        return escape_value(v) if isinstance(v, str) else v

    # Escape csv headers
    df = df.rename(columns=escape_values)

    # Escape csv values
    def escape_dataframe(target_df: pd.DataFrame) -> None:
        for name, column in target_df.items():
            if column.dtype == np.dtype(object):
                target_df[name] = column.map(escape_values)

    chunksize = kwargs.pop("chunksize", None)
    if isinstance(chunksize, int) and chunksize <= 0:
        chunksize = None

    logger.info(
        "Starting escaped CSV conversion rows=%s columns=%s chunksize=%s",
        len(df.index),
        len(df.columns),
        chunksize,
    )
    if chunksize is None:
        logger.info(
            "Escaping complete dataframe for CSV rows=%s columns=%s",
            len(df.index),
            len(df.columns),
        )
        escape_dataframe(df)
        logger.info(
            "Serializing complete dataframe to CSV rows=%s columns=%s",
            len(df.index),
            len(df.columns),
        )
        csv_output = df.to_csv(escapechar="\\", **kwargs)
        logger.info(
            "Finished escaped CSV conversion rows=%s columns=%s characters=%s",
            len(df.index),
            len(df.columns),
            len(csv_output),
        )
        return csv_output

    buffer = io.StringIO()
    base_kwargs = dict(kwargs)
    index = base_kwargs.get("index", True)
    header = base_kwargs.get("header", True)
    base_kwargs["index"] = index
    chunk_count = (len(df.index) + chunksize - 1) // chunksize

    for chunk_number, start in enumerate(range(0, len(df), chunksize)):
        chunk = df.iloc[start : start + chunksize].copy()
        logger.info(
            "Escaping CSV chunk chunk=%s/%s start_row=%s rows=%s buffer_characters=%s",
            chunk_number + 1,
            chunk_count,
            start,
            len(chunk.index),
            buffer.tell(),
        )
        escape_dataframe(chunk)
        chunk_kwargs = dict(base_kwargs)
        chunk_kwargs["header"] = header if chunk_number == 0 else False
        logger.info(
            "Serializing CSV chunk chunk=%s/%s rows=%s",
            chunk_number + 1,
            chunk_count,
            len(chunk.index),
        )
        chunk.to_csv(buffer, escapechar="\\", **chunk_kwargs)
        logger.info(
            "Serialized CSV chunk chunk=%s/%s rows=%s buffer_characters=%s",
            chunk_number + 1,
            chunk_count,
            len(chunk.index),
            buffer.tell(),
        )

    csv_output = buffer.getvalue()
    logger.info(
        "Finished escaped CSV conversion rows=%s columns=%s chunks=%s characters=%s",
        len(df.index),
        len(df.columns),
        chunk_count,
        len(csv_output),
    )
    return csv_output


def stream_escaped_csv(
    df_batches: Iterable[pd.DataFrame], **kwargs: Any
) -> Iterator[str]:
    def escape_values(v: Any) -> Union[str, Any]:
        return escape_value(v) if isinstance(v, str) else v

    def escape_dataframe(target_df: pd.DataFrame) -> None:
        for name, column in target_df.items():
            if column.dtype == np.dtype(object):
                target_df[name] = column.map(escape_values)

    base_kwargs = dict(kwargs)
    base_kwargs.pop("chunksize", None)
    header = base_kwargs.get("header", True)

    yielded = False
    for batch_number, batch_df in enumerate(df_batches):
        yielded = True
        chunk = batch_df.rename(columns=escape_values)
        escape_dataframe(chunk)
        chunk_kwargs = dict(base_kwargs)
        chunk_kwargs["header"] = header if batch_number == 0 else False
        buffer = io.StringIO()
        chunk.to_csv(buffer, escapechar="\\", **chunk_kwargs)
        logger.info(
            "Streamed CSV batch batch=%s rows=%s characters=%s",
            batch_number + 1,
            len(chunk.index),
            buffer.tell(),
        )
        yield buffer.getvalue()

    if not yielded:
        empty_kwargs = dict(base_kwargs)
        empty_kwargs["header"] = header
        buffer = io.StringIO()
        pd.DataFrame().to_csv(buffer, escapechar="\\", **empty_kwargs)
        yield buffer.getvalue()


def get_chart_csv_data(
    chart_url: str, auth_cookies: Optional[dict[str, str]] = None
) -> Optional[bytes]:
    content = None
    if auth_cookies:
        opener = urllib.request.build_opener()
        cookie_str = ";".join([f"{key}={val}" for key, val in auth_cookies.items()])
        opener.addheaders.append(("Cookie", cookie_str))
        response = opener.open(chart_url)
        content = response.read()
        if response.getcode() != 200:
            raise URLError(response.getcode())
    if content:
        return content
    return None


def get_chart_dataframe(
    chart_url: str, auth_cookies: Optional[dict[str, str]] = None
) -> Optional[pd.DataFrame]:
    # Disable all the unnecessary-lambda violations in this function
    # pylint: disable=unnecessary-lambda
    content = get_chart_csv_data(chart_url, auth_cookies)
    if content is None:
        return None

    result = json.loads(content.decode("utf-8"))
    # need to convert float value to string to show full long number
    pd.set_option("display.float_format", lambda x: str(x))
    df = pd.DataFrame.from_dict(result["result"][0]["data"])

    if df.empty:
        return None

    try:
        # if any column type is equal to 2, need to convert data into
        # datetime timestamp for that column.
        if GenericDataType.TEMPORAL in result["result"][0]["coltypes"]:
            for i in range(len(result["result"][0]["coltypes"])):
                if result["result"][0]["coltypes"][i] == GenericDataType.TEMPORAL:
                    df[result["result"][0]["colnames"][i]] = df[
                        result["result"][0]["colnames"][i]
                    ].astype("datetime64[ms]")
    except BaseException as err:
        logger.error(err)

    # rebuild hierarchical columns and index
    df.columns = pd.MultiIndex.from_tuples(
        tuple(colname) if isinstance(colname, list) else (colname,)
        for colname in result["result"][0]["colnames"]
    )
    df.index = pd.MultiIndex.from_tuples(
        tuple(indexname) if isinstance(indexname, list) else (indexname,)
        for indexname in result["result"][0]["indexnames"]
    )
    return df
