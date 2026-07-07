/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import {
  BinaryQueryObjectFilterClause,
  css,
  isAdhocColumn,
  QueryFormData,
  SupersetClient,
  t,
  useTheme,
} from '@superset-ui/core';
import { Icons } from '@superset-ui/core/components/Icons';
import { useCallback, useMemo } from 'react';
import RowCountLabel from 'src/components/RowCountLabel';
import { Tag } from 'src/components/Tag';
import { buildV1ChartDataPayload } from 'src/explore/exploreUtils';
import { getDrillPayload } from './utils';

export type TableControlsProps = {
  filters: BinaryQueryObjectFilterClause[];
  setFilters: (filters: BinaryQueryObjectFilterClause[]) => void;
  totalCount?: number;
  loading: boolean;
  onReload: () => void;
  formData?: QueryFormData;
};

export default function TableControls({
  filters,
  setFilters,
  totalCount,
  loading,
  onReload,
  formData,
}: TableControlsProps) {
  const theme = useTheme();
  const filterMap: Record<string, BinaryQueryObjectFilterClause> = useMemo(
    () =>
      Object.assign(
        {},
        ...filters.map(filter => ({
          [isAdhocColumn(filter.col)
            ? (filter.col.label as string)
            : filter.col]: filter,
        })),
      ),
    [filters],
  );

  const removeFilter = useCallback(
    colName => {
      const updatedFilterMap = { ...filterMap };
      delete updatedFilterMap[colName];
      setFilters([...Object.values(updatedFilterMap)]);
    },
    [filterMap, setFilters],
  );

  const handleDownloadCSV = useCallback(async () => {
    if (!formData) {
      return;
    }

    try {
      const drillPayload = getDrillPayload(formData, filters);
      const queryPayload = await buildV1ChartDataPayload({
        formData: {
          ...formData,
          ...drillPayload,
          row_limit: 100_000_000,
        },
        resultFormat: 'csv',
        resultType: 'samples',
        force: false,
        setDataMask: undefined,
        ownState: {},
      });

      const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
      const filename = `drill_detail_${timestamp}.csv`;

      await SupersetClient.postForm('/api/v1/chart/data', {
        form_data: JSON.stringify(queryPayload),
        filename,
      });
    } catch (error) {
      console.error('Error downloading CSV:', error);
    }
  }, [formData, filters]);

  const filterTags = useMemo(
    () =>
      Object.entries(filterMap)
        .map(([colName, { val, formattedVal }]) => ({
          colName,
          val: formattedVal ?? val,
        }))
        .sort((a, b) => a.colName.localeCompare(b.colName)),
    [filterMap],
  );

  return (
    <div
      css={css`
        display: flex;
        justify-content: space-between;
        padding: ${theme.sizeUnit / 2}px 0;
        margin-bottom: ${theme.sizeUnit * 2}px;
      `}
    >
      <div
        css={css`
          display: flex;
          flex-wrap: wrap;
        `}
      >
        {filterTags.map(({ colName, val }, index) => (
          <Tag
            editable
            onDelete={removeFilter.bind(null, colName)}
            index={index}
            id={index}
            key={colName}
            name={`${colName}=${val}`}
            data-test="filter-col"
          >
            <span
              css={css`
                margin-right: ${theme.sizeUnit}px;
              `}
            >
              {colName}
            </span>
            <strong data-test="filter-val">{val}</strong>
          </Tag>
        ))}
      </div>
      <div
        css={css`
          display: flex;
          align-items: center;
          height: min-content;
          gap: ${theme.sizeUnit * 3}px;
        `}
      >
        <RowCountLabel loading={loading && !totalCount} rowcount={totalCount} />
        {formData && (
          <span
            role="button"
            tabIndex={0}
            onClick={handleDownloadCSV}
            onKeyDown={e => {
              if (e.key === 'Enter' || e.key === ' ') {
                handleDownloadCSV();
              }
            }}
            css={css`
              cursor: pointer;
              color: ${theme.colorPrimary};
              &:hover {
                opacity: 0.75;
              }
            `}
          >
            {t('Download CSV')}
          </span>
        )}
        <Icons.ReloadOutlined
          iconColor={theme.colorIcon}
          iconSize="l"
          aria-label={t('Reload')}
          role="button"
          onClick={onReload}
        />
      </div>
    </div>
  );
}
