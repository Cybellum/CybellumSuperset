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
import Button from 'src/components/Button';
import { EmptyState as EmptyStateComponent } from 'src/components/EmptyState';
import { TableTab } from 'src/views/CRUD/types';
import { styled, t } from '@superset-ui/core';
import { withPrefix } from 'src/utils/routeUtils';
import { WelcomeTable } from './types';

const EmptyContainer = styled.div`
  min-height: 200px;
  display: flex;
  color: ${({ theme }) => theme.colors.grayscale.light2};
  flex-direction: column;
  justify-content: space-around;
`;

const ICONS = {
  [WelcomeTable.Charts]: 'empty-charts.svg',
  [WelcomeTable.Dashboards]: 'empty-dashboard.svg',
  [WelcomeTable.Recents]: 'union.svg',
  [WelcomeTable.SavedQueries]: 'empty-queries.svg',
} as const;

const REDIRECTS = {
  create: {
    [WelcomeTable.Charts]: withPrefix('/chart/add'),
    [WelcomeTable.Dashboards]: withPrefix('/dashboard/new'),
    [WelcomeTable.SavedQueries]: withPrefix('/sqllab?new=true'),
  },
  viewAll: {
    [WelcomeTable.Charts]: withPrefix('/chart/list'),
    [WelcomeTable.Dashboards]: withPrefix('/dashboard/list/'),
    [WelcomeTable.SavedQueries]: withPrefix('/savedqueryview/list/'),
  },
} as const;

export interface EmptyStateProps {
  tableName: WelcomeTable;
  tab?: string;
  otherTabTitle?: string;
}

export default function EmptyState({
  tableName,
  tab,
  otherTabTitle,
}: EmptyStateProps) {
  const getActionButton = () => {
    if (tableName === WelcomeTable.Recents) {
      return null;
    }

    const isFavorite = tab === TableTab.Favorite;
    const buttonText =
      tableName === WelcomeTable.SavedQueries
        ? isFavorite
          ? t('SQL Lab queries')
          : t('SQL query')
        : isFavorite
          ? t(tableName.toLowerCase())
          : tableName.slice(0, -1);

    const url = isFavorite
      ? REDIRECTS.viewAll[tableName]
      : REDIRECTS.create[tableName];

    return (
      <Button
        buttonStyle="default"
        onClick={() => {
          window.location.href = url;
        }}
      >
        {isFavorite
          ? t('See all %(tableName)s', { tableName: buttonText })
          : buttonText}
      </Button>
    );
  };

  const image =
    tab === TableTab.Favorite
      ? withPrefix('/static/assets/images/star-circle.svg')
      : ICONS[tableName];

  return (
    <EmptyContainer>
      <EmptyStateComponent
        image={image}
        size="large"
        description={t('Nothing here yet')}
      >
        {getActionButton()}
      </EmptyStateComponent>
    </EmptyContainer>
  );
}
