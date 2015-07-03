# -*- coding: utf-8 -*-
"""

@author: l.pauldelvaux
"""

import numpy as np
import pandas as pd
from format_careers import first_columns_career_table


def cumsum_na(x):
    return np.cumsum(x[~x.isnull()])


def format_unique_year(data, datasets, option=None, table_names=None):
    ''' To obtain only one workstate status/ wage per year. Selection is perfomed based on the following steps:
    1- basic scheme with only additional/previously missing information updated if complementary schemes exist
    2- higher reported wage selected...
    3- ... associated workstate'''
    careers = data['careers'].copy()
    careers = unique_yearly_unemployment(careers, datasets['unemployment'])
    careers = unique_yearly_dads(careers, datasets['dads'])
    assert careers['cadre'].notnull().any()
    if option and 'complementary' in option.keys() and option['complementary']:
        careers = unique_yearly_b200(careers)
        assert not(careers.duplicated(['noind', 'source', 'year'])).all()
        careers = update_basic_with_complementary(careers)
        assert careers['cadre'].notnull().any()
        careers = select_avpf_status(careers)
        assert careers['cadre'].notnull().any()
    else:
        assert not(careers.duplicated(['noind', 'source', 'year'])).all()
    unique_yearly_sal = unique_yearly_salbrut(careers)
    first_cols = ['noind', 'year', 'start_date', 'source', 'cc',
                  'sal_brut_deplaf', 'salbrut', 'source_salbrut', 'inwork_status', 'unemploy_status']
    data['careers_unique'] = first_columns_career_table(unique_yearly_workstate(careers, unique_yearly_sal),
        first_col = first_cols)
    assert data['careers_unique']['cadre'].notnull().any()
    careers = careers.sort(['noind', 'year', 'sal_brut_deplaf', 'inwork_status'], ascending=[1, 1, 1, 1])
    data['careers_draft'] = first_columns_career_table(careers, first_col= first_cols)
    return data


def select_avpf_status(table):
    ''' Select avpf status when conflict when an inwork status in the same year '''
    table = unique_yearly_avpf(table)
    table['source_nb'] = table.groupby(['noind', 'year'])['noind'].transform(lambda x: len(x))
    avpf = table.loc[table['source'].isin(['b200_09', 'b200_avpf_09']), :].copy()
    notavpf = table.loc[~table['source'].isin(['b200_09', 'b200_avpf_09']), :].copy()
    avpf = avpf.sort(['noind', 'year', 'sal_brut_deplaf']).drop_duplicates(['noind', 'year'], take_last=True)
    table = pd.concat([avpf, notavpf], ignore_index=True)
    return table


def unique_yearly_avpf(table):
    not_avpf = table.loc[table.source != 'b200_avpf_09', :].copy()
    avpf = table.loc[table.source == 'b200_avpf_09', :].copy()
    avpf['cc'] = avpf['cc'].astype(float)
    avpf = avpf.sort(['noind', 'year', 'sal_brut_deplaf']).drop_duplicates(['noind', 'year'], take_last=True)
    return pd.concat([avpf, not_avpf], ignore_index=True)


def unique_yearly_b200(table):
    ''' Return 1 row per year
    Note: Usually there is 1 row per id,year in the basic scheme data except for RG/MSA
    Selection rule: keep the 'most important' scheme (RG)'''
    not_b200 = table.loc[table.source != 'b200_09', :].copy()
    b200 = table.loc[table.source == 'b200_09', :].copy()
    b200['cc'] = b200['cc'].astype(float)
    b200 = b200.sort(['noind', 'year', 'cc']).drop_duplicates(['noind', 'year'])
    return pd.concat([b200, not_b200], ignore_index=True)


def unique_yearly_dads(table, dads_table_name):
    ''' Return 1 row per year
    Note: several information for a given year are due to multiple employment status
    Selection rule: sum wages over year and keep status of the longest period.'''
    not_dads = table.loc[table.source != dads_table_name, :].copy()
    dads = table.loc[table.source == dads_table_name, :].copy()
    dads['nb_month'] = (dads['end_date'] - dads['start_date']) / np.timedelta64(1, 'M')
    dads['sal_brut_deplaf_cum'] = dads.groupby(['noind', 'year'])['sal_brut_deplaf'].transform(cumsum_na)
    dads = dads.sort(['noind', 'year', 'nb_month']).drop_duplicates(['noind', 'year'], take_last=True)
    return pd.concat([not_dads, dads], ignore_index=True)


def unique_yearly_unemployment(table, unemploy_table_name):
    ''' Return the most appropriate state when several unemployment benefits status for the same year.
    Note: several informations for a given year are due to information given on a daily base in the unemployment data
    Selection rule: groupby status and keep the longest period.'''
    not_pe = table.loc[table.source != unemploy_table_name, :].copy()
    pe = table.loc[table.source == unemploy_table_name, :].copy()
    pe['nb_month'] = (pe['end_date'] - pe['start_date']) / np.timedelta64(1, 'M')
    pe_yearly = pe.groupby(['noind', 'year', 'unemploy_status'])['nb_month'].sum()
    pe_yearly = pe_yearly.unstack('unemploy_status').fillna(0).stack('unemploy_status').reset_index()
    pe_yearly.columns = ['noind', 'year', 'unemploy_status', 'nb_months']
    pe_yearly = pe_yearly.sort(['noind', 'year', 'nb_months']).drop_duplicates(['noind', 'year'], take_last=True)
    pe_yearly.rename(columns={'unemploy_status': 'unemploy_status_to_keep'}, inplace=True)
    pe = pd.merge(pe, pe_yearly, left_on = ['noind', 'year', 'unemploy_status'],
                  right_on = ['noind', 'year', 'unemploy_status_to_keep'], how='left')
    pe = pe.loc[pe.unemploy_status == pe.unemploy_status_to_keep, :]
    pe['sal_brut_deplaf_cumsum'] = pe.groupby(['noind', 'year'])['sal_brut_deplaf'].transform(cumsum_na)
    pe = pe.sort(['noind', 'year', 'sal_brut_deplaf']).drop_duplicates(['noind', 'year'], take_last=True)
    pe = pe.drop(['sal_brut_deplaf', 'unemploy_status_to_keep', 'sal_brut_deplaf_cumsum'], 1)
    pe.rename(columns={'sal_brut_deplaf_cumsum': 'sal_brut_deplaf'}, inplace=True)
    assert not(pe.duplicated(['noind', 'year'])).all()
    return pd.concat([pe, not_pe], ignore_index=True)


def unique_yearly_salbrut(table):
    ''' Rework on wages to keep the most accurate information for each year'''
    def _source(row, columns):
        null_col = list(row.isnull())
        rowl = list(row)
        i = null_col.index(0) if 0 in null_col else None
        if i is not None:
            idx = rowl.index(max(rowl))
            return columns[idx][16:] # 16 = len(sal_bruit_deplaf)
        else:
            return rowl.index(-1)
    df = table.copy()
    df['year'] = df['year'].astype(int)
    assert not(df.duplicated(['noind', 'year', 'source'])).all()
    df['sal_brut_deplaf'] = df['sal_brut_deplaf'].fillna(-1)
    dfs = df[['noind', 'year', 'source', 'sal_brut_deplaf']].drop_duplicates(['noind', 'year', 'source']).set_index(['noind', 'year', 'source']).unstack('source')
    dfs['salbrut'] = dfs.max(axis=1).replace(-1, np.nan)
    columns = [col[0] + '_' + col[1] for col in dfs.columns if col[0] != 'salbrut']
    dfs.columns = columns + ['salbrut']
    dfs = dfs.reset_index()
    dfs['source_salbrut'] = dfs.apply(lambda row: _source(row[columns], columns), axis=1)
    assert (dfs['source_salbrut'].isnull() == 0).all()
    assert not(dfs.duplicated(['noind', 'source_salbrut', 'year'])).all()
    return dfs[['noind', 'year', 'salbrut', 'source_salbrut']]


def unique_yearly_workstate(table_all, table_yearly_salbrut):
    ''' Define a unique workstate based on the aggregated information and sal_brut previously selected '''
    # initial_shape = table_yearly_salbrut.shape
    df = table_all.copy()
    df['source_salbrut'] = df['source']
    for var in ['full_time', 'cadre', 'inwork_status', 'unemploy_status']:
        to_impute = df.groupby(['noind', 'year'])[var].transform(lambda s: s.bfill().iloc[0])
        df.loc[df[var].isnull(), var] = to_impute[df[var].isnull()]
    dfs = table_yearly_salbrut.copy()
    dfs['helper'] = 4
    assert not(dfs.duplicated(['noind', 'source_salbrut', 'year'])).all()
    assert not(df.duplicated(['noind', 'source_salbrut', 'year'])).all()
    df = df.merge(dfs, on=['noind', 'year', 'source_salbrut'])
    careers = df.loc[df['helper'] == 4, :].drop('helper', 1)
    assert not(careers.duplicated(['noind', 'year'])).all()
    # print careers.shape, initial_shape
    # assert careers.shape[0] == initial_shape[0]
    careers.sort(['noind', 'year'], inplace=True)
    for var in ['full_time', 'cadre']:
        careers[var] = careers.groupby(['noind'])[var].fillna(method='ffill')
    return careers


def update_basic_with_complementary(table):
    ''' Extract information from complementary schemes and save it at the basic level '''
    assert 'b200_09' in list(table['source'])
    assert 'c200_09' in list(table['source'])
    table['cc'] = table['cc'].astype(float)
    table_c200 = table.loc[(table['source'] == 'c200_09') * (table['cc'].isin([5000.0, 6000.0])), :].copy()
    renames = dict([(old_name, old_name + '_c200') for old_name in table_c200.columns
                    if old_name not in ['noind', 'year']])
    table_c200.rename(columns=renames, inplace=True)
    table_c200['source_to_match'] = 'b200_09'
    table_c200['cc_to_match'] = 10.0
    # If both Agirc (5000) and Arcco (6000) on a same year -> we keep agirc
    table_c200 = table_c200.sort(['noind', 'year', 'cc_c200'])
    table_c200 = table_c200.drop_duplicates(['noind', 'year'])  # First = Agirc
    df = table.loc[table.source != 'c200_09', :].copy()
    table = pd.merge(df, table_c200, left_on=['noind', 'year', 'cc'],
                     right_on=['noind', 'year', 'cc_to_match'], how='left', sort=False)
    table['cadre'] = np.nan
    for var in ['cc', 'cc_c200']:
        table[var] = table[var].astype(float)
    info = (table['cc'] == 10)
    table.loc[info, 'cadre'] = (table.loc[info, 'cc_c200'] == 5000.0)
    assert table.loc[info, 'cadre'].notnull().any()
    rows_to_impute = table['sal_brut_deplaf'].isnull() | (table['sal_brut_deplaf_c200'] > table['sal_brut_deplaf'])
    table.loc[rows_to_impute, 'sal_brut_deplaf'] = table.loc[rows_to_impute, 'sal_brut_deplaf_c200']
    table.drop(renames.values() + ['cc_to_match', 'source_to_match'], 1, inplace=True)
    assert table['cadre'].notnull().any()
    return table
