# -*- coding: utf-8 -*-
"""

@author: l.pauldelvaux
"""
import gc
import numpy as np
import pandas as pd
from format_careers import first_columns_career_table
from format_workstates import define_workstates


def cumsum_na(x):
    return np.cumsum(x[~x.isnull()])


def format_unique_year(data, datasets, option=None, table_names=None):
    ''' To obtain only one workstate status/ wage per year. Selection is perfomed based on the following steps:
    1 - Select on obs by (indiv, year) in each data source
    1a - basic scheme with only additional/previously missing information updated if complementary schemes exist
    2 - Define appropriate workstates
    3 - higher reported wage selected...
    4 - ... associated workstate'''
    careers = data['careers'].copy()
    assert careers.shape[0] != 0
    data['careers_raw'] = data.pop('careers')
    assert sum(careers.cc.isnull()) == 0
    careers = unique_yearly_unemployment(careers, datasets['unemployment'])
    assert sum(careers.cc.isnull()) == 0
    careers = unique_yearly_dads(careers, datasets['dads'])
    assert sum(careers.cc.isnull()) == 0
    careers = unique_yearly_etat(careers, datasets['etat'])
    assert sum(careers.cc.isnull()) == 0
    if option and 'complementary' in option.keys() and option['complementary']:
        careers = unique_yearly_b200(careers)
        assert sum(careers.cc.isnull()) == 0
        careers = update_basic_with_complementary(careers)
        assert sum(careers.duplicated(['noind', 'source', 'year'])) == 0
        assert sum(careers.cc.isnull()) == 0
        assert careers['cadre'].notnull().any()
        careers = select_avpf_status(careers)
        assert sum(careers.cc.isnull()) == 0
        assert careers['cadre'].notnull().any()
    else:
        assert not(careers.duplicated(['noind', 'source', 'year'])).all()
    unique_yearly_sal = unique_yearly_salbrut(careers)
    careers = unique_yearly_workstate(careers, unique_yearly_sal, datasets)
    del unique_yearly_sal
    gc.collect()
    if option and 'complementary' in option.keys() and option['complementary']:
        assert sum(careers.cc.isnull()) == 0
    first_cols = ['noind', 'year', 'start_date', 'cc', 'salbrut', 'source_salbrut',
              'inwork_status', 'unemploy_status']
    data['careers'] = first_columns_career_table(careers, first_col = first_cols)
    return data


def select_avpf_status(table):
    ''' Select avpf status when conflict when an inwork status in the same year '''
    table = unique_yearly_avpf(table)
    table['source_nb'] = table.groupby(['noind', 'year'])['noind'].transform('count')
    avpf = table.loc[table['source'].isin(['b200_09', 'b200_avpf_09']), :].copy()
    notavpf = table.loc[~table['source'].isin(['b200_09', 'b200_avpf_09']), :].copy()
    avpf = avpf.sort(['noind', 'year', 'sal_brut_deplaf']).drop_duplicates(['noind', 'year'], take_last=True)
    table = pd.concat([avpf, notavpf], ignore_index=True)
    return table


def unique_yearly_avpf(table):
    not_avpf = table.loc[table.source != 'b200_avpf_09', :].copy()
    avpf = table.loc[table.source == 'b200_avpf_09', :].copy()
    avpf.loc[:, 'cc'] = avpf['cc'].astype(float)
    avpf = avpf.sort(['noind', 'year', 'sal_brut_deplaf']).drop_duplicates(['noind', 'year'], take_last=True)
    return pd.concat([avpf, not_avpf], ignore_index=True)


def unique_yearly_etat(table, etat_table_name):
    not_etat = table.loc[table.source != etat_table_name, :]
    etat = table.loc[table.source == etat_table_name, :]
    etat.loc[:, 'cc'] = etat['cc'].astype(float)
    etat.loc[:, 'sal_brut_deplaf_cum'] = etat.groupby(['noind', 'year'])['sal_brut_deplaf'].transform(cumsum_na)
    etat = etat.sort(['noind', 'year', 'nb_month']).drop_duplicates(['noind', 'year'], take_last=True)
    # etat.rename(columns={'sal_brut_deplaf_cum': 'sal_brut_deplaf'}, inplace=True)
    return pd.concat([etat, not_etat], ignore_index=True)


def unique_yearly_b200(table):
    ''' Return 1 row per year
    Note: Usually there is 1 row per id,year in the basic scheme data except for RG/MSA
    Selection rule: keep the 'most important' scheme (RG)'''
    not_b200 = table.loc[table.source != 'b200_09', :]
    b200 = table.loc[table.source == 'b200_09', :]
    b200.loc[:, 'cc'] = b200['cc'].astype(float)
    b200 = b200.sort(['noind', 'year', 'cc']).drop_duplicates(['noind', 'year'])
    assert sum(table.cc.isnull()) == 0
    return pd.concat([b200, not_b200], ignore_index=True)


def unique_yearly_dads(table, dads_table_name):
    ''' Return 1 row per year
    Note: several information for a given year are due to multiple employment status
    Selection rule: sum wages over year and keep status of the longest period.'''
    not_dads = table.loc[table.source != dads_table_name, :]
    dads = table.loc[table.source == dads_table_name, :]
    dads.loc[:, 'nb_month'] = (dads['end_date'] - dads['start_date']) / np.timedelta64(1, 'M')
    dads.loc[:, 'sal_brut_deplaf_cum'] = dads.groupby(['noind', 'year'])['sal_brut_deplaf'].transform(cumsum_na)
    dads = dads.sort(['noind', 'year', 'nb_month']).drop_duplicates(['noind', 'year'], take_last=True)
    return pd.concat([not_dads, dads], ignore_index=True)


def unique_yearly_unemployment(table, unemploy_table_name):
    ''' Return the most appropriate state when several unemployment benefits status for the same year.
    Note: several informations for a given year are due to information given on a daily base in the unemployment data
    Selection rule: groupby status and keep the longest period.'''
    not_pe = table.loc[table.source != unemploy_table_name, :]
    pe = table.loc[table.source == unemploy_table_name, :]
    to_impute = pe['unemploy_status'].isnull()
    pe.loc[to_impute * (pe['sal_brut_deplaf'] > 0), 'unemploy_status'] = 2
    pe.loc[to_impute * (pe['sal_brut_deplaf'] == 0), 'unemploy_status'] = 0
    assert sum(pe['unemploy_status'].isnull()) == 0
    pe.loc[:, 'nb_month'] = (pe['end_date'] - pe['start_date']) / np.timedelta64(1, 'M')
    pe_yearly = pe.groupby(['noind', 'year', 'unemploy_status'])['nb_month'].sum()
    pe_yearly = pe_yearly.unstack('unemploy_status').stack('unemploy_status').reset_index()
    pe_yearly.columns = ['noind', 'year', 'unemploy_status', 'nb_months']
    pe_yearly = pe_yearly.sort(['noind', 'year', 'nb_months']).drop_duplicates(['noind', 'year'], take_last=True)
    pe = pd.merge(pe, pe_yearly, on = ['noind', 'year', 'unemploy_status'], how='right')
    pe['sal_brut_deplaf_cumsum'] = pe.groupby(['noind', 'year'])['sal_brut_deplaf'].transform(cumsum_na)
    pe = pe.sort(['noind', 'year', 'sal_brut_deplaf']).drop_duplicates(['noind', 'year'], take_last=True)
    pe = pe.drop(['sal_brut_deplaf'], 1)
    assert pe.shape[0] == pe_yearly.shape[0]
    pe.rename(columns={'sal_brut_deplaf_cumsum': 'sal_brut_deplaf'}, inplace=True)
    assert sum(pe.duplicated(['noind', 'year'])) == 0
    assert sum(pe.cc.isnull()) == 0
    assert sum(not_pe.cc.isnull()) == 0
    return pd.concat([pe, not_pe], ignore_index=True)


def unique_yearly_salbrut(table):
    ''' Rework on wages to keep the most accurate information for each year'''
    def _source(row, columns):
        null_col = list(row.isnull())
        rowl = list(row)
        i = null_col.index(0) if 0 in null_col else None
        if i is not None:
            idx = rowl.index(max(rowl))
            return columns[idx][16:]  # 16 = len(sal_brut_deplaf)
        else:
            return rowl.index(-1)
    assert sum(table['cc'].isnull()) == 0

    df = table
    df.loc[:, 'year'] = df['year'].astype(int)
    assert not(df.duplicated(['noind', 'year', 'source'])).all()
    df.loc[:, 'sal_brut_deplaf'] = df['sal_brut_deplaf'].fillna(-1)
    # Assumption: Here, we only select the highest wage for a given year
    dfs = df[['noind', 'year', 'source', 'sal_brut_deplaf']].drop_duplicates(
        ['noind', 'year', 'source'], take_last=True).set_index(['noind', 'year', 'source']).unstack('source')
    del df
    gc.collect()
    dfs['salbrut'] = dfs.max(axis=1)
    cols = [str(col[0]) + '_' + str(col[1]) for col in dfs.columns if col[0] != 'salbrut']
    dfs.columns = cols + ['salbrut']
    dfs = dfs.reset_index()
    dfs.loc[:, 'source_salbrut'] = dfs.apply(lambda row: _source(row[cols], cols), axis=1)
    dfs.loc[:, 'salbrut'] = dfs['salbrut'].replace(-1, np.nan)
    assert not(dfs['source_salbrut'].isnull()).all()
    assert not(dfs.duplicated(['noind', 'source_salbrut', 'year'])).all()
    return dfs[['noind', 'year', 'salbrut', 'source_salbrut']].sort(['noind', 'year'])


def unique_yearly_workstate(table_all, table_yearly_salbrut, datasets):
    ''' Define a unique workstate based on the aggregated information and sal_brut previously selected '''
    # initial_shape = table_yearly_salbrut.shape
    df = table_all
    df.rename(columns={'source': 'source_salbrut'}, inplace=True)
    df.loc[df['cc'] == 12, 'fp_actif'] = 0
    df.loc[df['cc'] == 13, 'fp_actif'] = 1
    assert sum(df['cc'].isnull()) == 0
    dfs = table_yearly_salbrut.copy()
    assert sum(dfs.duplicated(['noind', 'source_salbrut', 'year'])) == 0
    df = df.merge(dfs, on=['noind', 'year', 'source_salbrut'], how='inner')
    del dfs
    gc.collect()
    assert sum(df['cc'].isnull()) == 0
    assert sum(df['source_salbrut'].isnull()) == 0
    assert sum(df.duplicated(['noind', 'year'])) == 0

    df = define_workstates(df, datasets)
    df.sort(['noind', 'year'], inplace=True)
    for var in ['full_time', 'cadre']:
        df.loc[df['cc'] == 10, var] = df.loc[df['cc'] == 10, :].groupby(['noind'])[var].fillna(method='ffill')
    return df


def update_basic_with_complementary(table):
    ''' Extract information from complementary schemes and save it at the basic level '''
    assert 'b200_09' in list(table['source'])
    assert 'c200_09' in list(table['source'])
    table.loc[:, 'cc'] = table['cc'].astype(float)
    table_c200 = table.loc[(table['source'] == 'c200_09') * (table['cc'].isin([5000.0, 6000.0])), :]
    renames = dict([(old_name, old_name + '_c200') for old_name in table_c200.columns
                    if old_name not in ['noind', 'year']])
    table_c200.rename(columns=renames, inplace=True)
    table_c200.loc[:, 'source_to_match'] = 'b200_09'
    table_c200.loc[:, 'cc_to_match'] = 10.0
    # If both Agirc (5000) and Arcco (6000) on a same year -> we keep agirc
    table_c200 = table_c200.sort(['noind', 'year', 'cc_c200'])
    table_c200 = table_c200.drop_duplicates(['noind', 'year'])  # First = Agirc
    table = table.loc[table.source != 'c200_09', :]
    table = pd.merge(table, table_c200, left_on=['noind', 'year', 'cc'],
                     right_on=['noind', 'year', 'cc_to_match'], how='left', sort=False)

    del table_c200
    gc.collect()
    for var in ['cc', 'cc_c200']:
        table[var] = table[var].astype(float)
    rows_to_impute = table['sal_brut_deplaf'].isnull() | (table['sal_brut_deplaf_c200'] > table['sal_brut_deplaf'])
    table.loc[rows_to_impute, 'sal_brut_deplaf'] = table.loc[rows_to_impute, 'sal_brut_deplaf_c200']
    table.drop(renames.values() + ['cc_to_match', 'source_to_match'], 1, inplace=True)
    return table
