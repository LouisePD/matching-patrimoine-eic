# -*- coding: utf-8 -*-
"""
@author: l.pauldelvaux
"""
import numpy as np
import pandas as pd
from matching_patrimoine_eic.base.format_individual_info import most_frequent
from matching_patrimoine_eic.base.format_careers import clean_earning
from matching_patrimoine_eic.base.load_data import temporary_store_decorator


@temporary_store_decorator()
def build_anaiss(temporary_store = None):
    ''' Returns a length(index)-vector of years of birth
    - collected from: b100 (check with c100)
    - code: 0 = Male / 1 = female '''
    df = temporary_store.select('eir2008_avant08', columns = ['noind', 'an'])
    anaiss = most_frequent(df, 'an')
    return anaiss


@temporary_store_decorator()
def build_civilstate(temporary_store = None):
    ''' Returns a length(index)-vector of civilstate:
    - collected from:
    - code: married == 1, single == 2, divorced  == 3, widow == 4, pacs == 5, couple == 6'''
    df = temporary_store.select('eir2008_avant08', columns=['noind', 'sm'])
    df['sm'] = clean_civilstate_level100(df['sm'])
    civilstate = most_frequent(df, 'sm')
    return civilstate


@temporary_store_decorator()
def build_nenf(temporary_store = None):
    ''' Returns a length(index)-vector of number of children
    Information is often missing so we use information provided from 3 variables'''
    df = temporary_store.select('eir2008_avant08', columns=['noind', 'nre', 'nrelev', 'nrecharge'])
    nenf1 = most_frequent(df[['noind', 'nre']], 'nre')
    nenf2 = most_frequent(df[['noind', 'nrelev']], 'nrelev')
    nenf3 = most_frequent(df[['noind', 'nrecharge']], 'nrecharge')
    nenf = nenf1
    nenf[nenf.isnull()] = nenf2[nenf.isnull()]
    nenf[nenf.isnull()] = nenf3[nenf.isnull()]
    return nenf


@temporary_store_decorator()
def build_sexe(temporary_store = None):
    ''' Returns a length(index)-vector of sexes
    - collected from: b100 (eventual add  with c100)
    - code: 0 = Male / 1 = female (initial code is 1/2)'''
    df = temporary_store.select('eir2008_avant08', columns = ['sexi', 'noind'])
    sexe = most_frequent(df, 'sexi')
    sexe = sexe.replace([1, 2], [0, 1])
    return sexe


def clean_civilstate_level100(x):
    ''' This function cleans civilstate statuts for 'b100' and 'c100' databases
    initial code: 1==single, 2==married, 3==widow, 4==divorced or separated,
    output code: married == 1, single == 2, divorced  == 3, widow == 4, pacs == 5, couple == 6'''
    x = x.convert_objects(convert_numeric=True).round()
    x.loc[x == 8] = np.nan
    x = x.replace([1, 2, 3, 4],
                  [2, 1, 4, 3])
    return x


def clean_civilstate_etat(x):
    ''' This function cleans civilstate statuts for the 'etat' database
    initial code: 1==single, 2==married, 3==widow, 4==widow, 5==divorced or separated,
    6==pacs, 7==En concubinage
    output code: married == 1, single == 2, divorced  == 3, widow == 4, pacs == 5, couple == 6'''
    x = x.convert_objects(convert_numeric=True).round()
    x.loc[x == 9] = np.nan
    x = x.replace([1, 2, 3, 4, 5, 6, 7],
                  [2, 1, 4, 4, 3, 5, 6])
    return x


@temporary_store_decorator()
def format_individual_info(temporary_store = None):
    ''' This function extracts individual information which is not time dependant
    and recorded at year 2009 from the set of EIC's databases.
    Internal coherence and coherence between data.
    When incoherent information a rule of prioritarisation is defined
    Note: People we want information on = people in b100/b200. '''
    index = sorted(set(temporary_store.select('eir2008_avant08')['noind']))
    columns = ['sexe', 'anaiss', 'nenf', 'civilstate']  # , 'findet']
    info_ind = pd.DataFrame(columns = columns, index = index)
    for variable_name in columns:
        info_ind[variable_name] = eval('build_' + variable_name)()
    info_ind['noind'] = index
    return info_ind


@temporary_store_decorator()
def format_pension_info(droit_direct = True, temporary_store = None):

    def fillna_by_noind(x):
        null_col = list(x.isnull())
        index = list(x.index)
        i_valid = null_col.index(0) if 0 in null_col else 0
        return x.fillna(x[index[i_valid]])

    df = temporary_store.select('eir2008_avant08')
    columns_to_keep = ['noind', 'cc', 'date_liquidation', 'date_jouissance', 'age_retraite']
    df.loc[:, 'cc'] = df['cc'].astype(float)
    assert sum(df['cc'].isnull()) == 0
    df.loc[:, 'date_liquidation'] = build_date_pension(df, 'liq')
    df.loc[:, 'date_jouissance'] = build_date_pension(df, 'ent')
    print df.loc[df['date_liquidation'].isnull() * df['date_jouissance'].isnull(), ['noind', 'an', 'cc', 'typdd']]
    if droit_direct:
        df = df.loc[~df['typdd'].isnull(), :]
        assert sum(df['date_liquidation'].isnull() * df['date_jouissance'].isnull()) == 0
    df.loc[df['date_liquidation'].isnull(), 'date_liquidation'] = \
        df.loc[df['date_liquidation'].isnull(), 'date_jouissance']
    df.loc[df['date_jouissance'].isnull(), 'date_jouissance'] = \
        df.loc[df['date_jouissance'].isnull(), 'date_liquidation']
    df.loc[:, 'age_retraite'] = df.loc[:, 'date_liquidation'].apply(lambda x: x.year) - df.loc[:, 'an']
    to_rename = {1: 'direct', 2: 'derive', 3: 'conj_charge', 4: 'tierce', 5: 'bonif_enf', 6: 'fsv', 7: '814',
                 8: 'aspa', 9: 'enf_c', 10: 'nbi', 11: 'other'}

    for i in range(1, 12):
        name = 'pension_' + to_rename[i]
        df[name] = clean_earning(df['m' + str(i)])
        columns_to_keep += [name]

    df = df.rename(columns={'majomindd': 'pension_mini', 'tdd1': 'trim_tot', 'tdd2': 'trim_cot'})
    df['pension_tot'] = clean_earning(df['mont'])
    vars_enf = ['nrelev', 'nrecharge']
    for var in vars_enf:
        df.loc[:, var] = df.groupby('noind')[var].transform(fillna_by_noind)
        df.loc[df['nre'].isnull(), 'nre'] = df.loc[df['nre'].isnull(), var]

    df.loc[:, 'nre'] = df.groupby('noind')['nre'].transform(fillna_by_noind).fillna(0)
    df.rename(columns={'nre': 'nenf'}, inplace=True)
    print df.columns
    columns_to_keep += ['pension_tot', 'nenf', 'pension_mini', 'taux_avantmico',
                        'taux_surcote', 'sam', 'trim_tot', 'trim_cot', 'trimsur', 'trimdec', 'typdd']

    return df[columns_to_keep]


def build_date_pension(df, prefix):
    date = pd.to_datetime((df[prefix + 'ddaa'] * 10000 + df[prefix + 'ddmm'] * 100 + df[prefix + 'ddjj']).apply(str),
                       format='%Y%m%d')
    return date
