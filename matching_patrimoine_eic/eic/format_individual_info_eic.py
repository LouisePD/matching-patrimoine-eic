# -*- coding: utf-8 -*-
"""
@author: l.pauldelvaux
"""
import numpy as np
import pandas as pd
from matching_patrimoine_eic.base.format_individual_info import most_frequent, variable_last_available
from matching_patrimoine_eic.base.format_individual_info import temporary_store_decorator


def build_anaiss(index):
    ''' Returns a length(index)-vector of years of birth
    - collected from: b100 (check with c100)
    - code: 0 = Male / 1 = female '''
    anaiss = variable_mode_consolidate('an', index)
    return anaiss


def build_civilstate(index):
    ''' Returns a length(index)-vector of civilstate:
    - collected from:
    - code: married == 1, single == 2, divorced  == 3, widow == 4, pacs == 5, couple == 6'''
    # data['b100_09']['sm'] = clean_civilstate_level100(data['b100_09']['sm'])
    # data['c100_09']['sm'] = clean_civilstate_level100(data['c100_09']['sm'])
    # data['etat_09']['sm'] = clean_civilstate_etat(data['etat_09']['sm'])
    varname_by_table = {'b100_09': {'names': ['sm', 'an_fin'], 'order': 1},
                        'c100_09': {'names': ['sm', 'an_fin'], 'order': 2},
                        'etat_09': {'names': ['sm', 'annee'], 'order': 3}}
    civilstate = variable_last_available(varname_by_table)
    return civilstate


def build_nenf(index):
    ''' Returns a length(index)-vector of number of children
    Information is often missing and/or et/or given for different years.
    Imputation rule last year (corresponding to 2009):
        1) per individual max of nenf in b100
        2) ... in c100
        3) ... in etat (becarefull: only dependant children)
        4) ... in EDP (becarefull: last update in 1999'''
    varname_by_table = {'b100_09': {'names': ['enf', 'an_fin'], 'order': 1},
                        'c100_09': {'names': ['enf', 'an_fin'], 'order': 2},
                        'etat_09': {'names': ['ne10', 'annee'], 'order': 3}}
    nenf = pd.DataFrame(index=index)
    nenf['y2009'] = variable_last_available(varname_by_table)
    #nenf['y1999'] = nenf_from_edp(data['edp_09'])
    nenf['y2009'][nenf.y2009.isnull()] = nenf['y2009'][nenf.y2009.isnull()]
    return nenf['y2009']


def build_sexe(index):
    ''' Returns a length(index)-vector of sexes
    - collected from: b100 (eventual add  with c100)
    - code: 0 = Male / 1 = female (initial code is 1/2)'''
    sexe = variable_mode_consolidate('sexi', index)
    sexe = sexe.replace([1, 2], [0, 1])
    return sexe


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
    indiv = temporary_store.select('b100_09')['noind']
    index = sorted(set(indiv))
    print index
    temporary_store.close()
    columns = ['sexe', 'anaiss', 'nenf', 'civilstate']  # , 'findet']
    info_ind = pd.DataFrame(columns = columns, index = index)
    for variable_name in columns:
        info_ind[variable_name] = eval('build_' + variable_name)(index)
    info_ind['noind'] = info_ind.index
    assert info_ind.shape[0] > 1
    return info_ind


def minmax_by_noind(data_var, var_name, index):
    ''' Build a dataframe with the given index and two columns: min and max
    Note: data_var contains noind and var_name'''
    to_return = pd.DataFrame(columns=['min', 'max'], index=index)
    to_return['min'] = data_var.groupby(['noind'], sort=True).min()
    to_return['max'] = data_var.groupby(['noind'], sort=True).max()
    return to_return


def nenf_from_edp(table_edp):
    ''' This function provides information on the number of children given in EDP.
    In this dataset, date of births are indicated: if 5 dates -> 5 children'''
    def _nenf(table_edp, year):
        name_variables = []
        for i in range(1, 10):
            name = 'an0' + str(i) + 'e' + str(year)
            name_variables += name
        for i in range(10, 16):
            name = 'an' + str(i) + 'e' + str(year)
            name_variables += name
        table = table_edp[[name_variables]].copy()
        table.set_index(table_edp.noind, inplace=True)
        nenf = table.notnull().sum(axis=1)
        return nenf
    nenf_90 = _nenf(table_edp, 90)
    nenf_99 = _nenf(table_edp, 99)
    nenf_99[nenf_99.isnull()] = nenf_90[nenf_99.isnull()]
    return nenf_99


@temporary_store_decorator()
def variable_mode_consolidate(var_name, index, temporary_store = None):
    ''' For a given variable in b100 update missing information with
    equivalent variable in c100 '''
    data_b_var = temporary_store.select('b100_09', columns = ['noind', var_name])
    variable = pd.DataFrame(most_frequent(data_b_var, var_name))

    data_c_var = temporary_store.select('c100_09', columns = ['noind', var_name])
    variable_c = pd.DataFrame(most_frequent(data_c_var, var_name))

    variable.update(variable_c, join = 'left', overwrite = False)
    return variable
