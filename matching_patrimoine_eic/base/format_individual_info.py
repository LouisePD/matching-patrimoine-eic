# -*- coding: utf-8 -*-
"""
@author: l.pauldelvaux
"""
import numpy as np
import pandas as pd
from load_data import temporary_store_decorator


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


def minmax_by_noind(data, var_name, index):
    ''' Build a dataframe with the given index and two columns: min and max'''
    data_var = data[['noind', var_name]].copy()
    to_return = pd.DataFrame(columns=['min', 'max'], index=index)
    to_return['min'] = data_var.groupby(['noind'], sort=True).min()
    to_return['max'] = data_var.groupby(['noind'], sort=True).max()
    return to_return


def most_frequent(table_var, var):
    ''' table_var contains only two columns 'noind' and var'''
    assert 'noind' in table_var
    assert var in table_var

    def _mode(x):
        try:
            return x.value_counts().index[0]
        except:
            return np.NaN

    mode_var = table_var.groupby(['noind'], sort=True).agg(lambda x: _mode(x))
    return mode_var[var]


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
def variable_last_available(var_name_by_table, temporary_store = None):
    ''' This function returns the last available information across all datasets
    var_nama_by_table: {table_name: {names: [targeted_variable_name, year_variable_name],
                                     order: integer} }'''
    tables_to_concat = list()
    for table_name, table_info in var_name_by_table.iteritems():
        var_names = table_info['names']
        order = table_info['order']
        table = temporary_store.select(table_name, columns=[['noind'] + var_names])
        table.rename(columns={var_names[0]: 'variable', var_names[1]: 'year'}, inplace=True)
        table.set_index(table.noind, inplace=True)

        table = table.loc[table['variable'].notnull(), :].sort('year')
        table['order'] = order
        tables_to_concat += [table]
    temporary_store.close()
    result = pd.concat(tables_to_concat, axis=0, ignore_index=True).sort(['noind', 'year', 'order'])
    result = result.drop_duplicates(['noind'])
    return result['variable']
