# -*- coding: utf-8 -*-
"""
Created on Wed Jun 03 16:43:33 2015

@author: l.pauldelvaux
"""
import gc
import pandas as pd
from matching_patrimoine_eic.eir.data_handler_eir import build_eir_data
from matching_patrimoine_eic.eic.data_handler_eic import build_eic_data
from matching_patrimoine_eic.base.select_data import select_complete_career
from matching_patrimoine_eic.base.load_data import store_to_hdf
from format_careers import propagate_cadre, imputation_years_missing_dads


def aggregation_info_individus(data, last_year_emp, selection):
    data['individus_eir'].columns = [col + '_eir' if col != 'noind' else col
                                    for col in data['individus_eir'].columns]
    data['individus_eic']['year_retired'] = last_year_emp // 100 + 1
    if selection in ['eic and eir', 'eir and eic']:
        data['individus'] = pd.merge(data['individus_eic'], data['individus_eir'],
                                 on='noind', suffixes=('_eic', '_eir'))
        assert data['individus'].shape[0] == data['individus_eir'].shape[0] == data['individus_eic'].shape[0]
    elif selection == 'eir':
        data['individus'] = pd.merge(data['individus_eic'], data['individus_eir'],
                                 on='noind', how='right', suffixes=('_eic', '_eir'))
        assert data['individus'].shape[0] == data['individus_eir'].shape[0]
    elif selection == 'eic':
        data['individus'] = pd.merge(data['individus_eic'], data['individus_eir'],
                                 on='noind', how='left', suffixes=('_eic', '_eir'))
        assert data['individus'].shape[0] == data['individus_eic'].shape[0]
    elif selection in ['eic or eir', 'eir or eic']:
        data['individus'] = pd.merge(data['individus_eic'], data['individus_eir'],
                                 on='noind', how='outer', suffixes=('_eic', '_eir'))
        assert data['individus'].shape[0] >= data['individus_eic'].shape[0]
        assert data['individus'].shape[0] >= data['individus_eir'].shape[0]
    else:
        print 'Option for selection does not exist'
    return data


def format_career(data, selection, imputation = False):
    ''' This function provides additional checks on career information and reformat this information to get 2 DataFrames
    (salbrut, workstate) -> one row per indiv et 1 col per year '''
    career = data['careers_eic']
    min_year = career['year'].min()
    max_year = career['year'].max()
    career.loc[:, 'year_til'] = (career['year'] * 100 + 1).astype(int)
    salbrut = career.pivot(index='noind', columns='year_til', values='salbrut')
    workstate = career.pivot(index='noind', columns='year_til', values='workstate')
    assert salbrut.shape[0] != 0
    assert workstate.shape[0] != 0
    try:
        assert min(salbrut.columns) == min_year * 100 + 1
    except:
        print salbrut.columns
        print min(salbrut.columns), min_year * 100 + 1
        assert min(salbrut.columns) == min_year * 100 + 1
    try:
        assert max(salbrut.columns) == max_year * 100 + 1
    except:
        print max(salbrut.columns), max_year * 100 + 1
        assert max(salbrut.columns) == max_year * 100 + 1
    assert workstate.shape == salbrut.shape
    assert workstate.shape[0] == data['individus_eic'].shape[0]
    if imputation:
        workstate = propagate_cadre(workstate)
        salbrut, workstate = imputation_years_missing_dads(salbrut, workstate)
    return salbrut, workstate


def aggregation_data(data, selection):
    data['salbrut'], data['workstate'] = format_career(data, selection, imputation = True)
    last_year_emp = data['workstate'].apply(pd.Series.last_valid_index, axis = 1).astype(float).values
    data = aggregation_info_individus(data, last_year_emp, selection)
    return {k: data.get(k, None) for k in ('individus', 'salbrut', 'workstate', 'pension_eir')}


def build_eic_eir_database(selection = 'eic and eir', options_selection=None,
                           test=False, id_select_from_eic = False):
    data_eic = build_eic_data(test=test, describe=False, options_selection=options_selection)
    if 'complete_career' in options_selection.keys() and options_selection['complete_career']:
        data_eic = select_complete_career(data_eic, target_var='salbrut', time_var='year', id_var='noind')
    if id_select_from_eic:
        options_selection['id_selected'] = list(set(data_eic['individus']['noind']))
        print "Nombre d'individus sélectionnés", len(list(set(data_eic['individus']['noind'])))
    data_eir = build_eir_data(test=False, describe=False, options_selection=options_selection)
    individus_eir = list(set(data_eir['individus']['noind']))
    individus_eic = list(set(data_eic['individus']['noind']))
    data = dict()
    selection = selection.lower()
    if selection in ['eic and eir', 'eir and eic']:
        individus_to_keep = list(set(individus_eir) & set(individus_eic))
    elif selection == 'eir':
        individus_to_keep = individus_eir
    elif selection == 'eic':
        individus_to_keep = individus_eic
    elif selection in ['eic or eir', 'eir or eic']:
        individus_to_keep = individus_eir + individus_eic
    else:
        print 'Option for selection does not exist'
    for source in ['eir', 'eic']:
        for name, table in eval('data_' + source).iteritems():
            data[name + '_' + source] = table.loc[table['noind'].isin(individus_to_keep), :]
    data = aggregation_data(data, selection)
    if test:
        store_to_hdf(data, 'C:\Users\l.pauldelvaux\Desktop\MThesis\Data\\test_final.h5')
    else:
        store_to_hdf(data, 'C:\Users\l.pauldelvaux\Desktop\MThesis\Data\\final.h5')
    print "   Yeeees! Your data has been stored, ready to use!"
    return data


def build_patrimoine_database(options_selection=None):
    return data


def data_til_format(data):
    pass


def build_data_eic_eir_til(selection='eir and eic', test=False, return_data = False):
    data = build_eic_eir_database(selection = selection,
                                  test = test,
                                  id_select_from_eic = True,
                                  options_selection=dict(first_generation = 1942,
                                                         last_generation = 1954,
                                                         first_year = 1952,
                                                         last_year = 2009,
                                                         complete_career = True))
    if return_data:
        return data
    else:
        del data
        gc.collect()

if __name__ == '__main__':
    data = build_eic_eir_database(selection='eir and eic',
                                  test=True,
                                  id_select_from_eic = True,
                                  options_selection=dict(first_generation = 1942,
                                                         last_generation = 1954,
                                                         first_year = 1952,
                                                         last_year = 2009,
                                                         complete_career = True))
