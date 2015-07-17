# -*- coding: utf-8 -*-
'''
Author: LPaul-Delvaux
Created on 18 may 2015
'''
import gc
import pandas as pd
from load_data import temporary_store_decorator


def codes_regimes_to_import(file_description):
    ''' Collect codes of regimes -basic and supplementary schemes -
    to import. output: {b200_09: {acronyme_regime: code_regime}
                        c200_09: {acronyme_regime: code_regime} '''
    tables_sheets_code = file_description.parse('association_codes_tables')
    tables = tables_sheets_code['Table']
    sheets_code = tables_sheets_code['Code_sheet']
    codes_regimes_to_import = dict()

    def _select(sheet_name):
        sheet_codes = file_description.parse(sheet_name)
        acronymes = sheet_codes.loc[sheet_codes['acronyme'] != '-', u'acronyme']
        codes_cc = sheet_codes.loc[sheet_codes['acronyme'] != '-', u'Code caisse (CC)']
        to_keep = dict(zip(acronymes, codes_cc))
        return to_keep
    for table, sheet_code in zip(tables, sheets_code):
        codes = _select(sheet_code)
        codes_regimes_to_import[table] = codes
    file_description.close()
    return codes_regimes_to_import


def select_complete_career(data, target_var=None, time_var=None, id_var='noind',
                           thresholds = {'nb_years_min': 30, 'missing_wages': 0.10}):
    ''' This function selects individuals with missing work employment status Throughout their career below a threshold
    Note: As selection is based on assumed workstate definitionsg, it can not be included as a step of select_data'''
    df = data['careers']
    assert sum(df[time_var].isnull()) == 0
    assert (df['time_unit'] == time_var).all()
    assert sum(df.duplicated([time_var, id_var])) == 0
    nb_time_by_indiv = df.groupby([id_var], sort = True)[time_var].count()
    min_by_indiv = df.groupby([id_var], sort = True)[time_var].min()
    max_by_indiv = df.groupby([id_var], sort = True)[time_var].max()
    range_years = max_by_indiv - min_by_indiv + 1
    assert (nb_time_by_indiv <= range_years).all()
    df['missing'] = (df[target_var].isnull()).astype(int)
    nb_unknown_years = df.groupby([id_var], sort = True)['missing'].sum()
    pct_missing = 1 - nb_time_by_indiv / range_years
    to_keep = (range_years >= thresholds['nb_years_min']) * (pct_missing <= thresholds['missing_wages'])
    indiv_to_keep = list(set(to_keep[to_keep].index))
    assert len(indiv_to_keep) < len(to_keep)
    try:
        assert len(indiv_to_keep) != 0
    except:
        print 'Options of selection - see below might be too restrictive: \n'.format(thresholds)
        print sum(range_years >= thresholds['nb_years_min'])
        print sum(pct_missing <= thresholds['missing_wages'])
        assert len(indiv_to_keep) != 0
    data['individus'] = data['individus'].sort(['noind'])
    if len(data['individus'].index) != len(nb_time_by_indiv.index):
        should_not_exist = [idx for idx in data['individus'].index if idx not in nb_time_by_indiv.index]
        print('People with individual information but no information on their career \n {}'.format(should_not_exist))
    df = data['individus']
    df.loc[df['noind'].isin(nb_time_by_indiv.index.values), 'nb_obs_career'] = nb_time_by_indiv.values.astype(int)
    df.loc[df['noind'].isin(min_by_indiv.index.values), 'min_year_career'] = min_by_indiv.astype(int)
    df.loc[df['noind'].isin(pct_missing.index.values), 'pct_missing'] = pct_missing.values
    df.loc[df['noind'].isin(max_by_indiv.index.values), 'max_year_career'] = max_by_indiv.astype(int)
    data['individus'] = df
    for table in data.keys():
        data[table] = data[table].loc[data[table]['noind'].isin(indiv_to_keep), :]
        print table
        assert data[table].shape[0] != 0
    return data


def select_generation(data, first_generation, last_generation):
    print "    Only generations between {} and {} have been selected".format(first_generation, last_generation)
    info_birth = data['individus'][['anaiss', 'noind']].copy()
    to_keep = (info_birth.anaiss >= first_generation) & (info_birth.anaiss <= last_generation)
    ind_to_keep = list(set(info_birth.loc[to_keep, 'noind']))
    for table in data.keys():
        data[table] = data[table].loc[data[table]['noind'].isin(ind_to_keep), :]
    return data


@temporary_store_decorator()
def select_generation_before_format(first_generation, last_generation,
                                    reference_table, var_naiss, temporary_store = None):
    print "    Only generations between {} and {} have been selected".format(first_generation, last_generation)
    if not first_generation:
        first_generation = 1900
    if not last_generation:
        last_generation = 2050
    info_birth = temporary_store.select(reference_table, columns=[var_naiss, 'noind'])
    to_keep = (info_birth[var_naiss] >= first_generation) & (info_birth[var_naiss] <= last_generation)
    ind_to_keep = list(set(info_birth.loc[to_keep, 'noind']))
    for table_name in temporary_store.keys():
        df = temporary_store.select(table_name, where='noind=ind_to_keep')
        temporary_store.remove(table_name)
        temporary_store.put(table_name, df, format='table', data_columns=True, min_itemsize = 20)


def select_regimes(table_career, code_regime_to_import_by_dataset):
    to_keep_code = []
    cond_dataset = []
    for dataset, regimes in code_regime_to_import_by_dataset.iteritems():
        print "    Selection of regimes for dataset {}: \n {} \n".format(dataset, regimes.keys())
        to_keep_code += regimes.values()
        cond_dataset += [dataset]
    to_keep = table_career['cc'].astype(float).isin(to_keep_code) | (table_career['source'] == 'pe200_09')
    table_career = table_career.loc[to_keep, :]
    return table_career


def select_data(data, file_description_path, options_selection):
    ''' This function selects the appropriate information from EIC
    - selection on sources of information/regimes (keeping a subset of rows)
    - selection on years (keeping a subset of rows)
    TODO: - selection on generation '''
    assert data['individus'].shape[0] > 1
    options_selection_d = dict(first_year=1952, last_year=2009, first_generation=1932, last_generation=2009)
    if not options_selection:
        options_selection = options_selection_d
    options_selection_d.update(options_selection)
    file_description = pd.ExcelFile(file_description_path)

    code_regime_to_import_by_dataset = codes_regimes_to_import(file_description)
    del file_description
    gc.collect()

    data_careers = data['careers']
    data_careers = select_regimes(data_careers, code_regime_to_import_by_dataset)
    assert data['individus'].shape[0] !=0
    del code_regime_to_import_by_dataset
    gc.collect

    data_careers = select_years(data_careers, options_selection_d['first_year'], options_selection_d['last_year'])
    data['careers'] = data_careers
    assert data['individus'].shape[0] !=0

    ind_to_keep = set(data_careers['noind'])
    data['individus'] = data['individus'].loc[data['individus']['noind'].isin(ind_to_keep), :]
    assert data['careers'].shape[0] != 0
    assert data['individus'].shape[0] !=0
    return data


def select_years(table_careers, first_year=False, last_year=False):
    if not first_year and not last_year:
        print "    No selection on years"
        return table_careers
    else:
        if first_year and last_year:
            print "    Only years between {} and {} have been selected".format(first_year, last_year)
        if not first_year:
            print "    Only years before {} have been selected".format(last_year)
            first_year = 0
        if not last_year:
            print "    Only years after {} have been selected".format(first_year)
            last_year = 9999
        # table_careers = table_careers.drop_duplicates(cols=['noind', 'cc', 'start_date', 'end_year'], take_last=True)
        start_year = table_careers['start_date'].apply(lambda x: x.year)
        to_keep = (start_year >= first_year) * (start_year <= last_year)
        table_careers = table_careers.loc[to_keep, :]
        return table_careers
