# -*- coding: utf-8 -*-
'''
Author: LPaul-Delvaux
Created on 18 may 2015
'''
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
    return codes_regimes_to_import


def select_complete_career(data, target_var=None, time_var=None, id_var='noind',
                           thresholds = {'nb_years_min': 30, 'missing_wages': 0.05}):
    ''' This function selects individuals with missing work employment status Throughout their career below a threshold
    Note: As selection is based on assumed workstate definitionsg, it can not be included as a step of select_data'''
    df = data['careers'].copy()
    nb_time_by_indiv = df.groupby([id_var])[time_var].count()
    df['missing'] = df[target_var].isnull()
    pct_missing = df.groupby([id_var])['missing'].sum() / nb_time_by_indiv
    to_keep = (nb_time_by_indiv >= thresholds['nb_years_min']) * (pct_missing <= thresholds['missing_wages'])
    indiv_to_keep = list(set(to_keep[to_keep == True].index))
    for table in data.keys():
        df = data[table]
        data[table] = df.loc[df['noind'].isin(indiv_to_keep), :]
    data['selection'] = pd.DataFrame({'nb_times': nb_time_by_indiv, 'missing': pct_missing, 'select': to_keep})
    data['individus']['nb_obs_career'] = nb_time_by_indiv
    return data


def select_generation(data, first_generation, last_generation):
    print "    Only generations between {} and {} have been selected".format(first_generation, last_generation)
    info_birth = data['individus'][['anaiss', 'noind']].copy()
    to_keep = (info_birth.anaiss >= first_generation) & (info_birth.anaiss <= last_generation)
    ind_to_keep = set(info_birth.loc[to_keep, 'noind'])
    for table in data.keys():
        data[table] = data[table].loc[data[table]['noind'].isin(ind_to_keep), :]
    return data


@temporary_store_decorator()
def select_generation_before_format(first_generation, last_generation,
                                    reference_table, var_naiss, temporary_store = None):
    print "    Only generations between {} and {} have been selected".format(first_generation, last_generation)
    info_birth = temporary_store.select(reference_table, columns=[var_naiss, 'noind'])
    to_keep = (info_birth[var_naiss] >= first_generation) & (info_birth[var_naiss] <= last_generation)
    ind_to_keep = set(info_birth.loc[to_keep, 'noind'])
    for table in temporary_store.keys():
        df = temporary_store.select(table, where='noind in ind_to_keep')
        temporary_store.put(table, df, format='table', data_columns=True, min_itemsize = 20)


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


def select_data(data_all, file_description_path, options_selection):
    ''' This function selects the appropriate information from EIC
    - selection on sources of information/regimes (keeping a subset of rows)
    - selection on years (keeping a subset of rows)
    TODO: - selection on generation '''
    options_selection_d = dict(first_year=1952, last_year=2009, first_generation=1932, last_generation=2009)
    if not options_selection:
        options_selection = options_selection_d
    options_selection_d.update(options_selection)
    file_description = pd.ExcelFile(file_description_path)
    code_regime_to_import_by_dataset = codes_regimes_to_import(file_description)
    data = data_all.copy()
    data_careers = data['careers']
    data_careers = select_regimes(data_careers, code_regime_to_import_by_dataset)
    data_careers = select_years(data_careers, options_selection_d['first_year'], options_selection_d['last_year'])
    data['careers'] = data_careers
    data = select_generation(data, options_selection_d['first_generation'], options_selection_d['last_generation'])
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
