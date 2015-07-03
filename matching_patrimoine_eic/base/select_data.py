# -*- coding: utf-8 -*-
'''
Author: LPaul-Delvaux
Created on 18 may 2015
'''
import pandas as pd

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


def select_available_carrer(career_table, threshold = 0.95):
    ''' This function selects individuals with known work employment status for at least threshold% of their career '''
    pass


def select_generation(data, first_generation, last_generation):
    print "    Only generations between {} and {} have been selected".format(first_generation, last_generation)
    info_birth = data['individus'][['anaiss', 'noind']].copy()
    to_keep = (info_birth.anaiss >= first_generation) & (info_birth.anaiss <= last_generation)
    ind_to_keep = set(info_birth.loc[to_keep, 'noind'])
    for table in data.keys():
        data[table] = data[table].loc[data[table]['noind'].isin(ind_to_keep), :]
    return data


def select_regimes(table_career, code_regime_to_import_by_dataset):
    for dataset, regimes in code_regime_to_import_by_dataset.iteritems():
        to_drop = (~table_career['cc'].astype(float).isin(regimes.values())) * (table_career['source'] == dataset)
        table_career = table_career.loc[~to_drop, :]
        print "    Selection of regimes for dataset {}: \n {} \n".format(dataset, regimes.keys())
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
    data_careers = data_all['careers']
    data_careers = select_regimes(data_careers, code_regime_to_import_by_dataset)
    data_careers = select_years(data_careers, options_selection_d['first_year'], options_selection_d['last_year'])
    data = select_generation(data_all, options_selection_d['first_generation'], options_selection_d['last_generation'])
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
        #table_careers = table_careers.drop_duplicates(cols=['noind', 'cc', 'start_date', 'end_year'], take_last=True)
        to_keep = (table_careers['start_date'].apply(lambda x: x.year) >= first_year) & (table_careers['start_date'].apply(lambda x: x.year) <= last_year)
        table_careers = table_careers.loc[to_keep, :]
        return table_careers
