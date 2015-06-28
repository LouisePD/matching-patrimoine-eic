# -*- coding: utf-8 -*-
'''
Author: LPaul-Delvaux
Created on 18 may 2015
'''
import datetime as dt
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


def format_dates_pe200(table):
    ''' Rework on pole-emploi database dates'''
    def _convert_stata_dates(stata_vector):
        ''' In Stata dates are coded in number of days from 01jan1960 (=0) '''
        initial_date = pd.to_datetime('1960-01-01', format="%Y-%m-%d")
        dates = initial_date + stata_vector.apply((lambda x: dt.timedelta(days= int(x))))
        return dates
    for date_var in ['pjcdtdeb', 'pjcdtfin']:
        table.loc[:, date_var] = _convert_stata_dates(table.loc[:, date_var])
    table.rename(columns={'pjcdtdeb': 'start_date', 'pjcdtfin': 'end_date'}, inplace=True)
    table['time_unit'] = 'day'
    return table


def format_dates_dads(table):
    def _convert_daysofyear(x):
        try:
            return int(x) - 1
        except:
            return 0
    table['start_date'] = pd.to_datetime(table['annee'], format="%Y")
    table['start_date'] += table['debremu'].apply((lambda x: dt.timedelta(days=_convert_daysofyear(x))))
    table['end_date'] = table['annee'].astype(str) + '-12-31'
    table.loc[:, 'end_date'] = pd.to_datetime(table.loc[:, 'end_date'], format="%Y-%m-%d")
    table['time_unit'] = 'year'
    table = table.drop(['annee', 'debremu'], axis=1)
    return table


def select_generation(data, first_generation=1934, last_generation=2009):
    info_birth = data['b100_09'][['noind', 'an']].copy().astype(int).drop_duplicates()
    ind_to_keep = set(info_birth.loc[(info_birth.an >= first_generation) & (info_birth.an <= last_generation), 'noind'])
    for dataset in data.keys():
        table = data[dataset]
        data[dataset] = table.loc[table['noind'].isin(ind_to_keep), :]
    return data


def select_regimes(data, code_regime_to_import_by_dataset):
    for dataset, regimes in code_regime_to_import_by_dataset.iteritems():
        table = data[dataset]
        table.loc[:, 'cc'] = table.loc[:, 'cc'].astype(int)
        to_keep = table.loc[:, 'cc'].isin(regimes.values())
        data[dataset] = table.loc[to_keep, :]
        print "    Selection of regimes for dataset {}: \n {} \n".format(dataset, regimes.keys())
    return data

