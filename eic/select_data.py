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
    codes_regimes_to_import = dict()

    def _select(sheet_name):
        sheet_codes = file_description.parse(sheet_name)
        acronymes = sheet_codes.loc[sheet_codes['acronyme'] != '-', u'acronyme']
        codes_cc = sheet_codes.loc[sheet_codes['acronyme'] != '-', u'Code caisse (CC)']
        to_keep = dict(zip(acronymes, codes_cc))
        return to_keep
    codes_regimes_to_import['b200_09'] = _select('codes_regimes_base')
    codes_regimes_to_import['b100_09'] = codes_regimes_to_import['b200_09']
    codes_regimes_to_import['c200_09'] = _select('codes_regimes_compl')
    codes_regimes_to_import['c100_09'] = codes_regimes_to_import['c200_09']
    return codes_regimes_to_import


def format_dates(data):
    ''' This function specifies Data in the appropriate format :
    noind start_date end_date variable time_unit'''
    data['pe200_09'] = format_dates_pe200(data['pe200_09'])
    data['etat_09'] = format_dates_level200(data['etat_09'])
    data['c200_09'] = format_dates_level200(data['c200_09'])
    data['b200_09'] = format_dates_level200(data['b200_09'])
    data['dads_09'] = format_dates_dads(data['dads_09'])
    return data


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


def format_dates_level200(table):
    table['start_date'] = pd.to_datetime(table['annee'], format="%Y")
    table['end_date'] = table['annee'].astype(str) + '-12-31'
    table.loc[:, 'end_date'] = pd.to_datetime(table.loc[:, 'end_date'], format="%Y-%m-%d")
    table['time_unit'] = 'year'
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


def select_data(data_all, file_description_path, first_year=False, last_year=False):
    ''' This function selects the appropriate information from EIC
    - selection on sources of information/regimes (keeping a subset of rows)
    - selection on years (keeping a subset of rows)
    TODO: - selection on generation '''
    file_description = pd.ExcelFile(file_description_path)
    code_regime_to_import_by_dataset = codes_regimes_to_import(file_description)
    data = select_regimes(data_all, code_regime_to_import_by_dataset)

    data = format_dates(data)
    data = select_years(data, first_year, last_year)

    data = select_individuals_fromb200(data)
    data = select_generation(data, first_generation=1942)
    return data


def select_individuals_fromb200(data):
    ''' This function keeps information only for individuals who are in the b200 database '''
    ind_to_keep = set(data['b200_09']['noind'])
    for dataset in data.keys():
        table = data[dataset]
        data[dataset] = table.loc[table['noind'].isin(ind_to_keep), :]
    return data


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


def select_years(data_all, first_year=False, last_year=False):
    if not first_year and not last_year:
        print "    No selection on years"
        return data_all
    else:
        if first_year and last_year:
            print "    Only years between {} and {} have been selected".format(first_year, last_year)
        if not first_year:
            print "    Only years before {} have been selected".format(last_year)
            first_year = 0
        if not last_year:
            print "    Only years after {} have been selected".format(first_year)
            last_year = 9999

        dataset_names = data_all.keys()
        dataset_by_year = ['b200_09', 'c200_09', 'pe200_09', 'dads_09', 'etat_09']
        dataset_selection = [dataset_name for dataset_name in dataset_names
                            if dataset_names in dataset_by_year]
        for dataset in dataset_selection:
            table = data_all[dataset]
            table = table.drop_duplicates(cols=['noind', 'cc', 'start_year', 'end_year'],
                                          take_last=True)
            to_keep = (table['start_date'].year >= first_year) & (table['start_date'].year <= last_year)
            table = table.loc[to_keep, :]
            data_all[dataset] = table
        return data_all
