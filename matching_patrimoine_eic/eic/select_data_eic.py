# -*- coding: utf-8 -*-
'''
Author: LPaul-Delvaux
Created on 18 may 2015
'''

import pandas as pd
from matching_patrimoine_eic.base.select_data import format_dates_dads, format_dates_pe200, codes_regimes_to_import, select_regimes, select_generation


def format_dates(data):
    ''' This function specifies Data in the appropriate format :
    noind start_date end_date variable time_unit'''
    data['pe200_09'] = format_dates_pe200(data['pe200_09'])
    data['etat_09'] = format_dates_level200(data['etat_09'])
    data['c200_09'] = format_dates_level200(data['c200_09'])
    data['b200_09'] = format_dates_level200(data['b200_09'])
    data['dads_09'] = format_dates_dads(data['dads_09'])
    return data


def format_dates_level200(table):
    table['start_date'] = pd.to_datetime(table['annee'], format="%Y")
    table['end_date'] = table['annee'].astype(str) + '-12-31'
    table.loc[:, 'end_date'] = pd.to_datetime(table.loc[:, 'end_date'], format="%Y-%m-%d")
    table['time_unit'] = 'year'
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
