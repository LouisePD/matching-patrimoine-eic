# -*- coding: utf-8 -*-
'''
Author: LPaul-Delvaux
Created on 18 may 2015
'''
import datetime as dt
import pandas as pd
from matching_patrimoine_eic.base.select_data import format_dates_dads, format_dates_pe200, codes_regimes_to_import



def format_dates(data):
    ''' This function specifies Data in the appropriate format :
    noind start_date end_date variable time_unit
    data['pe200_09'] = format_dates_pe200(data['pe200_09'])
    data['etat_09'] = format_dates_level200(data['etat_09'])
    data['c200_09'] = format_dates_level200(data['c200_09'])
    data['b200_09'] = format_dates_level200(data['b200_09'])
    data['dads_09'] = format_dates_dads(data['dads_09'])
    return data '''



def select_data(data_all, file_description_path, first_year=False, last_year=False):
    ''' This function selects the appropriate information from EIC
    - selection on sources of information/regimes (keeping a subset of rows)
    - selection on years (keeping a subset of rows)
    TODO: - selection on generation
    file_description = pd.ExcelFile(file_description_path)
    code_regime_to_import_by_dataset = codes_regimes_to_import(file_description)
    data = select_regimes(data_all, code_regime_to_import_by_dataset)

    data = format_dates(data)
    data = select_years(data, first_year, last_year)

    data = select_individuals_fromb200(data)
    data = select_generation(data, first_generation=1942)
    return data '''

