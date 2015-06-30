# -*- coding: utf-8 -*-
'''
Author: LPaul-Delvaux
Created on 18 may 2015
'''

import ConfigParser

from os import path
from format_careers_eir import format_career_tables, format_dates, preliminary_format
from format_individual_info_eir import format_individual_info
from matching_patrimoine_eic.base.format_careers import aggregate_career_table, final_career_table
from matching_patrimoine_eic.base.load_data import load_data
from matching_patrimoine_eic.base.select_data import select_data
from matching_patrimoine_eic.base.stat_describe import describe_individual_info, describe_missing


def format_data(data, path_storage=False):
    ''' Format datasets '''
    data = preliminary_format(data)
    data = format_dates(data)
    careers = format_career_tables(data)
    careers_formated = aggregate_career_table(careers)
    career_table = final_career_table(careers_formated)
    individual_info_formated = format_individual_info(data)
    data_formated = {'careers': career_table.sort(columns=['noind', 'start_date']),
                     'individus': individual_info_formated}
    return data_formated


def import_data(path_data, path_storage, datasets_to_import, file_description_path,
                options_selection=None, test=False, describe=False):
    ''' Main function to load EIR data and put it in the appropriate format
    Input data: raw data available for researchers (.dta format)
    Output: a dict containing two tables -> careers (1 row per indiv*year*status) and individus (1 row per indiv)'''
    data_raw = load_data(path_storage, path_storage, 'storageEIR_2008', file_description_path,
                         datasets_to_import, test=test, ref_table="eir2008_avant08")
    data = format_data(data_raw, path_storage)
    data = select_data(data, file_description_path, options_selection)
    if describe:
        describe_individual_info(data['individus'])
        describe_missing(data, 'sal_brut_deplaf')
    return data


def build_eir_data(test=False, options_selection=None):
    config_directory = path.normpath(path.join(path.dirname(__file__), '..', '..'))
    config = ConfigParser.ConfigParser()
    config.readfp(open(config_directory + '//config.ini'))
    path_data = config.get('EIR', 'path_data')
    path_storage = config.get('EIR', 'path_storage')
    file_description_path = path_storage + config.get('EIR', 'file_description_name')
    # 7000 = Pole Emploi, 8000 = Dads, 9001 = Etat
    datasets_to_import = ["eir2008_avant08", "eir2008_8000", "eir2008_7000", "eir2008_9001"]
    data = import_data(path_data, path_storage, datasets_to_import, file_description_path,
                       options_selection, test=True, describe=False)
    return data

if __name__ == '__main__':
    import time
    print "Début"
    t0 = time.time()
    data = build_eir_data()
    t1 = time.time()
    print '\n Time for importing data {}s.'.format('%.2f' % (t1 - t0))
