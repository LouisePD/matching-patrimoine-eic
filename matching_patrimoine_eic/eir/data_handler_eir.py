# -*- coding: utf-8 -*-
'''
Author: LPaul-Delvaux
Created on 18 may 2015
'''
from format_careers_eir import format_career_tables
from select_data_eir import select_data
from matching_patrimoine_eic.base.format_careers import aggregate_career_table, final_career_table
from matching_patrimoine_eic.base.format_individual_info import format_individual_info
from matching_patrimoine_eic.base.load_data import load_data


def format_data(data, path_storage=False, describe=False):
    ''' Format datasets '''
    if path_storage:
        pss_path = path_storage + 'pss.xlsx'
    else:
        pss_path = False
    careers = format_career_tables(data, pss_path)
    careers_formated = aggregate_career_table(careers)
    career_table = final_career_table(careers_formated)
    individual_info_formated = format_individual_info(data)
    data_formated = {'careers': career_table.sort(columns=['noind', 'start_date']),
                     'individus': individual_info_formated}
    if describe:
        from stat_describe_eic import describe_individual_info, describe_missing
        describe_individual_info(individual_info_formated)
        describe_missing(data_formated, 'sal_brut_deplaf')
    return data_formated


def import_data(path_data, path_storage, datasets_to_import, file_description_path):
    ''' Main function to load EIR data and put it in the appropriate format '''
    data_raw = load_data(path_storage, path_storage, 'storageEIR_2008', file_description_path,
                         datasets_to_import, test=True, ref_table="eir2008_avant08")
    data = format_data(data_raw, path_storage, describe=False)
    data = select_data(data, file_description_path, first_year = 1952, last_year = 2009)
    return data


if __name__ == '__main__':
    import time
    import ConfigParser
    print "Début"
    t0 = time.time()
    from os import path
    config_directory = path.normpath(path.join(path.dirname(__file__), '..', '..'))
    config = ConfigParser.ConfigParser()
    config.readfp(open(config_directory + '//config.ini'))
    path_data = config.get('EIR', 'path_data')
    path_storage = config.get('EIR', 'path_storage')
    file_description_path = path_storage + config.get('EIR', 'file_description_name')
    datasets_to_import = ["eir2008_8000", "eir2008_avant08", "eir2008_7000"]
    data = import_data(path_data, path_storage, datasets_to_import, file_description_path)
    t1 = time.time()
    print '\n Time for importing data {}s.'.format('%.2f' % (t1 - t0))
