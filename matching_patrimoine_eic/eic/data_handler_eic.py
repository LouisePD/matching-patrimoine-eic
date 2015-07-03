# -*- coding: utf-8 -*-
'''
Author: LPaul-Delvaux
Created on 18 may 2015
'''
from os import path

from format_careers_eic import format_career_tables, format_dates
from format_individual_info_eic import format_individual_info
from matching_patrimoine_eic.base.format_careers import aggregate_career_table, career_table_by_time_unit
from matching_patrimoine_eic.base.format_yearly import format_unique_year
from matching_patrimoine_eic.base.load_data import load_data
from matching_patrimoine_eic.base.select_data import select_data
from matching_patrimoine_eic.base.stat_describe import describe_individual_info, describe_missing


def format_data(data, time_unit='year', path_storage=False):
    ''' Format datasets '''
    data = format_dates(data)
    individual_info_formated = format_individual_info(data)
    if path_storage:
        pss_path = path_storage + 'pss.xlsx'
    else:
        pss_path = False
    careers = format_career_tables(data, pss_path)
    careers_formated = aggregate_career_table(careers)
    career_table = career_table_by_time_unit(careers_formated, time_unit)
    data_formated = {'careers': career_table.sort(columns=['noind', 'start_date']),
                     'individus': individual_info_formated}
    return data_formated


def import_data(path_data, path_storage, datasets_to_import, file_description_path,
                options_selection=None, test=False, describe=False):
    ''' Main function to load EIC data and put it in the appropriate format
    Input data: raw data available for researchers (.dta format)
    Output: a dict containing two tables -> careers (1 row per indiv*year*status) and individus (1 row per indiv)'''
    data_raw = load_data(path_storage, path_storage, 'storageEIC_2009', file_description_path,
                         datasets_to_import, test=test, ref_table='b100_09')
    data = format_data(data_raw, time_unit='year', path_storage=path_storage)
    data = select_data(data, file_description_path, options_selection)
    if describe:
        describe_individual_info(data['individus'])
        describe_missing(data, 'sal_brut_deplaf')
    return data


def build_eic_data(options_selection=None):
    config_directory = path.normpath(path.join(path.dirname(__file__), '..', '..'))
    config = ConfigParser.ConfigParser()
    config.readfp(open(config_directory + '//config.ini'))
    path_data = config.get('EIC', 'path_data')
    path_storage = config.get('EIC', 'path_storage')
    file_description_path = path_storage + config.get('EIC', 'file_description_name')
    datasets_to_import = ["b100_09", "b200_09", "c200_09", "c100_09", "dads_09", "pe200_09", "etat_09"]
    data = import_data(path_data, path_storage, datasets_to_import, file_description_path,
                       options_selection, test=True, describe=False)
    data = format_unique_year(data, option={'complementary': True})
    return data

if __name__ == '__main__':
    import time
    import ConfigParser
    print "Début"
    t0 = time.time()
    data = build_eic_data(options_selection=dict(first_generation = 1942, last_generation = 1960))
    t1 = time.time()
    print '\n Time for importing data {}s.'.format('%.2f' % (t1 - t0))
    #import cProfile
    #command = """import_data(path_data, path_storage, datasets_to_import, file_description_path)"""
    #cProfile.runctx(command, globals(), locals(), filename="OpenGLContext.profile")
