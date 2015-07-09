# -*- coding: utf-8 -*-
'''
Author: LPaul-Delvaux
Created on 18 may 2015
'''

import ConfigParser

from os import path

from format_careers_eir import format_career_tables, format_dates, preliminary_format
from format_individual_info_eir import format_individual_info, format_pension_info
from matching_patrimoine_eic.base.format_careers import aggregate_career_table, career_table_by_time_unit
from matching_patrimoine_eic.base.format_yearly import format_unique_year
from matching_patrimoine_eic.base.load_data import load_data
from matching_patrimoine_eic.base.select_data import select_data
from matching_patrimoine_eic.base.stat_describe import describe_individual_info, describe_missing


def format_data(datasets, time_unit='year', path_storage=False):
    ''' Format datasets '''
    preliminary_format()
    format_dates()
    individual_info_formated = format_individual_info()
    pension_info = format_pension_info()
    format_career_tables(datasets)
    careers_formated = aggregate_career_table(option='eir')
    career_table = career_table_by_time_unit(careers_formated, time_unit)
    data_formated = {'careers': career_table.sort(columns=['noind', 'start_date']),
                     'individus': individual_info_formated,
                     'pension': pension_info}
    return data_formated


# @profile(precision=4)
def import_data(path_data, path_storage, datasets_to_import, file_description_path,
                options_selection=None, test=False, describe=False):
    ''' Main function to load EIR data and put it in the appropriate format
    Input data: raw data available for researchers (.dta format)
    Output: a dict containing two tables -> careers (1 row per indiv*year*status) and individus (1 row per indiv)'''
    load_data(path_storage, path_storage, 'storageEIR_2008', file_description_path,
              datasets_to_import, test=test, ref_table='eir2008_avant08')
    # if 'first_generation' or 'last_generation' in options_selection:
    #    first = options_selection.get("first_generation", None)
    #   last = options_selection.get("last_generation", None)
    #   select_generation_before_format(first, last, 'b100_09', 'an')
    data = format_data(datasets_to_import, time_unit='year', path_storage=path_storage)
    data = select_data(data, file_description_path, options_selection)
    if describe:
        describe_individual_info(data['individus'])
        describe_missing(data, 'sal_brut_deplaf')
    return data


def build_eir_data(test=False, describe=False, options_selection=dict()):
    config_directory = path.normpath(path.join(path.dirname(__file__), '..', '..'))
    config = ConfigParser.ConfigParser()
    config.readfp(open(config_directory + '//config.ini'))
    all_options = dict(config.items('EIR'))
    path_data = all_options.get('path_data')
    path_storage = all_options.get('path_storage')
    file_description_path = path_storage + all_options.get('file_description_name')
    datasets = dict([(generic[:-6], name) for generic, name in all_options.iteritems() if generic[-5:] == 'table'])
    # 7000 = Pole Emploi, 8000 = Dads, 9001 = Etat
    data = import_data(path_data, path_storage, datasets, file_description_path,
                       options_selection, test=test, describe=describe)
    # data = format_unique_year(data, datasets)
    return data


if __name__ == '__main__':
    import time
    print "Début"
    t0 = time.time()
    data = build_eir_data(test=True)
    t1 = time.time()
    print '\n Time for importing data {}s.'.format('%.2f' % (t1 - t0))
