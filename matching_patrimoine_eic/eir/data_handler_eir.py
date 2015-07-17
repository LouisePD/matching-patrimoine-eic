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
from matching_patrimoine_eic.base.load_data import load_data, store_to_hdf
from matching_patrimoine_eic.base.select_data import select_data, select_generation_before_format
from matching_patrimoine_eic.base.stat_describe import describe_individual_info


def select_regimes_pension(data, codes_regimes_to_keep):
    # TODO: Rework on it in order to have a more general fct

    df = data['pension']
    data['pension'] = df.loc[df['cc'].isin(codes_regimes_to_keep.keys()), :]
    id_to_keep = set(list(data['pension']['noind']))
    for table in data.keys():
        data[table] = data[table].loc[data[table]['noind'].isin(id_to_keep), :]
    data['pension'].loc[:, 'regime'] = data['pension'].loc[:, 'cc'].replace(codes_regimes_to_keep)
    return data


def format_data(datasets, format_careers, time_unit='year', path_storage=False):
    ''' Format datasets '''
    if format_careers:
        preliminary_format()
        format_dates()
    individual_info_formated = format_individual_info()
    pension_info = format_pension_info()
    data_formated = {'individus': individual_info_formated,
                     'pension': pension_info}
    if format_careers:
        format_career_tables(datasets)
        careers_formated = aggregate_career_table(option='eir')
        career_table = career_table_by_time_unit(careers_formated, time_unit)
        data_formated['careers'] = career_table.sort(columns=['noind', 'start_date'])
    return data_formated


# @profile(precision=4)
def import_data(path_data, path_storage, datasets_to_import, file_description_path,
                options_selection=None, test=False, describe=False, format_careers = False):
    ''' Main function to load EIR data and put it in the appropriate format
    Input data: raw data available for researchers (.dta format)
    Output: a dict containing two tables -> careers (1 row per indiv*year*status) and individus (1 row per indiv)'''
    # TODO: Ne pas mettre en dure:
    cc_to_keep = dict([(10.0, 'RG'), (12.0, 'FP_a'), (13.0, 'FP_s'),
                       (5001.0, 'Agirc'), (6000.0, 'Arcco')])
    id_selected = options_selection.get('id_selected', None)
    first_generation = options_selection.get("first_generation", None)
    last_generation = options_selection.get("last_generation", None)
    load_data(path_storage, path_storage, 'storageEIR_2008', file_description_path,
              datasets_to_import, test=test, ref_table='eir2008_avant08', id_selected = id_selected)
    if (first_generation or last_generation) and not id_selected:
        select_generation_before_format(first_generation, last_generation, 'eir2008_avant08', 'an')
    data = format_data(datasets_to_import, format_careers, time_unit='year', path_storage=path_storage)
    if format_careers:
        data = select_data(data, file_description_path, options_selection)
    else:
        data = select_regimes_pension(data, cc_to_keep)
    if describe:
        describe_individual_info(data['individus'])
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
    file_storage_path = path_storage + 'final_eir.h5'
    store_to_hdf(data, file_storage_path)
    return data


if __name__ == '__main__':
    import time
    print "Début"
    t0 = time.time()
    data = build_eir_data(test=False,
                          options_selection=dict(complete_career = True,
                                                 first_generation = 1942,
                                                 last_generation = 1958,
                                                 id_selected = [6850, 9980, 21060, 40270]))
    t1 = time.time()
    print '\n Time for importing data {}s.'.format('%.2f' % (t1 - t0))
