# -*- coding: utf-8 -*-
'''
Author: LPaul-Delvaux
Created on 18 may 2015
'''
import ConfigParser

from os import path
from pandas import read_stata
from format_careers_patrimoine import format_career_table
from matching_patrimoine_eic.base.select_data import select_generation


def identind_noind_equivalence(table):
    table.sort(columns=['identind'], inplace=True)
    table['noind'] = range(table.shape[0])
    equivalence = table[['identind', 'noind']]
    table.drop('identind', axis=1, inplace=True)
    return table, equivalence


def format_data(data, path_storage=False):
    ''' Format datasets '''
    career_table = format_career_table(data)
    individual_info_formated = format_individual_info(data)
    data_formated = {'careers': career_table, # .sort(columns=['noind', 'start_date']),
                     'individus': individual_info_formated}
    return data_formated


def format_individual_info(table):
    info_ind = table[['noind', 'anais', 'enfant', 'pond']].rename(columns={'anais': 'anaiss'})
    info_ind.index = info_ind['noind']
    return info_ind


def load_data(path_dataset_to_import):
    df = read_stata(path_dataset_to_import + '.dta').convert_objects()
    return df


def import_data(path_data, path_storage, datasets_to_import):
    ''' Main function to load Patrimoine data.
    Input data: should be Patrimoine.h5 from Til (temporary individus.dta)
    Output: '''
    assert len(datasets_to_import) == 1
    path_dataset_to_import = path_storage + datasets_to_import[0]
    table_raw = load_data(path_dataset_to_import)
    table, identind_to_noind = identind_noind_equivalence(table_raw)
    data = format_data(table, path_storage)
    data = select_generation(data, first_generation = 1942, last_generation = 1954)
    return data


def build_patrimoine_data(data_source='dta'):
    if data_source == 'dta':
        config_directory = path.normpath(path.join(path.dirname(__file__), '..', '..'))
        config = ConfigParser.ConfigParser()
        config.readfp(open(config_directory + '//config.ini'))
        path_data = config.get('PATRIMOINE', 'path_data')
        path_storage = config.get('PATRIMOINE', 'path_storage')
        datasets_to_import = ["individu"]
        data = import_data(path_data, path_storage, datasets_to_import)
        return data
    elif data_source == 'Til':
        pass
    else:
        print 'Data source {} not taken into account'.format()

if __name__ == '__main__':
    import time
    print "Début"
    t0 = time.time()
    data = build_patrimoine_data()
    t1 = time.time()
    print '\n Time for importing data {}s.'.format('%.2f' % (t1 - t0))
