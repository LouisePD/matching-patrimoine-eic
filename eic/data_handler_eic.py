# -*- coding: utf-8 -*-
'''
Author: LPaul-Delvaux
Created on 18 may 2015
'''
from format_careers import format_career_tables
from format_individual_info import format_individual_info
from load_data import load_data_eic
from select_data import select_data


def format_data(data):
    ''' Format datasets '''
    careers = format_career_tables(data)
    individual_info_formated = format_individual_info(data)
    data_formated = {'careers': careers_formated, 'individus': individual_info_formated}
    return data_formated


def import_data(path_data, path_storage, datasets_to_import, file_description_path):
    ''' Main function to load EIC data and put it in the appropriate format '''
    data_raw = load_data_eic(path_storage, path_storage,
                             file_description_path, datasets_to_import, test=True)
    data = select_data(data_raw, file_description_path,
                       first_year = 1952, last_year = 2009)
    data = format_data(data)
    return data


if __name__ == '__main__':
    import time
    import ConfigParser
    print "Début"
    t0 = time.time()

    config = ConfigParser.ConfigParser()
    config.readfp(open('config.ini'))
    path_data = config.get('EIC', 'path_data')
    path_storage = config.get('EIC', 'path_storage')
    file_description_path = path_storage + config.get('EIC', 'file_description_name')
    datasets_to_import = ["b100_09", "b200_09", "c200_09", "c100_09", "dads_09", "pe200_09", "etat_09"]
    data = import_data(path_data, path_storage, datasets_to_import, file_description_path)

    t1 = time.time()
    print '\n Time for importing data {}s.'.format('%.2f' % (t1 - t0))
