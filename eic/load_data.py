# -*- coding: utf-8 -*-
'''
Author: LPaul-Delvaux
Created on 18 may 2015
'''

import pandas as pd
from pandas import read_stata
from os import listdir, remove
from os.path import isfile, join


def clean_dta_filename(file_name):
    if file_name[-4:] != '.dta':
        return False
    else:
        return file_name[:-4]


def close_hdf():
    for hdf5 in ['hdf5_read', 'hdf5_write', 'hdf5_append']:
        if hdf5 in globals():
            globals()[hdf5].close()


def create_hdf5_eic_test(file_storage, file_storage_test):
    ''' This function select a subsample of the real dataset '''
    if isfile(file_storage_test):
        hdf = pd.HDFStore(file_storage)
        hdf_test = pd.HDFStore(file_storage_test)
        if hdf.keys() == hdf_test.keys():
            hdf_test.close()
            hdf.close()
            pass
        else:
            hdf_test.close()
            hdf.close()
            remove(file_storage_test)
            create_hdf5_eic_test(file_storage, file_storage_test)
    else:
        "We build the corresponding hdf5 file"
        hdf = pd.HDFStore(file_storage)
        hdf_test = pd.HDFStore(file_storage_test, mode = "w", title = "Test file")
        for dataset in hdf.keys():
            df = pd.DataFrame(hdf.select(dataset, where = 'noind < 113')).reset_index(drop=True)
            hdf_test.put(dataset, df, format = 'table', data_columns = True, min_itemsize = 30)
        hdf_test.close()
        hdf.close()


def create_hdf5_eic(path_data, file_storage, datasets_to_import):
    ''' This function import raw datasets in .dta format and stored them in a .hd5
    (if not already created) '''
    import_stata = True
    try:
        hdf = pd.HDFStore(file_storage)
        datasets_in_hdf = [dataset[8:] for dataset in hdf.keys()]
        datasets_to_import = [dataset for dataset in datasets_to_import if dataset not in datasets_in_hdf]
        if datasets_to_import == []:
            import_stata = False
        hdf.close()
    except:
        pass
    if import_stata:
        print "List of tables to import", datasets_to_import
        hdf = pd.HDFStore(file_storage)
        for dataset in datasets_to_import:
            df = read_stata(path_data + dataset + '.dta').convert_objects()
            hdf.put('tables/' + dataset, df, format='table', data_columns=True)
            print(dataset, ' is now stored in HD5 format')
        hdf.close()


def load_data_eic(path_data, path_storage=None, file_description_path=None,
                  datasets_to_import=None, test=False):
    ''' This function loads te different stata tables, save them in a hdf5 file
    (if not already existing). If file_description is specified,
    only a subset of variables is kept (refering to file_description).
    Output: dict(dataset_name = pandas tables)'''
    if not path_storage:
        path_storage = path_data
    if not datasets_to_import:
        print "No list of datasets is given. We take the .dta files from the path_data directory"
        datasets_to_import = [clean_dta_filename(f) for f in listdir(path_data)
                              if isfile(join(path_data, f)) and clean_dta_filename(f)]
    storage_file = path_storage + 'storage_EIC2009.h5'
    create_hdf5_eic(path_data, storage_file, datasets_to_import)
    if test:
        print "We use a subsample of the extensive dataset"
        storage_test_file = path_storage + 'storage_EIC2009_test.h5'
        create_hdf5_eic_test(storage_file, storage_test_file)
        hdf = pd.HDFStore(storage_test_file)
    else:
        print "We use the extensive dataset"
        hdf = pd.HDFStore(storage_file)
    data = dict()
    if file_description_path:
        selection_to_import = True
        file_description = pd.ExcelFile(file_description_path)
        variables_to_import_by_dataset = variables_to_collect(file_description,
                                                              sheets_to_import=datasets_to_import,
                                                              return_list=True)
    else:
        selection_to_import = False
    for dataset in datasets_to_import:
        if selection_to_import:
            selection_variables = variables_to_import_by_dataset[dataset]
            # print "    The subset of imported variables is:", selection_variables
            data[dataset] = hdf.select('/tables/' + dataset, columns=selection_variables)
        else:
            data[dataset] = hdf.select('/tables/' + dataset)
    print "Raw datasets are now loaded with the following tables: \n", data.keys()
    close_hdf()
    return data


def variables_to_collect(file_description, sheets_to_import=None, info='Type', return_list=False):
    ''' This function builds a dict of variables to import from EIC - with associated info
    (Classe - D: demographic variables, W: work variables, P: pension variables/ Type - Cat, Num, Char/
    to_keep - list of variables to keep) - by table
    output: info_variables_by_dataset = {sheet_name : dict(var_name, var_type)}'''
    info_variables_by_dataset = dict()
    if not sheets_to_import:
        sheets_to_import = file_description.sheet_names()
    for sheet_name in sheets_to_import:
        sheet = file_description.parse(sheet_name)
        variables = list(sheet.loc[sheet['Classe'].isin(['D', 'P', 'W']), 'Nom variable'].str.lower())
        info_variables = list(sheet.loc[sheet['Classe'].isin(['D', 'P', 'W']), info])
        if return_list:
            info_variables_by_dataset[sheet_name] = variables  # .to_list()
        else:
            info_variables_by_dataset[sheet_name] = dict(zip(variables, info_variables))
    return info_variables_by_dataset
