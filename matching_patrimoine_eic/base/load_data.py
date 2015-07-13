# -*- coding: utf-8 -*-
'''
Author: LPaul-Delvaux
Created on 18 may 2015
'''
import ConfigParser
import gc
import pandas as pd
from pandas import read_stata
from os import listdir, remove, path
from os.path import isfile, join
from numpy import array, unique
from memory_profiler import profile


def clean_dta_filename(file_name):
    if file_name[-4:] != '.dta':
        return False
    else:
        return file_name[:-4]


def close_hdf():
    for hdf5 in ['hdf5_read', 'hdf5_write', 'hdf5_append']:
        if hdf5 in globals():
            globals()[hdf5].close()


def create_hdf5_test(file_storage, file_storage_test, nb_indiv, ref_table):
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
            create_hdf5_test(file_storage, file_storage_test, nb_indiv, ref_table)
    else:
        "We build the corresponding hdf5 file"
        from random import sample
        hdf = pd.HDFStore(file_storage)
        hdf_test = pd.HDFStore(file_storage_test, mode = "w", title = "Test file")
        if not ref_table:
            ref_table = hdf.keys()[0]
        selected_index = sample(unique(array(hdf.select('tables/' + ref_table, columns = ['noind']))), nb_indiv)
        for dataset in hdf.keys():
            df = pd.DataFrame(hdf.select(dataset, where = 'noind=' + str(selected_index))).reset_index(drop=True)
            hdf_test.put(dataset, df, format = 'table', data_columns = True, min_itemsize = 30)
        hdf_test.close()
        hdf.close()


def create_hdf5(path_data, file_storage, datasets_to_import):
    ''' This function import raw datasets in .dta format and stored them in a .hd5
    (if not already created) '''
    import_stata = True
    try:
def create_hdf5_select(file_storage, file_storage_select, id_selected):
    ''' This function select a subsample of the real dataset '''
    if isfile(file_storage_select):
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
        hdf_select = pd.HDFStore(file_storage_select)
        if hdf.keys() == hdf_select.keys():
            hdf_select.close()
            hdf.close()
            pass
        else:
            hdf_select.close()
            hdf.close()
            remove(file_storage_select)
            create_hdf5_select(file_storage, file_storage_select, id_selected)
    else:
        "We build the corresponding hdf5 file"
        hdf = pd.HDFStore(file_storage)
        for dataset in datasets_to_import:
            df = read_stata(path_data + dataset + '.dta').convert_objects()
            hdf.put('tables/' + dataset, df, format='table', data_columns=True)
            print(dataset, ' is now stored in HD5 format')
        hdf.close()


# @profile(precision=4)
def load_data(path_data, path_storage=None, hdf_name=None, file_description_path=None,
              datasets_to_import=None, test=False, nb_indiv=400, ref_table=None, id_selected=None):
    ''' This function loads te different stata tables, save them in a hdf5 file
    (if not already existing). If file_description is specified,
    only a subset of variables is kept (refering to file_description).
    Output: dict(dataset_name = pandas tables)'''
    datasets_to_import = [table for table in datasets_to_import.values()]
    if not path_storage:
        path_storage = path_data
    if not datasets_to_import:
        print "No list of datasets is given. We take the .dta files from the path_data directory"
        datasets_to_import = [clean_dta_filename(f) for f in listdir(path_data)
                              if isfile(join(path_data, f)) and clean_dta_filename(f)]
    if not hdf_name:
        hdf_name = 'storage'
    storage_file = path_storage + hdf_name + '.h5'
    create_hdf5(path_data, storage_file, datasets_to_import)
    if id_selected:
        print "We use a subsample of the extensive dataset with selected individuals"
        storage_select_file = path_storage + hdf_name + '_selected.h5'
        create_hdf5_select(storage_file, storage_select_file, id_selected)
        hdf = pd.HDFStore(storage_select_file)
    elif test:
        print "We use a subsample of the extensive dataset with {} individuals (randomly selected)".format(nb_indiv)
        storage_test_file = path_storage + hdf_name + '_test.h5'
        create_hdf5_test(storage_file, storage_test_file, nb_indiv, ref_table)
        hdf = pd.HDFStore(storage_test_file)
    else:
        print "We use the extensive dataset"
        hdf = pd.HDFStore(storage_file)
    if file_description_path:
        selection_to_import = True
        file_description = pd.ExcelFile(file_description_path)
        variables_to_import_by_dataset = variables_to_collect(file_description,
                                                              sheets_to_import=datasets_to_import)
    else:
        selection_to_import = False
    temp_file_path = temporary_store_path()
    temp = pd.HDFStore(temp_file_path, mode = "w", title = "Temporary file")
    for dataset in datasets_to_import:
        if selection_to_import:
            type_variables = variables_to_import_by_dataset[dataset]
            df = hdf.select('/tables/' + dataset, columns=type_variables.keys())
            df = type_variable_table(df, type_variables)
        else:
            df = hdf.select('/tables/' + dataset)
        temp.put(dataset, df, format='table', data_columns=True, min_itemsize = 20)
    print "Raw datasets are now loaded with the following tables: \n", datasets_to_import
    temp.close()
    hdf.close()
    close_hdf()


def temporary_store_path(file_name_tmp = None):
    config_directory = path.normpath(path.join(path.dirname(__file__), '..', '..'))
    config = ConfigParser.ConfigParser()
    config.readfp(open(config_directory + '//config.ini'))
    all_options = dict(config.items('TEMPORARY'))
    tmp_directory = all_options.get('path_storage')
    if not file_name_tmp:
        file_name_tmp = 'temporary_storage'
    assert file_name_tmp is not None
    if not file_name_tmp.endswith('.h5'):
        file_name_tmp = "{}.h5".format(file_name_tmp)
    file_path_tmp = path.join(tmp_directory, file_name_tmp)
    return file_path_tmp


def temporary_store_decorator():
    file_path = temporary_store_path()

    def actual_decorator(func):
        def func_wrapper(*args, **kwargs):
            temporary_store = pd.HDFStore(file_path)
            try:
                return func(*args, temporary_store = temporary_store, **kwargs)
            finally:
                gc.collect()
                temporary_store.close()
        return func_wrapper

    return actual_decorator


def type_variables_data(data, type_variables_by_dataset):
    for dataset in data.keys():
        type_variables = type_variables_by_dataset[dataset]
        for var_name, var_type in type_variables.iteritems():
            try:
                data[dataset][var_name] = type_variable_vect(data[dataset][var_name], var_type)
            except:
                print "Type {} not taken into account for {} in {}".format(var_type, var_name, dataset)
    return data


def type_variable_table(table, type_variables):
    for var_name, var_type in type_variables.iteritems():
        try:
            table[var_name] = type_variable_vect(table[var_name], var_type)
        except:
            print "Type {} not taken into account for {} in {}".format(var_type, var_name, table.name)
    return table


def type_variable_vect(vect, vtype):
    if vtype == 'Num':
        return vect.convert_objects(convert_numeric=True).round(2)
    elif vtype in ['Str', 'Alph']:
        return vect.astype(str)
    elif vtype in ['Int', 'Cat']:
        return vect.convert_objects(convert_numeric=True).round()
    else:
        print "Type not taken into account {}".format(vtype)
        return vect


def variables_to_collect(file_description, sheets_to_import=None, info='Type', return_list=False):
    ''' This function builds a dict of variables to import from the set of tables - with associated info
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
