# -*- coding: utf-8 -*-
"""
@author: l.pauldelvaux
"""
import numpy as np
import pandas as pd


def describe_individual_info(info_ind):
    for c in info_ind.columns:
        print "------------ %s -----------" % c
        print info_ind[c].value_counts()
    print info_ind.describe()


def describe_missing(data, var, id_var='noind', time_var='year', time_step='year'):
    ''' This function takes as argument a dataframe with a (id_var*time_var) per row format
    and describes the number of missing time_unit between minimum(time_var) and maximum(time_var)
    per individual '''
    careers = data['careers']
    year_birth = data['individus']['anaiss']
    df = careers[[id_var, time_var, var]].copy().groupby([id_var, time_var]).sum().reset_index()
    df['missing'] = df[var].isnull()
    min_time_name = 'Min. avail. {}'.format(time_step)
    max_time_name = 'Max. avail. {}'.format(time_step)
    nb_obs = df.groupby([id_var])[id_var].count()
    min_time_var = df.groupby([id_var])[time_var].min()
    max_time_var = df.groupby([id_var])[time_var].max()
    range_time_var = max_time_var - min_time_var + 1
    pct_missing = (df.groupby([id_var])['missing'].sum() + (range_time_var - nb_obs)) / range_time_var
    pct_missing2 = df.groupby([id_var])['missing'].sum() / range_time_var
    missing_by_ind = pd.DataFrame({min_time_name: min_time_var,
                             max_time_name: max_time_var,
                             'Pct. Missing': pct_missing.round(2),
                             'Pct. Missing when workstate': pct_missing2.round(2),
                             'Year of Birth': year_birth,
                             'Nb indiv.': df.groupby([id_var])[id_var].count()})

    mean_variables = [name for name in missing_by_ind.keys() if name not in ['Year of Birth', 'Nb indiv.']]
    missing_by_anaiss = missing_by_ind.groupby(['Year of Birth'])[mean_variables].mean().astype('f2')
    missing_by_anaiss = missing_by_anaiss.join(missing_by_ind.groupby(['Year of Birth'])['Nb indiv.'].sum())
    print missing_by_anaiss
    nb_obs = df.groupby([time_var])[id_var].count()
    df['missing'] = df[var].isnull()
    pct_missing = df.groupby([time_var])['missing'].sum() / nb_obs
    missing_by_year = pd.DataFrame({'Nb. Obs': nb_obs,
                                    'Pct. Missing': pct_missing.round(2)})
    print missing_by_year

