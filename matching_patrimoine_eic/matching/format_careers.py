# -*- coding: utf-8 -*-
"""
Created on Wed Jun 03 16:43:33 2015

@author: l.pauldelvaux
"""


def cadre_once(row):
    lrow = list(row)
    if 4 in lrow:
        index_first = lrow.index(4)
        lrow = [4 if x == 3 and i >= index_first else x for i, x in enumerate(lrow)]
        return lrow
    else:
        return row


def imputation_from_adjency(salbrut, workstate, year):
    date = (year * 100 + 1)
    next_date = ((year + 1) * 100 + 1)
    last_date = ((year - 1) * 100 + 1)
    to_impute = workstate.loc[:, date].isnull()
    imput = workstate.loc[to_impute, next_date]
    imput2 = workstate.loc[to_impute, last_date]
    imput.loc[imput.isnull()] = imput2.loc[imput.isnull()]
    workstate.loc[to_impute, date] = imput
    to_impute = (~workstate.loc[:, date].isnull()) * (salbrut.loc[:, date].isnull())
    salbrut.loc[to_impute, date] = salbrut[[last_date, next_date]].mean(skipna=True)
    return salbrut, workstate


def propagate_cadre(workstate):
    ''' This function assume that when a person has been 'cadre' once in his life,
    he is 'cadre' for the rest of his life '''
    workstate = workstate.apply(cadre_once, axis=1)
    return workstate


def imputation_years_missing_dads(salbrut, workstate):
    ''' Règles d'imputation pour les années complètement manquantes de DADs:
    1- On propage d'abord le workstate si missing
    2- On propage ensuite le salaire'''
    missing_year_dads = [1979, 1981, 1987]
    for miss_year in missing_year_dads:
        salbrut, workstate = imputation_from_adjency(salbrut, workstate, miss_year)
    return salbrut, workstate
