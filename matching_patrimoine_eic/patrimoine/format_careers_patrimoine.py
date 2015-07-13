# -*- coding: utf-8 -*-
"""
Specific function to format Patrimoine information

@author: l.pauldelvaux
"""
import numpy as np
from pandas import DataFrame


def format_career_table(table):
    ''' This function recreates an aggregate table of career with
    Output: 1 row per indiv*time_unit*status'''
    ind = correction_carriere(table)
    noinds = table['noind'].astype(int)
    survey_year = 2009
    date_deb = int(min(ind['cydeb1']))
    n_noind = len(noinds)
    calend = np.zeros((n_noind, survey_year - date_deb), dtype=int)
    nb_events = range(1, 17)

    cols_deb = ['cydeb' + str(i) for i in nb_events]
    tab_deb = ind[cols_deb].fillna(0).astype(int).values

    cols_act = ['cyact' + str(i) for i in nb_events]
    tab_act = np.empty((n_noind, len(nb_events) + 1), dtype=int)
    tab_act[:, 0] = -1
    tab_act[:, 1:] = ind.loc[:, cols_act].fillna(0).astype(int)
    col_idx = np.zeros(n_noind, dtype=int)
    for year in range(date_deb, survey_year):
        to_change = (tab_deb[noinds, col_idx] == year) & (col_idx < 15)
        col_idx[to_change] += 1
        calend[:, year - date_deb] = tab_act[noinds, col_idx]
    colnames = [100 * year + 1 for year in range(date_deb, survey_year)]
    workstates = DataFrame(calend, columns=colnames)
    workstates['noind'] = noinds
    return workstates


def correction_carriere(ind):
    ''' Preliminary corrections on careers '''
    # Note faire attention à la numérotation à partir de 0
    # TODO: faire une verif avec des asserts
    ind['cydeb1'] = ind['prodep']
    liste1 = [6723, 7137, 10641, 21847, 30072, 31545, 33382]
    liste1 = [x - 1 for x in liste1]
    ind.iloc[liste1, 'cydeb1'] = ind.loc[liste1, 'anais'] + 20
    ind.iloc[15206, 'cydeb1'] = 1963
    ind.iloc[27800, 'cydeb1'] = 1999
    ind['modif'] = ""
    ind.iloc[liste1 + [15206, 27800], 'modif'] = "cydeb1_manq"

    ind.iloc[10833, 'cyact3'] = 4
    ind.iloc[23584, 'cyact2'] = 11
    ind.iloc[27816, 'cyact3'] = 5
    ind.iloc[[10833, 23584, 27816], 'modif'] = "cyact manq"
    var = ["cyact", "cydeb", "cycaus", "cytpto"]
    # Note : la solution ne semble pas être parfaite au sens qu'elle ne résout pas tout
    # cond : gens pour qui on a un probleme de date
    cond0 = (ind['cyact2'].notnull()) & (ind['cyact1'].isnull()) & \
    ((ind['cydeb1'] == ind['cydeb2']) | (ind['cydeb1'] > ind['cydeb2']) | (ind['cydeb1'] == (ind['cydeb2'] - 1)))
    cond0.iloc[8297] = True
    ind.loc[cond0, 'modif'] = "decal act"
    # on decale tout de 1 à gauche en espérant que ça résout le problème
    for k in range(1, 16):
        var_k = [x + str(k) for x in var]
        var_k1 = [x + str(k + 1) for x in var]
        ind.loc[cond0, var_k] = ind.loc[cond0, var_k1]

    # si le probleme n'est pas resolu, le souci était sur cycact seulement, on met une valeur
    cond1 = ind['cyact2'].notnull() & ind['cyact1'].isnull() & \
    ((ind['cydeb1'] == ind['cydeb2']) | (ind['cydeb1'] > ind['cydeb2']) | (ind['cydeb1'] == (ind['cydeb2'] - 1)))
    ind.loc[cond1, 'modif'] = "cyact1 manq"
    ind.loc[cond1 & (ind['cyact2'] != 4), 'cyact1'] = 4
    ind.loc[cond1 & (ind['cyact2'] == 4), 'cyact1'] = 2

    cond2 = ind['cydeb1'].isnull() & (ind['cyact1'].notnull() | ind['cyact2'].notnull())
    ind.loc[cond2, 'modif'] = "jeact ou anfinetu manq"
    return ind
