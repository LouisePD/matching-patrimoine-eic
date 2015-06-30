# -*- coding: utf-8 -*-
"""
@author: l.pauldelvaux
"""
import numpy as np
import pandas as pd
from matching_patrimoine_eic.base.format_individual_info import most_frequent


def build_anaiss(data, index):
    ''' Returns a length(index)-vector of years of birth
    - collected from: b100 (check with c100)
    - code: 0 = Male / 1 = female '''
    anaiss = most_frequent(data['eir2008_avant08'], 'an')
    return anaiss


def build_civilstate(data, index):
    ''' Returns a length(index)-vector of civilstate:
    - collected from:
    - code: married == 1, single == 2, divorced  == 3, widow == 4, pacs == 5, couple == 6'''
    data['eir2008_avant08']['sm'] = clean_civilstate_level100(data['eir2008_avant08']['sm'])
    civilstate = most_frequent(data['eir2008_avant08'], 'sm')
    return civilstate


def build_nenf(data, index):
    ''' Returns a length(index)-vector of number of children
    Information is often missing so we use information provided from 3 variables'''
    nenf1 = most_frequent(data['eir2008_avant08'], 'nre')
    nenf2 = most_frequent(data['eir2008_avant08'], 'nrelev')
    nenf3 = most_frequent(data['eir2008_avant08'], 'nrecharge')
    nenf = nenf1
    nenf[nenf.isnull()] = nenf2[nenf.isnull()]
    nenf[nenf.isnull()] = nenf3[nenf.isnull()]
    return nenf


def build_sexe(data, index):
    ''' Returns a length(index)-vector of sexes
    - collected from: b100 (eventual add  with c100)
    - code: 0 = Male / 1 = female (initial code is 1/2)'''
    sexe = most_frequent(data['eir2008_avant08'], 'sexi')
    sexe = sexe.replace([1, 2], [0, 1])
    return sexe


def clean_civilstate_level100(x):
    ''' This function cleans civilstate statuts for 'b100' and 'c100' databases
    initial code: 1==single, 2==married, 3==widow, 4==divorced or separated,
    output code: married == 1, single == 2, divorced  == 3, widow == 4, pacs == 5, couple == 6'''
    x = x.convert_objects(convert_numeric=True).round()
    x.loc[x == 8] = np.nan
    x = x.replace([1, 2, 3, 4],
                  [2, 1, 4, 3])
    return x


def clean_civilstate_etat(x):
    ''' This function cleans civilstate statuts for the 'etat' database
    initial code: 1==single, 2==married, 3==widow, 4==widow, 5==divorced or separated,
    6==pacs, 7==En concubinage
    output code: married == 1, single == 2, divorced  == 3, widow == 4, pacs == 5, couple == 6'''
    x = x.convert_objects(convert_numeric=True).round()
    x.loc[x == 9] = np.nan
    x = x.replace([1, 2, 3, 4, 5, 6, 7],
                  [2, 1, 4, 4, 3, 5, 6])
    return x


def format_individual_info(data):
    ''' This function extracts individual information which is not time dependant
    and recorded at year 2009 from the set of EIC's databases.
    Internal coherence and coherence between data.
    When incoherent information a rule of prioritarisation is defined
    Note: People we want information on = people in b100/b200. '''
    index = sorted(set(data['eir2008_avant08']['noind']))
    columns = ['sexe', 'anaiss', 'nenf', 'civilstate']  # , 'findet']
    info_ind = pd.DataFrame(columns = columns, index = index)
    for variable_name in columns:
        info_ind[variable_name] = eval('build_' + variable_name)(data, index)
    info_ind['noind'] = info_ind.index
    return info_ind
