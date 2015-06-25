# -*- coding: utf-8 -*-
"""
Specific function to format EIC tables

@author: l.pauldelvaux
"""

import numpy as np
import pandas as pd
from matching_patrimoine_eic.base.format_careers import benefits_from_pe, clean_earning


def format_career_dads(data_dads):
    workstate_variables = ['cda', 'cs1', 'domempl', 'tain']
    formated_dads = data_dads[['noind', 'start_date', 'end_date', 'time_unit'] + workstate_variables].copy()
    formated_dads['sal_brut_deplaf'] = wages_from_dads(data_dads)
    return formated_dads


def format_career_etat(data_etat):
    workstate_variables = ['st', 'stag', 'enreg']
    formated_etat = data_etat[['noind', 'start_date', 'end_date', 'time_unit'] + workstate_variables].copy()
    formated_etat['sal_brut_deplaf'] = wages_from_etat(data_etat)
    return formated_etat


def format_career_l200(data_l200, level, pss):
    wages_from = 'wages_from_' + level
    workstate_variables = ['st', 'statutp', 'cc']
    formated_l200 = data_l200[['noind', 'start_date', 'end_date', 'time_unit'] + workstate_variables].copy()
    formated_l200['sal_brut_plaf'], formated_l200['sal_brut_deplaf'] = eval(wages_from)(data_l200, pss)
    return formated_l200


def format_career_pe200(data_pe):
    workstate_variables = ['pjcall2']
    formated_pe = data_pe[['noind', 'start_date', 'end_date', 'time_unit'] + workstate_variables].copy()
    formated_pe['sal_brut_deplaf'] = benefits_from_pe(data_pe)
    return formated_pe


def format_career_tables(data, pss_path):
    formated_careers = dict()
    tables_regime = [table for table in ['b200_09', 'c200_09'] if table in data.keys()]
    pss_by_year = pss_vector_from_Excel(pss_path)
    pss_by_year.drop_duplicates(['year'], inplace=True)
    imputed_avpf_b200 = imputation_avpf(data['b200_09'].copy())
    for table_name in tables_regime:
        format_table = format_career_l200(data[table_name], level = table_name[:-3], pss = pss_by_year)
        format_table['source'] = table_name
        formated_careers[table_name] = format_table.sort(['noind', 'start_date'])
    formated_careers['b200_09'] = pd.concat([formated_careers['b200_09'], imputed_avpf_b200])
    tables_other = [table for table in ['dads_09', 'etat_09', 'pe200_09'] if table in data.keys()]
    for table_name in tables_other:
        format_table = eval('format_career_' + table_name[:-3])(data[table_name])
        format_table['source'] = table_name
    return formated_careers


def imputation_avpf(data_b200):
    ''' This function deal with AVPF status (wages imputed for pension rights when AVPF status can be claimed).
    Output: Additional rows (with source = data_b200_AVPF) '''
    workstate_variables = ['st', 'statutp', 'cc']
    avpf_variables = ['avpf', 'ntregc', 'ntregcempl']
    avpf_b200 = data_b200[['noind', 'start_date', 'end_date', 'time_unit']
                            + workstate_variables + avpf_variables].copy()
    avpf_b200 = avpf_b200.loc[(avpf_b200.avpf > 0), :]
    avpf_b200['sal_brut_deplaf'] = clean_earning(avpf_b200.loc[:, 'avpf'])
    avpf_b200['avpf_status'] = avpf_b200.ntregc - avpf_b200.ntregcempl > avpf_b200.ntregcempl
    avpf_b200.drop(avpf_variables, axis=1, inplace=True)
    avpf_b200['source'] = 'b200_09_avpf'
    return avpf_b200


def imputation_deplaf_from_plaf(sal_brut_deplaf, sal_brut_plaf, years, pss_by_year, nb_pss = 1):
    ''' This functions updated sal_brut_deplaf when wages have not actually be caped.'''
    years = pd.DataFrame({'year': years})
    pss_threshold = years.merge(pss_by_year, how='left', on=['year'])['pss'] * nb_pss
    nb_miss_ini = sum(sal_brut_deplaf.isnull())
    assert len(sal_brut_deplaf) == len(sal_brut_plaf) == len(pss_threshold)
    condition_imputation = ((sal_brut_plaf <= pss_threshold + 10) * (sal_brut_deplaf.isnull())).astype(int)
    helper = pd.DataFrame({'to be imputed': sal_brut_deplaf, 'to impute': sal_brut_plaf * condition_imputation})
    sal_brut_deplaf = helper.sum(axis=1)
    assert len(sal_brut_deplaf) == len(sal_brut_plaf)
    assert sum(sal_brut_deplaf.isnull()) <= nb_miss_ini
    return sal_brut_deplaf


def pss_vector_from_Excel(pss_file_path):
    ''' This functions creates a dataframe with columns ['year', 'pss'] which provides the annual PSS per year '''
    pss_by_year = pd.ExcelFile(pss_file_path).parse('PSS')[['pss', 'date']].iloc[1:94, :]
    pss_by_year['date'] = pss_by_year['date'].astype(str).apply(lambda x: x[:4]).astype(int)
    pss_by_year.loc[pss_by_year.date < 2002, 'pss'] = pss_by_year.loc[pss_by_year.date < 2002, 'pss'] / 6.55957
    pss_by_year.rename(columns={'date': 'year'}, inplace=True)
    return pss_by_year


def wages_from_b200(data_b200, pss_by_year):
    for earning in ['remu', 'remutot']:
        data_b200.loc[:, earning] = clean_earning(data_b200.loc[:, earning])
    sal_brut_plaf_ini = data_b200.loc[:, 'remu'].copy()
    sal_brut_deplaf_ini = data_b200.loc[:, 'remutot'].copy()
    years = data_b200.start_date.apply(lambda x: x.year)
    assert len(sal_brut_deplaf_ini) == len(sal_brut_plaf_ini) == len(years)
    sal_brut_deplaf = imputation_deplaf_from_plaf(sal_brut_deplaf_ini, sal_brut_plaf_ini, years, pss_by_year)
    assert len(sal_brut_deplaf) == len(sal_brut_plaf_ini)
    return sal_brut_plaf_ini, sal_brut_deplaf


def wages_from_c200(data_c200, pss_by_year):
    ''' This function reworks on wages at the c200 level:
    - gather information split in different variables (remuxxx)
    Hyp: as the cap for tranche C is very high, we consider that sal_brut_deplaf can be
    approximate by sal_brut_plaf when missing'''
    for earning in ['remuta', 'remutb', 'remutc', 'remu', 'remutot']:
        data_c200.loc[:, earning] = clean_earning(data_c200.loc[:, earning])
    sal_tranches = data_c200.loc[:, ['remuta', 'remutb', 'remutc']].copy().replace(0, np.nan).sum(1)
    sal_brut_plaf_ini = data_c200.loc[:, 'remu'].copy()
    sal_brut_plaf = sal_brut_plaf_ini + sal_brut_plaf_ini.isnull().astype(int) * sal_tranches
    sal_brut_deplaf_ini = data_c200.loc[:, 'remutot'].copy()
    years = data_c200.start_date.apply(lambda x: x.year)
    assert len(sal_brut_deplaf_ini) == len(sal_brut_plaf) == len(years)
    sal_brut_deplaf = imputation_deplaf_from_plaf(sal_brut_deplaf_ini, sal_brut_plaf, years, pss_by_year, nb_pss=8)
    assert len(sal_brut_deplaf) == len(sal_brut_plaf)
    return sal_brut_plaf, sal_brut_deplaf


def wages_from_dads(data_dads):
    sal_brut_deplaf = clean_earning(data_dads.loc[:, 'sb'])
    return sal_brut_deplaf


def wages_from_etat(data_etat):
    data_etat.loc[:, 'brut'] = clean_earning(data_etat.loc[:, 'brut'])
    sal_brut_deplaf = data_etat[['sbrut']]
    return sal_brut_deplaf
