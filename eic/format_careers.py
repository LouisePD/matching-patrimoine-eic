# -*- coding: utf-8 -*-
"""
Created on Wed Jun 03 16:43:33 2015

@author: l.pauldelvaux
"""
import numpy as np

def benefits_from_pe(data_pe):
    benefits = clean_earning(data_pe.loc[:, 'pjctaux'])
    return benefits


def clean_earning(vec):
    vec.loc[vec == -1] = np.nan
    opposite = - vec.loc[vec < 0].copy()
    vec.loc[vec < 0] = opposite
    return np.round(vec, 2)


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


def format_career_l200(data_l200, level):
    wages_from = 'wages_from_' + level
    workstate_variables = ['st', 'statutp', 'cc']
    formated_l200 = data_l200[['noind', 'start_date', 'end_date', 'time_unit'] + workstate_variables].copy()
    formated_l200['sal_brut_plaf'], formated_l200['sal_brut_deplaf'] = eval(wages_from)(data_l200)
    return formated_l200


def format_career_pe200(data_pe):
    workstate_variables = ['pjcall2']
    formated_pe = data_pe[['noind', 'start_date', 'end_date', 'time_unit'] + workstate_variables].copy()
    formated_pe['sal_brut_deplaf'] = benefits_from_pe(data_pe)
    return formated_pe


def format_career_tables(data):
    formated_careers = dict()
    tables_regime = [table for table in ['b200_09', 'c200_09']
                                if table in data.keys()]
    for table_name in tables_regime:
        format_table = format_career_l200(data[table_name], level=table_name[:-3])
        formated_careers[table_name] = format_table.sort(['noind', 'start_date'])
    tables_other = [table for table in ['dads_09', 'etat_09', 'pe200_09']
                            if table in data.keys()]
    for table_name in tables_other:
        format_table = eval('format_career_' + table_name[:-3])(data[table_name])
        formated_careers[table_name] = format_table.sort(['noind', 'start_date'])
    return formated_careers


def wages_from_b200(data_b200):
    for earning in ['remu', 'remutot']:
        data_b200.loc[:, earning] = clean_earning(data_b200.loc[:, earning])
    sal_brut_plaf = data_b200.loc[:, 'remu']
    sal_brut_deplaf = data_b200[['remutot']]
    return sal_brut_plaf, sal_brut_deplaf


def wages_from_c200(data_c200):
    ''' This function reworks on wages at the c200 level:
    - gather information split in different variables (remuxxx) '''
    for earning in ['remuta', 'remutb', 'remutc', 'remu', 'remutot']:
        data_c200.loc[:, earning] = clean_earning(data_c200.loc[:, earning])
    sal_tranches = data_c200[['remuta', 'remutb', 'remutc']].replace(0, np.nan).sum(1)
    sal_brut_plaf = data_c200.loc[:, 'remu'] + data_c200.loc[:, 'remu'].isnull() * sal_tranches
    sal_brut_deplaf = data_c200[['remutot']]
    return sal_brut_plaf, sal_brut_deplaf


def wages_from_dads(data_dads):
    sal_brut_deplaf = clean_earning(data_dads.loc[:, 'sb'])
    return sal_brut_deplaf


def wages_from_etat(data_etat):
    data_etat.loc[:, 'brut'] = clean_earning(data_etat.loc[:, 'brut'])
    sal_brut_deplaf = data_etat[['sbrut']]
    return sal_brut_deplaf
