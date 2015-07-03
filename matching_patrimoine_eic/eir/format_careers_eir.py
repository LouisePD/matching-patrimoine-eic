# -*- coding: utf-8 -*-
"""
Specific function to format EIR tables

@author: l.pauldelvaux
"""

from matching_patrimoine_eic.base.format_careers import format_dates_dads, format_dates_unemployment, format_dates_level200
from matching_patrimoine_eic.base.format_careers import format_career_dads, format_career_etat, format_career_unemployment


def build_sbrut(table_etat):
    ''' This functions rebuilts the 'sbrut' variable according EIC 2009 imputation rules '''
    sbrut = table_etat['brut'] + table_etat['prim'] * (table_etat['prim'] > 0) + table_etat['ir'] + table_etat['sft']
    return sbrut


def format_career_tables(data):
    formated_careers = dict()
    # 7000 = Pole Emploi, 8000 = Dads, 9001 = Etat
    fct_equivalent_by_table = {"eir2008_8000": 'dads', "eir2008_7000": 'pe200', "eir2008_9001": 'etat'}
    tables = dict([(table, 'format_career_' + fct)
                for table, fct in fct_equivalent_by_table.iteritems()
                if table in data.keys()])
    for table_name, fct in tables.iteritems():
        format_table = eval(fct)(data[table_name])
        format_table['source'] = table_name
        formated_careers[table_name] = format_table.sort(['noind', 'start_date'])
    return formated_careers


def format_dates(data):
    ''' This function specifies Data in the appropriate format :
    noind start_date end_date variable(s) time_unit'''
    data['eir2008_7000'] = format_dates_unemployment(data['eir2008_7000'])
    data['eir2008_9001'] = format_dates_level200(data['eir2008_9001'])
    data['eir2008_8000'] = format_dates_dads(data['eir2008_8000'])
    return data


def preliminary_format(data):
    data['eir2008_8000'].rename(columns={'an': 'annee', 'ai': 'an'}, inplace=True)
    data['eir2008_7000'].drop(['pjcdtdeb', 'pjcdtfin'], axis=1, inplace=True)
    data['eir2008_7000'].rename(columns={'pjcdtdeb2': 'pjcdtdeb', 'pjcdtfin2': 'pjcdtfin'}, inplace=True)
    data['eir2008_9001']['sbrut'] = build_sbrut(data['eir2008_9001'])
    return data
