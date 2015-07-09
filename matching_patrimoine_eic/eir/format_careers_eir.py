# -*- coding: utf-8 -*-
"""
Specific function to format EIR tables

@author: l.pauldelvaux
"""

from matching_patrimoine_eic.base.format_careers import format_dates_dads, format_dates_unemployment, format_dates_level200
from matching_patrimoine_eic.base.format_careers import format_career_dads, format_career_etat, format_career_unemployment
from matching_patrimoine_eic.base.load_data import temporary_store_decorator


def build_sbrut(table_etat):
    ''' This functions rebuilts the 'sbrut' variable according EIC 2009 imputation rules '''
    sbrut = table_etat['brut'] + table_etat['prim'] * (table_etat['prim'] > 0) + table_etat['ir'] + table_etat['sft']
    return sbrut


def format_career_tables(datasets):
    # 7000 = Pole Emploi, 8000 = Dads, 9001 = Etat
    to_format = dict((k, v) for k, v in datasets.iteritems() if k in ['unemployment', 'dads', 'etat'])
    for generic_name, table_name in to_format.iteritems():
        eval('format_career_' + generic_name)(table_name)


def format_dates():
    ''' This function specifies Data in the appropriate format :
    noind start_date end_date variable(s) time_unit'''
    format_dates_unemployment('eir2008_7000')
    format_dates_level200('eir2008_9001')
    format_dates_dads('eir2008_8000')


@temporary_store_decorator()
def preliminary_format(temporary_store = None):
    df = temporary_store.select('eir2008_8000')
    df.rename(columns={'an': 'annee', 'ai': 'an'}, inplace=True)
    temporary_store.remove('eir2008_8000')
    temporary_store.put('eir2008_8000', df, format='table', data_columns=True, min_itemsize = 20)

    df = temporary_store.select('eir2008_7000')
    df.drop(['pjcdtdeb', 'pjcdtfin'], axis=1, inplace=True)
    df.rename(columns={'pjcdtdeb2': 'pjcdtdeb', 'pjcdtfin2': 'pjcdtfin'}, inplace=True)
    temporary_store.remove('eir2008_7000')
    temporary_store.put('eir2008_7000', df, format='table', data_columns=True, min_itemsize = 20)

    df = temporary_store.select('eir2008_9001')
    df['sbrut'] = build_sbrut(df)
    temporary_store.remove('eir2008_9001')
    temporary_store.put('eir2008_9001', df, format='table', data_columns=True, min_itemsize = 20)
