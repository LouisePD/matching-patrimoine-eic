# -*- coding: utf-8 -*-
"""
Created on Wed Jun 03 16:43:33 2015

@author: l.pauldelvaux
"""
import datetime as dt
import numpy as np
import pandas as pd


def aggregate_career_table(careers):
    ''' This function create an aggregate database with all the available information on careers '''
    tables_by_source = list(careers.values())
    aggregate_table = pd.concat(tables_by_source).sort(['noind', 'start_date', 'end_date'])
    aggregate_table = first_columns_career_table(aggregate_table)
    return aggregate_table


def benefits_from_pe(data_pe):
    benefits = clean_earning(data_pe.loc[:, 'pjctaux'])
    return benefits


def additional_rows(table_career):
    def _fill_variables(row):
        years, values = yearly_value_converter(row.sal_brut_deplaf, row.time_unit, row.start_date, row.end_date)
        col_y = ['year_{}'.format(i) for i in range(len(years))]
        col_v = ['value_{}'.format(i) for i in range(len(years))]
        row[col_y] = years
        row[col_v] = values
        return row
    table = table_career.copy()
    year_vars = ['year_{}'.format(i) for i in range(20)]
    value_vars = ['value_{}'.format(i) for i in range(20)]
    for year_var, value_var in zip(year_vars, value_vars):
        table[year_var] = np.nan
        table[value_var] = np.nan
    table = table.apply(_fill_variables, axis = 1)

    id_vars = [var_name for var_name in table.columns if var_name not in year_vars + value_vars]
    df_years = pd.melt(table, id_vars = id_vars, value_vars = year_vars,
                       var_name = 'var_year', value_name = 'year_from_melt')
    df_values = pd.melt(table, id_vars = ['noind', 'start_date'], value_vars = value_vars,
                        var_name = 'var_sal_brut', value_name = 'sal_brut_deplaf_from_melt')
    assert (df_years['noind'] == df_values['noind']).all
    df_values.drop(['noind', 'start_date'], inplace = True, axis=1)
    assert df_years.shape[0] == df_values.shape[0]
    df = pd.concat([df_years, df_values], axis=1, join_axes=[df_years.index])
    df = df.loc[df.sal_brut_deplaf_from_melt.notnull(), :]
    df.drop(['sal_brut_deplaf', 'year', 'var_sal_brut', 'var_year'], inplace = True, axis=1)
    df.rename(columns={'sal_brut_deplaf_from_melt': 'sal_brut_deplaf', 'year_from_melt': 'year'}, inplace=True)
    df['time_unit'] = 'year'
    return df.sort(['noind', 'year', 'start_date'])


def careers_to_year(table):
    career = table.copy()
    start_date = career.start_date
    end_date = career.end_date
    career['year'] = start_date.apply(lambda x: x.year)
    day_to_year = (career['time_unit'] == 'day')
    nb_years = (end_date.apply(lambda x: x.year) - start_date.apply(lambda x: x.year))
    nb_days = (end_date - start_date) / np.timedelta64(1, 'D')
    for sal in ['sal_brut_deplaf', 'sal_brut_plaf']:
        to_select = (day_to_year == 1) & (nb_years == 0)
        career.loc[to_select, sal] = career.loc[to_select, sal] * nb_days.loc[to_select]
    career.loc[to_select, 'time_unit'] = 'year'
    to_add = additional_rows(career.loc[(day_to_year == 1) * (nb_years > 0), :])
    career = career.append(to_add, ignore_index=True)
    # TODO: to work on if number of months exceed 12
    month_to_year = (career['time_unit'] == 'month')
    nb_months = (career.loc[month_to_year, 'end_date'] - career.loc[month_to_year, 'end_date']) / np.timedelta64(1, 'M')
    for sal in ['sal_brut_deplaf', 'sal_brut_plaf']:
        career.loc[month_to_year, sal] = career.loc[month_to_year, sal] * nb_months
    career.loc[month_to_year, 'time_unit'] = 'year'
    career = first_columns_career_table(career,
                            first_col= ['noind', 'year', 'start_date', 'end_date', 'cc', 'sal_brut_deplaf', 'source'])
    career = career.loc[career.time_unit == 'year', :].reset_index()
    career.drop(['index', 'time_unit'], axis=1, inplace=True)
    career.sort(['noind', 'year', 'start_date', 'end_date'], inplace = True)
    return career


def clean_earning(vec):
    vec.loc[vec == -1] = np.nan
    opposite = - vec.loc[vec < 0].copy()
    vec.loc[vec < 0] = opposite
    return np.round(vec, 2)


def final_career_table(careers, time_unit='year'):
    ''' This function selects information to keep from the different sources to build (workstate, sal_brut, firm).
    Workstate/sal_brut are rebuilt in 3 steps:
    - format at the appropriate time_unit level
    - selection of the most accurate information on sali for each year
    - Imputation of missing information '''
    rule_prior = {'dads_09': 1, 'etat_09': 2, 'b200_09': 3, 'c200_09': 4}
    careers['order'] = careers['source'].copy().replace(rule_prior)
    if time_unit == 'year':
        careers = careers_to_year(careers)
        return careers


def first_columns_career_table(table, first_col=None):
    if not first_col:
        first_col = ['noind', 'start_date', 'end_date', 'time_unit', 'cc', 'sal_brut_deplaf']
    other_col = [col for col in table.columns if col not in first_col]
    table = table.reindex_axis(first_col + other_col, axis = 1)
    return table


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
    tables_regime = [table for table in ['b200_09', 'c200_09'] if table in data.keys()]
    for table_name in tables_regime:
        format_table = format_career_l200(data[table_name], level=table_name[:-3])
        format_table['source'] = table_name
        formated_careers[table_name] = format_table.sort(['noind', 'start_date'])

    tables_other = [table for table in ['dads_09', 'etat_09', 'pe200_09'] if table in data.keys()]
    for table_name in tables_other:
        format_table = eval('format_career_' + table_name[:-3])(data[table_name])
        format_table['source'] = table_name
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


def yearly_value_converter(value, time_unit, start_date, end_date):
    ''' This functions takes a value, its associated time unit and starting and final dates
    of the period to return:
    - a vector of accurate years
    - a vector of associate yearly amounts '''
    if time_unit == 'day':
        nb_years = end_date.year - start_date.year
        if nb_years == 0:
            nb_days = (end_date - start_date) / np.timedelta64(1, 'D')
            return start_date.year, value * nb_days
        elif nb_years == 1:
            end_date1 = pd.to_datetime(str(start_date.year) + '-12-31', format="%Y-%m-%d")
            year1, value1 = yearly_value_converter(value, 'day', start_date, end_date1)
            start_date2 = end_date1 + dt.timedelta(days= 1)
            year2, value2 = yearly_value_converter(value, 'day', start_date2, end_date)
            return [year1, year2], [value1, value2]
        else:
            end_date1 = pd.to_datetime(str(start_date.year) + '-12-31', format="%Y-%m-%d")
            year1, value1 = yearly_value_converter(value, 'day', start_date, end_date1)
            years = [year1]
            values = [value1]
            for year in range(start_date.year + 1, end_date.year):
                years += [year]
                values += [365 * value]
            start_datef = pd.to_datetime(str(end_date.year) + '-01-01', format="%Y-%m-%d")
            yearf, valuef = yearly_value_converter(value, 'day', start_datef, end_date)
            years += [yearf]
            values += [valuef]
            return years, values
