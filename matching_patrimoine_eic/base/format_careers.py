# -*- coding: utf-8 -*-
"""
Created on Wed Jun 03 16:43:33 2015

@author: l.pauldelvaux
"""
import datetime as dt
import numpy as np
import pandas as pd


def aggregate_career_table(careers):
    ''' This function create an aggregate database with all the available information on careers:
    Output: 1 row per indiv*time_unit*status'''
    tables_by_source = list(careers.values())
    aggregate_table = pd.concat(tables_by_source).sort(['noind', 'start_date', 'end_date'])
    aggregate_table = first_columns_career_table(aggregate_table)
    return aggregate_table


def additional_rows(table_career, var_value):

    def _fill_variables(row, var_value):
        years, values, starts, ends = yearly_value_converter(row[var_value], row.time_unit,
                                                             row.start_date, row.end_date)
        col_y = ['year_{}'.format(i) for i in range(len(years))]
        col_v = ['value_{}'.format(i) for i in range(len(years))]
        col_s = ['start_{}'.format(i) for i in range(len(years))]
        col_e = ['end_{}'.format(i) for i in range(len(years))]
        row[col_y] = years
        row[col_v] = values
        row[col_s] = starts
        row[col_e] = ends
        return row
    table = table_career.copy()
    year_vars = ['year_{}'.format(i) for i in range(20)]
    value_vars = ['value_{}'.format(i) for i in range(20)]
    start_vars = ['start_{}'.format(i) for i in range(20)]
    end_vars = ['end_{}'.format(i) for i in range(20)]
    for year, value, start, end in zip(year_vars, value_vars, start_vars, end_vars):
        table[year] = np.nan
        table[value] = np.nan
        table[start] = np.nan
        table[end] = np.nan
    table = table.apply(lambda x: _fill_variables(x, var_value), axis = 1)

    id_vars = [var_name for var_name in table.columns if var_name not in year_vars + value_vars + start_vars + end_vars]
    df_years = pd.melt(table, id_vars = id_vars, value_vars = year_vars,
                       var_name = 'var_year', value_name = 'year_from_melt')
    to_concat = [df_years]
    for to_add in ['value', 'start', 'end']:
        df_type = pd.melt(table, id_vars = ['noind', 'start_date'], value_vars = eval(to_add + '_vars'),
                        var_name = 'var_' + to_add, value_name = to_add + '_from_melt')
        assert (df_years['noind'] == df_type['noind']).all()
        df_type.drop(['noind', 'start_date'], inplace = True, axis=1)
        assert df_years.shape[0] == df_type.shape[0]
        to_concat += [df_type]
    df = pd.concat(to_concat, axis=1, join_axes=[df_years.index])
    df = df.loc[df.value_from_melt.notnull(), :]
    df.drop([var_value, 'year', 'start_date', 'end_date', 'var_value', 'var_year', 'var_start', 'var_end'],
            inplace = True, axis=1)
    df.rename(columns={'value_from_melt': var_value, 'year_from_melt': 'year',
                       'end_from_melt': 'end_date', 'start_from_melt': 'start_date'}, inplace=True)
    df['time_unit'] = 'year'
    print df.columns
    return df.sort(['noind', 'year', 'start_date'])


def benefits_from_pe(data_pe):
    benefits = clean_earning(data_pe.loc[:, 'pjctaux'])
    return benefits


def careers_to_year(table):
    career = table.copy()
    start_date = career.start_date
    end_date = career.end_date
    career['year'] = start_date.apply(lambda x: x.year)
    day_to_year = (career['time_unit'] == 'day')
    nb_years = (end_date.apply(lambda x: x.year) - start_date.apply(lambda x: x.year))
    nb_days = (end_date - start_date) / np.timedelta64(1, 'D')
    variables_sal = [var for var in table.columns if var in ['sal_brut_deplaf', 'sal_brut_plaf']]
    for sal in variables_sal:
        to_select = (day_to_year == 1) * (nb_years == 0)
        career.loc[to_select, sal] = career.loc[to_select, sal] * nb_days.loc[to_select]
    career.loc[to_select, 'time_unit'] = 'year'
    to_add = additional_rows(career.loc[(day_to_year == 1) * (nb_years > 0), :], var_value = 'sal_brut_deplaf')
    career = career.append(to_add, ignore_index=True)
    # TODO: to work on if number of months exceed 12
    month_to_year = (career['time_unit'] == 'month')
    nb_months = (career.loc[month_to_year, 'end_date'] - career.loc[month_to_year, 'end_date']) / np.timedelta64(1, 'M')
    for sal in variables_sal:
        career.loc[month_to_year, sal] = career.loc[month_to_year, sal] * nb_months
    career.loc[month_to_year, 'time_unit'] = 'year'
    career = first_columns_career_table(career,
                            first_col = ['noind', 'year', 'start_date', 'end_date', 'cc', 'sal_brut_deplaf', 'source'])
    career = career.loc[career.time_unit == 'year', :].reset_index()
    career.drop(['index'], axis=1, inplace=True)
    return career


def clean_earning(vec):
    vec.loc[vec == -1] = np.nan
    opposite = - vec.loc[vec < 0].copy()
    vec.loc[vec < 0] = opposite
    return np.round(vec, 2)


def crosstable_imputation(data, initial_variable, aggregated_variable):
    ''' To identify equivalence in categorical variable based on aggregation of an other one '''
    crosstable = pd.crosstab(data[initial_variable], data[aggregated_variable])
    assert crosstable.shape[1] < crosstable.shape[0]
    assert ((crosstable != 0).sum(axis=1) == 1).all
    equivalence_by_to_impute_cat = dict([(mode, list(crosstable[crosstable[mode] != 0].index))
                                            for mode in crosstable.columns])
    return equivalence_by_to_impute_cat


def career_table_by_time_unit(careers, time_unit='year'):
    ''' This function selects information to keep from the different sources to build (workstate, sal_brut, firm).
    Workstate/sal_brut are rebuilt in 3 steps:
    - format at the appropriate time_unit level
    - selection of the most accurate information on sali for each year
    - Imputation of missing information '''
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
    workstate_variables = ['st', 'enreg']
    formated_etat = data_etat[['noind', 'start_date', 'end_date', 'time_unit'] + workstate_variables].copy()
    formated_etat['sal_brut_deplaf'] = wages_from_etat(data_etat)
    return formated_etat


def format_career_pe200(data_pe):
    workstate_variables = ['unemploy_status', 'pjcall2']
    try:
        equivalence_pjcall2_by_type_all = crosstable_imputation(data_pe, 'pjcall2', 'type_all')
    except:
        equivalence_pjcall2_by_type_all = {0.0: ['', 'NI'],
                                           2.0: ['01', '02', '04', '05', '21', '22',
                                                 '23', '24', '25', '27', '28', '40']}
    data_pe['unemploy_status'] = np.nan
    for mode, associated_values in equivalence_pjcall2_by_type_all.iteritems():
        data_pe.loc[data_pe['pjcall2'].isin(associated_values), 'unemploy_status'] = mode
        # data_pe.loc[data_pe['unemploy_status'].isnull(), 'unemploy_status'] = data_pe.loc[data_pe['unemploy_status'].isnull(), 'type_all']
    formated_pe = data_pe[['noind', 'start_date', 'end_date', 'time_unit'] + workstate_variables].copy()
    formated_pe['sal_brut_deplaf'] = benefits_from_pe(data_pe)
    return formated_pe


def format_dates_dads(table):
    def _convert_daysofyear(x):
        try:
            return int(x) - 1
        except:
            return 0
    table['start_date'] = pd.to_datetime(table['annee'], format="%Y")
    table['start_date'] += table['debremu'].apply((lambda x: dt.timedelta(days=_convert_daysofyear(x))))
    table['end_date'] = table['annee'].astype(str) + '-12-31'
    table.loc[:, 'end_date'] = pd.to_datetime(table.loc[:, 'end_date'], format="%Y-%m-%d")
    table['time_unit'] = 'year'
    table = table.drop(['annee', 'debremu'], axis=1)
    return table


def format_dates_level200(table):
    table['start_date'] = pd.to_datetime(table['annee'], format="%Y")
    table['end_date'] = table['annee'].astype(str) + '-12-31'
    table.loc[:, 'end_date'] = pd.to_datetime(table.loc[:, 'end_date'], format="%Y-%m-%d")
    table['time_unit'] = 'year'
    return table


def format_dates_pe200(table):
    ''' Rework on pole-emploi database dates'''
    def _convert_stata_dates(stata_vector):
        ''' In Stata dates are coded in number of days from 01jan1960 (=0) '''
        initial_date = pd.to_datetime('1960-01-01', format="%Y-%m-%d")
        dates = initial_date + stata_vector.apply((lambda x: dt.timedelta(days= int(x))))
        return dates
    for date_var in ['pjcdtdeb', 'pjcdtfin']:
        table.loc[:, date_var] = _convert_stata_dates(table.loc[:, date_var])
    table.rename(columns={'pjcdtdeb': 'start_date', 'pjcdtfin': 'end_date'}, inplace=True)
    table['time_unit'] = 'day'
    return table


def regimes_by_year(table):
    df = table.copy()
    df['helper'] = 1
    df['nb_obs'] = df.groupby(['noind', 'year'])['helper'].transform(np.cumsum)
    df.drop('helper', 1, inplace=True)
    regimes_by_year = df[['noind', 'year', 'cc', 'nb_obs']].set_index(['noind', 'year', 'nb_obs']).unstack('nb_obs')
    regimes_by_year.columns = range(df['nb_obs'].max())
    regimes_by_year['regime_by_year'] = regimes_by_year[0].astype(str)
    for i in range(1, df['nb_obs'].max()):
        regimes_by_year['regime_by_year'] += ', ' + regimes_by_year[i].astype(str)
    regimes_by_year['regimes_by_year'] = regimes_by_year['regime_by_year'].str.replace(', nan', '')
    regimes_by_year = regimes_by_year.reset_index()
    regimes_by_year = regimes_by_year[['noind', 'year', 'regimes_by_year']]
    table = table.merge(regimes_by_year, on=['noind', 'year'], how='left')
    return table


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
            return start_date.year, value * nb_days, start_date, end_date
        elif nb_years == 1:
            end_date1 = pd.to_datetime(str(start_date.year) + '-12-31', format="%Y-%m-%d")
            year1, value1, start_date1, end_date1 = yearly_value_converter(value, 'day', start_date, end_date1)
            start_date2 = end_date1 + dt.timedelta(days= 1)
            year2, value2, start_date2, end_date2 = yearly_value_converter(value, 'day', start_date2, end_date)
            start_dates = [start_date1, start_date2]
            end_dates = [end_date1, end_date2]
            return [year1, year2], [value1, value2], start_dates, end_dates
        else:
            end_date1 = pd.to_datetime(str(start_date.year) + '-12-31', format="%Y-%m-%d")
            year1, value1, start_date1, end_date1 = yearly_value_converter(value, 'day', start_date, end_date1)
            years = [year1]
            values = [value1]
            start_dates = [start_date1]
            end_dates = [end_date1]
            for year in range(start_date.year + 1, end_date.year):
                years += [year]
                values += [365 * value]
                start_dates += [pd.to_datetime(str(year) + '-01-01', format="%Y-%m-%d")]
                end_dates += [pd.to_datetime(str(year) + '-12-31', format="%Y-%m-%d")]
            start_datef = pd.to_datetime(str(end_date.year) + '-01-01', format="%Y-%m-%d")
            yearf, valuef, start_datef, end_datef = yearly_value_converter(value, 'day', start_datef, end_date)
            years += [yearf]
            values += [valuef]
            start_dates += [start_datef]
            end_dates += [end_datef]
            return years, values, start_dates, end_dates


