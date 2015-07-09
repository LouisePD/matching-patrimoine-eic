# -*- coding: utf-8 -*-
"""
Created on Wed Jun 03 16:43:33 2015

@author: l.pauldelvaux
"""
import datetime as dt
import numpy as np
import pandas as pd


@temporary_store_decorator()
def aggregate_career_table(option='eic', temporary_store = None):
    ''' This function create an aggregate database with all the available information on careers:
    Output: 1 row per indiv*time_unit*status'''
    to_concat = []
    tables_to_collect = ['b200_09', 'etat_09', 'dads_09', 'pe200_09', 'c200_09']
    if option == 'eir':
        tables_to_collect = ['eir2008_7000', 'eir2008_9001', 'eir2008_8000']
    for table in tables_to_collect:
        df = temporary_store.select(table)
        to_concat += [df]
    temporary_store.close()
    aggregate_table = pd.concat(to_concat, axis=0, ignore_index = True).sort(['noind', 'start_date', 'end_date'])
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
        careers = regimes_by_year(careers)
        return careers


def first_columns_career_table(table, first_col=None):
    if not first_col:
        first_col = ['noind', 'start_date', 'end_date', 'time_unit', 'cc', 'sal_brut_deplaf']
    other_col = [col for col in table.columns if col not in first_col]
    table = table.reindex_axis(first_col + other_col, axis = 1)
    return table


@temporary_store_decorator()
def format_career_dads(name_table, temporary_store = None):
    workstate_variables = ['cda', 'cs1', 'domempl', 'tain', 'ce', 'sb']
    id_variables = ['noind', 'start_date', 'end_date', 'time_unit']
    formated_dads = temporary_store.select(name_table, columns=id_variables + workstate_variables)
    formated_dads['sal_brut_deplaf'] = clean_earning(formated_dads.loc[:, 'sb'])
    for var in ['full_time', 'unemploy_status', 'inwork_status']:
        formated_dads[var] = np.nan
    formated_dads['inwork_status'] = 1
    # A: We assume full-time position if missing
    formated_dads['full_time'] = (formated_dads['ce'].isin(['P', 'D', ''])).astype(int)
    formated_dads['cs1'] = formated_dads['cs1'].astype(float)
    formated_dads['cadre'] = formated_dads['cda'].isin(['C']) | formated_dads['cs1'].isin([3.0, 4.0])
    formated_dads.drop(workstate_variables, 1, inplace=True)
    formated_dads['source'] = name_table
    formated_dads = formated_dads.sort(['noind', 'start_date'])
    temporary_store.remove(name_table)
    temporary_store.put(name_table, formated_dads, format='table', data_columns=True, min_itemsize = 20)


@temporary_store_decorator()
def format_career_etat(name_table, temporary_store = None):
    workstate_variables = ['enreg', 'quot', 'ce', 'rss', 'sbrut']
    id_variables = ['noind', 'start_date', 'end_date', 'time_unit']
    formated_etat = temporary_store.select(name_table, columns=id_variables + workstate_variables)
    formated_etat['quot'] = formated_etat['quot'].astype(float)
    formated_etat['sal_brut_deplaf'] = clean_earning(formated_etat['sbrut'])
    for var in ['full_time', 'unemploy_status', 'inwork_status', 'fp_actif']:
        formated_etat[var] = np.nan
    cond = formated_etat['quot'].notnull()
    formated_etat.loc[cond, 'full_time'] = (formated_etat.loc[cond, 'quot'].isin([0])).astype(int)
    formated_etat.loc[formated_etat['ce'] == 5, 'unemploy_status'] = 2  # chomage indemnisé
    formated_etat.loc[formated_etat['ce'].isin([2, 3, 4]), 'inwork_status'] = True
    formated_etat.loc[formated_etat['ce'].isin([0, 1, 5, 6]), 'inwork_status'] = False
    formated_etat.loc[formated_etat['rss'] == 8, 'fp_actif'] = True
    formated_etat.drop(workstate_variables, 1, inplace=True)
    return formated_etat


def format_career_unemployment(data_pe):
    workstate_variables = ['unemploy_status', 'pjcall2', 'full_time', 'inwork_status']
    # try:
    #    equivalence_pjcall2_by_type_all = crosstable_imputation(data_pe, 'pjcall2', 'type_all')
    # except:
    equivalence_pjcall2_by_type_all = {0.0: ['', 'NI'],
                                       2.0: ['01', '02', '04', '05', '21', '22', '33', '54', '82', '18', '47',
                                             '23', '24', '25', '27', '28', '40', '43']}

    for var in ['full_time', 'unemploy_status', 'inwork_status']:
        data_pe[var] = np.nan
    for mode, associated_values in equivalence_pjcall2_by_type_all.iteritems():
        data_pe.loc[data_pe['pjcall2'].isin(associated_values), 'unemploy_status'] = mode
    data_pe['sal_brut_deplaf'] = benefits_from_pe(data_pe)
    data_pe['inwork_status'] = 0
    data_pe['source'] = name_table
    data_pe = data_pe.sort(['noind', 'start_date'])
    data_pe.drop(['pjctaux'], axis=1, inplace = True)
    temporary_store.remove(name_table)
    temporary_store.put(name_table, data_pe, format='table', data_columns=True, min_itemsize = 20)


@temporary_store_decorator()
def format_dates_dads(name_table, temporary_store = None):
    def _convert_daysofyear(x):
        try:
            return int(x) - 1
        except:
            return 0
    table = temporary_store.select(name_table)
    table['start_date'] = pd.to_datetime(table['annee'], format="%Y")
    table['start_date'] += table['debremu'].apply((lambda x: dt.timedelta(days=_convert_daysofyear(x))))
    table['end_date'] = table['annee'].astype(str) + '-12-31'
    table.loc[:, 'end_date'] = pd.to_datetime(table.loc[:, 'end_date'], format="%Y-%m-%d")
    table['time_unit'] = 'year'
    table = table.drop(['annee', 'debremu'], axis=1)
    temporary_store.remove(name_table)
    temporary_store.put(name_table, table, format='table', data_columns=True, min_itemsize = 20)


@temporary_store_decorator()
def format_dates_level200(name_table, temporary_store = None):
    table = temporary_store.select(name_table)
    table['start_date'] = pd.to_datetime(table['annee'], format="%Y")
    table['end_date'] = table['annee'].astype(str) + '-12-31'
    table.loc[:, 'end_date'] = pd.to_datetime(table.loc[:, 'end_date'], format="%Y-%m-%d")
    table['time_unit'] = 'year'
    return table


@temporary_store_decorator()
def format_dates_unemployment(name_table, temporary_store = None):
    ''' Rework on pole-emploi database dates'''
    def _convert_stata_dates(stata_vector):
        ''' In Stata dates are coded in number of days from 01jan1960 (=0) '''
        initial_date = pd.to_datetime('1960-01-01', format="%Y-%m-%d")
        dates = initial_date + stata_vector.apply((lambda x: dt.timedelta(days= int(x))))
        return dates
    table = temporary_store.select(name_table)
    for date_var in ['pjcdtdeb', 'pjcdtfin']:
        table.loc[:, date_var] = _convert_stata_dates(table.loc[:, date_var])
    table.rename(columns={'pjcdtdeb': 'start_date', 'pjcdtfin': 'end_date'}, inplace=True)
    table['time_unit'] = 'day'
    temporary_store.remove(name_table)
    temporary_store.put(name_table, table, format='table', data_columns=True, min_itemsize = 20)


def variable_by_year(table, target_var, name_output_var):
    ''' Creates a variable recording a list of values for a given year*indiv '''
    df = table.copy()
    df['helper'] = 1
    df['nb_obs'] = df.groupby(['noind', 'year'])['helper'].transform(np.cumsum)
    df.drop('helper', 1, inplace=True)
    var_by_year = df[['noind', 'year', 'nb_obs', target_var]].set_index(['noind', 'year', 'nb_obs']).unstack('nb_obs')
    var_by_year.columns = range(df['nb_obs'].max())
    var_by_year[name_output_var] = var_by_year[0].astype(str)
    for i in range(1, df['nb_obs'].max()):
        var_by_year[name_output_var] += ', ' + var_by_year[i].astype(str)
    var_by_year = var_by_year.reset_index()
    var_by_year = var_by_year[['noind', 'year', name_output_var]]
    var_by_year[name_output_var] = var_by_year[name_output_var].str.replace(', nan', '')
    var_by_year[name_output_var] = var_by_year[name_output_var].str.replace('nan, ', '')
    return var_by_year


def regimes_by_year(table):
    '''  Creates variables contenaining a list of values for a given year*indiv of (i) schemes,
    (ii) schemes with non-missing earnings.
    This function also updates the cadre dummy   '''
    df = table.copy()
    df.loc[df.source == 'pe200_09', 'cc'] = 4
    regimes_by_year = variable_by_year(df, 'cc', 'regimes_by_year')
    df = df.merge(regimes_by_year, on=['noind', 'year'], how='left')

    df['cc2'] = (df['sal_brut_deplaf'] > 0) * df['cc']
    salbrut_by_year = variable_by_year(df, 'cc2', 'salbrut_by_year')
    salbrut_by_year['salbrut_by_year'] = salbrut_by_year['salbrut_by_year'].str.replace(', 0.0', '')
    salbrut_by_year['salbrut_by_year'] = salbrut_by_year['salbrut_by_year'].apply(lambda x: x[5:] if x[:4] == '0.0,' else x)
    df = df.merge(salbrut_by_year, on=['noind', 'year'], how='left')

    condition_prive = df['regimes_by_year'].str.contains('|'.join(['10.0, ', ', 10.0']))
    condition_cadre = df['regimes_by_year'].str.contains('5000.0')
    condition_noncadre = df['regimes_by_year'].str.contains('6000.0')
    df.loc[condition_prive * condition_cadre, 'cadre'] = True
    df.loc[condition_prive * condition_noncadre, 'cadre'] = False
    return df


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


