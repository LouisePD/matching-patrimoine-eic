# -*- coding: utf-8 -*-
"""
Created on Wed Jun 03 16:43:33 2015

@author: l.pauldelvaux
"""
from matching_patrimoine_eic.eic import data_handler_eic
from matching_patrimoine_eic.patrimoine import data_handler_patrimoine


def dates_long_to_wide(table, target_var, date_var, time_unit):
    ''' This functions takes a long panda DataFrame as an argument and returns a numpy table with one column per year '''
    if 'time_unit' == 'year':




if __name__ == '__main__':
    sharp_match_var = ['sexe', 'generation']
    matched_data = matching(sharp_match_var)