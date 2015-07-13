# -*- coding: utf-8 -*-
"""
@author: l.pauldelvaux
"""
from eic_eir import create_eic_eir_database
from matching_patrimoine_eic.patrimoine import data_handler_patrimoine


def sharp_match():
    ''' Split data according variables we want a sharp match on to perform further matching '''








def matching(sharp_match_var, clustering_var):
    ''' This function provides a statistical matching of the EIC-EIR database and the Patrimoine Survey.
    Steps of the statistical matching process:
        1 - sharp match on sharp_match_var
        2 - Clustering on clustering_var
        3 - Match on careers (DHD technique with a call to a R script) '''



if __name__ == '__main__':
    sharp_match_vars = ['sexe', 'generation', 'nb_years']
    clustering_vars = ['sal_brut', 'nb_enf']
    data_eic_eir = create_eic_eir_database(first_generation= 1942, last_generation=1954, fill_gap_generation=False)
    data_patrimoine = data_handler_patrimoine()
    matched_data = matching(data_eic_eir, data_patrimoine, sharp_match_vars, clustering_vars)
