# -*- coding: utf-8 -*-
"""
@author: l.pauldelvaux
"""

import numpy as np
import pandas as pd


def define_workstates(career_table, datasets):
    ''' To understand how we define the following workstates, oen could refer to the .xls file with the description
    of this creation '''
    df = career_table.copy()
    df['workstate'] = np.nan
    for status in ['retired', 'student', 'avpf_status', 'unemploy_status', 'inwork_status', 'fp_actif']:
        if status not in df.columns:
            print u" {} is not provided in the using dataset".format(status)
            df[status] = False  # not taken into account yet
    conditions_by_workstate = {2: (df['unemploy_status'] == 2) & (df['source_salbrut'] == datasets['unemployment']),
                               # Unemployed receiving benefits
                               3: (df['cadre'] == 1) * (df['cc'] == 10),
                               # Private sector, executive
                               4: ((df['cadre'] != 1) * (df['regimes_by_year'].str.contains('21.0') == False) *
                                   (df['cc'] == 10)),
                               # Private sector, non-executive (Note: cadre could be NaN)
                               5: (df['cc'] == 13),  # Public sector, actif
                               6: (df['cc'] == 12),  # Public sector, sedentaire
                               # 5: (df['source_salbrut'] == "12") | (df['cc'] == "12") ,  # Public sector, actif
                               # 6: (df['salbrut_by_year'].str.contains("13")) * (df['cc'] != 10),  # Public sector, sedentaire
                               7: (df['cc'] == 40),  # Self-employed,
                               8: (df['avpf_status'] == 1),  # AVPF
                               9: (df['unemploy_status'] == 3) & (df['source_salbrut'] == datasets['unemployment']),
                               # Early retirement
                               10: df['retired'],  # Retirment
                               11: df['student'],  # student
                               12: ((df['cc'] == 21) | ((df['salbrut_by_year'].str.contains('21.0')) * (df['cc'] == 10) *
                                   (df['salbrut_by_year'].str.contains('6000') == False))), #
                               13: (df['cc'] == 22),  # Self-employed farmer
                               15: (df['unemploy_status'] == 0) & (df['source_salbrut'] == datasets['unemployment'])
                               # Unemployed without benefits
                               }
    exclusion_check = pd.concat([condition.astype(float).fillna(0)
                                for condition in conditions_by_workstate.values()], axis=1).sum(axis=1)
    try:
        assert (exclusion_check <= 1).all()
    except:
        print df.loc[(exclusion_check > 1), ['year', 'noind', 'cadre',
                     'cc', 'source_salbrut', 'salbrut', 'regimes_by_year', 'salbrut_by_year']]
        print pd.concat([condition.astype(float).fillna(0)
                                for condition in conditions_by_workstate.values()], axis=1).loc[(exclusion_check > 1), :]
        assert (exclusion_check <= 1).all()
    for code, condition in conditions_by_workstate.iteritems():
        df.loc[condition.fillna(0), 'workstate'] = code  # Missing status as ignored, ie we will keep them missing
    return df
