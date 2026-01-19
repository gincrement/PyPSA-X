# -*- coding: utf-8 -*-

"""
PyPSA PtX / µgrid Optimizer

@author(s):
    M. Groissbock (OET)

---

MIT License

Copyright (c) <year> <copyright holders>

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
associated documentation files (the "Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the
following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial
portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO
EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
USE OR OTHER DEALINGS IN THE SOFTWARE.

"""


default_xls_filename = 'PyPSA_PtX_AB_v1.0.0.xlsx'
default_xls_filename = 'PyPSA_PtX_AB_test.xlsx'


# -----------------------------------------------------------------------------
# No need to change code below this line ...
# -----------------------------------------------------------------------------

__version__ = '0.9.1'

from datetime import datetime

print (f'\nPtX / µgrid Optimizer v{__version__}')
print (f'(c) {datetime.now().year}\n')
print (('+' + '-'*80 + '+'))
print ('| MIT License                                                                    |')
print (('| ' + '='*11 + ' '*68 + '|'))
print ('| Permission is hereby granted, free of charge, to any person obtaining a copy   |')
print ('| of this software and associated documentation files (the "Software"), to deal  |')
print ('| in the Software without restriction, including without limitation the rights   |')
print ('| to use,copy, modify, merge, publish, distribute, sublicense, and/or sell       |')
print ('| copies of the Software, and to permit persons to whom the Software is          |')
print ('| furnished to do so.                                                            |')
print (('+' + '-'*80 + '+\n'))

import os
import sys

# check provided arguments on the command line
if (len(sys.argv) > 1) and \
   (sys.argv[0] != ''):
    if os.path.exists(sys.argv[1]):
        xls_filename = sys.argv[1]
    #
    else:
        print (f'error! provided XLS file does not exist: {sys.argv[1]}\n')
        print (f'fallback to default XLS file: {default_xls_filename}\n')
        xls_filename = default_xls_filename
#
else:
    xls_filename = default_xls_filename

# check if the Excel file exists
if not os.path.exists(xls_filename):
    print (f'error! the provided input file "{xls_filename}" does not exist\n')
    sys.exit (-1)

deactivate_network_viewers = True

try:
    from packaging import version
    import pypsa
    # make sure to use the new API
    pypsa.options.api.new_components_api = True
    pypsa.options.set_option("params.statistics.nice_names", False)
    pypsa.options.set_option("params.statistics.round", 2)
    pypsa.options.set_option("debug.runtime_verification", False)
    #
    if version.parse (pypsa.__version__) < version.parse ('1.0.0'):
        print ('\nerror! installed PyPSA version ({pypsa.__version__)) not supported! need at least v1.0.0')
        sys.exit (-1)
    #
    import linopy
    from linopy.remote.oetc import OetcCredentials, OetcHandler, OetcSettings
    import pandas as pd
    pd.options.display.float_format = '{:,.2f}'.format
    pd.options.display.max_rows = 100
    pd.options.display.max_columns = 100
    pd.options.display.width = 300
    import numpy as np
    import sys
    import uuid
    import copy
    #
    # especially useful for Windows users
    import tempfile
    tempfile.tempdir = os.getcwd()
    #
    # surpess Pythons warning messaging
    import warnings
    warnings.filterwarnings('ignore')
    #
    # surpress PyPSA's logging messages
    import logging
    logging.basicConfig(level=logging.ERROR)
    #
    print ('imported all necessary libraries')
    #
except Exception as e: 
    print (f'error! not able to import the key package(s)\n{e}\n')
    sys.exit(-1)

# optional packages are related to optional HTML network viewer and SVG result
# graph
try:
    from pypsa_network_viewer import html_viewer
    network_viewer = True
#
except:
    network_viewer = False

try:
    import matplotlib.pyplot as plt
    import networkx as nx
    networkx_viewer = True
#
except:
    networkx_viewer = False

try:
    import tsam.timeseriesaggregation as tsam
    tsam_avail = True
#
except:
    tsam_avail = False

if deactivate_network_viewers:
    network_viewer = False
    networkx_viewer = False

kwargs = {}

# SUPPORT FUNCTIONS -----------------------------------------------------------

#
# source:
# https://stackoverflow.com/questions/449560/how-do-i-determine-the-size-of-an-object-in-python
#
def getsize(obj):
    from types import ModuleType, FunctionType
    from gc import get_referents
    #
    # Custom objects know their class.
    # Function objects seem to know way too much, including modules.
    # Exclude modules as well.
    BLACKLIST = type, ModuleType, FunctionType
    #
    """sum size of object & members."""
    if isinstance(obj, BLACKLIST):
        raise TypeError('getsize() does not take argument of type: '+ str(type(obj)))
    #
    seen_ids = set()
    size = 0
    objects = [obj]
    #
    while objects:
        need_referents = []
        #
        for obj in objects:
            if (not isinstance(obj, BLACKLIST)) and \
               (id(obj) not in seen_ids):
                seen_ids.add(id(obj))
                size += sys.getsizeof(obj)
                need_referents.append(obj)
        #
        objects = get_referents(*need_referents)
    #
    return size


def read_excel_data(
        xls_file: str, 
        target_folder: str = None, 
        csv_subfolder: str = None, 
        temp_file: str = None, 
        sheets_list: list = None, 
        sheets_ts_list: list = None,
    ) -> None:
    """
    Read the input Excel file (aka as Assumption Book) which contains all
    the necessary PyPSA sheets to be exported (e.g., carriers, buses, 
    generators, generators-p_max_pu, links).
    
    Parameters
    ----------
    xls_file : str
        Name of the Excel file to be considered.

    target_folder: str = None
        Name of the folder to store all temporary and final files (e.g.,
        initial CSV files, base and result NC files).

    csv_subfolder: str = None
        Name of the folder to store the original CSV files in.

    temp_file: str = None
        Name of the temporary NC file after reading the CSV file and adjusting
        options based on the scenario definition in the assumption book.

    sheets_list: list = None
        List of PyPSA static sheets to read (if exist).

    sheets_ts_list: list = None
        List of PyPSA dynamic sheets to read (if exist).

    Returns
    -------
    None
    """
    #
    # check if the required folder exist, if not create them
    if not (os.path.exists(f'{target_folder}') or \
            os.path.exists(f'{target_folder}/{csv_subfolder}')):
        os.makedirs(f'{target_folder}')
        os.makedirs(f'{target_folder}/{csv_subfolder}')
    #
    # collect the time of the xls file
    xlstime = os.path.getmtime(xls_file)
    #
    # check if the NC base case file is available, if yes collect its timestamp
    if os.path.isfile(f'{target_folder}/{temp_file}'):
        nctime = os.path.getmtime(f'{target_folder}/{temp_file}')
    #
    else:
        nctime  = xlstime
    #
    comps = pypsa.descriptors.nominal_attrs
    # if the base case NC file is younger as the Excel file, recreate it
    if (xlstime >= nctime):
        print (f'read energy system details from {xls_file}...')
        #
        all_sheets = sheets_list + sheets_ts_list
        all_sheets.sort()
        #
        # Create CSV files for all identified PyPSA worksheets
        for sheet_name in all_sheets:
            try:
                # read the excel file sheet by sheet
                data = pd.read_excel(
                    xls_file, 
                    sheet_name=sheet_name)
                #
                # set the PyPSA version of the current installation to avoid
                # a warning message
                if sheet_name == 'network':
                    data.pypsa_version = pypsa.__version__
                #
                # and save the individual CSV file
                csv_file = f'{target_folder}/{csv_subfolder}/{sheet_name}.csv'
                data.to_csv(
                    csv_file, 
                    index=False)
                #
                print (f'x) {sheet_name}')
            #
            except:
                pass
        #
        print ('\ntransfer it into an NC file ...')
        # createa a new PyPSA network
        n = pypsa.Network()
        n.set_snapshots(
            pd.date_range(f'{str(globals()['project_cod'])}-01-01',
                          freq='1h', 
                          periods=globals()['total_snapshot']))
        # load the just created CSV files
        n.import_from_csv_folder(f'{target_folder}/{csv_subfolder}')
        n.consistency_check()
        #
        if (eval(globals()['debug_mode'])) and \
           (globals()['hours_to_optimize'] < 8760):
            print (f'\ndebugging is enabled, therefore only {globals()['hours_to_optimize']} hours are considered in the optimization')
            n.set_snapshots(
                pd.date_range(
                    f'{globals()['project_cod']}-01-01', freq='h', 
                    periods=globals()['hours_to_optimize']))
        #
        inv_periods = np.array(eval(globals()['investment_periods']))
        n.set_investment_periods(inv_periods)
        #
        inv_weightings = np.array(eval(globals()['investment_weighting']))
        #
        for year in n.investment_period_weightings.index:
            pos = np.where(n.investment_period_weightings.index == year)
            n.investment_period_weightings.loc[year] = inv_weightings[pos]
        #
        # check if time segmentation should be done
        if (tsam_avail) and \
           (eval(globals()['do_segmentation'])):
            print (f'info! use segmentation {globals()['segmentation_duration']}h steps (on average) ...')
            #
            resolution = globals()['segmentation_duration']
            hours = len(n.snapshots)
            # calculate number of segments equivalent to resolution
            segments = int(hours / resolution)
            #
            # concatenate and normalize all time series with min-max normalization
            df_all = pd.core.frame.DataFrame()
            collected_columns = 0
            #
            # loop through all components
            for c in comps:
                df = n.c[c].dynamic
                #
                for col in df:
                    # only add columns with available data
                    if df[col].count().sum().sum() > 0:
                        collected_columns += 1
                        df_tmp = df[col].add_prefix(f'{c}-{col}-').copy()
                        df_all = pd.concat([
                            df_all,
                            df_tmp], axis=1)
            #
            # loop through all collected columns and remove the once with constant values
            for col in df_all.columns:
                if df_all[col].std() < 0.1e-10:
                    df_all.drop(col, axis=1, inplace=True)
            #
            df_all = df_all.reset_index()
            if 'period' in df_all.columns:
                df_all.drop('period', axis=1, inplace=True)
            #
            df_all.set_index('timestep', inplace=True)
            df_norm = (df_all - df_all.min()) / (df_all.max() - df_all.min())
            #
            # use `tsam` to run segmentation clustering algorithm
            agg = tsam.TimeSeriesAggregation(
                df_norm,
                hoursPerPeriod=len(df_norm),
                noTypicalPeriods=1,
                noSegments=segments,
                segmentation=True,
                solver=globals()['solver_name'],
            )
            agg2 = agg.createTypicalPeriods()
            #
            # translate segments into time stamps and calculate new weightings
            weightings = agg2.index.get_level_values('Segment Duration')
            offsets = np.insert(np.cumsum(weightings[:-1]), 0, 0)
            #
            weightings = n.snapshot_weightings.iloc[offsets].mul(weightings, axis=0).\
                reset_index().set_index('timestep')
            #
            # aggregate the hourly time series by averaging over the segments
            mapping = (
                pd.Series(weightings.index, index=weightings.index).reindex(df_all.index).ffill()
            )
            #
            # loop through all collected columns and adjust the values based on 
            # the mapping from the previous steps
            for long_col in df_all.columns:
                c = long_col[:long_col.find('-')]
                col = long_col[(long_col.find('-')+1):\
                               long_col.find('-', long_col.find('-')+1)]
                t = long_col[(long_col.find('-', long_col.find('-')+1)+1):]
                n.c[c].dynamic[col][t] = \
                    n.c[c].dynamic[col][t].groupby(by=["timestep"], 
                                                   group_keys=mapping).mean().values
            #
            # set new segmented snapshots and adjust weightings
            weightings = weightings.reset_index().set_index(['period', 'timestep'])
            n.set_snapshots(weightings.index)
            n.snapshot_weightings = weightings
        #
        # save them into a base case NC file
        save_network(
            n, 
            f'{target_folder}/{temp_file}')
    #
    else:
        print (f'no need to re-read energy system details from {xls_file}\n')
    #
    return None

def read_all_params(
        xls_file: str
    ) -> tuple [pd.core.frame.DataFrame, 
                pd.core.frame.DataFrame, 
                pd.core.frame.DataFrame]:
    """
    Read the optimization and scenario settings from the input Excel file 
    (aka as Assumption Book).
    
    Parameters
    ----------
    xls_file : str
        Name of the Excel file to be considered.

    Returns
    -------
    df_params: list
        Optimization settings to be considered.

    scen_params: list
        Scenarios details to be considered.

    stoch_params: list
        Stochstic optimization details to be considered.
    """
    #
    own_sheets = [
        'opt_params', 
        'scen_params',
        'stoch_params']
    #
    # define some variables
    defaults = {
        # debug settings
        'debug_mode': 'False',
        'hours_to_optimize': (24*3),
        # Other settings
        'target_folder': f'./run_{uuid.uuid4().hex}',
        'csv_subfolder': 'csv_model',
        'temp_file': 'base_model.nc',
        'result_file': 'result',
        'use_oetc': 'False',
        'run_scenarios': 'False',
        'modular_representation': 'False',
        'transmission_losses': '0',
        'do_operational_constraints': 'True',
        'do_investment_constraints': 'True',
        'do_milp_constraints': 'True',
        'do_strict_unsimultaneous_dis+charging': 'False',
        'assign_all_duals': 'False',
        'small_limit': 0.00001,
        'project_cod': datetime.now().year,
        'link_ports': 2,
        'total_snapshot': 8760,
        'do_segmentation': 'False',
        'segmentation_duration': 3, # in hours
        # general solver settings
        'mipgap': 0.001,
        'timelimit_in_hours': 1,
        'timelimit': 3600,
        'log_to_console': 1,
        'output_flag': 1,
        # stochastic optimization settings
        'run_stochastic_runs': 'False',
        'stoch_alpha': 0.9,
        'stoch_omega': 0.5,
        'stoch_case_definition': "{'P49': 0.49, 'P51': 0.51}",
        # MGA settings
        'run_mga_runs': 'False',
        'mga_slack': 0.0,
        'mga_slacks': '[0.01, 0.02, 0.05, 0.10]',
        # Rolling Horizon (RH) settings
        'run_rollinghorizon_after_optimization': 'False',
        'rollinghorizon_horizon': (24+12),
        'rollinghorizon_overlap': 12,
        # security constraint settings
        'run_security_constrained_optimization': 'False',
        # some other settings
        'investment_periods': '[2031]', # periods to optimize
        'investment_weighting': '[1]', # weighting of periods; e.g, gap between 
                                       # multiple years; e.g., 5 or 10
        'primary_optimization': 'pathway',
        # some reserve margin settings
        'rm_activate': 'False',
        'rm_factor_a': 1,
        'rm_factor_b': 0.15,
        'rm_factor_c': 0,
        'rm_factor_d': 1,
        'rm_factor_e': 0,
        'rm_max_generator': 'False',
        'rm_load_class': 'Load',
        'rm_load_name': 'LOAD_EL_01',
    }
    #
    print (f'\nread optimization and scenario settings from {xls_file} ...')
    data = pd.read_excel(
        xls_file, 
        sheet_name=own_sheets)
    #
    df_params = []
    scen_params = []
    stoch_params = []
    #
    for sheet_name, sheet_data in data.items():
        df = copy.deepcopy(sheet_data[2:])
        #
        if sheet_name == 'opt_params':
            print ('x) optimization parameters')
            df.rename(columns={
                df.columns[1]: 'variable',
                df.columns[2]: 'value',
                df.columns[3]: 'remark',
                }, inplace=True)
            df.set_index('variable', inplace=True)
            df.drop(labels=[df.columns[0]], axis=1, inplace=True)
            df_params = df.copy()
        #
        elif sheet_name == 'scen_params':
            print ('x) scenario parameters')
            df.rename(columns={
                df.columns[1]: 'scenario',
                df.columns[2]: 'action',
                df.columns[3]: 'class',
                df.columns[4]: 'technology_name',
                df.columns[5]: 'column',
                df.columns[6]: 'value',
                df.columns[7]: 'active',
                }, inplace=True)
            df.drop(df.columns[0], axis=1, inplace=True)
            #
            if eval(df_params.loc['run_scenarios'].value):
                scen_params = df[df.active == 'True']
            #
            else:
                scen_params = df[(df.active == 'True') & \
                                 (df.scenario == df[df.active == 'True'].scenario.unique()[0])]
        #
        elif sheet_name == 'stoch_params':
            print ('x) stochastic optimization parameters')
            df.rename(columns={
                df.columns[1]: 'scenario',
                df.columns[2]: 'action',
                df.columns[3]: 'class',
                df.columns[4]: 'technology_name',
                df.columns[5]: 'column',
                df.columns[6]: 'value',
                df.columns[7]: 'active',
                }, inplace=True)
            df.drop(df.columns[0], axis=1, inplace=True)
            #
            stoch_params = df[df.active == 'True']
        #
        else:
            print (f'info! sheet {sheet_name} is not configured ...')
    #
    print ('done.\n')
    #
    for default in defaults:
        if default in df_params.index:
            globals()[f'{default}'] = df_params[df_params.index == default].value.iloc[0]
        #
        else:
            globals()[f'{default}'] = defaults[default]
    #
    if globals()['primary_optimization'] == 'pathway':
        globals()['multi_investment_periods'] = 'False'
    #
    else:
        globals()['multi_investment_periods'] = 'True'
    #
    print ('hours_to_optimize:', globals()['hours_to_optimize'])
    #
    return df_params, scen_params, stoch_params

def get_solver_setting (
    ) -> dict:
    """
    Set the solver settings to control its behaviour during optimization.
    
    Parameters
    ----------
    None

    Returns
    -------
    solver_options: dict
        Options to control the chosen optimization solver.
    """
    #
    if globals()['solver_name'] == 'gurobi':
        solver_options = {-
            # general solver settings
            'mipgap' : globals()['mipgap'],
            'outputflag' : globals()['output_flag'],
            'logtoconsole' : globals()['log_to_console'],
            'timelimit': globals()['timelimit'],
            #
            # individual settings
            'threads': 0,
            'presolve': 1, 
            'method': 4,
            'numericfocus': 0,
            'crossover': -1,
            }
    #
    elif globals()['solver_name'] == 'highs':
         solver_options = {
             # general solver settings
             'mip_abs_gap' : globals()['mipgap'],
             'output_flag' : bool(globals()['output_flag']),
             'log_to_console' : bool(globals()['log_to_console']),
             'time_limit': globals()['timelimit'],
             #
             # individual settings
             'threads': 0,
             'presolve': 'choose', # "off", "choose" or "on"; default: "choose"
             'solver': 'choose', # "simplex", "choose", "ipm" or "pdlp".
                                 # If "simplex"/"ipm"/"pdlp"; default: "choose"
             'run_crossover': 'choose', # "off", "choose" or "on"; default: "on"
             }
    #
    elif globals()['solver_name'] == 'cplex':
         solver_options = {
             # general solver settings
             'mipgap' : globals()['mipgap'],
             'outlev ' : globals()['output_flag'],
             'log_to_console' : bool(globals()['log_to_console']),
             'timelimit': globals()['timelimit'],
             #
             # individual settings
             'threads': 0,
             'presolve': 1, 
             'method': 2,
             'solutiontype': 2,
             'numericfocus': 0,
             }
    #
    else:
         solver_options = {}
    #
    return solver_options

def validate_scenario_adjustments(
        temp_file: str, 
        df_scens: pd.core.frame.DataFrame,
    ) -> bool:
    """
    Validate the provided scenario adjustments.
    
    Parameters
    ----------
    temp_file: str
        Name of the PyPSA network test adjustments against.

    scenrios:
        DataFrame containing the required scenario adjustments.

    Returns
    -------
    all_adjustments_ok: bool
        Boolean of all envisioned adjustments are possible.
    """
    #
    print ('load the temporary network model to validate the scenario changes ...')
    n = pypsa.Network()
    n.import_from_netcdf(
        temp_file)
    all_adjustments_ok = True
    #
    for scenario in df_scens.scenario.unique():
        # load the temporary model to ensure starting from the same point
        for index, row in df_scens[(df_scens.scenario == scenario)].iterrows():
            if row['class'] != 'Python':
                df = n.c[row['class']].static
            #
            if row['action'] == 'set':
                if row['class'] == 'Python': # change variable in Python
                    if not row['column'] in globals():
                        print (f'info! set variable {row['column']} = {row['value']} would not work')
                        all_adjustments_ok = False
                #
                else:
                    if not (row['technology_name'] in df.index and \
                            row['column'] in df.columns):
                        print (f'info! set {row['class']}.{row['technology_name']}.{row['column']} = {row['value']} would not work')
                        all_adjustments_ok = False
            #
            elif row['action'] == 'del':
                if not (row['technology_name'] in df.index):
                    print (f'info! del {row['class']}.{row['technology_name']} would not work')
                    all_adjustments_ok = False
    #
    print ('')
    return all_adjustments_ok

def update_network(
        n: pypsa.Network(),
        df_adjusts: pd.core.frame.DataFrame, 
        scenario: str,
        stoch_scenarios: str = None,
    ) -> pypsa.Network:
    """
    Reads the basecase PyPSA network and adjust settings for a given scenario
    based on the data in the DataFrame scenarios.
    
    Parameters
    ----------
    n: pypsa.Network
        PyPSA network containing the initial netowrk to optimize.

    df_adjusts:
        DataFrame containing the required scenario adjustments.

    scenario:
        Name of the current scenario to adjust the PyPSA network for.

    Returns
    -------
    n: pypsa.Network
        PyPSA network containing the new scenario to optimize.
    """
    #
    if stoch_scenarios == None:
        # loop for non-stochastic adjustments
        #
        for index, row in df_adjusts[(df_adjusts.scenario == scenario)].iterrows():
            if row['class'] != 'Python':
                df = n.c[row['class']].static
            #
            if row['action'] == 'set':
                if row['class'] == 'Python': # change variable in Python
                    if row['column'] in globals():
                        print (f'x) set variable {row['column']} = {row['value']}')
                        globals()[row['column']] = row['value']
                    #
                    else:
                        print (f'info! set variable {row['column']} = {row['value']} does not work')
                #
                else:
                    if row['technology_name'] in df.index and \
                       row['column'] in df.columns:
                        print (f'x) set {row['class']}.{row['technology_name']}.{row['column']} = {row['value']}')
                        df.loc[row['technology_name'], row['column']] = row['value']
                    #
                    else:
                        print (f'info! set {row['class']}.{row['technology_name']}.{row['column']} = {row['value']} does not work')
            #
            elif row['action'] == 'del':
                if row['technology_name'] in df.index:
                    print (f'x) del {row['class']}.{row['technology_name']}')
                    n.remove(row['class'], row['technology_name'])
                #
                else:
                    print (f'info! del {row['class']}.{row['technology_name']} does not work')
    #
    else:
        # loop for stochastic adjustments
        #
        for index, row in df_adjusts.iterrows():
            if row['class'] != 'Python':
                df = n.c[row['class']].static
                dfd = n.c[row['class']].dynamic
            #
            if row['action'] == 'set':
                # check within the static dataframe
                if row['scenario'] in df.index and \
                   row['column'] in df.columns and \
                      row['technology_name'] in df.index.get_level_values('name'):
                    print (f'x) {row['scenario']}: set {row['class']}.{row['technology_name']}.{row['column']} = {row['value']}')
                    df.loc[[(row['scenario'], row['technology_name'])], row['column']] = row['value']
                #
                # check within the dynamic dataframe
                if row['scenario'] in df.index and \
                   row['column'] in df.columns and \
                      row['technology_name'] in df.index.get_level_values('name'):
                    print (f'x) {row['scenario']}: set {row['class']}.{row['technology_name']}.{row['column']} = {row['value']}')
                    df.loc[[(row['scenario'], row['technology_name'])], row['column']] = row['value']
                #
                else:
                    print (f'info! {row['scenario']}: set {row['class']}.{row['technology_name']}.{row['column']} = {row['value']} does not work')
            #
            elif row['action'] == 'del':
                # check within the static dataframe
                if row['scenario'] in df.index and \
                   row['column'] in df.columns and \
                      row['technology_name'] in df.index.get_level_values('name'):
                    print (f'x) {row['scenario']}: del {row['class']}.{row['technology_name']}.{row['column']} = {row['value']}')
                    df.loc[[(row['scenario'], row['technology_name'])], row['column']] = 0 # row['value']
                #
                # check within the dynamic dataframe
                if row['column'] in dfd and \
                   (row['scenario'], row['technology_name']) in dfd['p_set'].columns:
                    print (f'x) {row['scenario']}: del {row['class']}.{row['technology_name']}.{row['column']} = {row['value']}')
                    dfd[row['column']][[(row['scenario'], row['technology_name'])]] = 0
    #
    return n

def read_and_update_network(
        temp_file: str, 
        df_scens: pd.core.frame.DataFrame = pd.core.frame.DataFrame(), 
        scenario: str = None
    ) -> pypsa.Network:
    """
    Reads the basecase PyPSA network and adjust settings for a given scenario
    based on the data in the DataFrame scenarios.
    
    Parameters
    ----------
    temp_file: str
        Name of the PyPSA network test adjustments against.

    scenrios:
        DataFrame containing the required scenario adjustments.

    scenario:
        Name of the current scenario to adjust the PyPSA network for.

    Returns
    -------
    n: pypsa.Network
        PyPSA network containing the new scenario to optimize.
    """
    #
    n = pypsa.Network()
    n.import_from_netcdf(
        temp_file)
    #
    if len(df_scens) > 0:
        n = update_network(
            n,
            df_scens, 
            scenario)
    #
    comps = pypsa.descriptors.nominal_attrs
    col ='mod'
    #
    # create individual units for the technologies having 'nom_mod' defined
    if eval(globals()['modular_representation']):
        # loop through all components
        for c in comps:
            # >> Exclude Line and Transformer as the power flow implementation in
            #    PyPSA can't deal with extendable line and transformer components
            if c not in ['Line', 'Transformer', 'StorageUnit']:
                attr = comps[c]
                df = n.c[c].static.query(f'{attr}_{col} > 0')
                #
                for idx, row in df.iterrows():
                    num_adds = int(row[f'{attr}_max'] // row[f'{attr}_{col}'])
                    print (f'info! add {num_adds} modules for {c}.{idx}')
                    #
                    # create new lines based on the 
                    dfs = pd.concat([df[df.index == idx].copy()] * num_adds)
                    dfs2 = dfs.reset_index()
                    #
                    for i in range(1, num_adds+1):
                        # ensure a slightly difference in the economic ranking of the 
                        # individual capital_cost's
                        dfs2.at[i-1, 'name'] = f'{dfs2.at[i-1, 'name']}_mod_{str(i).rjust(len(str(num_adds)), '0')}'
                        dfs2.at[i-1, 'capital_cost'] -= (num_adds - i + 2) * 0.0001
                        dfs2.at[i-1, f'{attr}_max'] = dfs2.at[i-1, f'{attr}_{col}']
                        dfs2.at[i-1, f'{attr}_mod'] = 0
                    #
                    dfs = dfs2.set_index('name')
                    #
                    # add all the new candidates
                    n.add(
                        c, 
                        name=dfs.index, 
                        **dfs)
    #
    # remove unused and/or unusable components (nom=0 & nom_max=0)
    n = remove_unused_details (n)
    inv_periods = np.array(eval(globals()['investment_periods']))
    #
    return n, inv_periods

def get_pypsa_component_lists(
    ) -> tuple [list, 
                list]:
    """
    Get the list of possible static and dynamic PyPSA DataFrames to be read
    from the Excel assumption book.
    
    Parameters
    ----------
    None

    Returns
    -------
    sheets_list: list
        PyPSA's static DataFrame options.

    sheets_ts_list: list
        PyPSA's dynamic DataFrame options.
    """
    #
    n = pypsa.Network()
    sheets_list = ['network']  # possible components within the pypsa module
    sheets_ts_list = []        # possible time-dependent details
    #
    for key in n.c.keys():
        c = n.c[key].name
        if c not in ['SubNetwork']:
            sheets_list.append(key)
            #
            for idx in n.c[c].dynamic:
                sheets_ts_list.append(f'{key}-{idx}')
    #
    # make sure that enough ports are considered
    for i in range(0, globals()['link_ports']+1):
        if f'links-p{i}' not in sheets_ts_list:
            sheets_ts_list.append(f'links-efficiency{i}')
            sheets_ts_list.append(f'links-p{i}')
    #
    return sheets_list, sheets_ts_list

def save_network(
        n: pypsa.Network, 
        file_name: str,
    ) -> None:
    """
    Save a given PyPSA network as NetCDF file.
    
    Parameters
    ----------
    n: pypsa.Network
        PyPSA network to save into the NetCDF file.

    file_name: str
        File name to save the PyPSA network into.

    Returns
    -------
    None
    """
    #
    print (f'save PyPSA network as "{file_name}"')
    # delete target file if exists
    if os.path.exists(file_name):
        os.remove(file_name)
    #
    # save the result
    n.export_to_netcdf(file_name)
    #
    return None

def save_network_svg(
        n: pypsa.Network,
        file_name: str,
        small_limit: float = 0.0001
    ) -> nx.Graph():
    """
    Save a given PyPSA network as SVG file.
    
    Parameters
    ----------
    n: pypsa.Network
        PyPSA network to save into a SVG file.

    file_name: str
        File name to save the PyPSA network into.

    small_limit: float = 0.0001
        Minimum size of technology to be shown on the SVG file.

    Returns
    -------
    G: nx.Graph
        Result graph
    """
    #
    print (f'save network as "{file_name}"')
    # collect the colors of the carriers
    comps = pypsa.descriptors.nominal_attrs
    c1 = 'Bus'
    c2 = 'Carrier'
    colors = n.c[c1].static.join(n.c[c2].static, on='carrier', 
                                 lsuffix='_c', rsuffix='_o')[['carrier', 'color']]
    #
    # to avoid networkx' random positioning we use the random seed feature
    np.random.seed(42)
    G = nx.Graph()
    #
    # add all buses as they have distribution technologies
    c = 'Bus'
    df = n.c[c].static
    #
    for bus in df.index:
        node_color = colors[colors.index == bus].color.iloc[0]
        G.add_node(
            node_for_adding = bus, 
            node_color = node_color)
    #
    # add load nodes
    c = 'Load'
    df = n.c[c].static.query('p_set > 0')
    #
    for load in df.index:
        node_color = colors[colors.index == df.bus[load]].color.iloc[0]
        G.add_node(
            node_for_adding = load, 
            node_color = node_color)
    #
    # get the largest 
    max_activity = max(
        n.c['Link'].dynamic['p0'].abs().sum().sum(),
        n.c['Generator'].dynamic['p'].abs().sum().sum(),
        n.c['Store'].dynamic['p'].abs().sum().sum(),
        n.c['StorageUnit'].dynamic['p'].abs().sum().sum()/2) / 10
    #
    if not small_limit: small_limit = globals()['small_limit']
    #
    # loop through all components
    for c in comps:
        attr = comps[c]
        df = n.c[c].static.query(f'{attr}_opt > '+str(small_limit))
        #
        for t in df.index:
            if 'bus0' in df.columns:
                node_color = colors[colors.index == df.bus0.loc[t]].color.iloc[0]
            #
            else:
                node_color = 'black'
            #
            # add the technology nodes (e.g., generator, link, transformer)
            G.add_node(
                node_for_adding = t, 
                node_color = 'black')
            #
            # if there is a bus0 there are more buses
            if 'bus0' in df.columns:
                buses = [col for col in df if col.startswith('bus')]
                #
                for bus in buses:
                    edge_from = df[df.index == t]['bus0'].values[0]
                    edge_to = df[df.index == t][bus].values[0]
                    #
                    if len(edge_to) > 0:
                        edge_color = colors[colors.index == df.loc[t][bus]].color.iloc[0]
                        #
                        if t in n.c[c].dynamic[f'p{bus[3:]}']:
                            tech_sum = n.c[c].dynamic[f'p{bus[3:]}'][t].abs().sum()
                        #
                        else:
                            tech_sum = 0
                        #
                        edge_width = max(0.1,  tech_sum / max_activity)
                        G.add_edge(
                            # u_of_edge = edge_from, 
                            u_of_edge = t, 
                            v_of_edge = edge_to,
                            color = edge_color, 
                            weight = edge_width,
                            style = 'solid',
                            label = t)
            #
            # otherwise there is only the column 'bus'
            else:
                bus = 'bus'
                if c in ['Generator']:
                    edge_from = t
                    edge_to = df[df.index == t][bus].values[0]
                #
                else:
                    edge_from = df[df.index == t][bus].values[0]
                    edge_to = t
                #
                edge_color = colors[colors.index == df.loc[t][bus]].color.iloc[0]
                if t in n.c[c].dynamic['p']:
                    tech_sum = n.c[c].dynamic['p'][t].abs().sum()
                #
                else:
                    tech_sum = 0
                #
                if tech_sum > 0:
                    edge_width = max(0.1,  tech_sum / max_activity)
                    G.add_edge(
                        u_of_edge = edge_from, 
                        v_of_edge = edge_to,
                        color = edge_color, 
                        weight = edge_width,
                        style = 'solid',
                        label = t)
    #
    # add load edges
    c = 'Load'
    df = n.c[c].static.query('p_set > 0')
    #
    for load in df.index:
        edge_from = load
        edge_to = df.bus[load]
        edge_color = colors[colors.index == df.bus[load]].color.iloc[0]
        tech_sum = n.c[c].dynamic['p_set'][load].abs().sum()
        #
        if tech_sum > 0:
            edge_width = max(0.1,  tech_sum / max_activity)
            G.add_edge(
                u_of_edge = edge_from, 
                v_of_edge = edge_to,
                color = edge_color, 
                weight = edge_width,
                style = 'solid',
                label = t)
    #
    edge_colors = nx.get_edge_attributes(G, 'color').values()
    weights = nx.get_edge_attributes(G, 'weight').values()
    labels = dict([((n1, n2), d['label']) for n1, n2, d in G.edges(data=True)])
    # styles = nx.get_edge_attributes(G, 'style').values()
    # fig = plt.figure(1, figsize=(20, 10), dpi=300)
    plt.figure(1, figsize=(16, 8), dpi=300)
    # pos = nx.circular_layout(G)
    # pos = nx.spring_layout(G, seed=5)
    # pos = nx.fruchterman_reingold_layout(G, k=0.54, iterations=100)
    pos = nx.arf_layout(G)
    nx.draw(
        G = G,
        pos = pos, 
        edge_color = edge_colors, 
        width = list(weights),
        node_color = 'lightgreen',
        font_size = 12)
    #
    nx.draw_networkx_edge_labels(
        G = G, 
        pos = pos, 
        edge_labels = labels, 
        label_pos = 0.5,
        font_color = 'black',
        font_size = 6)
    #
    plt.title('Optimized energy system')
    plt.savefig(file_name, dpi=300)
    plt.show()
    plt.close()
    #
    return G

def remove_unused_details(
        n: pypsa.Network
    ) -> pypsa.Network:
    """
    Save a given PyPSA network as SVG file.
    
    Parameters
    ----------
    n: pypsa.Network
        PyPSA network to remove unused options from (having NOM and NOM_MAX
        defined as 0).

    Returns
    -------
    n: pypsa.Network
        Updated PyPSA network.
    """
    #
    # remove technologies with nom = 0 and nom_max = 0
    comps = pypsa.descriptors.nominal_attrs
    unused = []
    #
    # loop through all components
    for c in comps:
        attr = comps[c]
        #
        # if nom and nom_max is 0 the technology option is not used
        df = n.c[c].static.query(f'({attr} == 0) & ({attr}_max == 0)')
        if len(df) > 0:
            unused.append(df.index)
            n.remove(c, df.index)
        #
    #
    print ('\nremove unused technologies:')
    #
    for t in unused:
        print (f'x) {t}')
    #
    # remove unused buses
    df1 = pd.Series()
    #
    # loop through all components
    for c in comps:
        attr = comps[c]
        df = n.c[c].static
        #
        # for t in df.index:
        for col in [col for col in df.columns if col.startswith('bus')]:
            for t, row in df.iterrows():
                val1 = df[col].loc[t]
                #
                if (val1 != '') and \
                    ((df[attr].loc[t] > 0) or
                     (df[f'{attr}_max'].loc[t] > 0)):
                    df1 = pd.concat([df1, pd.Series(df[col].loc[t])])
    #
    c = 'Bus'
    df = n.c[c].static.index[~n.c[c].static.index.isin(df1)]
    if len(df) > 0:
        n.remove('Bus', df)
        print ('\nunused buses:')
        #
        for bus in list(df):
            print (f'x) {bus}')
    else:
        print ()
    #
    return n

def adjust_for_rollinghorizon(
        n: pypsa.Network
    ) -> pypsa.Network:
    """
    Adjust network for rolling horizon run.
    
    Parameters
    ----------
    n: pypsa.Network
        PyPSA network to adjust for rolling horizon run.

    Returns
    -------
    n: pypsa.Network
        Updated PyPSA network.
    """
    #
    # adjust committable and extendable for the rolling horizon process
    comps = pypsa.descriptors.nominal_attrs
    #
    # loop through all components
    for c in comps:
        attr = comps[c]
        df = n.c[c].static
        #
        if attr+'_extendable' in df.columns:
            df[attr+'_extendable'] = False
        #
        # curtailing technologies should be available just in case
        if '_curtail_option' in df.columns:
            df.loc[(df['_curtail_option'] == True) & (df[attr+'_opt']>0), attr] = np.inf
        #
        # emergency technologies should be available just in case
        if '_emergency_option' in df.columns:
            df.loc[df['_emergency_option'] == True, attr] = np.inf
        #
        if 'committable' in df.columns:
            df['committable'] = True
        #
        if 'cyclic_state_of_charge' in df.columns:
            df['cyclic_state_of_charge'] = False
        #
        if 'e_cyclic' in df.columns:
            df['e_cyclic'] = False
        #
        if 'state_of_charge_initial' in df.columns:
            df['state_of_charge_initial'] = 0
        # 
        if 'e_initial' in df.columns:
            df['e_initial'] = 0
    #
    return n
                    
def create_summaries(
        n: pypsa.Network, 
        scenario: str, 
        list_results: list,
        list_supplies: list,
        list_balances: list,
        list_curtailments: list,
        create_html: bool = False,
    ) -> [list, list, list]:
    """
    Shows the result in a standardized way and keeps the result in a 
    dictiionary.

    Parameters
    ----------
    n: pypsa.Network
        PyPSA network to get the details from.

    scenario: str
        Scenario name.

    list_results: list
        List of results before adding this result.

    list_supplies: list
        List of supplies before adding this result.

    list_balances: list
        List of energy balances before adding this result.

    list_curtailments: list
        List of curtailments before adding this result.
    Returns
    -------
    list_results: list
        List of results after adding this result.

    list_supplies: list
        List of supplies after adding this result.

    list_balances: list
        List of energy balances after adding this result.

    list_curtailments: list
        List of curtailments after adding this result.
    """
    #
    # save results as a NC file
    save_network(
        n, 
        f'{globals()['target_folder']}/{globals()['result_file']}_{scenario}.nc')
    #
    if n.has_scenarios:
        no_cases = len(eval(globals()['stoch_case_definition']))
    else:
        no_cases = 1
    #
    print ('\nlist of optimized technologies:')
    comps = pypsa.descriptors.nominal_attrs
    data = []
    #
    # loop through all components
    for c in comps:
        df = n.c[c].static
        attr = comps[c]
        #
        for t in df.query(f'{attr}_opt > '+str(globals()['small_limit'])).index.get_level_values('name').unique():
            if n.has_scenarios:
                data.append([t, df[df.index.get_level_values('name') == t][f'{attr}_opt'].mean()])
            else:
                data.append([t, df[f'{attr}_opt'].loc[t]])
    #
    data.append(['_', '-'])
    data.append(['_duration(sec)', (n.duration)])
    data.append(['_timesteps(-)', (float(len(n.snapshots)))])
    data.append(['_memory(MBytes)', (getsize(n)/10**6)])
    data.append(['__obj(mn)', (n.objective/1e6)])
    data.append(['__totex(mn)', (n.statistics.opex(nice_names=False).sum().sum() + \
                                 n.statistics.capex(nice_names=False).sum().sum())/1e6/no_cases])
    data.append(['___capex(mn)', (n.statistics.capex(nice_names=False).sum().sum())/1e6/no_cases])
    data.append(['___opex(mn)', (n.statistics.opex(nice_names=False).sum().sum())/1e6/no_cases])
    #
    # convert data array into a DataFrame and set index
    df_res = pd.DataFrame(
        data, 
        columns=['technology', scenario]).set_index('technology')
    #
    # show results summary table (capacities being built/used)
    print (df_res.sort_index())
    list_results.append(pd.DataFrame(df_res))
    #
    # prepare energy balance statistics
    if n.has_scenarios:
        levels = [0,3]
    else:
        levels = [0,1]
    #
    df = pd.DataFrame(
        n.statistics.energy_balance(nice_names=False)).groupby(level=levels, axis=0).mean()
    df.columns = [scenario]
    list_supplies.append(df)
    #
    # prepare supply statistics
    if n.has_scenarios:
        levels = [0,2]
    else:
        levels = [0,1]
    #
    df = pd.DataFrame(
        n.statistics.supply(nice_names=False)).groupby(level=levels, axis=0).mean()
    df.columns = [scenario]
    list_balances.append(df)
    #
    # prepare supply statistics
    if n.has_scenarios:
        levels = [0,2]
    else:
        levels = [0,1]
    #
    df = pd.DataFrame(
        n.statistics.curtailment(nice_names=False)).groupby(level=levels, axis=0).mean()
    df.columns = [scenario]
    list_curtailments.append(df)
    #
   # optional: generate interactive result viewer (HTML)
    if network_viewer:
        html_viewer(
            n,
            file_name = f'{globals()['target_folder']}/{globals()['result_file']}_{scenario}.html',
            title = f'Network Analysis ({scenario})')
    #
    # optional: generate SVG network
    if networkx_viewer:
        save_network_svg(
            n, 
            f'{globals()['target_folder']}/{globals()['result_file']}_{scenario}.svg')
    #
    return list_results, list_supplies, list_balances, list_curtailments

def set_optimized_capacities(
        n: pypsa.Network, 
        period: int = 0,
    ) -> pd.core.frame.DataFrame:
    """
    Set nom capacities based on optimization result (nom_opt).

    Parameters
    ----------
    n: pypsa.Network
        PyPSA network to be adjusted.

    period: int
        Year to be fixed.

    Returns
    -------
    None
    """
    #
    comps = pypsa.descriptors.nominal_attrs
    #
    if n.is_solved:
        print ('................ set nom = nom_opt ..................')
        #
        # loop through all components
        for c in comps:
            attr = comps[c]
            if period > 0:
                print (c)
                df = n.c[c].static
                df[df.build_year == period][attr] = \
                    df[df.build_year == period][f'{attr}_opt']
                df[df.build_year == period][f'{attr}_min'] = \
                    df[df.build_year == period][f'{attr}_opt']
                df[df.build_year == period][f'{attr}_max'] = \
                    df[df.build_year == period][f'{attr}_opt']
                df.loc[df.build_year == period][f'{attr}_extendable'] = \
                    False
            #
            else:
                df[attr] = df[f'{attr}_opt']
                df[f'{attr}_min'] = df[f'{attr}_opt']
                df[f'{attr}_max'] = df[f'{attr}_opt']
                df.loc[f'{attr}_extendable'] = False
    #
    else:
        print ('!! cannot set optimal capacities as n.is_solved = False !!')
    #
    return None

def show_case_comparison(
        list_results: list,
        list_supplies: list, 
        list_balances: list,
        list_curtailments: list,
    ) -> pd.core.frame.DataFrame:
    """
    Creates a case comparison table.

    Parameters
    ----------
    list_results: list
        List of collected results.

    list_supplies: list
        List of collected supply overviews.

    list_balances: list
        List of collected energy balances.

    list_curtailments: list
        List of curtailments.

    Returns
    -------
    df: pd.core.frame.DataFrame
        Table with capacities of all conducted runs.
    """
    print ('\n'+'-'*40+'\n')
    df = pd.concat(list_results, axis=1).sort_index().replace(np.nan, '-')
    next_col = None
    col_pos = 1
    df_col = 1
    #
    for col in df.columns:
        # ignore the first column as it is not possible to compare it
        # against another result
        if next_col:
            new_col = f'{df_col}:{df_col+1}'
            df.insert(col_pos, new_col, 0)
            #
            for index, row in df.iterrows():
                if (df[next_col].loc[index] != '-') and \
                   (df[col].loc[index] != '-'):
                       df[new_col].loc[index] = \
                           df[col].loc[index] / df[next_col].loc[index]
                #
                else:
                    df[new_col].loc[index] = '-'
            #
            next_col = col
            df_col = df_col + 1
            col_pos = col_pos + 2
        #
        else:
            next_col = col
    print (f'{df}\n')
    print (f'{pd.concat(list_supplies, axis=1).sort_index().replace(np.nan, '-')}\n')
    print (f'{pd.concat(list_balances, axis=1).sort_index().replace(np.nan, '-')}\n')
    print (f'{pd.concat(list_curtailments, axis=1).sort_index().replace(np.nan, '-')}\n')
    #
    return df

def do_optimization(
        n: pypsa.Network,
        inv_periods: np.ndarray,
    ) -> tuple [pypsa.Network, 
                str, 
                str]:
    """
    Initiate the optimization run.

    Parameters
    ----------
    n: pypsa.Network
        PyPSA network to be adjusted.

    inv_periods: list
        List of years to assess.

    Returns
    -------
    n: pypsa.Network
        PyPSA network to be adjusted.

    status: str
        Status of the optimization.

    tc: str
        Termination code of the optimization.
    """
    #
    start_time = datetime.now()
    #
    # do the investment optimization (either pathway or myopic)
    if globals()['primary_optimization'] == 'pathway':
        print ('\ndo full horizon / pathway optimization')
        #
        # start optimizing the network
        status, tc = n.optimize(
            snapshots = n.snapshots,
        	solver_name = globals()['solver_name'], 
            multi_investment_periods = False,
        	extra_functionality = extra_functionalities,
            # number of tangents used for the piecewise linear approximation
            transmission_losses = int(globals()['transmission_losses']),
            assign_all_duals = bool(globals()['assign_all_duals']),
            # kwargs includes e.g., solver_options
            **kwargs)
    #
    elif globals()['primary_optimization'] == 'myopic':
        print ('\ndo year by year / myopic optimization')
        #
        for period in inv_periods:
            # limit the snapshots to the current year
            snapshots = n.snapshots[n.snapshots.get_level_values('period') == period]
            status, tc = n.optimize(
                snapshots = snapshots,
                multi_investment_periods = True,
                solver_name = globals()['solver_name'],
                extra_functionality = extra_functionalities,
                # number of tangents used for the piecewise linear approximation
                transmission_losses = int(globals()['transmission_losses']),
                assign_all_duals = bool(globals()['assign_all_duals']),
                # kwargs includes e.g., solver_options
                **kwargs)
            #
            # if one execution was not successful, stop it
            if status != 'ok':
                return n, status, tc
            #
            set_optimized_capacities(n, period)
    #
    else:
        print (f'\nerror! Provided primary optimization method not allowed: {globals()['primary_optimization']}')
        sys.exit (0)
    #
    end_time = datetime.now()
    n.duration = (end_time - start_time).total_seconds()
    print (f'optimization duration: {round(n.duration,2)}\n')
    #
    return n, status, tc

def do_all_runs(
        n: pypsa.Network, 
        df_scens: pd.core.frame.DataFrame,
        df_stochs: pd.core.frame.DataFrame,
        kwargs
    ) -> tuple [pypsa.Network, 
                list, 
                list, 
                list]:
    """
    Do the necessary optimization tasks.

    Parameters
    ----------
    n: pypsa.Network
        PyPSA network to be adjusted.

    df_scen: pd.core.frame.DataFrame
        Scenario information being considered.

    df_stochs: pd.core.frame.DataFrame
        Stochastic optimization information being considered.

    kwargs: 
        keyword arguments.

    Returns
    -------
    list_results: list
        List of collected results.

    list_supplies: list
        List of collected supply overviews.

    list_balances: list
        List of collected energy balances.

    list_curtailments: list
        List of curtailments.

    status: str
        Status of the optimization.

    tc: str
        Termination code of the optimization.
    """
    #
    list_results = []
    list_supplies = []
    list_balances = []
    list_curtailments = []
    #
    # loop through scenario definitions
    if globals()['run_scenarios'] == 'False':
        print ('info! only one scenario will be assessed\n')
    #
    for scenario in df_scens.scenario.unique():
        print ('-'*40+'\n')
        print (f'>>>>> do scenario {scenario}')
        # load the temporary model to ensure starting from the same point
        print ('load the temporary network model ...')
        n, inv_periods = read_and_update_network(
            f'{globals()['target_folder']}/{globals()['temp_file']}', 
            df_scens, 
            scenario)
        #
        # do the investment optimization
        n, status, tc = do_optimization(
            n, 
            inv_periods)
        #
        # if optimization was successful, save the result and keep the 
        # results so they can be shown at the end of all runs
        if status == 'ok':
            list_results, list_supplies, list_balances, list_curtailments = \
                create_summaries(
                    n, 
                    scenario,
                    list_results,
                    list_supplies, 
                    list_balances,
                    list_curtailments)
            #
            if not eval(globals()['multi_investment_periods']):
                w = n.investment_period_weightings.objective
            #
            else:
                w = [1]
            #
            m = n.model
            # save the results of the first run for potential MGA runs
            if eval(globals()['run_mga_runs']):
                globals()['optimal_cost'] = m.objective.value
                globals()['fixed_cost'] = (n.statistics.installed_capex().sum() * w).sum()
            #
            # do dispatch after optimization 
            if eval(globals()['run_rollinghorizon_after_optimization']):
                print ('\ndo rolling horizon (dispatch only) optimization after investment optimization ...')
                horizon = globals()['rollinghorizon_horizon']
                overlap = globals()['rollinghorizon_overlap']
                print (f'using {horizon} hours horizon with {overlap} hours overlap')
                #
                # fix the optimal capacities
                n.optimize.fix_optimal_capacities()
                n = adjust_for_rollinghorizon(n)
                #
                # save conditions for later reseting
                save_cond_invest = globals()['do_investment_constraints']
                #
                globals()['do_investment_constraints'] = 'False'
                #
                start_time = datetime.now()
                n.optimize.optimize_with_rolling_horizon(
                    horizon = horizon,
                    overlap = overlap,
                	solver_name = globals()['solver_name'], 
                	extra_functionality = extra_functionalities,
                    # number of tangents used for the piecewise linear approximation
                    transmission_losses = int(globals()['transmission_losses']),
                    assign_all_duals = bool(globals()['assign_all_duals']),
                    # kwargs includes e.g., solver_options
                    **kwargs)
                #
                # restore the saved conditions
                globals()['do_investment_constraints'] = save_cond_invest
                #
                end_time = datetime.now()
                n.duration = (end_time - start_time).total_seconds()
                print (f'optimization duration: {round(n.duration,2)}\n')
                #
                list_results, list_supplies, list_balances, list_curtailments = \
                    create_summaries(
                        n, 
                        f'{scenario}_rh', 
                        list_results,
                        list_supplies, 
                        list_balances,
                        list_curtailments)
                #
                c = 'Generator'
                df = n.c[c].static
                emerg_power = n.c[c].dynamic.p.sum() \
                    [df._emergency_option].sum().round(1)
                print (f'\nconsumed emergency energy: {round(emerg_power,2)} MWh')
    #
    return n, list_results, list_supplies, list_balances, list_curtailments, status, tc


# EXTRA FUNCTIONALITY FUNCTIONS ----------------------------------------------

def validate_technology_exists(
        n: pypsa.Network, 
        c: str = None, 
        t: str = None
    ) -> bool:
    """
    Validate if a given technology name is available in a given component type.
    
    Parameters
    ----------
    n: pypsa.Network
        PyPSA network to asses the validity of a given component and technology 
        combination.

    c: str = None,
        Component type to validate.

    t: str = None,
        Technology name to validate.

    Returns
    -------
    True | False: bool
        Status if given technology is available in the named component type.
    """
    #
    # check if the parameter is valid
    comps = pypsa.descriptors.nominal_attrs
    df = n.c[c].static
    #
    if (c in comps.keys()) and \
        (t in df[df.index.get_level_values('name') == t].index.get_level_values('name')):
        return True
    #
    else:
        print (c, t)
        print (f'info! technology {c}.{t} does not exist')
        return False

def link_capacities(
        n: pypsa.Network
    ) -> None:
    """
    Link the capacities of individual technology options (e.g., CHARGER_BES and
    DISCHARGER_BES). The following columns in the individual DataFrames needs to
    be defined:
        _linked_class
        _linked_technology
        _linked_sign
        _linked_multiplier, and
        _linked_rhs

    Parameters
    ----------
    n: pypsa.Network
        PyPSA network to get the details from.

    Returns
    -------
    None
    """
    #
    m = n.model
    comps = pypsa.descriptors.nominal_attrs
    col = '_linked_class'
    #
    # loop through all components
    for c in comps:
        df = n.c[c].static
        #
        if col in df.columns:
            # loop through all technology options of the class
            for t in df.index.get_level_values('name').unique():
                row = df.loc[t]
                #
                # the scenario name comes before the technology name
                if n.has_scenarios:
                    t = t[1]
                #
                # if column '_linked_class' is defined prepare adding the constraint
                if not str(row[col]) in ['', 'nan'] and \
                    '_linked_class' in row and \
                    '_linked_technology' in row and \
                    '_linked_sign' in row and \
                    '_linked_multiplier' in row and \
                    '_linked_rhs' in row:
                    class1 = c
                    tech1 = t
                    class2 = row['_linked_class']
                    tech2 = row['_linked_technology']
                    sign = row['_linked_sign']
                    factor1 = row['_linked_multiplier']
                    factor2 = row['_linked_rhs']
                    #
                    # validate if the linked technologies do exist    
                    valid1 = validate_technology_exists(n, class1, tech1)
                    valid2 = validate_technology_exists(n, class2, tech2)
                    constr_name = f'Link-capacities-{class1}.{tech1}-{class2}.{tech2}'
                    #
                    # if the technology defintion is valid, add the constraint
                    if valid1 and valid2:
                        # get the variables
                        nom_col1 = comps[class1]
                        nom_col2 = comps[class2]
                        #
                        link1_cap = m.variables[f'{class1}-{nom_col1}'].loc[tech1]
                        link2_cap = m.variables[f'{class2}-{nom_col2}'].loc[tech2]
                        #
                        # add the constraints
                        if constr_name not in n.model.constraints:
                            con = link1_cap - factor1 * link2_cap == factor2
                            con.sign = sign
                            m.add_constraints(con, name=constr_name)
                            print (f'added {constr_name}')
                    #
                    else:
                        print (f'info! constraint "{constr_name}" is misconfigured')
    #
    return None

def link_operation(
        n: pypsa.Network
    ) -> None:
    """
    Link the operation of individual technology options. The following columns 
    in the individual DataFrames needs to be defined:
        _match_oper_class
        _match_oper_technology, and
        _match_oper_multiplier

    ----------
    n: pypsa.Network
        PyPSA network to get the details from.

    Returns
    -------
    None
    """
    #
    m = n.model
    comps = pypsa.descriptors.nominal_attrs
    col = '_match_oper_class'
    #
    # loop through all components
    for c in comps:
        df = n.c[c].static
        #
        if col in df.columns:
            for t, row in df.iterrows():
                if n.has_scenarios:
                    t = t[1]
                #
                # if column '-linked_class' is defined prepare adding the constraint
                if not str(row[col]) in ['', 'nan'] and \
                    '_match_oper_class' in row and \
                    '_match_oper_technology' in row and \
                    '_match_oper_sign' in row and \
                    '_match_oper_multiplier' in row:
                    class1 = c
                    tech1 = t
                    class2 = row['_match_oper_class']
                    tech2 = row['_match_oper_technology']
                    sign = row['_match_oper_sign']
                    factor1 = row['_match_oper_multiplier']
                    factor2 = 0 # val['_match_oper_rhs']
                    #
                    # validate if the linked technologies do exist    
                    valid1 = validate_technology_exists(n, class1, tech1)
                    valid2 = validate_technology_exists(n, class2, tech2)
                    constr_name = f'Link-operations-{class1}.{tech1}-{class2}.{tech2}'
                    #
                    # if the technology defintion is valid, add the constraint
                    if valid1 and valid2:
                        # get the variables
                        op_col1 = comps[class1][0]
                        op_col2 = comps[class2][0]
                        link1_flow = m.variables[f'{class1}-{op_col1}'].loc[:, tech1]
                        link2_flow = m.variables[f'{class2}-{op_col2}'].loc[:, tech2]
                        #
                        # add the constraints
                        con = link1_flow - factor1 * link2_flow == factor2
                        con.sign = sign
                        m.add_constraints(con, name=constr_name)
                        print (f'added {constr_name}')
                    #
                    else:
                        print (f'info! constraint "{constr_name}" is misconfigured')
    #
    return None

def limit_hourly_operation_by_capacity(
        n: pypsa.Network
    ) -> None:
    """
    Limit the operation of individual technology options based on the capacity
    of another technology. The following columns in the individual DataFrames 
    needs to be defined:
        _limit_op_class
        _limit_op_technology,
        _limit_op_sign, and 
        _limit_op_factor

    Parameters
    ----------
    n: pypsa.Network
        PyPSA network to get the details from.

    Returns
    -------
    None
    """
    #
    m = n.model
    comps = pypsa.descriptors.nominal_attrs
    col = '_limit_op_class'
    data = []
    #
    # loop through all components
    for c in comps:
        df = n.c[c].static
        #
        if col in df.columns:
            # loop through all technology options of the class
            for t, row in df.iterrows():
                # if column '-linked_class' is defined prepare adding the constraint
                if not str(row[col]) in ['', 'nan'] and \
                    '_limit_op_class' in row and \
                    '_limit_op_technology' in row and \
                    '_limit_op_class' in row and \
                    '_limit_op_sign' in row and \
                    '_limit_op_factor' in row:
                    data.append([
                        c, 
                        t, 
                        df[col].loc[t], 
                        df['_limit_op_technology'].loc[t],
                        df['_limit_op_sign'].loc[t],
                        df['_limit_op_factor'].loc[t]])
    #
    # convert it into a dataframe
    df = pd.DataFrame(
        data, 
        columns=['class', 'technology',
                 'limit_op_class', 'limit_op_tech', 'limit_op_sign', 'limit_op_factor'])
    #
    # loop trough the individual limiting technologies
    i = 0
    for t in df.limit_op_tech.unique():
        c = df.limit_op_class[df.limit_op_tech == t][0]
        valids = []
        rhs = []
        #
        # and collect the once refering to them
        for _, row in df[(df.limit_op_tech == t) & \
                         (df.limit_op_class == c)].iterrows():
            # validate the technology option
            valids.append(validate_technology_exists(n, row['limit_op_class'], row['limit_op_tech']))
            factor = row['limit_op_factor']
            col = f'{row['limit_op_class']}-{comps[row['limit_op_class']][0]}'
            #
            i = i + 1
            if i == 1:
                rhs = ((factor * m.variables[col].loc[:, row['limit_op_tech']]))
            #
            else:
                rhs = rhs + ((factor * m.variables[col].loc[:, row['limit_op_tech']]))
        #
        # Get the hourly link flow variables for all technologies across all snapshots
        constr_name = f'Limit-operations-by-capacity-{c}_{t}'
        #
        if all(valids):
            # collect details to limit the sum for each time step
            nom_col = comps[c]
            #
            link_cap = m.variables[f'{c}-{nom_col}'].loc[t]
            #
            # add the constraint
            con = rhs <= link_cap
            con.sign = row.limit_op_sign
            m.add_constraints(con, name=constr_name)
            print (f'added {constr_name}')
        #
        else:
            print (f'info! constraint "{constr_name}" is misconfigured')
    #
    return None

def minimum_load_if_operates(
        n: pypsa.Network,
        bigM: float = 1e8,
    ) -> None:
    """
    Adds constraints to define a min_pu level if in operation. Currently
    necessary for expandable technology options. The following column in the 
    individual DataFrames needs to be defined:
        _min_pu_if_in_op

    Parameters
    ----------
    n: pypsa.Network
        PyPSA network to get the details from.

    bigM: float = 1e8
        Big M variable.

    Returns
    -------
    None
    """
    #
    m = n.model
    # defines the variables and constraints only for the required technology options
    comps = pypsa.descriptors.nominal_attrs
    col = '_min_pu_if_in_op'
    #
    # loop through all components
    for c in comps:
        df = n.c[c].static
        if (len(df) > 0) and \
           (col in df.columns):
            for t in df.query(f'{col} > 0.0').index:
                # create the binary status variable
                var_name = f'{c}-{t}-hourly-opstatus'
                if var_name not in m.variables:
                    status = m.add_variables (
                        name=var_name, 
                        binary=True,
                        coords=[n.snapshots])
                #
                else:
                    status = m.variables[var_name]
                #
                # get the variables
                op_col = comps[c][0]
                attr = comps[c]
                dispatch = m.variables[f'{c}-{op_col}'].loc[:, t]
                capacity = m.variables[f'{c}-{attr}'].loc[t]
                min_pu = df[col].loc[t]
                #
                # add the constraints
                constr_name = f'{c}-{t}-min_load_if_in_operation'
                if constr_name not in m.constraints:
                    m.add_constraints(
                        dispatch >= min_pu * capacity - bigM * (1 - status),
                        name=constr_name)
                    print (f'added {constr_name}')
                #
                constr_name = f'{c}-{t}-is_online_if_in_operation'
                if constr_name not in m.constraints:
                    m.add_constraints(
                        dispatch <= bigM * status,
                        name=constr_name)
                    print (f'added {constr_name}')
                #
                # adjust the objective function
                m.objective -= 1.0 * status.sum()
    #
    return None

def invest_if_installed(
        n: pypsa.Network,
        bigM: float = 1e8,
    ) -> None:
    """
    Adds constraints to add a specific cost if a specific technology options is
    being installed. The following column in the individual DataFrames needs 
    to be defined:
        _capital_cost_if_inst

    Parameters
    ----------
    n: pypsa.Network
        PyPSA network to get the details from.

    bigM: float = 1e8
        Big M variable.

    Returns
    -------
    None
    """
    #
    m = n.model
    comps = pypsa.descriptors.nominal_attrs
    col = '_capital_cost_if_inst'
    #
    # loop through all components
    for c in comps:
        df = n.c[c].static
        if (len(df) > 0) and \
           (col in df.columns):
            for t in df.query(f'{col} > 0.0').index.get_level_values('name').unique():
                # create the binary status variable
                var_name = f'{c}-{t}-installed'
                if var_name not in m.variables:
                    idx = df[df.index.get_level_values('name') == t].index.get_level_values('name').unique()
                    is_installed = m.add_variables(
                        name=var_name, 
                        binary=True, 
                        # coords=[df.query(f'name == "{t}"').index])
                        coords=[idx])
                #
                else:
                    is_installed = m.variables[var_name]
                #
                # get the variables
                nom_col = comps[c]
                link_cap = m.variables[f'{c}-{nom_col}'].loc[t]
                #
                if n.has_scenarios:
                    invest = df[df.index.get_level_values('name') == t][col]
                else:
                    invest = df[col][t]
                #
                # add the constraints
                constr_name = f'{c}-{t}-is_not_installed'
                if constr_name not in m.constraints:
                    m.add_constraints(
                        link_cap <= bigM * is_installed,
                        name=constr_name)
                    print (f'added {constr_name}')
                #
                # adjust the objective function
                m.objective += invest * is_installed.sum()
    #
    return None

def min_capacity_if_installed(
        n: pypsa.Network,
        bigM: float = 1e8,
    ) -> None:
    """
    Adds constraints to limit the expansion of technology options to a minimum
    if it is installed. The following column in the individual DataFrames needs 
    to be defined:
        _nom_min_if_inst

    Parameters
    ----------
    n: pypsa.Network
        PyPSA network to get the details from.

    bigM: float = 1e8
        Big M variable.

    Returns
    -------
    None
    """
    #
    m = n.model
    col = '_nom_min_if_inst'
    comps = pypsa.descriptors.nominal_attrs
    #
    # loop through all components
    for c in comps:
        df = n.c[c].static
        if (len(df) > 0) and \
           (col in df.columns):
            for t in df.query(f'{col} > 0.0').index.get_level_values('name').unique():
                # create the binary status variable
                var_name = f'{c}-{t}-installed'
                if var_name not in m.variables:
                    idx = df[df.index.get_level_values('name') == t].index.get_level_values('name').unique()
                    is_installed = m.add_variables(
                        name=var_name, binary=True, 
                        # coords=[df.query(f'name == "{t}"').index])
                        coords=[idx])
                #
                else:
                    is_installed = m.variables[var_name]
                #
                # get the variables
                attr = comps[c]
                link_cap = m.variables[f'{c}-{attr}'].loc[t]
                min_cap = df[df.index.get_level_values('name') == t][col].unique()
                #
                # add the constraints
                constr_name = f'{c}-min_capacity_if_installed-{t}'
                if constr_name not in m.constraints:
                    m.add_constraints(
                        link_cap >= min_cap * is_installed,
                        name=constr_name)
                    print (f'added {constr_name}')
                #
                constr_name = f'{c}-is_not_installed-{t}'
                if constr_name not in m.constraints:
                    m.add_constraints(
                        link_cap <= bigM * is_installed,
                        name=constr_name)
                    print (f'added {constr_name}')
    #
    return None

def background_marginal_cost(
        n: pypsa.Network,
    ) -> None:
    """
    Adds constraints to consider marignal cost in the objective function 
    without having real marginal costs to be considered. This can be used to
    minimize charging and discharging of power at the same time to destroy
    energy instead of curtailing it. The following column in the individual 
    DataFrames needs to be defined:
        _bg_marginal_cost

    Parameters
    ----------
    n: pypsa.Network
        PyPSA network to get the details from.

    Returns
    -------
    None
    """
    #
    m = n.model
    comps = pypsa.descriptors.nominal_attrs
    col = '_bg_marginal_cost'
    #
    # loop through all components
    for c in comps:
        df = n.c[c].static
        if (len(df) > 0) and \
           (col in df.columns):
            for t in df.query(f'{col} > 0.0').index.get_level_values('name').unique():
                # get the operational variable
                if c == 'StorageUnit':
                    op_col = f'{comps[c][0]}_store'
                #
                else:
                    op_col = comps[c][0]
                #
                if n.has_scenarios:
                    tech_flow = m.variables[f'{c}-{op_col}'].loc[:, t]
                    # adjust the objective function
                    m.objective += tech_flow.sum() * df[df.index.get_level_values('name') == t][col]
                else:
                    tech_flow = m.variables[f'{c}-{op_col}'].loc[:, t]
                    # adjust the objective function
                    m.objective += tech_flow.sum() * df[col][t]
                #
                print (f'added {c}.{t}.{col}')
    #
    return None

def shared_technology_potential(
        n: pypsa.Network,
    ) -> None:
    """
    Limit the capacity additions for a group of technologies.
    Useful if e.g. several WT's are allowed to be built but constraint by space.
    The following column in the individual DataFrames needs to be defined:
        _shared_potential

    Parameters
    ----------
    n: pypsa.Network
        PyPSA network to get the details from.

    Returns
    -------
    None
    """
    #
    m = n.model
    comps = pypsa.descriptors.nominal_attrs
    col = '_shared_potential'
    df_list = []
    #
    # loop through all components
    for c in comps:
        df = n.c[c].static
        if (not df.empty) and \
           (col in df.columns):
            attr = comps[c]
            df[col] = df[col].astype(str)
            df_copy = df[(df[col] > '') & (df[col] != 'nan')].copy()
            df_copy['technology_type'] = c
            df_list.append(df_copy[['technology_type', col, attr+'_max']])
    #
    df_concat = pd.concat(df_list, sort=False)
    #
    # get unique shared potential strings
    for t in df_concat[col].unique():
        # get all technologies of the current shared potential string
        con = 0
        #
        for c in df_concat[df_concat[col] == t].iterrows():
            # combine them into a constraint
            attr = comps[c[1].technology_type]
            index = c[0]
            link_cap = m.variables[f'{c[1].technology_type}-{attr}'].loc[index]
            max_cap = df_concat[attr+'_max'][index]
            #
            if max_cap > 0:
                # link1_cap = m.variables[f'{class1}-{nom_col1}'].loc[tech1]
                con = con + link_cap / max_cap
            #
            else:
                print (f'warning! technology {index} needs to define its {attr}')
        #
        if type(con) == linopy.expressions.LinearExpression:
            constr_name = f'Shared-potential-{t}'
            m.add_constraints(
                con <= 1,
                name=constr_name)
            print (f'added {constr_name}')
        else:
            print (f'info! nothing to do: {con}, {t}')
    #
    return None

def force_technology_capacity(
        n: pypsa.Network,
    ) -> None:
    """
    Force the capacity additions of a group of components to be <=, =, or >= of 
    a given constraint.
    The following column in the individual DataFrames needs to be defined:
        _limit_cap_string
        _limit_cap_sign
        _limit_cap

    Parameters
    ----------
    n: pypsa.Network
        PyPSA network to get the details from.

    Returns
    -------
    None
    """
    #
    m = n.model
    comps = pypsa.descriptors.nominal_attrs
    col = '_limit_cap_string'
    #
    # loop through all components
    for c in comps:
        df = n.c[c].static
        #
        if col in df.columns:
            if df[df[col].fillna('') > ''][col].unique().any():
                for tech in df[df[col] > ''][col].unique():
                    constr_name = f'Force-capacity-{tech}'
                    con = 0
                    cap = 0
                    #
                    # loop through all technology options of the indiviual class
                    for t, row in df[df[col] == tech].iterrows():
                        sign = row['_limit_cap_sign']
                        cap = row['_limit_cap']
                        #
                        # get capacity variable
                        nom_col = comps[c]
                        #
                        link_cap = m.variables[f'{c}-{nom_col}'].loc[t]
                        #
                        con += link_cap
                    #
                    con2 = con <= cap
                    con2.sign = sign
                    #
                    # add the constraints
                    m.add_constraints(con2, name=constr_name)
                    print (f'added {constr_name}')
    #
    return None

def mga_settings(
        n: pypsa.Network, 
    ) -> None:
    """
    Adds a constrain for the MGA approach.

    Parameters
    ----------
    n: pypsa.Network
        PyPSA network to get the details from.

    Returns
    -------
    None
    """
    #
    m = n.model
    if not n.is_solved:
        msg = 'Network needs to be solved with "n.optimize()" before adding near-optimal constraint.'
        raise ValueError(msg)
    #
    # optimal_cost and fixed_cost is taken from the first run
    optimal_cost = globals()['optimal_cost']
    fixed_cost = globals()['fixed_cost']
    #
    objective = m.objective
    if not isinstance(objective, (linopy.LinearExpression | linopy.QuadraticExpression)):
        objective = objective.expression
    #
    if 'mga_slack' in globals():
        slack = globals()['mga_slack']
    #
    else:
        slack = 0.01
    #
    constr_name = 'MGA-budget-constraint'
    m.add_constraints(
        objective + fixed_cost >= (1 + slack) * optimal_cost,
        name=constr_name)
    print (f'added {constr_name}')
    #
    return None

def strict_unsimultaneous_charging_discharging(
        n: pypsa.Network, 
        bigM: float = 1e8,
    ) -> None:
    """
    Adds constraints for a strict enforced unsimultaneous charging and discharging.
    The following column in the individual DataFrames needs to be defined:
        _strict_binary_op

    Parameters
    ----------
    n: pypsa.Network
        PyPSA network to get the details from.

    bigM: float = 1e8
        Big M variable.

    Returns
    -------
    None
    """
    #
    m = n.model
    comps = pypsa.descriptors.nominal_attrs
    col = '_strict_binary_op'
    df_list = []
    #
    # loop through all components
    for c in ['Link']: # comps:
        df = n.c[c].static
        #
        if (not df.empty) and \
           (col in df.columns):
            df[col] = df[col].astype(str)
            df_copy = df[(df[col] > '') & (df[col] != 'nan')].copy()
            df_copy['technology_type'] = c
            df_list.append(df_copy[['technology_type', col]])
    #
    df_concat = pd.concat(df_list, sort=False)
    #
    # get unique shared potential strings
    for idx in df_concat.groupby(col, axis=0).count().iterrows():
        # valid configurations have two entries
        if idx[1][0] != 2:
            print (f'warning! configuration of strict binary operation {idx[0]} is not correct')
            pass
        #
        else:
            # get all technologies of the current shared potential string
            c = []
            t = []
            #
            for i, row in df_concat[df_concat[col] == idx[0]].iterrows():
                # there are alwaqys tow (2) lines
                #
                c.append(row.technology_type)
                t.append(i)
            #
            # combine them into a constraint
            str_idx = idx[0]
            c1 = c[0]
            c2 = c[1]
            t1 = t[0]
            t2 = t[1]
            #
            attr1 = comps[c1]
            attr2 = comps[c2]
            #
            charge_p = m.variables[f'{c1}-{attr1[0]}'].loc[:, f'{t1}']
            discharge_p = m.variables[f'{c2}-{attr2[0]}'].loc[:, f'{t2}']
            #
            var_name = f'{str_idx}-operation1_bin'
            if var_name not in m.variables:
                bin_var = m.add_variables(
                    binary = True, 
                    dims = ['snapshot'], 
                    coords = {'snapshot': n.snapshots}, 
                    name = var_name)
            #
            else:
                bin_var = m.variables[var_name]
            #
            var_name = f'{str_idx}-operation2_bin'
            if var_name not in m.variables:
                one = m.add_variables(
                    lower = 1.0, 
                    upper = 1.0, 
                    dims = ['snapshot'], 
                    coords = {'snapshot': n.snapshots}, 
                    name = var_name)
            #
            else:
                one = m.variables[var_name]
            #
            constr_name = f'{str_idx}-strict-on-charge'
            m.add_constraints(
                charge_p <= bigM * bin_var, 
                name = constr_name)
            print (f'added {constr_name}')
            constr_name = f'{str_idx}-strict-on-discharge'
            m.add_constraints(
                discharge_p <= bigM * (one - bin_var), 
                name = constr_name)
            print (f'added {constr_name}')
    #
    return None

def remove_KVL_constraints(
        n: pypsa.Network, 
    ) -> None:
    """
    Remmoves the Kirchhoff-Voltage-Law constrains. As a result does ignore AC 
    and DC settings.

    Parameters
    ----------
    n: pypsa.Network
        PyPSA network to get the details from.

    Returns
    -------
    None
    """
    #
    constr = 'Kirchhoff-Voltage-Law'
    if constr in n.model.constraints:
        n.model.remove_constraints(constr)
    #
    return None

def extra_functionalities(
        n: pypsa.Network, 
        snapshots: pd.core.indexes.datetimes.DatetimeIndex
    ) -> None:
    """
    Adds extra functionalities into the PyPSA model above the features provided
    out of the shelf.

    Parameters
    ----------
    n: pypsa.Network
        PyPSA network to get the details from.

    snapshots: pd.core.indexes.datetimes.DatetimeIndex
        Snapshots being considered in the current PyPSA model.

    Returns
    -------
    None
    """
    #
    # LP functionalities
    print ('add LP constraints if/as needed ...')
    #
    background_marginal_cost(n)
    #
    if eval(globals()['do_operational_constraints']):
        link_operation(n)
        limit_hourly_operation_by_capacity(n)
    #
    if eval(globals()['do_investment_constraints']):
        link_capacities(n)
        shared_technology_potential(n)
        force_technology_capacity(n)
    #
    # MILP functionalities
    if eval(globals()['do_milp_constraints']):
        print ('add MILP constraints if/as needed ...\n')
        #
        if eval(globals()['do_strict_unsimultaneous_dis+charging']):
            strict_unsimultaneous_charging_discharging(n)
        #
        if eval(globals()['do_operational_constraints']):
            minimum_load_if_operates(n)
        #
        if eval(globals()['do_investment_constraints']):
            invest_if_installed(n)
            min_capacity_if_installed(n)
    #
    remove_KVL_constraints(n)
    #
    return None

def main(
    ) -> None:
    """
    Main function.

    Parameters
    ----------
    None

    Returns
    -------
    None
    """
    #
    # read the optimization settings and scenario definitions
    opt_params, df_scens, df_stochs = read_all_params(xls_filename)
    #
    if eval(globals()['use_oetc']):
        try:
            print ('initiating OETC ...')
            globals()['solver_name'] = 'gurobi'
            # oetc = solving.get('oetc', None)
            oetc = {}
            oetc['name'] = 'PyPSA-ptx_test' # without a GUI not that relevant 
                                            # at the moment
            oetc['authentication_server_url'] = 'http://34.34.8.15:5050'
            oetc['orchestrator_server_url'] = 'http://34.34.8.15:5000'
            oetc['cpu_cores'] = 4 # adjust to your needs, keep in mind that RAM 
                                  # allocation is this value times 8
            oetc['disk_space_gb'] = 20 # adjust to your needs
            oetc['delete_worker_on_error'] = False # makes debugging easier if 
                                                   # an error occurs during solving
            oetc['credentials'] = OetcCredentials(
                email=os.environ['OETC_EMAIL'], password=os.environ['OETC_PASSWORD'])
            oetc['solver'] = globals()['solver_name']
            oetc['solver_options'] = get_solver_setting()
            oetc_settings = OetcSettings(**oetc)
            oetc_handler = OetcHandler(oetc_settings)
            kwargs['remote'] = oetc_handler
        #
        except Exception as e: 
            print (f'info! not able to initiate OETC\n{e}')
            globals()['solver_name'] = 'highs'
            globals()['use_oetc'] = 'False'
    #
    else:
        globals()['solver_name'] = 'highs'
    #
    kwargs['solver_options'] = get_solver_setting()
    print (f'OETC usage: {globals()['use_oetc']}')
    #
    if eval(globals()['use_oetc']):
        print (f'OET solver to use: {globals()['solver_name']}\n')
    #
    else:
        print (f'Local solver to use: {globals()['solver_name']}\n')
    #
    # get the PyPSA list of possible components
    sheets_list, sheets_ts_list = get_pypsa_component_lists()
    #
    # craete and/or get the base network
    read_excel_data(
        xls_filename, 
        globals()['target_folder'], 
        globals()['csv_subfolder'], 
        globals()['temp_file'], 
        sheets_list, 
        sheets_ts_list)
    #
    # validate if all scenario adjustments are in general ok
    all_adjustments_ok = validate_scenario_adjustments(
        f'{globals()['target_folder']}/{globals()['temp_file']}', 
        df_scens)
    #
    # get the active scenarios only
    unique_scens = df_scens.sort_values('scenario').scenario.unique()
    print (f'{len(unique_scens)} active scenario(s) (ordered by name):')
    print (f'{list(unique_scens)}\n')
    print (f'{len(df_scens)} active lines for scenario adjustments:\n', df_scens)
    #
    if eval(globals()['run_stochastic_runs']):
        print (f'{len(df_stochs)} active lines for stochastic adjustments:\n', df_stochs)
    #
    list_results = []
    list_supplies = []
    list_balances = []
    list_curtailments = []
    #
    # import the just created PyPSA network details and check for consistency
    print ('\nload the temporary network model ...')
    n, inv_periods = read_and_update_network(
        f'{globals()['target_folder']}/{globals()['temp_file']}',
        pd.core.frame.DataFrame(),
        df_scens.scenario.unique()[0])
    #
    if all_adjustments_ok:
        n, list_results, list_supplies, list_balances, list_curtailments, \
            status, tc = do_all_runs(
                n, 
                df_scens,
                df_stochs,
                kwargs)
        #
        if status != 'ok':
            print ('\nError! check your settings!')
            print (f'optimization ended with status "{status}" and termination code "{tc}"')
        #
        if len(list_results) > 1:
            df = show_case_comparison(
                list_results,
                list_supplies, 
                list_balances,
                list_curtailments)
            #
            df.to_excel(f'{globals()['target_folder']}/run_comparison.xlsx')
        #
        else:
            df = pd.DataFrame()
    #
    else:
        print ('\nerror! Could not validate all scenario settings')
    #
    return n, list_results, list_supplies, list_balances, list_curtailments, df
    
# MAIN ------------------------------------------------------------------------

if __name__ == '__main__':
    n, list_results, list_supplies, list_balances, list_curtailments, df = main()
