# -*- coding: utf-8 -*-
# SPDX-FileCopyrightText: PyPSA-X Contributors
# SPDX-License-Identifier: MIT

"""

 PyPSA PtX / µgrid Optimizer

  pppppp   yy       yy   pppppp    ssssss    aaaaaa        xx    xx
 pp    pp   yy     yy   pp    pp  ss              aa        xx  xx
 pp    pp    yy   yy    pp    pp   ssssss   aaaaaaaa  ---    xxxx
 pp    pp     yy yy     pp    pp        ss  aa    aa        xx  xx
 ppppppp       yyy      ppppppp    ssssss    aaaaaa        xx    xx
 pp            yy       pp
 pp           yy        pp
 pp          yy         pp

---

MIT License

Copyright (c) 2025-2026, PyPSA-X Contributors

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
associated documentation files (the "Software"), to deal in the Software without restriction,
including without limitation the rights to use, copy, modify, merge, publish, distribute,
sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or
substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT
OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

#
from importlib.util import find_spec
import argparse

#
if find_spec("pypsa"):
    import pandas as pd
    import numpy as np
    import xarray as xr
    import networkx as nx
    import pypsa
    import sys
    import os
    from datetime import datetime as dt
#
else:
    print("error! PyPSA is not installed\n")
    sys.exit(-1)
#
default_case_dir = "cases/default"
__version__ = "0.9.3"
#
headers = [
    "",
    "",
    " pppppp   yy       yy   pppppp    ssssss    aaaaaa        xx    xx",
    "pp    pp   yy     yy   pp    pp  ss              aa        xx  xx ",
    "pp    pp    yy   yy    pp    pp   ssssss   aaaaaaaa  ---    xxxx  ",
    "pp    pp     yy yy     pp    pp        ss  aa    aa        xx  xx ",
    "ppppppp       yyy      ppppppp    ssssss    aaaaaa        xx    xx",
    "pp            yy       pp ",
    "pp           yy        pp ",
    "pp          yy         pp ",
    "",
    "",
    f"PtX / µgrid Optimizer v{__version__}",
    f"(c) 2025-{dt.now().year} by PyPSA-X Contributors",
    "",
    "+" + "-" * 80 + "+",
    "| MIT License " + " " * 67 + "|",
    "| " + "=" * 11 + " " * 68 + "|",
    "| Permission is hereby granted, free of charge, to any person obtaining a copy   |",
    '| of this software and associated documentation files (the "Software"), to deal  |',
    "| in the Software without restriction, including without limitation the rights   |",
    "| to use,copy, modify, merge, publish, distribute, sublicense, and/or sell       |",
    "| copies of the Software, and to permit persons to whom the Software is          |",
    "| furnished to do so." + " " * 60 + "|",
    "+" + "-" * 80 + "+",
    "",
]
#
deactivate_network_viewers = True
#
if find_spec("pypsa"):
    from packaging import version
    import importlib
    import sys
    import uuid
    import copy
    import linopy
    from linopy.remote.oetc import (
        ComputeProvider,
        OetcCredentials,
        OetcHandler,
        OetcSettings,
    )
    import tempfile
    import warnings
    import logging
    import ast
    import json
    import re
    import datetime as _dt
    import jsonschema
    import yaml

    #
    PYPSA_VERSION = importlib.metadata.version("pypsa")
    PYPSA_VERSION_NEEDED = "1.1.0"  # for tsam feature
    LINOPY_VERSION = importlib.metadata.version("linopy")
    HIGHS_VERSION = importlib.metadata.version("highspy")
    #
    # make sure to use the new API
    pypsa.options.set_option("api.new_components_api", True)
    pypsa.options.set_option("api.legacy_string_dtype", False)
    pypsa.options.set_option("params.statistics.nice_names", False)
    pypsa.options.set_option("params.statistics.drop_zero", True)
    pypsa.options.set_option("params.statistics.round", 2)
    pypsa.options.set_option("debug.runtime_verification", False)
    # linopy.options["semantics"] = "v1"
    #
    if version.parse(PYPSA_VERSION) < version.parse(PYPSA_VERSION_NEEDED):
        print(
            f"\nerror! installed PyPSA version ({PYPSA_VERSION}) not supported!"
            f" need at least v{PYPSA_VERSION_NEEDED}"
        )
        sys.exit(-1)
    #
    # adjust some pandas settings
    pd.options.display.float_format = "{:,.2f}".format
    pd.options.display.max_rows = 100
    pd.options.display.max_columns = 100
    pd.options.display.width = 300
    #
    # especially useful for Windows users
    tempfile.tempdir = os.getcwd()
    #
    # suppress Pythons warning messaging
    warnings.filterwarnings("ignore")
    #
    # suppress PyPSA's logging messages
    logging.basicConfig(level=logging.ERROR)
    #
    print("imported all necessary libraries")
    print(f"PyPSA: v{PYPSA_VERSION}")
    print(f"linopy: v{LINOPY_VERSION}")
    print(f"HiGHS: v{HIGHS_VERSION}")
#
else:
    print("error! PyPSA is not installed\n")
    sys.exit(-1)

# optional packages are related to optional HTML network viewer and SVG result
# graph
if find_spec("pypsa_network_viewer"):
    from pypsa_network_viewer import html_viewer

    network_viewer = True
#
else:
    network_viewer = False
#
if find_spec("matplotlib.pyplot"):
    import matplotlib.pyplot as plt

    matplotlib_pyplot = True
#
else:
    matplotlib_pyplot = False
#
if find_spec("networkx"):
    import networkx as nx

    networkx_viewer = True
#
else:
    networkx_viewer = False
#
if find_spec("tsam"):
    tsam_avail = True
#
else:
    tsam_avail = False
#
if deactivate_network_viewers:
    network_viewer = False
    networkx_viewer = False


def parse_arguments() -> tuple[str, str]:
    """
    Print the header and read the input (case folder or Excel assumption book) from the
    command line. Only called when the script is executed, so that other tools can import
    this file to reuse the case folder functions. With --xls-to-case, an Excel assumption
    book is converted into a case folder and the script exits.

    Returns
    -------
    input_source : str
        Case folder or Excel file to be considered.

    input_type : str
        Either 'case' or 'excel'.
    """
    #
    for header in headers:
        print(header)
    #
    # check provided arguments on the command line
    parser = argparse.ArgumentParser()
    input_group = parser.add_mutually_exclusive_group()
    input_group.add_argument(
        "--folder",
        type=str,
        help="Case folder (YAML + CSV) to read configuration and settings from "
        f"(default: '{default_case_dir}'); create one from an Excel book with "
        "'--xls-to-case <book.xlsx> <folder>'.",
    )
    input_group.add_argument(
        "--xls",
        type=str,
        help="Excel assumption book to read configuration and settings from "
        "(legacy input, alternative to --folder).",
    )
    input_group.add_argument(
        "--xls-to-case",
        nargs=2,
        metavar=("XLSX", "CASE_DIR"),
        help="Convert an Excel assumption book into a case folder (case.yaml + CSV) and exit.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="With --xls-to-case: overwrite an existing case folder.",
    )
    args = parser.parse_args()
    #
    if args.xls_to_case:
        xlsx, case_dir = args.xls_to_case
        if not os.path.isfile(xlsx):
            print(f'error! provided input file does not exist: "{xlsx}"\n')
            sys.exit(-1)
        convert_xlsx_to_case(xlsx, case_dir, args.force)
        sys.exit(0)
    #
    # input_source is either a case folder (input_type 'case') or an Excel
    # assumption book (input_type 'excel')
    if args.xls:
        input_type = "excel"
        input_source = args.xls
        #
        if not os.path.isfile(input_source):
            print(f'error! provided input file does not exist: "{input_source}"\n')
            sys.exit(-1)
    #
    else:
        input_type = "case"
        input_source = args.folder if args.folder else default_case_dir
        #
        if not os.path.isfile(os.path.join(input_source, "case.yaml")):
            print(
                "error! provided case folder does not exist or has no case.yaml: "
                f'"{input_source}"\n'
            )
            sys.exit(-1)
    #
    return input_source, input_type


# SUPPORT FUNCTIONS -----------------------------------------------------------


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
        raise TypeError("getsize() does not take argument of type: " + str(type(obj)))
    #
    seen_ids = set()
    size = 0
    objects = [obj]
    #
    while objects:
        need_referents = []
        #
        for obj in objects:
            if (not isinstance(obj, BLACKLIST)) and (id(obj) not in seen_ids):
                seen_ids.add(id(obj))
                size += sys.getsizeof(obj)
                need_referents.append(obj)
        #
        objects = get_referents(*need_referents)
    #
    return size


# source:
# https://stackoverflow.com/questions/2257441/random-string-generation-with-upper-case-letters-and-digits/23728630#23728630
#
def random_string(string_length=10):
    """Returns a random string of length string_length."""
    import uuid

    random = str(uuid.uuid4())  # Convert UUID format to a Python string.
    random = random.upper()  # Make all characters uppercase.
    random = random.replace("-", "")  # Remove the UUID '-'.
    return random[0:string_length]  # Return the random string.


# CASE FOLDER INPUT (YAML + CSV) ---------------------------------------------
#
# Case-folder (YAML + CSV) input handling for PyPSA-X.
#
# A case folder replaces the Excel assumption book (e.g. PyPSA_PtX_AB_default.xlsx):
#
#     <case>/
#     ├── case.yaml            all settings in one file, sections:
#     │                          settings     optimization settings          (former sheet: opt_params)
#     │                          assumptions  economic / project assumptions (former sheet: AB, upper block)
#     │                          scenarios    scenario changes               (former sheet: scen_params)
#     │                          stochastic   stochastic case changes        (former sheet: stoch_params)
#     │                          timeseries   profiles -> PyPSA time-varying attributes
#     ├── technologies.csv     technology parameters          (former sheet: AB, lower block)
#     ├── network/*.csv        topology + per-asset overrides (former sheets: carriers, buses, generators, ...)
#     └── timeseries/
#         └── profiles.csv     raw hourly profiles            (former sheet: raw_data)
#
# `build_tables()` turns a case folder into plain PyPSA tables (what the Excel formulas did) and
#

# ---------------------------------------------------------------------------------------------
# technology parameter -> component attribute mapping
#
# For every component table, the attribute is looked up as (row._component, parameter) in
# technologies.csv (case-insensitive, like Excel's XLOOKUP). If not found, the default is used.
# A non-empty cell in network/<table>.csv always wins over the lookup (per-asset override).
# ---------------------------------------------------------------------------------------------

_UC = [  # unit commitment / ramping attributes: (attribute, default)
    ("start_up_cost", 0),
    ("shut_down_cost", 0),
    ("stand_by_cost", 0),
    ("min_up_time", 0),
    ("min_down_time", 0),
    ("up_time_before", 0),
    ("down_time_before", 0),
    ("ramp_limit_up", 1),
    ("ramp_limit_down", 1),
    ("ramp_limit_start_up", 0),
    ("ramp_limit_shut_down", 0),
]

_COST = {
    "capital_cost": ("capital_cost", 0),
    "_capital_cost_if_inst": ("capital_cost_if_inst", 0),
    "marginal_cost": ("vom", 0),
    "_bg_marginal_cost": ("bg_vom", 0),
    "_retirement_gain": ("retirement_gain", 0),
}


def _nom(prefix: str) -> dict:
    return {
        f"{prefix}_nom_extendable": ("extendable", "True"),
        f"{prefix}_nom": ("nom", 0),
        f"{prefix}_nom_min": ("nom_min", 0),
        "_nom_min_if_inst": ("nom_min_if_inst", 0),
        f"{prefix}_nom_max": ("nom_max", 0),
        f"{prefix}_nom_mod": ("module_size", 0),
    }


PARAM_MAP = {
    "generators": {
        **_nom("p"),
        "committable": ("committable", "False"),
        "p_min_pu": ("min_pu", 0),
        "p_max_pu": ("max_pu", 1),
        "_min_pu_if_in_op": ("min_pu_if_in_op", 0),
        **_COST,
        "sign": ("sign", 1),
        "e_sum_max": ("e_sum_max", "inf"),
        "e_sum_min": ("e_sum_min", "-inf"),
        "_max_op_hours": ("max_op_hours", "inf"),
        **{a: (a, d) for a, d in _UC},
    },
    "links": {
        **_nom("p"),
        "committable": ("committable", "False"),
        "p_min_pu": ("min_pu", 0),
        "p_max_pu": ("max_pu", 1),
        "_min_pu_if_in_op": ("min_pu_if_in_op", 0),
        "efficiency": ("efficiency1", 0),
        "efficiency2": ("efficiency2", 0),
        "efficiency3": ("efficiency3", 0),
        "efficiency4": ("efficiency4", 0),
        "efficiency5": ("efficiency5", 0),
        **_COST,
        **{a: (a, d) for a, d in _UC},
    },
    "stores": {
        **_nom("e"),
        "e_cyclic": ("cyclic_SoC", "True"),
        "e_min_pu": ("min_pu", 0),
        "e_max_pu": ("max_pu", 1),
        **_COST,
        "standing_loss": ("standing_loss", 0),  # given in %, converted below
    },
    "storage_units": {
        **{k: v for k, v in _nom("p").items() if k != "p_nom_mod"},
        "cyclic_state_of_charge": ("cyclic_SoC", "True"),
        "sign": ("sign", 1),
        **_COST,
        "max_hours": ("max_hours", 1),
        "efficiency_store": ("charging_efficiency", 0),
        "efficiency_dispatch": ("discharging_efficiency", 0),
        "standing_loss": ("standing_loss", 0),  # given in %, converted below
    },
    "lines": {
        "type": ("type", "TBD"),
        **_nom("s"),
        **{
            k: v
            for k, v in _COST.items()
            if k not in ("marginal_cost", "_bg_marginal_cost")
        },
    },
    "transformers": {
        **_nom("s"),
        **{
            k: v
            for k, v in _COST.items()
            if k not in ("marginal_cost", "_bg_marginal_cost")
        },
        "tap_ratio": ("tap_ratio", 1),
    },
}

# attributes given in percent in technologies.csv
PERCENT_ATTRS = {"standing_loss"}
# attributes multiplied with the line length
PER_LENGTH_ATTRS = {"lines": {"capital_cost", "_capital_cost_if_inst"}}
# bus column used to derive carrier / unit
BUS_COLUMN = {
    "generators": "bus",
    "stores": "bus",
    "storage_units": "bus",
    "loads": "bus",
    "links": "bus0",
    "lines": "bus0",
    "transformers": "bus0",
}
UNIT_COLUMN = {
    "generators": "_p_nom_unit",
    "links": "_p_nom_unit",
    "storage_units": "_p_nom_unit",
    "stores": "_e_nom_unit",
    "lines": "_s_nom_unit",
    "transformers": "_s_nom_unit",
}
COMPONENT_TABLES = [
    "carriers",
    "buses",
    "generators",
    "links",
    "stores",
    "storage_units",
    "lines",
    "transformers",
    "shunt_impedances",
    "loads",
]
# helper columns of the Excel book which carry no model information
EXCEL_HELPER_COLUMNS = {"_unique", "_no_references"}

# ---------------------------------------------------------------------------------------------
# case.yaml 'settings' (nested) -> legacy flat setting names used by pypsa-x(2).py
# ---------------------------------------------------------------------------------------------

CONFIG_KEYS = [
    # (path in case.yaml 'settings', legacy global name)
    ("debug.enabled", "debug_mode"),
    ("debug.hours_to_optimize", "hours_to_optimize"),
    ("output.target_folder", "target_folder"),
    ("output.csv_subfolder", "csv_subfolder"),
    ("output.temp_file", "temp_file"),
    ("output.result_file", "result_file"),
    ("compute.use_oetc", "use_oetc"),
    ("segmentation.enabled", "do_segmentation"),
    ("segmentation.function", "segmentation_function"),
    ("segmentation.hours", "segmentation_hours"),
    ("scenarios.run_all", "run_scenarios"),
    ("network.modular_representation", "modular_representation"),
    ("network.transmission_losses", "transmission_losses"),
    ("network.remove_kvl_constraints", "remove_kvl_constraints"),
    ("constraints.investment", "do_investment_constraints"),
    ("constraints.operational", "do_operational_constraints"),
    ("constraints.milp", "do_milp_constraints"),
    (
        "constraints.strict_unsimultaneous_dis_charging",
        "do_strict_unsimultaneous_dis+charging",
    ),
    ("constraints.maintenance.enabled", "do_maintenance_planing"),
    ("constraints.maintenance.mode", "maintenance_mode"),
    ("duals.assign_all", "assign_all_duals"),
    ("small_limit", "small_limit"),
    ("economics.discount_factor", "discount_factor"),
    ("economics.years_of_construction", "years_of_construction"),
    ("economics.project_cod", "project_cod"),
    ("economics.years_of_operation", "years_of_operation"),
    ("solver.mipgap", "mipgap"),
    ("solver.timelimit_hours", "timelimit_in_hours"),
    ("solver.log_to_console", "log_to_console"),
    ("solver.output_flag", "output_flag"),
    ("stochastic.enabled", "run_stochastic_runs"),
    ("stochastic.alpha", "stoch_alpha"),
    ("stochastic.omega", "stoch_omega"),
    ("stochastic.cases", "stoch_case_definition"),
    ("mga.enabled", "run_mga_runs"),
    ("mga.slack", "mga_slack"),
    ("mga.slacks", "mga_slacks"),
    ("rolling_horizon.enabled", "run_rollinghorizon_after_optimization"),
    ("rolling_horizon.horizon", "rollinghorizon_horizon"),
    ("rolling_horizon.overlap", "rollinghorizon_overlap"),
    ("security_constrained.enabled", "run_security_constrained_optimization"),
    ("investment.periods", "investment_periods"),
    ("investment.weighting", "investment_weighting"),
    ("investment.primary_optimization", "primary_optimization"),
    ("investment.objective_function", "objective_function"),
    ("nminus1.enabled", "nminus1_activate"),
    ("cfe.matching", "cfe_matching"),
    ("cfe.type", "cfe_type"),
    ("cfe.renewables", "cfe_rens"),
    ("reserve_margin.enabled", "rm_activate"),
    ("reserve_margin.factors.a", "rm_factor_a"),
    ("reserve_margin.factors.b", "rm_factor_b"),
    ("reserve_margin.factors.c", "rm_factor_c"),
    ("reserve_margin.factors.d", "rm_factor_d"),
    ("reserve_margin.factors.e", "rm_factor_e"),
    ("reserve_margin.max_generator", "rm_max_generator"),
    ("reserve_margin.load.class", "rm_load_class"),
    ("reserve_margin.load.name", "rm_load_name"),
]
# legacy settings that stay real Python lists (all other lists/dicts are passed as strings,
# because pypsa-x.py eval()s them)
NATIVE_LIST_KEYS = {"cfe_rens"}


def _legacy(value, key: str):
    """Convert a native YAML value to the representation the legacy code expects."""
    if isinstance(value, bool):
        return "True" if value else "False"
    if isinstance(value, (list, dict)) and key not in NATIVE_LIST_KEYS:
        return repr(value)
    return value


def _get_path(d: dict, path: str):
    for part in path.split("."):
        if not isinstance(d, dict) or part not in d:
            return None
        d = d[part]
    return d


def _read_yaml(path: str) -> dict:
    if not os.path.isfile(path):
        return {}
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


CASE_FILE = "case.yaml"
CASE_SECTIONS = ("settings", "assumptions", "scenarios", "stochastic", "timeseries")


SCHEMA_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "case.schema.json"
)
_validated = {}  # path -> mtime of the last successful validation


def validate_case(case_dir: str) -> dict:
    """
    Load <case_dir>/case.yaml and validate it against case.schema.json.
    Raises ValueError listing every problem (with its location in the file).
    """
    path = os.path.join(case_dir, CASE_FILE)
    if not os.path.isfile(path):
        raise FileNotFoundError(f"missing {path}")
    data = _read_yaml(path)
    mtime = os.path.getmtime(path)
    if _validated.get(path) == mtime:
        return data
    with open(SCHEMA_FILE, encoding="utf-8") as f:
        schema = json.load(f)
    validator = jsonschema.Draft202012Validator(schema)
    errors = sorted(validator.iter_errors(data), key=lambda e: list(e.absolute_path))
    if errors:
        lines = [f"{path} is not valid ({len(errors)} problem(s)):"]
        for e in errors:
            where = ".".join(str(p) for p in e.absolute_path) or "<top level>"
            lines.append(f"  - {where}: {e.message}")
        raise ValueError("\n".join(lines))
    _validated[path] = mtime
    return data


def read_case_section(case_dir: str, section: str) -> dict:
    """Return one top-level section of the validated <case_dir>/case.yaml (empty dict if missing)."""
    if section not in CASE_SECTIONS:
        raise KeyError(f"unknown case.yaml section '{section}'")
    return validate_case(case_dir).get(section) or {}


def _to_number(value):
    """Convert strings that hold numbers into numbers; leave everything else unchanged."""
    if isinstance(value, str):
        s = value.strip()
        if s == "":
            return value
        try:
            f = float(s)
        except ValueError:
            return value
        if s.lower() in ("inf", "-inf", "+inf", "nan"):
            return value  # keep text, PyPSA parses it
        return int(f) if f.is_integer() and "." not in s and "e" not in s.lower() else f
    return value


def _equal(a, b, rtol: float = 1e-9) -> bool:
    """Value equality used to detect overrides (string/bool/number tolerant)."""
    a, b = _to_number(a), _to_number(b)
    a_nan = a is None or (isinstance(a, float) and np.isnan(a)) or a == ""
    b_nan = b is None or (isinstance(b, float) and np.isnan(b)) or b == ""
    if a_nan or b_nan:
        return a_nan and b_nan
    if isinstance(a, (bool, np.bool_)) or isinstance(b, (bool, np.bool_)):
        return str(a).lower() == str(b).lower()
    if isinstance(a, (int, float, np.number)) and isinstance(
        b, (int, float, np.number)
    ):
        return bool(np.isclose(float(a), float(b), rtol=rtol, atol=1e-12))
    return str(a).strip().lower() == str(b).strip().lower()


# ---------------------------------------------------------------------------------------------
# assumptions
# ---------------------------------------------------------------------------------------------


def derive_assumptions(raw: dict) -> dict:
    """Add all values the AB sheet calculated with formulas (WACC, annuity, targets, ...)."""
    a = dict(raw)
    a["equity_share"] = 1 - a["debt_share"]
    a["wacc_nominal_before_tax"] = (
        a["interest_nominal"] * a["debt_share"]
        + a["equity_share"] * a["cost_of_equity_nominal"]
    )
    a["wacc_nominal"] = (
        a["interest_nominal"] * a["debt_share"] * (1 - a["corporate_tax"])
        + a["equity_share"] * a["cost_of_equity_nominal"]
    )
    a["wacc_real"] = (1 + a["wacc_nominal"]) / (1 + a["inflation"]) - 1
    a["annuity_factor"] = (
        a["wacc_real"] if a["financial_term"] == "real" else a["wacc_nominal"]
    )
    a["wacc_to_use"] = a["annuity_factor"]
    a["y_end"] = a["y_cod"] + a["project_lifetime"]
    a["construction_period"] = a["y_cod"] - a["y_ntp"]
    adjust = str(a.get("plant_availability_adjustment", True)).lower() != "false"
    availability = a["plant_availability"] if adjust else 1
    a["production_target_hour"] = a["production_target_year"] / 8760 / availability
    a["production_hourly_adjusted"] = a["production_target_hour"]
    a["CUR"] = a["currency"]
    return a


# ---------------------------------------------------------------------------------------------
# technologies
# ---------------------------------------------------------------------------------------------


def read_technologies(path: str, assumptions: dict) -> pd.DataFrame:
    """
    Read technologies.csv (long format) and resolve `capital_cost = auto`:
        capital_cost = invest_cost * annuity_factor + fom * invest_cost
    """
    df = pd.read_csv(path, dtype=str, keep_default_na=False).astype(object)
    df["value"] = df["value"].map(_to_number)
    df["_key"] = df["component"].str.lower() + "|" + df["parameter"].str.lower()
    #
    lookup = dict(zip(df["_key"], df["value"]))
    for i, row in df.iterrows():
        if (
            str(row["parameter"]).lower() == "capital_cost"
            and str(row["value"]).lower() == "auto"
        ):
            comp = row["component"].lower()
            invest = _to_number(lookup.get(f"{comp}|invest_cost", 0)) or 0
            fom = _to_number(lookup.get(f"{comp}|fom", 0)) or 0
            df.at[i, "value"] = invest * assumptions["annuity_factor"] + fom * invest
    return df


def _tech_lookup(tech: pd.DataFrame):
    values = dict(zip(tech["_key"], tech["value"]))
    units = dict(zip(tech["_key"], tech["unit"]))

    def get(component, parameter, default, what="value"):
        key = f"{str(component).lower()}|{parameter.lower()}"
        src = values if what == "value" else units
        return src[key] if key in src else default

    return get


# ---------------------------------------------------------------------------------------------
# network tables
# ---------------------------------------------------------------------------------------------


def read_network_table(path: str) -> pd.DataFrame:
    """Read a network/*.csv file. Empty cells stay empty strings (= not set)."""
    df = pd.read_csv(path, dtype=str, keep_default_na=False).astype(object)
    return df.apply(lambda col: col.map(_to_number)).astype(object)


def resolve_table(
    table: str,
    df: pd.DataFrame,
    tech: pd.DataFrame,
    a: dict,
    buses: pd.DataFrame,
    carriers: pd.DataFrame,
) -> pd.DataFrame:
    """Fill a topology table with values from technologies.csv and assumptions."""
    df = df.astype(object)
    get = _tech_lookup(tech)
    bus_carrier = dict(zip(buses["name"], buses["carrier"]))
    carrier_unit = dict(zip(carriers["carrier"], carriers["_unit"]))
    bus_unit = {b: carrier_unit.get(c, "") for b, c in bus_carrier.items()}

    def fill(col, values):
        if col not in df.columns:
            df[col] = pd.Series("", index=df.index, dtype=object)
        mask = df[col].map(lambda v: v == "" or v is None)
        df.loc[mask, col] = pd.Series(values, index=df.index)[mask]

    if table in PARAM_MAP:
        comp = (
            df["_component"]
            if "_component" in df.columns
            else pd.Series("", index=df.index)
        )
        for attr, (param, default) in PARAM_MAP[table].items():
            vals = [get(c, param, default) for c in comp]
            if attr in PERCENT_ATTRS:
                vals = [v / 100 if isinstance(v, (int, float)) else v for v in vals]
            if attr in PER_LENGTH_ATTRS.get(table, set()):
                vals = [
                    v * _to_number(length) if isinstance(v, (int, float)) else v
                    for v, length in zip(
                        vals, df.get("length", pd.Series(1, index=df.index))
                    )
                ]
            fill(attr, vals)
        if "_capital_cost_if_inst" in PARAM_MAP[table]:
            fill(
                "_capital_cost_unit",
                [get(c, "capital_cost", "", what="unit") for c in comp],
            )
        fill("build_year", [a["y_cod"]] * len(df))
        fill("lifetime", [a["project_lifetime"]] * len(df))

    if table in BUS_COLUMN:
        bus = df[BUS_COLUMN[table]]
        fill("carrier", [bus_carrier.get(b, "") for b in bus])
        if table in UNIT_COLUMN:
            fill(UNIT_COLUMN[table], [bus_unit.get(b, "") for b in bus])

    if table == "buses":
        fill("unit", [carrier_unit.get(c, "") for c in df["carrier"]])

    if table == "loads":
        units = [bus_unit.get(b, "") for b in df["bus"]]
        fill("total_output_unit", units)
        fill("p_set_unit", units)
        product = str(a["production_product"]).lower()
        active = [str(c).lower() == product for c in df["carrier"]]
        fill("active", ["True" if x else "False" for x in active])
        fill("p_set", [a["production_target_hour"] if x else 0 for x in active])
        fill(
            "_supply_mode",
            [a["production_supply_frequency"] if x else "-" for x in active],
        )

    return df


# ---------------------------------------------------------------------------------------------
# time series
# ---------------------------------------------------------------------------------------------


def supply_mode_profile(
    index: pd.DatetimeIndex, frequency: str, seed: int = 42
) -> pd.Series:
    """Python version of the raw_data 'Supply_Mode' column formula."""
    s = pd.Series(0.0, index=index)
    last_hour = index.hour == 23
    end_of_month = (index.day == index.days_in_month) & last_hour
    frequency = str(frequency).lower()
    if frequency == "constant":
        s[:] = 1.0
    elif frequency == "monthly":
        s[end_of_month] = index[end_of_month].days_in_month * 24
    elif frequency == "quarterly":
        s[end_of_month & index.month.isin([3, 6, 9, 12])] = 2190
    elif frequency == "half-annual":
        s[end_of_month & index.month.isin([6, 12])] = 4380
    elif frequency == "annual":
        s[end_of_month & (index.month == 12)] = 8760
    elif frequency == "weekly":
        end_of_week = last_hour & (index.weekday == 4)  # Friday 23:00
        s[end_of_week] = 8760 / max(int(end_of_week.sum()), 1)
    elif frequency == "random":
        rng = np.random.default_rng(seed)
        s[:] = rng.integers(75, 126, len(index)) / 100
    else:
        raise ValueError(f"unknown production_supply_frequency '{frequency}'")
    return s


def build_timeseries(case_dir: str, a: dict) -> tuple[pd.DataFrame, dict]:
    """Return (snapshots DataFrame, {'<table>-<attr>': DataFrame})."""
    ts_dir = os.path.join(case_dir, "timeseries")
    spec = read_case_section(case_dir, "timeseries")
    profiles = pd.read_csv(
        os.path.join(ts_dir, "profiles.csv"), index_col=0, parse_dates=True
    )
    profiles.index.name = "snapshot"
    if "Supply_Mode" not in profiles.columns:
        profiles["Supply_Mode"] = supply_mode_profile(
            profiles.index,
            a["production_supply_frequency"],
            spec.get("random_seed", 42),
        )
    #
    snapshots = pd.DataFrame(index=profiles.index)
    for col, val in (spec.get("snapshot_weightings") or {}).items():
        snapshots[col] = val
    #
    series = {}
    for sheet, columns in (spec.get("series") or {}).items():
        df = pd.DataFrame(index=profiles.index)
        for name, rule in columns.items():
            scale = rule.get("scale", 1)
            if isinstance(scale, str):
                scale = a[scale]
            if "profile" in rule:
                df[name] = profiles[rule["profile"]] * scale
            elif "constant" in rule:
                df[name] = float(rule["constant"]) * scale
            else:
                raise ValueError(f"{sheet}.{name}: need 'profile' or 'constant'")
        series[sheet] = df
    return snapshots, series


# ---------------------------------------------------------------------------------------------
# case -> PyPSA tables
# ---------------------------------------------------------------------------------------------


def build_tables(case_dir: str) -> dict:
    """Build all PyPSA tables (static + time series) from a case folder."""
    a = derive_assumptions(read_case_section(case_dir, "assumptions"))
    tech = read_technologies(os.path.join(case_dir, "technologies.csv"), a)
    net_dir = os.path.join(case_dir, "network")
    raw = {}
    for table in COMPONENT_TABLES:
        path = os.path.join(net_dir, f"{table}.csv")
        if os.path.isfile(path):
            raw[table] = read_network_table(path)
    carriers = raw.get("carriers", pd.DataFrame(columns=["carrier", "_unit"]))
    buses = resolve_table("buses", raw["buses"], tech, a, raw["buses"], carriers)
    tables = {"carriers": carriers, "buses": buses}
    for table, df in raw.items():
        if table not in tables:
            tables[table] = resolve_table(table, df, tech, a, buses, carriers)
    #
    tables["network"] = pd.DataFrame(
        [
            {
                "name": a["project_name"],
                "remark": " PtX / µgrid Optimizer",
                "pypsa_version": "1.0.0",
                "_annuity_basis": a["financial_term"],
                "_wacc": a["wacc_to_use"],
                "_annuity": a["annuity_factor"],
                "_currency": a["currency"],
                "_base_year": a["y_base"],
                "_ntp_year": a["y_ntp"],
                "_cod_year": a["y_cod"],
                "_plant_availability": a["plant_availability"],
                "_project_location": a["project_location"],
                "_project_country": a["project_country"],
            }
        ]
    )
    snapshots, series = build_timeseries(case_dir, a)
    return {
        "tables": tables,
        "snapshots": snapshots,
        "series": series,
        "assumptions": a,
    }


def write_pypsa_csv_folder(built: dict, folder: str) -> None:
    """Write built tables in the CSV layout that pypsa.Network(folder) imports."""
    os.makedirs(folder, exist_ok=True)
    for f in os.listdir(folder):
        if f.endswith(".csv"):
            os.remove(os.path.join(folder, f))
    for table, df in built["tables"].items():
        df = df.replace("", np.nan)
        if table == "carriers":
            df = df.rename(columns={"carrier": "name"})
        df.to_csv(os.path.join(folder, f"{table}.csv"), index=False)
    # PyPSA parses dates only from a 'snapshot' column (not from the index)
    built["snapshots"].reset_index().to_csv(os.path.join(folder, "snapshots.csv"))
    for sheet, df in built["series"].items():
        df.to_csv(os.path.join(folder, f"{sheet}.csv"))


def case_mtime(case_dir: str) -> float:
    """Newest modification time of all files in the case folder."""
    newest = os.path.getmtime(case_dir)
    for root, _, files in os.walk(case_dir):
        for f in files:
            newest = max(newest, os.path.getmtime(os.path.join(root, f)))
    return newest


# ---------------------------------------------------------------------------------------------
# config + scenarios -> legacy DataFrames / settings
# ---------------------------------------------------------------------------------------------


def read_config(case_dir: str, a: dict, n_snapshots: int, link_ports: int) -> dict:
    """Return the flat legacy settings (only keys present in case.yaml:settings or derivable)."""
    cfg = read_case_section(case_dir, "settings")
    flat = {}
    for path, key in CONFIG_KEYS:
        value = _get_path(cfg, path)
        if value is not None:
            flat[key] = _legacy(value, key)
    # values the Excel book calculated from the assumptions
    derived = {
        "discount_factor": a["annuity_factor"],
        "years_of_construction": a["construction_period"],
        "project_cod": a["y_cod"],
        "years_of_operation": a["project_lifetime"],
        "investment_periods": repr([a["y_cod"]]),
        "total_snapshot": n_snapshots,
        "link_ports": link_ports,
    }
    for key, value in derived.items():
        flat.setdefault(key, value)
    if "timelimit_in_hours" in flat:
        flat["timelimit"] = 3600 * flat["timelimit_in_hours"]
    return flat


NONE = (
    np.nan
)  # placeholder for 'not applicable' (the Excel book used 'None', read as NaN)


def _flatten_changes(block: dict) -> list:
    """
    Flatten {scenario: {active, set, del, inactive: {set, del}}} into legacy rows.
    'inactive' holds changes that are kept for reference but switched off.
    """
    rows = []
    for scenario, spec in (block or {}).items():
        rows += _flatten_spec(
            scenario, spec, "True" if spec.get("active", True) else "False"
        )
        if spec.get("inactive"):
            rows += _flatten_spec(scenario, spec["inactive"], "False")
    return rows


def _flatten_spec(scenario: str, spec: dict, active: str) -> list:
    rows = []
    if True:
        for cls, items in (spec.get("set") or {}).items():
            if cls == "Python":
                for column, value in items.items():
                    rows.append(
                        [
                            scenario,
                            "set",
                            "Python",
                            NONE,
                            column,
                            _legacy(value, column),
                            active,
                        ]
                    )
            else:
                for name, attrs in items.items():
                    for column, value in attrs.items():
                        rows.append([scenario, "set", cls, name, column, value, active])
        for cls, names in (spec.get("del") or {}).items():
            # list of names, or {name: {column: value}} (stochastic deletes zero a column)
            if isinstance(names, dict):
                for name, attrs in names.items():
                    for column, value in (attrs or {NONE: NONE}).items():
                        rows.append([scenario, "del", cls, name, column, value, active])
            else:
                for name in names:
                    rows.append([scenario, "del", cls, name, NONE, NONE, active])
    return rows


SCEN_COLUMNS = [
    "scenario",
    "action",
    "class",
    "technology_name",
    "column",
    "value",
    "active",
]


def read_scenarios(case_dir: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return scen_params and stoch_params DataFrames in the legacy (Excel) layout."""
    scen = pd.DataFrame(
        _flatten_changes(read_case_section(case_dir, "scenarios")), columns=SCEN_COLUMNS
    )
    stoch = pd.DataFrame(
        _flatten_changes(read_case_section(case_dir, "stochastic")),
        columns=SCEN_COLUMNS,
    )
    return scen, stoch


# EXCEL ASSUMPTION BOOK -> CASE FOLDER CONVERTER ---------------------------
#
# python pypsa-x.py --xls-to-case PyPSA_PtX_AB_default.xlsx cases/default [--force]

# opt_params names in the workbook that differ from the names used in the code
OPT_PARAM_RENAMES = {
    "maintenance_start": "maintenance_mode",
    "do_capacity_constraints": "do_investment_constraints",
    "do_strict_unsimultanious_dis+charging": "do_strict_unsimultaneous_dis+charging",
}
# derived economic values (calculated by derive_assumptions() in this file)
DERIVED_ASSUMPTIONS = {
    "equity_share",
    "wacc_nominal",
    "wacc_real",
    "annuity_factor",
    "y_end",
    "construction_period",
    "production_hourly_adjusted",
}
CAPEX_PATTERN = re.compile(
    r'^=SUMIFS\(Technical_assumption\[value\],.*"invest_cost"\)\*annuity_factor'
    r'\+SUMIFS\(Technical_assumption\[value\],.*"fom"\)\*SUMIFS\(Technical_assumption\[value\],.*"invest_cost"\)$'
)


# ---------------------------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------------------------


def _clean(v):
    """Excel cell value -> plain python value for YAML/CSV."""
    if isinstance(v, str):
        s = v.strip()
        if s in ("True", "False"):
            return s == "True"
        return _to_number(s)
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating, float)):
        f = float(v)
        return int(f) if f.is_integer() and abs(f) < 1e15 else f
    if isinstance(v, _dt.datetime):
        return v.date().isoformat()
    return v


def _yaml_scalar(v) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    if v is None:
        return "null"
    if isinstance(v, float):
        r = repr(v)
        if (
            "e" in r and "." not in r.split("e")[0]
        ):  # YAML 1.1 needs '1.0e-05', not '1e-05'
            r = r.replace("e", ".0e", 1)
        return r
    if isinstance(v, int):
        return repr(v)
    if isinstance(v, (list, dict)):
        return yaml.safe_dump(v, default_flow_style=True, sort_keys=False).strip()
    s = str(v)
    if (
        s == ""
        or re.search(r"[:#\[\]{},&*!|>'\"%@`]|^\s|\s$", s)
        or s.lower() in ("true", "false", "yes", "no", "null", "on", "off", "~")
        or re.fullmatch(r"[-+0-9.eE]+", s)
    ):
        return '"' + s.replace("\\", "\\\\").replace('"', '\\"') + '"'
    return s


def _emit(d: dict, comments: dict, indent: int = 0, prefix: str = "") -> list:
    lines = []
    for k, v in d.items():
        path = f"{prefix}{k}"
        pad = "  " * indent
        if isinstance(v, dict) and v and not path.endswith("stochastic.cases"):
            lines.append(f"{pad}{k}:")
            lines.extend(_emit(v, comments, indent + 1, path + "."))
        else:
            c = comments.get(path)
            line = f"{pad}{k}: {_yaml_scalar(v)}"
            lines.append(f"{line:<60} # {c}" if c else line)
    return lines


def _set_path(d: dict, path: str, value):
    parts = path.split(".")
    for p in parts[:-1]:
        d = d.setdefault(p, {})
    d[parts[-1]] = value


def _block_rows(ws, ws_f, first_row: int, stop_label: str = None):
    """Yield (row, B..G cached values, E formula) of an AB block until stop_label."""
    for r in range(first_row, ws.max_row + 1):
        label = ws.cell(r, 2).value
        if stop_label and label == stop_label:
            return
        if label is None:
            continue
        yield r, [ws.cell(r, c).value for c in range(2, 8)], ws_f.cell(r, 5).value


# ---------------------------------------------------------------------------------------------
# converters
# ---------------------------------------------------------------------------------------------


def convert_ab(wb, wb_f, out: str) -> dict:
    ws, ws_f = wb["AB"], wb_f["AB"]
    labels = {ws.cell(r, 2).value: r for r in range(1, ws.max_row + 1)}
    econ_start = labels["Economic Assumption"] + 2
    tech_start = labels["Technical Assumption"] + 2
    #
    # --- assumptions.yaml
    lines = [
        "# Economic and project assumptions (former sheet 'AB', block 'Economic Assumption').",
        "# Derived values (equity share, WACC, annuity factor, end year, construction period,",
        "# hourly production target) are calculated by derive_assumptions() in pypsa-x.py.",
        "",
    ]
    raw = {}
    for r, (comp, _date, param, value, unit, comment), _ in _block_rows(
        ws, ws_f, econ_start, "Technical Assumption"
    ):
        if not param or param in DERIVED_ASSUMPTIONS:
            continue
        value = _clean(value)
        raw[param] = value
        note = " | ".join(
            str(x) for x in (comp, unit, comment) if x not in (None, "-", "")
        )
        line = f"{param}: {_yaml_scalar(value)}"
        lines.append(f"{line:<45} # {note}" if note else line)
    with open(os.path.join(out, "assumptions.yaml"), "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    a = derive_assumptions(raw)
    #
    # --- technologies.csv
    rows = []
    for r, (comp, date, param, value, unit, comment), formula in _block_rows(
        ws, ws_f, tech_start
    ):
        if param is None:
            continue
        formula = formula.text if hasattr(formula, "text") else formula
        rows.append(
            {
                "component": comp,
                "parameter": param,
                "value": _clean(value),
                "unit": "" if unit is None else unit,
                "last_update": _clean(date) if date else "",
                "comment": "" if comment in (None, "-") else comment,
                "excel_formula": formula
                if isinstance(formula, str) and formula.startswith("=")
                else "",
            }
        )
    tech = pd.DataFrame(rows)
    # capital_cost: use 'auto' where the standard annuity formula was used
    lut = {
        (c.lower(), p.lower()): v
        for c, p, v in zip(tech.component, tech.parameter, tech.value)
    }
    n_auto = 0
    for i, row in tech.iterrows():
        if row.parameter == "capital_cost" and CAPEX_PATTERN.match(
            str(row.excel_formula)
        ):
            invest = lut.get((row.component.lower(), "invest_cost"), 0) or 0
            fom = lut.get((row.component.lower(), "fom"), 0) or 0
            if _equal(
                invest * a["annuity_factor"] + fom * invest, row.value, rtol=1e-6
            ):
                tech.at[i, "value"] = "auto"
                tech.at[i, "excel_formula"] = ""
                n_auto += 1
    tech.to_csv(os.path.join(out, "technologies.csv"), index=False)
    print(
        f"x) technologies.csv: {len(tech)} rows ({n_auto} capital_cost rows set to 'auto')"
    )
    return a


def convert_config(wb, out: str, a: dict, n_snapshots: int, link_ports: int) -> dict:
    ws = wb["opt_params"]
    path_of = {key: path for path, key in CONFIG_KEYS}
    cfg, comments, seen, skipped = {}, {}, set(), []
    for r in range(4, ws.max_row + 1):
        name, value, comment = (ws.cell(r, c).value for c in (2, 3, 4))
        if name is None or value == "N/A":
            continue
        name = OPT_PARAM_RENAMES.get(name, name)
        if name in seen:
            skipped.append(f"{name} (duplicate in row {r}, first one wins)")
            continue
        seen.add(name)
        if name == "timelimit" or name not in path_of:
            if name != "timelimit":
                skipped.append(f"{name} (not used by the code)")
            continue
        value = _clean(value)
        if isinstance(value, str) and value[:1] in "[{":
            value = ast.literal_eval(value)
        # values the book derived from the assumptions are left out (derived at run time)
        derived = {
            "discount_factor": a["annuity_factor"],
            "years_of_construction": a["construction_period"],
            "project_cod": a["y_cod"],
            "years_of_operation": a["project_lifetime"],
            "investment_periods": [a["y_cod"]],
            "total_snapshot": n_snapshots,
            "link_ports": link_ports,
        }
        if name in derived and (value == derived[name] or _equal(value, derived[name])):
            continue
        _set_path(cfg, path_of[name], value)
        if comment and comment != "-":
            comments[path_of[name]] = str(comment).replace("\n", " ")
    header = [
        "# Optimization settings (former sheet 'opt_params').",
        "# Left out = default of pypsa-x.py. Derived when left out: economics.*,",
        "# investment.periods (= [y_cod]), solver timelimit (= 3600 * timelimit_hours).",
        "",
    ]
    with open(os.path.join(out, "config.yaml"), "w", encoding="utf-8") as f:
        f.write("\n".join(header + _emit(cfg, comments)) + "\n")
    for s in skipped:
        print(f"   info! opt_params: skipped {s}")
    return cfg


def convert_scenarios(wb, out: str) -> None:
    def block(sheet):
        ws = wb[sheet]
        result, flags, rows_by_flag = {}, {}, {}
        for r in range(4, ws.max_row + 1):
            scen, action, cls, name, column, value, active = (
                ws.cell(r, c).value for c in range(2, 9)
            )
            if scen is None:
                continue
            flags.setdefault(str(scen), set()).add(str(active))
            spec = result.setdefault(str(scen), {})
            rows_by_flag.setdefault(str(scen), []).append(str(active))
            if str(active) != "True":
                spec = spec.setdefault("inactive", {})
            if action == "set":
                if cls == "Python":
                    spec.setdefault("set", {}).setdefault("Python", {})[column] = (
                        _clean(value)
                    )
                else:
                    spec.setdefault("set", {}).setdefault(cls, {}).setdefault(name, {})[
                        column
                    ] = _clean(value)
            elif action == "del":
                if column in (None, "None"):  # value is not used for a plain delete
                    spec.setdefault("del", {}).setdefault(cls, {})[name] = None
                else:
                    spec.setdefault("del", {}).setdefault(cls, {}).setdefault(name, {})[
                        column
                    ] = _clean(value)

        def tidy(spec):
            for cls, names in list((spec.get("del") or {}).items()):
                if all(v is None for v in names.values()):
                    spec["del"][cls] = list(names)
                else:
                    spec["del"][cls] = {k: (v or {}) for k, v in names.items()}
            if "inactive" in spec:
                tidy(spec["inactive"])

        for spec in result.values():
            tidy(spec)
        for scen, f in flags.items():
            if "True" not in f:  # whole scenario switched off
                inactive = result[scen].pop("inactive", {})
                result[scen] = {"active": False, **inactive}
            elif len(f) > 1:
                print(
                    f"   info! {sheet}: scenario '{scen}' has inactive rows -> kept under 'inactive:'"
                )
        return result

    data = {"scenarios": block("scen_params"), "stochastic": block("stoch_params")}
    header = (
        "# Scenario definitions (former sheets 'scen_params' and 'stoch_params').\n"
        "# Per scenario:  set: {<Class>: {<name>: {<attribute>: <value>}}}  |  set: {Python: {<setting>: <value>}}\n"
        "#                del: {<Class>: [<name>, ...]}  |  active: false\n"
        "#                inactive: {set: ..., del: ...}   (switched-off changes, kept for reference)\n\n"
    )
    with open(os.path.join(out, "scenarios.yaml"), "w", encoding="utf-8") as f:
        f.write(header + yaml.safe_dump(data, sort_keys=False, allow_unicode=True))


def convert_network(xlsx: str, out: str, a: dict) -> tuple[dict, list]:
    sheets = pd.read_excel(xlsx, sheet_name=None)
    tech = read_technologies(os.path.join(out, "technologies.csv"), a)
    net_dir = os.path.join(out, "network")
    os.makedirs(net_dir, exist_ok=True)
    overrides = []

    #
    def prep(df):
        df = df.drop(
            columns=[
                c
                for c in df.columns
                if c in EXCEL_HELPER_COLUMNS or str(c).startswith("Unnamed")
            ]
        )
        df = df.dropna(how="all")
        return (
            df.astype(object)
            .apply(lambda col: col.map(lambda v: "" if pd.isna(v) else _clean(v)))
            .astype(object)
        )

    #
    carriers = prep(sheets["carriers"])
    carriers.to_csv(os.path.join(net_dir, "carriers.csv"), index=False)
    buses_excel = prep(sheets["buses"])
    #
    result = {}
    for table in COMPONENT_TABLES:
        if table == "carriers" or table not in sheets:
            continue
        excel = buses_excel if table == "buses" else prep(sheets[table])
        derivable = set(PARAM_MAP.get(table, {}))
        if table in PARAM_MAP:
            derivable |= {"build_year", "lifetime"}
            if "_capital_cost_if_inst" in PARAM_MAP[table]:
                derivable.add("_capital_cost_unit")
        if table in BUS_COLUMN:
            derivable.add("carrier")
        if table in UNIT_COLUMN:
            derivable.add(UNIT_COLUMN[table])
        if table == "buses":
            derivable.add("unit")
        if table == "loads":
            derivable |= {
                "total_output_unit",
                "p_set_unit",
                "active",
                "p_set",
                "_supply_mode",
            }
        derivable &= set(excel.columns)
        #
        blank = excel.copy().astype(object)
        for col in derivable:
            blank[col] = ""
        resolved = resolve_table(table, blank, tech, a, buses_excel, carriers)
        topo = excel.copy().astype(object)
        for col in derivable:
            for i in excel.index:
                if _equal(excel.at[i, col], resolved.at[i, col], rtol=1e-6):
                    topo.at[i, col] = ""
                else:
                    overrides.append(
                        f"{table}.{excel.at[i, excel.columns[0]]}.{col}: excel={excel.at[i, col]!r} derived={resolved.at[i, col]!r}"
                    )
        # drop columns that are completely empty and fully derivable
        empty = [c for c in derivable if (topo[c] == "").all()]
        topo = topo.drop(columns=empty)
        topo.to_csv(os.path.join(net_dir, f"{table}.csv"), index=False)
        result[table] = excel
    return result, overrides


def convert_timeseries(xlsx: str, out: str, a: dict, statics: dict) -> int:
    ts_dir = os.path.join(out, "timeseries")
    os.makedirs(ts_dir, exist_ok=True)
    raw = pd.read_excel(xlsx, sheet_name="raw_data", header=6)
    raw = raw.dropna(subset=["Date"]).set_index("Date")
    raw.index.name = "snapshot"
    helper = {"Month", "End Of Month", "End of Week", "Year"}
    profiles = raw[
        [c for c in raw.columns if c not in helper and not str(c).startswith("Unnamed")]
    ]
    profiles = profiles.astype(float)
    #
    sheets = pd.read_excel(xlsx, sheet_name=None, index_col=0)
    spec = {"series": {}}
    extra = {}
    named_scales = {
        k: a[k]
        for k in ("production_target_hour", "production_target_year", "CO2_ETS")
        if k in a
    }
    for sheet, df in sheets.items():
        if (
            "-" not in sheet
            or sheet.startswith("Z_")
            or sheet.split("-")[0] not in COMPONENT_TABLES
        ):
            continue
        df = df.dropna(how="all")
        if df.empty or df.shape[1] == 0:
            continue
        table, attr = sheet.split("-", 1)
        rules = {}
        for col in df.columns:
            s = df[col].astype(float).values
            # constant?
            if np.allclose(s, s[0]):
                st = statics.get(table)
                if st is not None and attr in st.columns:
                    sv = st.set_index(st.columns[0]).get(attr, {}).get(col)
                    if sv is not None and _equal(sv, s[0], rtol=1e-9):
                        print(
                            f"   info! {sheet}.{col}: constant = static value {s[0]} -> no time series needed"
                        )
                        continue
                rules[col] = {"constant": float(s[0])}
                continue
            # scaled profile?
            for p in profiles.columns:
                ps = profiles[p].values
                nz = np.abs(ps) > 1e-12
                if not nz.any() or not np.allclose(s[~nz], 0):
                    continue
                k = float(np.median(s[nz] / ps[nz]))
                if np.allclose(s, ps * k, rtol=1e-9, atol=1e-12):
                    rule = {"profile": p}
                    named = [
                        n
                        for n, v in named_scales.items()
                        if np.isclose(k, v, rtol=1e-9)
                    ]
                    if named:
                        rule["scale"] = named[0]
                    elif not np.isclose(k, 1):
                        rule["scale"] = round(k, 12)
                    rules[col] = rule
                    break
            else:
                name = f"{sheet}:{col}"
                extra[name] = df[col].astype(float).values
                rules[col] = {"profile": name}
        if rules:
            spec["series"][sheet] = rules
    for name, values in extra.items():
        profiles[name] = values
    #
    weights = pd.read_excel(xlsx, sheet_name="snapshots", index_col=0)
    w = {
        c: float(weights[c].iloc[0])
        for c in weights.columns
        if c in ("objective", "stores", "generators")
    }
    if w:
        spec = {"snapshot_weightings": w, **spec}
    #
    profiles.to_csv(os.path.join(ts_dir, "profiles.csv"))
    header = (
        "# Mapping of raw profiles (profiles.csv) to PyPSA time-varying attributes.\n"
        "#   <table>-<attribute>: {<name>: {profile: <column>, scale: <number|assumption name>}}\n"
        "#                        {<name>: {constant: <number>}}\n"
        "# If profiles.csv has no 'Supply_Mode' column, it is generated from\n"
        "# production_supply_frequency (section 'assumptions'), using random_seed for 'random'.\n\n"
    )
    with open(os.path.join(ts_dir, "timeseries.yaml"), "w", encoding="utf-8") as f:
        f.write(header + yaml.safe_dump(spec, sort_keys=False))
    print(
        f"x) timeseries: {profiles.shape[1]} profile column(s), {sum(len(v) for v in spec['series'].values())} mapped series"
    )
    return len(profiles)


def merge_yaml(out: str) -> None:
    """Merge the per-topic YAML files into one case.yaml (comments are kept)."""
    parts = [
        ("settings", "config.yaml"),
        ("assumptions", "assumptions.yaml"),
        (
            None,
            "scenarios.yaml",
        ),  # already has top-level keys 'scenarios' and 'stochastic'
        ("timeseries", os.path.join("timeseries", "timeseries.yaml")),
    ]
    schema = os.path.relpath(SCHEMA_FILE, os.path.abspath(out)).replace(os.sep, "/")
    lines = [
        f"# yaml-language-server: $schema={schema}",
        "# PyPSA-X case definition (replaces the Excel assumption book), see case.schema.json.",
        "# Sections: settings | assumptions | scenarios | stochastic | timeseries",
        "# Tables: technologies.csv, network/*.csv, timeseries/profiles.csv",
        "",
    ]
    for section, fname in parts:
        path = os.path.join(out, fname)
        with open(path, encoding="utf-8") as f:
            text = f.read().rstrip("\n").split("\n")
        n_head = 0
        while n_head < len(text) and (
            text[n_head].startswith("#") or text[n_head] == ""
        ):
            n_head += 1
        head = [line for line in text[:n_head] if line]
        body = text[n_head:]
        lines += ["# " + "-" * 93] + head
        if section is None:
            lines += body
        else:
            lines.append(f"{section}:")
            lines += [("  " + line) if line else "" for line in body]
        lines.append("")
        os.remove(path)
    with open(os.path.join(out, CASE_FILE), "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    validate_case(out)  # parse + schema check
    print(f"x) {CASE_FILE}: sections {', '.join(CASE_SECTIONS)}")


def convert_xlsx_to_case(xlsx: str, case_dir: str, force: bool = False) -> None:
    """
    Convert an Excel assumption book (e.g. PyPSA_PtX_AB_default.xlsx) into a case folder
    (case.yaml + CSV tables). The workbook must have been saved by Excel (cached formula
    results are used). Values the case builder derives itself (technology lookups, units,
    carriers, WACC, capital_cost, ...) are only written where they differ from the derived value.
    """
    import openpyxl

    #
    if os.path.exists(case_dir) and os.listdir(case_dir) and not force:
        sys.exit(f"error! '{case_dir}' exists and is not empty (use --force)")
    os.makedirs(case_dir, exist_ok=True)
    #
    print(f"convert '{xlsx}' -> '{case_dir}' ...")
    wb = openpyxl.load_workbook(xlsx, data_only=True)
    wb_f = openpyxl.load_workbook(xlsx, data_only=False)
    #
    a = convert_ab(wb, wb_f, case_dir)
    statics, overrides = convert_network(xlsx, case_dir, a)
    n_snapshots = convert_timeseries(xlsx, case_dir, a, statics)
    links = statics.get("links", pd.DataFrame())
    link_ports = sum(
        1
        for c in links.columns
        if re.fullmatch(r"bus\d+", str(c)) and (links[c] != "").any()
    )
    convert_config(wb, case_dir, a, n_snapshots, link_ports)
    convert_scenarios(wb, case_dir)
    merge_yaml(case_dir)
    #
    print(
        f"x) network: {len(overrides)} per-asset override(s) kept (Excel value != derived value):"
    )
    for o in overrides:
        print(f"     {o}")
    print("done.")


def read_network_data(
    source: str,
    source_type: str = "case",
    target_folder: str = None,
    temp_file: str = None,
) -> None:
    """
    Build the PyPSA base network either from a case folder (YAML + CSV, see
    see the CASE FOLDER INPUT section) or from an Excel file (aka as Assumption Book) which contains
    all the necessary PyPSA sheets (e.g., carriers, buses, generators,
    generators-p_max_pu, links). For a case folder, the resolved PyPSA tables are
    written to '<target_folder>/<csv_subfolder>' and imported from there.

    Parameters
    ----------
    source : str
        Case folder or Excel file to be considered.

    source_type : str = "case"
        Type of the source; either 'case' or 'excel'.

    target_folder: str = None
        Name of the folder to store all temporary and final files (e.g.,
        initial CSV files, base and result NC files).

    temp_file: str = None
        Name of the temporary NC file after reading the source.

    Returns
    -------
    None
    """
    #
    # check if the required folder exist, if not create them
    if not os.path.exists(f"{target_folder}"):
        os.makedirs(f"{target_folder}")
    #
    # collect the (newest) modification time of the source file(s)
    if source_type == "excel":
        source_time = os.path.getmtime(source)
    #
    else:
        source_time = case_mtime(source)
    #
    # check if the NC base case file is available, if yes collect its timestamp
    if os.path.isfile(f"{target_folder}/{temp_file}"):
        nc_time = os.path.getmtime(f"{target_folder}/{temp_file}")
    #
    else:
        nc_time = source_time
    #
    # if the base case NC file is older than the source, recreate it
    if source_time >= nc_time:
        print(f"read energy system details from {source} ...")
        #
        if source_type == "excel":
            # create PyPSA network directly from Excel file
            n = pypsa.Network(source)
        #
        else:
            csv_folder = f"{target_folder}/{globals()['csv_subfolder']}"
            built = build_tables(source)
            write_pypsa_csv_folder(built, csv_folder)
            # create PyPSA network from the resolved CSV folder
            n = pypsa.Network(csv_folder)
        #
        n.consistency_check()
        #
        if (eval(globals()["debug_mode"])) and (globals()["hours_to_optimize"] < 8760):
            print(
                f"\ndebugging is enabled, therefore only {globals()['hours_to_optimize']} hours are considered in the optimization"
            )
            n.set_snapshots(
                pd.date_range(
                    f"{globals()['project_cod']}-01-01",
                    freq="1h",
                    periods=globals()["hours_to_optimize"],
                )
            )
        #
        inv_periods = np.array(eval(globals()["investment_periods"]))
        n.set_investment_periods(inv_periods)
        inv_weightings = np.array(eval(globals()["investment_weighting"]))
        #
        for year in n.investment_period_weightings.index:
            pos = np.where(n.investment_period_weightings.index == year)
            n.investment_period_weightings.loc[year] = inv_weightings[pos]
        #
        # save base case as NC file
        save_network(n, f"{target_folder}", f"{temp_file}")
    #
    else:
        print(f"no need to re-read energy system details from {source}")
        print(f"NC file '{target_folder}/{temp_file}' is up-to-date\n")
    #
    return None


def _read_excel_params(
    excel_file: str,
    run_scenarios_default: str,
) -> tuple[pd.core.frame.DataFrame, pd.core.frame.DataFrame, pd.core.frame.DataFrame]:
    """
    Read the optimization, scenario and stochastic settings from the sheets
    'opt_params', 'scen_params' and 'stoch_params' of an Excel file (aka as
    Assumption Book).
    """
    #
    own_sheets = ["opt_params", "scen_params", "stoch_params"]
    data = pd.read_excel(excel_file, sheet_name=own_sheets)
    #
    df_params = []
    scen_params = []
    stoch_params = []
    #
    scen_columns = [
        "scenario",
        "action",
        "class",
        "technology_name",
        "column",
        "value",
        "active",
    ]
    #
    for sheet_name, sheet_data in data.items():
        df = copy.deepcopy(sheet_data[2:])
        #
        if sheet_name == "opt_params":
            print("x) optimization parameters")
            df.rename(
                columns={
                    df.columns[1]: "variable",
                    df.columns[2]: "value",
                    df.columns[3]: "remark",
                },
                inplace=True,
            )
            df.set_index("variable", inplace=True)
            df.drop(labels=[df.columns[0]], axis=1, inplace=True)
            df_params = df.copy()
        #
        elif sheet_name in ["scen_params", "stoch_params"]:
            df.rename(
                columns={df.columns[i + 1]: col for i, col in enumerate(scen_columns)},
                inplace=True,
            )
            df.drop(df.columns[0], axis=1, inplace=True)
            active = df[df.active == "True"]
            #
            if sheet_name == "scen_params":
                print("x) scenario parameters")
                run_scenarios = (
                    df_params.loc["run_scenarios"].value
                    if "run_scenarios" in df_params.index
                    else run_scenarios_default
                )
                #
                if str(run_scenarios) == "True":
                    scen_params = active
                #
                else:
                    scen_params = active[active.scenario == active.scenario.unique()[0]]
            #
            else:
                print("x) stochastic optimization parameters")
                stoch_params = active
    #
    return df_params, scen_params, stoch_params


def _read_case_params(
    case_dir: str,
    run_scenarios_default: str,
) -> tuple[pd.core.frame.DataFrame, pd.core.frame.DataFrame, pd.core.frame.DataFrame]:
    """
    Read the optimization, scenario and stochastic settings from a case folder
    (case.yaml: settings, assumptions, scenarios, stochastic).
    """
    #
    built = build_tables(case_dir)
    links = built["tables"].get("links", pd.DataFrame())
    link_ports = sum(
        1
        for col in links.columns
        if col.startswith("bus") and col[3:].isdigit() and (links[col] != "").any()
    )
    settings = read_config(
        case_dir, built["assumptions"], len(built["snapshots"]), link_ports
    )
    #
    print("x) optimization parameters")
    df_params = pd.DataFrame({"value": pd.Series(settings, dtype=object)}).rename_axis(
        "variable"
    )
    #
    print("x) scenario parameters")
    df_scen, df_stoch = read_scenarios(case_dir)
    active = df_scen[df_scen.active == "True"]
    if str(settings.get("run_scenarios", run_scenarios_default)) == "True":
        scen_params = active
    #
    else:
        scen_params = active[active.scenario == active.scenario.unique()[0]]
    #
    print("x) stochastic optimization parameters")
    stoch_params = df_stoch[df_stoch.active == "True"]
    #
    return df_params, scen_params, stoch_params


def read_all_params(
    source: str,
    source_type: str = "case",
) -> tuple[pd.core.frame.DataFrame, pd.core.frame.DataFrame, pd.core.frame.DataFrame]:
    """
    Read the optimization and scenario settings either from a case folder
    (case.yaml: settings, assumptions, scenarios, stochastic) or from an Excel
    file (aka as Assumption Book; sheets opt_params, scen_params, stoch_params).

    Parameters
    ----------
    source : str
        Case folder or Excel file to be considered.

    source_type : str = "case"
        Type of the source; either 'case' or 'excel'.

    Returns
    -------
    df_params: list
        Optimization settings to be considered.

    scen_params: list
        Scenarios details to be considered.

    stoch_params: list
        Stochastic optimization details to be considered.
    """
    #
    # define some variables
    defaults = {
        # debug settings
        "debug_mode": "False",
        "hours_to_optimize": (24 * 3),
        # Other settings
        "target_folder": f"./run_{uuid.uuid4().hex}",
        "csv_subfolder": "csv_model",  # resolved PyPSA CSV tables built from the case folder
        "temp_file": "base_model.nc",
        "result_file": "result",
        "use_oetc": "False",
        "do_segmentation": "False",
        "segmentation_function": "segment",
        "segmentation_hours": 4,
        "run_scenarios": "False",
        "modular_representation": "False",
        "transmission_losses": "0",
        "remove_kvl_constraints": "False",
        "do_maintenance_planing": "False",
        "maintenance_mode": "monthly",
        "do_operational_constraints": "True",
        "do_investment_constraints": "True",
        "do_milp_constraints": "True",
        "do_strict_unsimultaneous_dis+charging": "False",
        "assign_all_duals": "True",
        "small_limit": 0.00001,
        "discount_factor": 0.06,
        "years_of_construction": 4,
        "project_cod": dt.now().year,
        "years_of_operation": 25,
        "link_ports": 2,
        "total_snapshot": 8760,
        # general solver settings
        "mipgap": 0.001,
        "timelimit_in_hours": 1,
        "timelimit": 3600,
        "log_to_console": 0,
        "output_flag": 0,
        # stochastic optimization settings
        "run_stochastic_runs": "False",
        "stoch_alpha": 0.9,
        "stoch_omega": 0.5,
        "stoch_case_definition": "{'P49': 0.49, 'P51': 0.51}",
        # MGA settings
        "run_mga_runs": "False",
        "mga_slack": 0.0,
        "mga_slacks": "[0.01, 0.02, 0.05, 0.10]",
        # Rolling Horizon (RH) settings
        "run_rollinghorizon_after_optimization": "False",
        "rollinghorizon_horizon": (24 + 12),
        "rollinghorizon_overlap": 12,
        # security constraint settings
        "run_security_constrained_optimization": "False",
        # some other settings
        "investment_periods": "[2031]",  # periods to optimize
        "investment_weighting": "[1]",  # weighting of periods; e.g, gap between
        # multiple years; e.g., 5 or 10
        "primary_optimization": "pathway",
        "objective_function": "annuity+o&m",  # 'annuity-o&m', 'npv'
        # N-1 operation
        "nminus1_activate": "False",
        # hourly matching settings
        "cfe_matching": 0.0,
        "cfe_type": "year",
        "cfe_rens": ["solar", "wind", "biomass", "geothermal"],
        # some reserve margin settings
        "rm_activate": "False",
        "rm_factor_a": 1,
        "rm_factor_b": 0.15,
        "rm_factor_c": 0,
        "rm_factor_d": 1,
        "rm_factor_e": 0,
        "rm_max_generator": "False",
        "rm_load_class": "Load",
        "rm_load_name": "LOAD_EL_01",
    }
    #
    print(f"\nread optimization and scenario settings from {source} ...")
    if source_type == "excel":
        df_params, scen_params, stoch_params = _read_excel_params(
            source, defaults["run_scenarios"]
        )
    #
    else:
        df_params, scen_params, stoch_params = _read_case_params(
            source, defaults["run_scenarios"]
        )
    #
    print("done.\n")
    #
    for default in defaults:
        if default in df_params.index:
            globals()[f"{default}"] = df_params[df_params.index == default].value.iloc[
                0
            ]
        #
        else:
            globals()[f"{default}"] = defaults[default]
    #
    if globals()["primary_optimization"] == "pathway":
        globals()["multi_investment_periods"] = "False"
    #
    else:
        globals()["multi_investment_periods"] = "True"
    #
    return df_params, scen_params, stoch_params


def get_solver_setting() -> dict:
    """
    Set the solver settings to control its behavior during optimization.

    Parameters
    ----------
    None

    Returns
    -------
    solver_options: dict
        Options to control the chosen optimization solver.
    """
    #
    if globals()["solver_name"] == "gurobi":
        solver_options = {
            # general solver settings
            "mipgap": globals()["mipgap"],
            "outputflag": globals()["output_flag"],
            "logtoconsole": globals()["log_to_console"],
            "timelimit": globals()["timelimit"],
            #
            # individual settings
            "threads": 0,
            "presolve": 1,
            "method": 4,
            "numericfocus": 0,
            "crossover": -1,
        }
    #
    elif globals()["solver_name"] == "highs":
        solver_options = {
            # general solver settings
            "mip_abs_gap": globals()["mipgap"],
            "output_flag": bool(globals()["output_flag"]),
            "log_to_console": bool(globals()["log_to_console"]),
            "time_limit": globals()["timelimit"],
            #
            # individual settings
            "threads": 0,
            "presolve": "choose",  # "off", "choose" or "on"; default: "choose"
            "solver": "choose",  # "simplex", "choose", "ipm" or "pdlp".
            # If "simplex"/"ipm"/"pdlp"; default: "choose"
            "run_crossover": "choose",  # "off", "choose" or "on"; default: "on"
            "user_objective_scale": -6,
        }
    #
    elif globals()["solver_name"] == "cplex":
        solver_options = {
            # general solver settings
            "mipgap": globals()["mipgap"],
            "outlev ": globals()["output_flag"],
            "log_to_console": bool(globals()["log_to_console"]),
            "timelimit": globals()["timelimit"],
            #
            # individual settings
            "threads": 0,
            "presolve": 1,
            "method": 2,
            "solutiontype": 2,
            "numericfocus": 0,
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

    df_scens: pd.core.frame.DataFrame
        DataFrame containing the required scenario adjustments.

    Returns
    -------
    all_adjustments_ok: bool
        Boolean of all envisioned adjustments are possible.
    """
    #
    print("load the temporary network model to validate the scenario changes ...")
    n = pypsa.Network(temp_file)
    all_adjustments_ok = True
    #
    for scenario in df_scens.scenario.unique():
        # load the temporary model to ensure starting from the same point
        for index, row in df_scens[(df_scens.scenario == scenario)].iterrows():
            if row["class"] != "Python":
                df = n.c[row["class"]].static
            #
            if row["action"] == "set":
                if row["class"] == "Python":  # change variable in Python
                    if row["column"] not in globals():
                        print(
                            f"info! set variable {row['column']} = {row['value']} would not work"
                        )
                        all_adjustments_ok = False
                #
                else:
                    if not (
                        row["technology_name"] in df.index
                        and row["column"] in df.columns
                    ):
                        print(
                            f"info! set {row['class']}.{row['technology_name']}.{row['column']} = {row['value']} would not work"
                        )
                        all_adjustments_ok = False
            #
            elif row["action"] == "del":
                if row["technology_name"] not in df.index:
                    print(
                        f"info! del {row['class']}.{row['technology_name']} would not work"
                    )
                    all_adjustments_ok = False
    #
    print("")
    return all_adjustments_ok


def update_network(
    n: pypsa.Network,
    df_adjusts: pd.core.frame.DataFrame,
    scenario: str,
    stoch_scenarios: str = None,
) -> pypsa.Network:
    """
    Reads the base case PyPSA network and adjust settings for a given scenario
    based on the data in the DataFrame scenarios.

    Parameters
    ----------
    n: pypsa.Network
        PyPSA network containing the initial network to optimize.

    df_adjusts: pd.core.frame.DataFrame
        DataFrame containing the required scenario adjustments.

    scenario: str
        Name of the current scenario to adjust the PyPSA network for.

    Returns
    -------
    n: pypsa.Network
        PyPSA network containing the new scenario to optimize.
    """
    #
    if stoch_scenarios is None:
        # loop for non-stochastic adjustments
        #
        for index, row in df_adjusts[(df_adjusts.scenario == scenario)].iterrows():
            if row["class"] != "Python":
                df = n.c[row["class"]].static
            #
            if row["action"] == "set":
                if row["class"] == "Python":  # change variable in Python
                    if row["column"] in globals():
                        print(f"x) set variable {row['column']} = {row['value']}")
                        globals()[row["column"]] = row["value"]
                    #
                    else:
                        print(
                            f"info! set variable {row['column']} = {row['value']} does not work"
                        )
                #
                else:
                    if (
                        row["technology_name"] in df.index
                        and row["column"] in df.columns
                    ):
                        print(
                            f"x) set {row['class']}.{row['technology_name']}.{row['column']} = {row['value']}"
                        )
                        df.loc[row["technology_name"], row["column"]] = row["value"]
                    #
                    else:
                        print(
                            f"info! set {row['class']}.{row['technology_name']}.{row['column']} = {row['value']} does not work"
                        )
            #
            elif row["action"] == "del":
                if row["technology_name"] in df.index:
                    print(f"x) del {row['class']}.{row['technology_name']}")
                    n.remove(row["class"], row["technology_name"])
                #
                else:
                    print(
                        f"info! del {row['class']}.{row['technology_name']} does not work"
                    )
    #
    else:
        # loop for stochastic adjustments
        #
        for index, row in df_adjusts.iterrows():
            if row["class"] != "Python":
                df = n.c[row["class"]].static
                dfd = n.c[row["class"]].dynamic
            #
            if row["action"] == "set":
                # check within the static dataframe
                if (
                    row["scenario"] in df.index
                    and row["column"] in df.columns
                    and row["technology_name"] in df.index.get_level_values("name")
                ):
                    print(
                        f"x) {row['scenario']}: set {row['class']}.{row['technology_name']}.{row['column']} = {row['value']}"
                    )
                    df.loc[
                        [(row["scenario"], row["technology_name"])], row["column"]
                    ] = row["value"]
                #
                # check within the dynamic dataframe
                if (
                    row["scenario"] in df.index
                    and row["column"] in df.columns
                    and row["technology_name"] in df.index.get_level_values("name")
                ):
                    print(
                        f"x) {row['scenario']}: set {row['class']}.{row['technology_name']}.{row['column']} = {row['value']}"
                    )
                    df.loc[
                        [(row["scenario"], row["technology_name"])], row["column"]
                    ] = row["value"]
                #
                else:
                    print(
                        f"info! {row['scenario']}: set {row['class']}.{row['technology_name']}.{row['column']} = {row['value']} does not work"
                    )
            #
            elif row["action"] == "del":
                # check within the static dataframe
                if (
                    row["scenario"] in df.index
                    and row["column"] in df.columns
                    and row["technology_name"] in df.index.get_level_values("name")
                ):
                    print(
                        f"x) {row['scenario']}: del {row['class']}.{row['technology_name']}.{row['column']} = {row['value']}"
                    )
                    df.loc[
                        [(row["scenario"], row["technology_name"])], row["column"]
                    ] = 0  # row['value']
                #
                # check within the dynamic dataframe
                if (
                    row["column"] in dfd
                    and (row["scenario"], row["technology_name"])
                    in dfd["p_set"].columns
                ):
                    print(
                        f"x) {row['scenario']}: del {row['class']}.{row['technology_name']}.{row['column']} = {row['value']}"
                    )
                    dfd[row["column"]][[(row["scenario"], row["technology_name"])]] = 0
    #
    return n


def read_and_update_network(
    temp_file: str,
    df_scens: pd.core.frame.DataFrame = pd.core.frame.DataFrame(),
    scenario: str = None,
    first_call: bool = False,
) -> pypsa.Network:
    """
    Reads the base PyPSA network and adjust settings for a given scenario
    based on the data in the DataFrame scenarios.

    Parameters
    ----------
    temp_file: str
        Name of the PyPSA network test adjustments against.

    df_scens: pd.core.frame.DataFrame
        DataFrame containing the required scenario adjustments.

    scenario: str
        Name of the current scenario to adjust the PyPSA network for.

    first_call: bool
        Indicates if the function is called the first time or from do_all_runs function.

    Returns
    -------
    n: pypsa.Network
        PyPSA network containing the new scenario to optimize.
    """
    #
    n = pypsa.Network(temp_file)
    #
    if len(df_scens) > 0:
        n = update_network(n, df_scens, scenario)
    #
    comps = pypsa.descriptors.nominal_attrs
    col = "mod"
    #
    # create individual units for the technologies having 'nom_mod' defined
    if eval(globals()["modular_representation"]):
        # loop through all components
        for c in comps:
            # >> Exclude Line, Transformer, and StorageUnits as PyPSA's power flow
            #    implementation can't deal with extendable-units in this components
            if c not in ["Line", "Transformer", "StorageUnit"]:
                attr = comps[c]
                df = n.c[c].static.query(f"{attr}_{col} > 0")
                #
                for idx, row in df.iterrows():
                    num_adds = int(row[f"{attr}_max"] // row[f"{attr}_{col}"])
                    print(f"info! add {num_adds} modules for {c}.{idx}")
                    #
                    # create new lines based on the
                    dfs = pd.concat([df[df.index == idx].copy()] * num_adds)
                    dfs2 = dfs.reset_index()
                    #
                    for i in range(1, num_adds + 1):
                        # ensure a slightly difference in the economic ranking of the
                        # individual capital_cost's
                        dfs2.at[i - 1, "name"] = (
                            f"{dfs2.at[i - 1, 'name']}_mod_{str(i).rjust(len(str(num_adds)), '0')}"
                        )
                        dfs2.at[i - 1, "capital_cost"] -= (num_adds - i + 2) * 0.0001
                        dfs2.at[i - 1, f"{attr}_max"] = dfs2.at[i - 1, f"{attr}_{col}"]
                        dfs2.at[i - 1, f"{attr}_mod"] = 0
                    #
                    dfs = dfs2.set_index("name")
                    #
                    # add all the new candidates
                    n.add(c, name=dfs.index, **dfs)
    #
    inv_periods = np.array(eval(globals()["investment_periods"]))
    #
    # remove unused and/or unusable components (nom=0 & nom_max=0)
    if not first_call:
        n = remove_unused_details(n)
        #
        # save network as preliminary result file
        save_network(
            n,
            f"{globals()['target_folder']}",
            f"{globals()['result_file']}_{scenario}_rev0.nc",
        )
    #
    return n, inv_periods


def save_network(
    n: pypsa.Network,
    path_name: str,
    file_name: str,
) -> None:
    """
    Save a given PyPSA network as NetCDF file.

    Parameters
    ----------
    n: pypsa.Network
        PyPSA network to save into the NetCDF file.

    path_name: str
        Path name to save the PyPSA network into.

    file_name: str
        File name to save the PyPSA network into.

    Returns
    -------
    None
    """
    #
    nc_file = f"{path_name}/{file_name}"
    print(f'save PyPSA network as "{nc_file}"')
    # delete target file if exists
    if os.path.exists(nc_file):
        os.remove(nc_file)
    #
    # save the result
    n.export_to_netcdf(nc_file)
    #
    is_solved = type(n.model) is not type(None)
    if is_solved and n.model.status == "optimal" and matplotlib_pyplot:
        print("optimization succeeded, now creating summaries...\n")
        n.statistics.capex().dropna().droplevel(0).sort_values().plot.barh()
        plt.savefig(
            f"{path_name}/capex_breakdown_{file_name.split('.')[0]}.png",
            bbox_inches="tight",
        )
        n.statistics.opex().dropna().droplevel(0).sort_values().plot.barh()
        plt.savefig(
            f"{path_name}/opex_breakdown_{file_name.split('.')[0]}.png",
            bbox_inches="tight",
        )
        n.statistics.capacity().dropna().droplevel(0).sort_values().plot.barh()
        plt.savefig(
            f"{path_name}/capacity_breakdown_{file_name.split('.')[0]}.png",
            bbox_inches="tight",
        )
        #
        # Select timeframe
        start, end = "2025-01-01", "2025-01-07"
        n.generators_t.p.loc[start:end].groupby(
            n.generators.carrier, axis=1
        ).sum().plot.area(figsize=(12, 6))
        plt.ylabel("Power Output [MW]")
        plt.title("Dispatch by Carrier")
        plt.savefig(
            f"{path_name}/dispatch_by_carrier_{file_name.split('.')[0]}.png",
            bbox_inches="tight",
        )
    #
    return None


def save_network_svg(
    n: pypsa.Network, file_name: str, small_limit: float = 0.0001
) -> nx.Graph:
    """
    Save a given PyPSA network as SVG file.

    Parameters
    ----------
    n: pypsa.Network
        PyPSA network to save into a SVG file.

    file_name: str
        File name to save the PyPSA network into.

    small_limit: float
        Minimum size of technology to be shown on the SVG file.

    Returns
    -------
    G: nx.Graph
        Result graph
    """
    #
    print(f'save network as "{file_name}"')
    # collect the colors of the carriers
    comps = pypsa.descriptors.nominal_attrs
    c1 = "Bus"
    c2 = "Carrier"
    colors = n.c[c1].static.join(
        n.c[c2].static, on="carrier", lsuffix="_c", rsuffix="_o"
    )[["carrier", "color"]]
    #
    # to avoid networkx' random positioning we use the random seed feature
    np.random.seed(42)
    G = nx.Graph()
    #
    # add all buses as they have distribution technologies
    c = "Bus"
    df = n.c[c].static
    #
    for bus in df.index:
        node_color = colors[colors.index == bus].color.iloc[0]
        G.add_node(node_for_adding=bus, node_color=node_color)
    #
    # add load nodes
    c = "Load"
    df = n.c[c].static.query("p_set > 0")
    #
    for load in df.index:
        node_color = colors[colors.index == df.bus[load]].color.iloc[0]
        G.add_node(node_for_adding=load, node_color=node_color)
    #
    # get the largest
    max_activity = (
        max(
            n.c["Link"].dynamic["p0"].abs().sum().sum(),
            n.c["Generator"].dynamic["p"].abs().sum().sum(),
            n.c["Store"].dynamic["p"].abs().sum().sum(),
            n.c["StorageUnit"].dynamic["p"].abs().sum().sum() / 2,
        )
        / 10
    )
    #
    if not small_limit:
        small_limit = globals()["small_limit"]
    #
    # loop through all components
    for c in comps:
        attr = comps[c]
        df = n.c[c].static.query(f"{attr}_opt > " + str(small_limit))
        #
        for t in df.index:
            if "bus0" in df.columns:
                node_color = colors[colors.index == df.bus0.loc[t]].color.iloc[0]
            #
            else:
                node_color = "black"
            #
            # add the technology nodes (e.g., generator, link, transformer)
            G.add_node(node_for_adding=t, node_color="black")
            #
            # if there is a bus0 there are more buses
            if "bus0" in df.columns:
                buses = [col for col in df if col.startswith("bus")]
                #
                for bus in buses:
                    edge_from = df[df.index == t]["bus0"].values[0]
                    edge_to = df[df.index == t][bus].values[0]
                    #
                    if len(edge_to) > 0:
                        edge_color = colors[colors.index == df.loc[t][bus]].color.iloc[
                            0
                        ]
                        #
                        if t in n.c[c].dynamic[f"p{bus[3:]}"]:
                            tech_sum = n.c[c].dynamic[f"p{bus[3:]}"][t].abs().sum()
                        #
                        else:
                            tech_sum = 0
                        #
                        edge_width = max(0.1, tech_sum / max_activity)
                        G.add_edge(
                            # u_of_edge = edge_from,
                            u_of_edge=t,
                            v_of_edge=edge_to,
                            color=edge_color,
                            weight=edge_width,
                            style="solid",
                            label=t,
                        )
            #
            # otherwise there is only the column 'bus'
            else:
                bus = "bus"
                if c in ["Generator"]:
                    edge_from = t
                    edge_to = df[df.index == t][bus].values[0]
                #
                else:
                    edge_from = df[df.index == t][bus].values[0]
                    edge_to = t
                #
                edge_color = colors[colors.index == df.loc[t][bus]].color.iloc[0]
                if t in n.c[c].dynamic["p"]:
                    tech_sum = n.c[c].dynamic["p"][t].abs().sum()
                #
                else:
                    tech_sum = 0
                #
                if tech_sum > 0:
                    edge_width = max(0.1, tech_sum / max_activity)
                    G.add_edge(
                        u_of_edge=edge_from,
                        v_of_edge=edge_to,
                        color=edge_color,
                        weight=edge_width,
                        style="solid",
                        label=t,
                    )
    #
    # add load edges
    c = "Load"
    df = n.c[c].static.query("p_set > 0")
    #
    for load in df.index:
        edge_from = load
        edge_to = df.bus[load]
        edge_color = colors[colors.index == df.bus[load]].color.iloc[0]
        tech_sum = n.c[c].dynamic["p_set"][load].abs().sum()
        #
        if tech_sum > 0:
            edge_width = max(0.1, tech_sum / max_activity)
            G.add_edge(
                u_of_edge=edge_from,
                v_of_edge=edge_to,
                color=edge_color,
                weight=edge_width,
                style="solid",
                label=t,
            )
    #
    edge_colors = nx.get_edge_attributes(G, "color").values()
    weights = nx.get_edge_attributes(G, "weight").values()
    labels = dict([((n1, n2), d["label"]) for n1, n2, d in G.edges(data=True)])
    # styles = nx.get_edge_attributes(G, 'style').values()
    # fig = plt.figure(1, figsize=(20, 10), dpi=300)
    plt.figure(1, figsize=(16, 8), dpi=300)
    # pos = nx.circular_layout(G)
    # pos = nx.spring_layout(G, seed=5)
    # pos = nx.fruchterman_reingold_layout(G, k=0.54, iterations=100)
    pos = nx.arf_layout(G)
    nx.draw(
        G=G,
        pos=pos,
        edge_color=edge_colors,
        width=list(weights),
        node_color="lightgreen",
        font_size=12,
    )
    #
    nx.draw_networkx_edge_labels(
        G=G, pos=pos, edge_labels=labels, label_pos=0.5, font_color="black", font_size=6
    )
    #
    plt.title("Optimized energy system")
    plt.savefig(file_name, dpi=300)
    plt.show()
    plt.close()
    #
    return G


def remove_unused_details(n: pypsa.Network) -> pypsa.Network:
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
        df = n.c[c].static.query(f"({attr} == 0) & ({attr}_max == 0)")
        if len(df) > 0:
            unused.append(df.index)
            n.remove(c, df.index)
        #
    #
    print("\nremove unused technologies:")
    #
    for t in unused:
        print(f"x) {t.to_list()}")
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
        for col in [col for col in df.columns if col.startswith("bus")]:
            for t, row in df.iterrows():
                val1 = df[col].loc[t]
                #
                if (val1 != "") and (
                    (df[attr].loc[t] > 0) or (df[f"{attr}_max"].loc[t] > 0)
                ):
                    df1 = pd.concat([df1, pd.Series(df[col].loc[t])])
    #
    c = "Bus"
    df = n.c[c].static.index[~n.c[c].static.index.isin(df1)]
    if len(df) > 0:
        n.remove("Bus", df)
        print("\nunused buses:")
        #
        for bus in list(df):
            print(f"x) {bus}")
    #
    print()
    #
    c = "Load"
    df = n.c[c].static.index[~n.c[c].static.active]
    if len(df) > 0:
        n.remove("Load", df)
        print("unused loads:")
        #
        for load in list(df):
            print(f"x) {load}")
    #
    print()
    #
    return n


def adjust_for_rollinghorizon(n: pypsa.Network) -> pypsa.Network:
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
        if attr + "_extendable" in df.columns:
            df[attr + "_extendable"] = False
        #
        # curtailing technologies should be available just in case
        if "_curtail_option" in df.columns:
            df.loc[(df["_curtail_option"]) & (df[attr + "_opt"] > 0), attr] = np.inf
        #
        # emergency technologies should be available just in case
        if "_emergency_option" in df.columns:
            df.loc[df["_emergency_option"], attr] = np.inf
        #
        if "committable" in df.columns:
            df["committable"] = True
        #
        if "cyclic_state_of_charge" in df.columns:
            df["cyclic_state_of_charge"] = False
        #
        if "e_cyclic" in df.columns:
            df["e_cyclic"] = False
        #
        if "state_of_charge_initial" in df.columns:
            df["state_of_charge_initial"] = 0
        #
        if "e_initial" in df.columns:
            df["e_initial"] = 0
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
) -> list:
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
        f"{globals()['target_folder']}",
        f"{globals()['result_file']}_{scenario}.nc",
    )
    #
    if n.has_scenarios:
        no_cases = len(eval(globals()["stoch_case_definition"]))
    else:
        no_cases = 1
    #
    print("\nlist of optimized technologies:")
    comps = pypsa.descriptors.nominal_attrs
    data = []
    #
    # loop through all components
    for c in comps:
        df = n.c[c].static
        attr = comps[c]
        #
        for t in (
            df.query(f"{attr}_opt > " + str(globals()["small_limit"]))
            .index.get_level_values("name")
            .unique()
        ):
            if n.has_scenarios:
                data.append(
                    [
                        t,
                        df[df.index.get_level_values("name") == t][
                            f"{attr}_opt"
                        ].mean(),
                    ]
                )
            else:
                data.append([t, df[f"{attr}_opt"].loc[t]])
    #
    data.append(["_", "-"])
    data.append(["_duration(sec)", (n.duration)])
    data.append(["_timesteps(-)", (float(len(n.snapshots)))])
    data.append(["_memory(MBytes)", (getsize(n) / 10**6)])
    data.append(["__obj(mn)", (n.objective / 1e6)])
    data.append(
        [
            "__totex(mn)",
            (n.statistics.opex().sum().sum() + n.statistics.capex().sum().sum())
            / 1e6
            / no_cases,
        ]
    )
    data.append(
        [
            "___capex(mn)",
            (n.statistics.capex().sum().sum()) / 1e6 / no_cases,
        ]
    )
    data.append(
        [
            "___opex(mn)",
            (n.statistics.opex().sum().sum()) / 1e6 / no_cases,
        ]
    )
    #
    # convert data array into a DataFrame and set index
    df_res = pd.DataFrame(data, columns=["technology", scenario]).set_index(
        "technology"
    )
    #
    # show results summary table (capacities being built/used)
    print(df_res.sort_index())
    list_results.append(pd.DataFrame(df_res))
    #
    # prepare energy balance statistics
    if n.has_scenarios:
        levels = [0, 3]
    else:
        levels = [0, 1]
    #
    df = (
        pd.DataFrame(n.statistics.energy_balance().dropna())
        .groupby(level=levels)
        .mean()
    )
    df.columns = [scenario]
    list_supplies.append(df)
    #
    # prepare supply statistics
    if n.has_scenarios:
        levels = [0, 2]
    else:
        levels = [0, 1]
    #
    df = pd.DataFrame(n.statistics.supply().dropna()).groupby(level=levels).mean()
    df.columns = [scenario]
    list_balances.append(df)
    #
    # prepare supply statistics
    if n.has_scenarios:
        levels = [0, 2]
    else:
        levels = [0, 1]
    #
    df = pd.DataFrame(n.statistics.curtailment().dropna()).groupby(level=levels).mean()
    df.columns = [scenario]
    list_curtailments.append(df)
    #
    # optional: generate interactive result viewer (HTML)
    if network_viewer:
        html_viewer(
            n,
            file_name=f"{globals()['target_folder']}/{globals()['result_file']}_{scenario}.html",
            title=f"Network Analysis ({scenario})",
        )
    #
    # optional: generate SVG network
    if networkx_viewer:
        save_network_svg(
            n, f"{globals()['target_folder']}/{globals()['result_file']}_{scenario}.svg"
        )
    #
    return list_results, list_supplies, list_balances, list_curtailments


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
    print("\n" + "-" * 40 + "\n")
    df = pd.concat(list_results, axis=1).sort_index().replace(np.nan, "-")
    next_col = None
    col_pos = 1
    df_col = 1
    #
    for col in df.columns:
        # ignore the first column as it is not possible to compare it
        # against another result
        if next_col:
            new_col = f"{df_col}:{df_col + 1}"
            df.insert(col_pos, new_col, 0)
            #
            for index, row in df.iterrows():
                if index != "_":
                    if (
                        (df[next_col].loc[index] != "-")
                        and (df[col].loc[index] != "-")
                        and (df[next_col].loc[index] != 0)
                        and (df[col].loc[index] != 0)
                    ):
                        try:
                            df[new_col].loc[index] = (
                                df[col].loc[index] / df[next_col].loc[index]
                            )
                        except ZeroDivisionError:
                            print(
                                f"error! cannot calculate ratio for {index} ({next_col} / {col}): {df[col].loc[index]} / {df[next_col].loc[index]}"
                            )
                    #
                    # else:
                    #    df[new_col].loc[index] = "-"
            #
            next_col = col
            df_col = df_col + 1
            col_pos = col_pos + 2
        #
        else:
            next_col = col
    #
    print(f"{df}\n")
    print(f"{pd.concat(list_supplies, axis=1).sort_index().replace(np.nan, '-')}\n")
    print(f"{pd.concat(list_balances, axis=1).sort_index().replace(np.nan, '-')}\n")
    print(f"{pd.concat(list_curtailments, axis=1).sort_index().replace(np.nan, '-')}\n")
    #
    return df


def do_optimization(
    n: pypsa.Network,
    inv_periods: np.ndarray,
    kwargs: dict,
) -> tuple[pypsa.Network, str, str]:
    """
    Initiate the optimization run.

    Parameters
    ----------
    n: pypsa.Network
        PyPSA network to be adjusted.

    inv_periods: list
        List of years to assess.

    kwargs: dict
        List of keyword arguments

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
    globals()["do_segmentation"] = "True"
    globals()["segmentation_hours"] = 24
    #
    # check if time segmentation should be done
    if (tsam_avail) and (eval(globals()["do_segmentation"])):
        resolution = globals()["segmentation_hours"]
        hours = len(n.snapshots)
        # calculate number of segments equivalent to resolution
        segments = int(hours / resolution)
        #
        if not version.parse(PYPSA_VERSION) >= version.parse(PYPSA_VERSION_NEEDED):
            print(
                "\nerror! installed PyPSA version ({PYPSA_VERSION)) does not support segmentation! need at least v{PYPSA_VERSION_NEEDED}"
            )
            sys.exit(-1)
        #
        print(
            f"use segmentation {globals()['segmentation_hours']}h steps / {segments} segments ..."
        )
        #
        if globals()["segmentation_function"] == "resample":
            print(f"segmentation function: {globals()['segmentation_function']}")
            n.cluster.temporal.resample(f"{hours}h")
        #
        elif globals()["segmentation_function"] == "downsample":
            print(f"segmentation function: {globals()['segmentation_function']}")
            n.cluster.temporal.downsample(hours)
        #
        elif globals()["segmentation_function"] == "segment":
            print(f"segmentation function: {globals()['segmentation_function']}")
            if not n.has_periods:
                n.cluster.temporal.segment(segments)
            #
            else:
                print(
                    'warning! network has investment periods defined, therefore switch to "resample" ...'
                )
                n.cluster.temporal.resample(f"{hours}h")
        #
        else:
            print(
                f"error! undefined segmentation_function '{globals()['segmentation_function']}'"
            )
            sys.exit(-1)
    #
    start_time = dt.now()
    #
    # do the investment optimization (either pathway or myopic)
    if globals()["primary_optimization"] == "pathway":
        print("\ndo full horizon / pathway optimization")
        #
        # start optimizing the network
        if globals()["objective_function"] == "annuity+o&m":
            print("use standard min(annuity+O&M) target function")
            status, tc = n.optimize(
                snapshots=n.snapshots,
                solver_name=globals()["solver_name"],
                multi_investment_periods=False,
                extra_functionality=extra_functionalities,
                transmission_losses=int(globals()["transmission_losses"]),
                assign_all_duals=bool(globals()["assign_all_duals"]),
                include_objective_constant=True,
                # kwargs includes e.g., solver_options
                **kwargs,
            )
        #
        elif globals()["objective_function"] == "npv":
            print("use adjusted max(NPV) target function")
            n.optimize.create_model(include_objective_constant=True)
            m = n.model
            discount_rate = globals()["discount_factor"]
            investment_years = globals()["years_of_construction"]
            operation_years = globals()["years_of_operation"]
            df_capex = sum(
                1 / (1 + discount_rate) ** y for y in range(1, investment_years + 1)
            )
            df_opex = sum(
                1 / (1 + discount_rate) ** (investment_years + y)
                for y in range(1, operation_years + 1)
            )
            #
            capex = 0
            comps = pypsa.descriptors.nominal_attrs
            for c in comps:
                df = n.c[c].static
                #
                if f"{comps[c]}_extendable" in df.columns:
                    mask = df[f"{comps[c]}_extendable"]
                    var_name = f"{c}-{comps[c]}"
                    #
                    if mask.any():
                        var = m.variables[var_name].loc[mask.index[mask]]
                        capital_cost = df.loc[mask, "capital_cost"]
                        capex += (var * capital_cost).sum()
            #
            # is it assumed to invest capex in constant shares over the investment period
            capex /= investment_years
            #
            opex = 0
            weights = n.snapshot_weightings["objective"]
            col = "_bg_marginal_cost"
            for c in n.branch_components | n.one_port_components:
                if "marginal_cost" in n.c[c]["attrs"]:
                    if c in m.variables:
                        var = m.variables[f"{c}-p"]
                        mc = n.df(c)["marginal_cost"]
                        bg = 0
                        #
                        if col in df.cols:
                            bg = n.df(c)["_background_cost"]
                        #
                        opex += ((var * (mc + bg)) * weights).sum()
            #
            m.objective = -(opex * df_opex + capex * df_capex)
            m.objective.sense = "max"
            status = "nok"
            tc = "normal"
            #
            status, tc = n.optimize.solve_model(
                solver_name=globals()["solver_name"],
                multi_investment_periods=False,
                extra_functionality=extra_functionalities,
                transmission_losses=int(globals()["transmission_losses"]),
                assign_all_duals=bool(globals()["assign_all_duals"]),
                # kwargs includes e.g., solver_options
                **kwargs,
            )
        #
        else:
            print(
                f"\nerror! Provided objective function method not allowed: {globals()['objective_function']}"
            )
            sys.exit(0)

    #
    elif globals()["primary_optimization"] == "myopic":
        print("\ndo year by year / myopic optimization")
        #
        for period in inv_periods:
            sns = n.snapshots
            # limit the snapshots to the current year
            snapshots = sns[sns.get_level_values("period") == period]
            status, tc = n.optimize(
                snapshots=snapshots,
                multi_investment_periods=True,
                solver_name=globals()["solver_name"],
                extra_functionality=extra_functionalities,
                transmission_losses=int(globals()["transmission_losses"]),
                assign_all_duals=bool(globals()["assign_all_duals"]),
                include_objective_constant=True,
                # kwargs includes e.g., solver_options
                **kwargs,
            )
            #
            # if one execution was not successful, stop it
            if status != "ok":
                return n, status, tc
            #
            n.optimize.fix_optimal_capacities()
    #
    else:
        print(
            f"\nerror! Provided primary optimization method not allowed: {globals()['primary_optimization']}"
        )
        sys.exit(0)
    #
    end_time = dt.now()
    n.duration = (end_time - start_time).total_seconds()
    print(f"optimization duration: {round(n.duration, 2)}\n")
    #
    return n, status, tc


def do_all_runs(
    n: pypsa.Network,
    df_scens: pd.core.frame.DataFrame,
    df_stochs: pd.core.frame.DataFrame,
    kwargs: dict,
) -> tuple[pypsa.Network, list, list, list]:
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
    if globals()["run_scenarios"] == "False":
        print("info! only one scenario will be assessed\n")
    #
    for scenario in df_scens.scenario.unique():
        print("-" * 40 + "\n")
        print(f">>>>> initiate scenario '{scenario}'")
        # load the temporary model to ensure starting from the same point
        print("load the temporary network model ...")
        n, inv_periods = read_and_update_network(
            f"{globals()['target_folder']}/{globals()['temp_file']}", df_scens, scenario
        )
        #
        # do the investment optimization
        n, status, tc = do_optimization(n, inv_periods, kwargs)
        #
        # if optimization was successful, save the result and keep the
        # results so they can be shown at the end of all runs
        if status == "ok":
            list_results, list_supplies, list_balances, list_curtailments = (
                create_summaries(
                    n,
                    scenario,
                    list_results,
                    list_supplies,
                    list_balances,
                    list_curtailments,
                )
            )
            #
            if not eval(globals()["multi_investment_periods"]):
                w = n.investment_period_weightings.objective
            #
            else:
                w = [1]
            #
            m = n.model
            # save the results of the first run for potential MGA runs
            if eval(globals()["run_mga_runs"]):
                globals()["optimal_cost"] = m.objective.value
                globals()["fixed_cost"] = (
                    n.statistics.installed_capex(drop_zero=True).sum() * w
                ).sum()
            #
            # do dispatch after optimization
            if eval(globals()["run_rollinghorizon_after_optimization"]):
                print(
                    "\ndo rolling horizon (dispatch only) optimization after investment optimization ..."
                )
                horizon = globals()["rollinghorizon_horizon"]
                overlap = globals()["rollinghorizon_overlap"]
                print(f"using {horizon} hours horizon with {overlap} hours overlap")
                #
                # fix the optimal capacities
                n.optimize.fix_optimal_capacities()
                n = adjust_for_rollinghorizon(n)
                #
                # save conditions for later resetting
                save_cond_invest = globals()["do_investment_constraints"]
                #
                globals()["do_investment_constraints"] = "False"
                #
                start_time = dt.now()
                n.optimize.optimize_with_rolling_horizon(
                    horizon=horizon,
                    overlap=overlap,
                    solver_name=globals()["solver_name"],
                    extra_functionality=extra_functionalities,
                    transmission_losses=int(globals()["transmission_losses"]),
                    assign_all_duals=bool(globals()["assign_all_duals"]),
                    # kwargs includes e.g., solver_options
                    **kwargs,
                )
                #
                # restore the saved conditions
                globals()["do_investment_constraints"] = save_cond_invest
                #
                end_time = dt.now()
                n.duration = (end_time - start_time).total_seconds()
                print(f"optimization duration: {round(n.duration, 2)}\n")
                #
                list_results, list_supplies, list_balances, list_curtailments = (
                    create_summaries(
                        n,
                        f"{scenario}_rh",
                        list_results,
                        list_supplies,
                        list_balances,
                        list_curtailments,
                    )
                )
                #
                c = "Generator"
                df = n.c[c].static
                emerg_power = (
                    n.c[c].dynamic.p.sum()[df._emergency_option].sum().round(1)
                )
                print(f"\nconsumed emergency energy: {round(emerg_power, 2)} MWh")
    #
    return n, list_results, list_supplies, list_balances, list_curtailments, status, tc


# EXTRA FUNCTIONALITY FUNCTIONS ----------------------------------------------


def validate_technology_exists(n: pypsa.Network, c: str = None, t: str = None) -> bool:
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
    if (c in comps.keys()) and (
        t in df[df.index.get_level_values("name") == t].index.get_level_values("name")
    ):
        return True
    #
    else:
        print(c, t)
        print(f"info! technology {c}.{t} does not exist")
        return False


def link_capacities(n: pypsa.Network) -> None:
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
    col = "_linked_class"
    #
    # loop through all components
    for c in comps:
        df = n.c[c].static
        #
        if col in df.columns:
            # loop through all technology options of the class
            for t in df.index.get_level_values("name").unique():
                row = df.loc[t]
                #
                # the scenario name comes before the technology name
                if n.has_scenarios:
                    t = t[1]
                #
                # if column '_linked_class' is defined prepare adding the constraint
                if (
                    str(row[col]) not in ["", "nan"]
                    and "_linked_class" in row
                    and "_linked_technology" in row
                    and "_linked_sign" in row
                    and "_linked_multiplier" in row
                    and "_linked_rhs" in row
                ):
                    class1 = c
                    tech1 = t
                    class2 = row["_linked_class"]
                    tech2 = row["_linked_technology"]
                    sign = row["_linked_sign"]
                    factor1 = row["_linked_multiplier"]
                    factor2 = row["_linked_rhs"]
                    #
                    # validate if the linked technologies do exist
                    valid1 = validate_technology_exists(n, class1, tech1)
                    valid2 = validate_technology_exists(n, class2, tech2)
                    constr_name = f"Link-capacities-{class1}.{tech1}-{class2}.{tech2}"
                    #
                    # if the technology definition is valid, add the constraint
                    if valid1 and valid2:
                        # get the variables
                        nom_col1 = comps[class1]
                        nom_col2 = comps[class2]
                        #
                        link1_cap = m.variables[f"{class1}-{nom_col1}"].loc[tech1]
                        link2_cap = m.variables[f"{class2}-{nom_col2}"].loc[tech2]
                        #
                        # add the constraints
                        if constr_name not in m.constraints:
                            con = link1_cap - factor1 * link2_cap == factor2
                            con.sign = sign
                            m.add_constraints(con, name=constr_name)
                            print(f"x) added constraint {constr_name}")
                    #
                    else:
                        print(f'info! constraint "{constr_name}" is misconfigured')
    #
    return None


def link_operation(n: pypsa.Network) -> None:
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
    col = "_match_oper_class"
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
                if (
                    str(row[col]) not in ["", "nan"]
                    and "_match_oper_class" in row
                    and "_match_oper_technology" in row
                    and "_match_oper_sign" in row
                    and "_match_oper_multiplier" in row
                ):
                    class1 = c
                    tech1 = t
                    class2 = row["_match_oper_class"]
                    tech2 = row["_match_oper_technology"]
                    sign = row["_match_oper_sign"]
                    factor1 = row["_match_oper_multiplier"]
                    factor2 = 0  # val['_match_oper_rhs']
                    #
                    # validate if the linked technologies do exist
                    valid1 = validate_technology_exists(n, class1, tech1)
                    valid2 = validate_technology_exists(n, class2, tech2)
                    constr_name = f"Link-operations-{class1}.{tech1}-{class2}.{tech2}"
                    #
                    # if the technology definition is valid, add the constraint
                    if valid1 and valid2:
                        # get the variables
                        op_col1 = comps[class1][0]
                        op_col2 = comps[class2][0]
                        link1_flow = m.variables[f"{class1}-{op_col1}"].loc[:, tech1]
                        link2_flow = m.variables[f"{class2}-{op_col2}"].loc[:, tech2]
                        #
                        # add the constraints
                        con = link1_flow - factor1 * link2_flow == factor2
                        con.sign = sign
                        m.add_constraints(con, name=constr_name)
                        print(f"x) added constraint {constr_name}")
                    #
                    else:
                        print(f'info! constraint "{constr_name}" is misconfigured')
    #
    return None


def limit_hourly_operation_by_capacity(n: pypsa.Network) -> None:
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
    col = "_limit_op_class"
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
                if (
                    str(row[col]) not in ["", "nan"]
                    and "_limit_op_class" in row
                    and "_limit_op_technology" in row
                    and "_limit_op_class" in row
                    and "_limit_op_sign" in row
                    and "_limit_op_factor" in row
                ):
                    data.append(
                        [
                            c,
                            t,
                            df[col].loc[t],
                            df["_limit_op_technology"].loc[t],
                            df["_limit_op_sign"].loc[t],
                            df["_limit_op_factor"].loc[t],
                        ]
                    )
    #
    # convert it into a dataframe
    df = pd.DataFrame(
        data,
        columns=[
            "class",
            "technology",
            "limit_op_class",
            "limit_op_tech",
            "limit_op_sign",
            "limit_op_factor",
        ],
    )
    #
    # loop through the individual limiting technologies
    i = 0
    for t in df.limit_op_tech.unique():
        c = df.limit_op_class[df.limit_op_tech == t][0]
        valids = []
        rhs = []
        #
        # and collect the once referring to them
        for _, row in df[(df.limit_op_tech == t) & (df.limit_op_class == c)].iterrows():
            # validate the technology option
            valids.append(
                validate_technology_exists(
                    n, row["limit_op_class"], row["limit_op_tech"]
                )
            )
            factor = row["limit_op_factor"]
            col = f"{row['limit_op_class']}-{comps[row['limit_op_class']][0]}"
            #
            i = i + 1
            if i == 1:
                rhs = factor * m.variables[col].loc[:, row["limit_op_tech"]]
            #
            else:
                rhs = rhs + (factor * m.variables[col].loc[:, row["limit_op_tech"]])
        #
        # Get the hourly link flow variables for all technologies across all snapshots
        constr_name = f"Limit-operations-by-capacity-{c}_{t}"
        #
        if all(valids):
            # collect details to limit the sum for each time step
            nom_col = comps[c]
            #
            link_cap = m.variables[f"{c}-{nom_col}"].loc[t]
            #
            # add the constraint
            con = rhs <= link_cap
            con.sign = row.limit_op_sign
            m.add_constraints(con, name=constr_name)
            print(f"x) added constraint {constr_name}")
        #
        else:
            print(f'info! constraint "{constr_name}" is misconfigured')
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
    sns = n.snapshots
    # defines the variables and constraints only for the required technology options
    comps = pypsa.descriptors.nominal_attrs
    col = "_min_pu_if_in_op"
    #
    # loop through all components
    for c in comps:
        df = n.c[c].static
        if (len(df) > 0) and (col in df.columns):
            for t in df.query(f"{col} > 0.0").index:
                # create the binary status variable
                var_name = f"{c}-{t}-hourly-opstatus"
                if var_name not in m.variables:
                    status = m.add_variables(name=var_name, binary=True, coords=[sns])
                #
                else:
                    status = m.variables[var_name]
                #
                # get the variables
                op_col = comps[c][0]
                attr = comps[c]
                dispatch = m.variables[f"{c}-{op_col}"].loc[:, t]
                capacity = m.variables[f"{c}-{attr}"].loc[t]
                min_pu = df[col].loc[t]
                #
                # add the constraints
                constr_name = f"{c}-{t}-min_load_if_in_operation"
                if constr_name not in m.constraints:
                    m.add_constraints(
                        dispatch >= min_pu * capacity - bigM * (1 - status),
                        name=constr_name,
                    )
                    print(f"x) added constraint {constr_name}")
                #
                constr_name = f"{c}-{t}-is_online_if_in_operation"
                if constr_name not in m.constraints:
                    m.add_constraints(dispatch <= bigM * status, name=constr_name)
                    print(f"x) added constraint {constr_name}")
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
    col = "_capital_cost_if_inst"
    #
    # loop through all components
    for c in comps:
        df = n.c[c].static
        if (len(df) > 0) and (col in df.columns):
            for t in df.query(f"{col} > 0.0").index.get_level_values("name").unique():
                # create the binary status variable
                var_name = f"{c}-{t}-installed"
                if var_name not in m.variables:
                    idx = (
                        df[df.index.get_level_values("name") == t]
                        .index.get_level_values("name")
                        .unique()
                    )
                    is_installed = m.add_variables(
                        name=var_name,
                        binary=True,
                        # coords=[df.query(f'name == "{t}"').index])
                        coords=[idx],
                    )
                #
                else:
                    is_installed = m.variables[var_name]
                #
                # get the variables
                nom_col = comps[c]
                link_cap = m.variables[f"{c}-{nom_col}"].loc[t]
                #
                if n.has_scenarios:
                    invest = df[df.index.get_level_values("name") == t][col]
                else:
                    invest = df[col][t]
                #
                # add the constraints
                constr_name = f"{c}-{t}-is_not_installed"
                if constr_name not in m.constraints:
                    m.add_constraints(link_cap <= bigM * is_installed, name=constr_name)
                    print(f"x) added constraint {constr_name}")
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
    col = "_nom_min_if_inst"
    comps = pypsa.descriptors.nominal_attrs
    #
    # loop through all components
    for c in comps:
        df = n.c[c].static
        if (len(df) > 0) and (col in df.columns):
            for t in df.query(f"{col} > 0.0").index.get_level_values("name").unique():
                # create the binary status variable
                var_name = f"{c}-{t}-installed"
                if var_name not in m.variables:
                    idx = (
                        df[df.index.get_level_values("name") == t]
                        .index.get_level_values("name")
                        .unique()
                    )
                    is_installed = m.add_variables(
                        name=var_name,
                        binary=True,
                        # coords=[df.query(f'name == "{t}"').index])
                        coords=[idx],
                    )
                #
                else:
                    is_installed = m.variables[var_name]
                #
                # get the variables
                attr = comps[c]
                link_cap = m.variables[f"{c}-{attr}"].loc[t]
                min_cap = df[df.index.get_level_values("name") == t][col].unique()
                #
                # add the constraints
                constr_name = f"{c}-min_capacity_if_installed-{t}"
                if constr_name not in m.constraints:
                    m.add_constraints(
                        link_cap >= min_cap * is_installed, name=constr_name
                    )
                    print(f"x) added constraint {constr_name}")
                #
                constr_name = f"{c}-is_not_installed-{t}"
                if constr_name not in m.constraints:
                    m.add_constraints(link_cap <= bigM * is_installed, name=constr_name)
                    print(f"x) added constraint {constr_name}")
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
    col = "_bg_marginal_cost"
    #
    # loop through all components
    for c in comps:
        df = n.c[c].static
        if (len(df) > 0) and (col in df.columns):
            for t in df.query(f"{col} > 0.0").index.get_level_values("name").unique():
                # get the operational variable
                if c == "StorageUnit":
                    op_col = f"{comps[c][0]}_store"
                #
                else:
                    op_col = comps[c][0]
                #
                if n.has_scenarios:
                    tech_flow = m.variables[f"{c}-{op_col}"].loc[:, t]
                    # adjust the objective function
                    m.objective += (
                        tech_flow.sum()
                        * df[df.index.get_level_values("name") == t][col]
                    )
                else:
                    tech_flow = m.variables[f"{c}-{op_col}"].loc[:, t]
                    # adjust the objective function
                    m.objective += tech_flow.sum() * df[col][t]
                #
                print(f"x) added constraint {c}.{t}.{col}")
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
    col = "_shared_potential"
    df_list = []
    #
    # loop through all components
    for c in comps:
        df = n.c[c].static
        if (not df.empty) and (col in df.columns):
            attr = comps[c]
            df[col] = df[col].astype(str)
            df_copy = df[(df[col] > "") & (df[col] != "nan")].copy()
            df_copy["technology_type"] = c
            df_list.append(df_copy[["technology_type", col, attr + "_max"]])
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
            link_cap = m.variables[f"{c[1].technology_type}-{attr}"].loc[index]
            max_cap = df_concat[attr + "_max"][index]
            #
            if max_cap > 0:
                # link1_cap = m.variables[f'{class1}-{nom_col1}'].loc[tech1]
                con = con + link_cap / max_cap
            #
            else:
                print(f"warning! technology {index} needs to define its {attr}")
        #
        if type(con) is linopy.expressions.LinearExpression:
            constr_name = f"Shared-potential-{t}"
            m.add_constraints(con <= 1, name=constr_name)
            print(f"x) added constraint {constr_name}")
        else:
            print(f"info! nothing to do: {con}, {t}")
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
    col = "_limit_cap_string"
    #
    # loop through all components
    for c in comps:
        df = n.c[c].static
        #
        if col in df.columns:
            if df[df[col].fillna("") > ""][col].unique().any():
                for tech in df[df[col] > ""][col].unique():
                    constr_name = f"Force-capacity-{tech}"
                    con = 0
                    cap = 0
                    #
                    # loop through all technology options of the individual class
                    for t, row in df[df[col] == tech].iterrows():
                        sign = row["_limit_cap_sign"]
                        cap = row["_limit_cap"]
                        #
                        # get capacity variable
                        nom_col = comps[c]
                        #
                        link_cap = m.variables[f"{c}-{nom_col}"].loc[t]
                        #
                        con += link_cap
                    #
                    con2 = con <= cap
                    con2.sign = sign
                    #
                    # add the constraints
                    m.add_constraints(con2, name=constr_name)
                    print(f"x) added constraint {constr_name}")
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
    optimal_cost = globals()["optimal_cost"]
    fixed_cost = globals()["fixed_cost"]
    #
    objective = m.objective
    if not isinstance(
        objective, (linopy.LinearExpression | linopy.QuadraticExpression)
    ):
        objective = objective.expression
    #
    if "mga_slack" in globals():
        slack = globals()["mga_slack"]
    #
    else:
        slack = 0.01
    #
    constr_name = "MGA-budget-constraint"
    m.add_constraints(
        objective + fixed_cost >= (1 + slack) * optimal_cost, name=constr_name
    )
    print(f"x) added constraint {constr_name}")
    #
    return None


def add_REN_matching(
    n: pypsa.Network,
    share: float = 0.8,
    frequency: str = "year",
    renewable_carriers: list = ["wind"],
):
    """
    Adds a minimum renewable/wind share constraint over a specified time resolution.

    Parameters
    ----------
    n: pypsa.Network
        PyPSA network to get the details from.
    [...]

    Returns
    -------
    None
    """
    #
    print("add_REN_matching not ready yet ...")
    #
    return None
    #
    # TODO
    #
    m = n.model
    sns = n.snapshots
    res_gens = n.generators.query("carrier in @renewable_carriers").index
    if res_gens.empty:
        raise ValueError("No matching generators found for specified carriers.")
    gen_p = m["Generator-p"]
    gen_dim = [d for d in gen_p.dims if d != "snapshot"][0]
    weights = n.snapshot_weightings.generators.loc[sns]
    weighted_gen_p = gen_p * weights
    res_gen = weighted_gen_p.sel({gen_dim: res_gens}).sum(dim=gen_dim)
    tot_gen = weighted_gen_p.sum(dim=gen_dim)
    freq = frequency.lower()
    if freq in ["hour"]:
        pass
    elif freq in ["day", "week", "month"]:
        group_freq = {
            "day": "D",
            "week": "W",
            "month": "MS",
        }[freq]
        snapshot_dates = pd.Series(sns, index=sns)
        grouping = snapshot_dates.dt.to_period(group_freq).astype(str)
        res_gen = res_gen.groupby(grouping).sum()
        tot_gen = tot_gen.groupby(grouping).sum()
    elif freq in ["year", "total"]:
        res_gen = res_gen.sum(dim="snapshot")
        tot_gen = tot_gen.sum(dim="snapshot")
    lhs = res_gen - (share * tot_gen)
    constr_name = f"wind_share_{freq}"
    m.add_constraints(lhs >= 0, name=constr_name)
    return None


def add_ramping_costs(n: pypsa.Network):
    """
    Adds constraints to include cost on chaning output on top of the marginal costs.

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
    sns = n.snapshots
    if len(sns) < 2:
        return
    #
    comps = pypsa.descriptors.nominal_attrs
    col = ["_ramp_up_cost", "_ramp_down_cost"]
    #
    # loop through all components
    for c in comps:
        df = n.c[c].static
        attr = comps[c][:1]
        #
        # check if the necessary columns exists in current dataframe
        missing = set(col) - set(df.columns)
        if not missing:
            # only consider options that have any ramp cost defined
            tech_mask = (df.get(col[0], 0) != 0) | (df.get(col[1], 0) != 0)
            ramping_tech = df.index[tech_mask]
            if ramping_tech.empty:
                continue
            #
            # create named linopy coordinates
            snapshot_idx = pd.Index(sns[1:], name="snapshot")
            tech_idx = pd.Index(ramping_tech, name=c)
            #
            # add ramp-up and ramp-down decision variables for filtered technology options
            delta_p_up = m.add_variables(
                lower=0, coords=[snapshot_idx, tech_idx], name=f"{c}-delta_p_up"
            )
            delta_p_down = m.add_variables(
                lower=0,
                coords=[snapshot_idx, tech_idx],
                name=f"{c}-delta_p_down",
            )
            #
            # slice power variables for the selected technology options across time
            p = m.variables[f"{c}-{attr}"].loc[:, ramping_tech]
            p_curr = p.loc[sns[1:]]
            p_prev = p.loc[sns[:-1]].assign_coords(snapshot=sns[1:])
            #
            # add linear balance constraint across time and technology options
            m.add_constraints(
                p_curr - p_prev == delta_p_up - delta_p_down, name=f"{c}-ramp_balance"
            )
            # extract ramping cost parameters aligned with ramping_gens
            cost_up = df.loc[ramping_tech, "_ramp_up_cost"]
            cost_down = df.loc[ramping_tech, "_ramp_down_cost"]
            #
            # add objective terms (Linopy broadcasts series/dataarrays automatically)
            m.objective += (delta_p_up * cost_up + delta_p_down * cost_down).sum()
    #
    return None


def block_constraints(n: pypsa.Network):
    """
    Adds constraints for N-1 operation of generators and storage units.

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
    sns = n.snapshots
    #
    comps = pypsa.descriptors.nominal_attrs
    col = "_hours_per_block"
    n_sns = len(sns)
    #
    for c_name, var_suffix in comps.items():
        df = n.components[c_name].static
        if df.empty or col not in df.columns:
            continue
        #
        hours_series = df[col].fillna(1).astype(int)
        var_key = f"{c_name}-{var_suffix[0]}"
        if var_key not in m.variables:
            continue
        #
        var_obj = m.variables[var_key]
        dim_name = [d for d in var_obj.dims if d != "snapshot"][0]
        active_names = var_obj.coords[dim_name].values
        #
        for block_size, group in df.groupby(hours_series):
            if block_size <= 1:
                continue
            #
            elem_names = pd.Index(group.index).intersection(active_names)
            if elem_names.empty:
                continue
            #
            block_ids = np.arange(n_sns) // block_size
            block_start_mask = block_ids != np.roll(block_ids, 1)
            block_start_mask[0] = True
            ref_indices = np.where(block_start_mask)[0]
            ref_snapshots = sns[ref_indices[block_ids]]
            var_sub = var_obj.loc[{dim_name: elem_names}]
            var_ref = var_sub.sel(snapshot=ref_snapshots).assign_coords(snapshot=sns)
            m.add_constraints(
                var_sub - var_ref == 0,
                name=f"{c_name}-{var_suffix}_block_{block_size}h",
            )


def reserve_constraints(
    n: pypsa.Network,
) -> None:
    """
    Adds constraints for reserve margin consideration.
    The following column in the individual DataFrames needs to be defined:
        _rm_participation

    Parameters
    ----------
    n: pypsa.Network
        PyPSA network to get the details from.

    Returns
    -------
    None
    """
    #
    print("fct reserve_constraints not active yet!")
    #
    return True
    #
    # TODO needs some tuning, or?
    # x) reserve contribution is limited by ramp rates
    # x) also links should be able to contribute (e.g., charger, discharger)
    #    but only if the battery has capacity to support the relevant action
    #    e.g. storage level above reserve contribution for at least X hours (1, or what?)
    # x) reserve at least the size of the largest unit being built or is built
    #
    m = n.model
    sns = n.snapshots
    col = "_rm_participation"
    comps = pypsa.descriptors.nominal_attrs
    #
    # reserve_req >= Fa * ( ( Fb * load + Fc**2 )**(Fd) ) - Fe
    Fa = globals()["rm_factor_a"]  # 1
    Fb = globals()["rm_factor_b"]  # 0.15
    Fc = globals()["rm_factor_c"]  # 0
    Fd = globals()["rm_factor_d"]  # 1
    Fe = globals()["rm_factor_e"]  # 0
    #
    # loop through all components
    for c in ["Generator"]:  # comps:
        df = n.c[c].static
        if col in df.columns:
            #
            # create the binary status variable
            var_name = f"{c}-p_reserve_up"
            if var_name not in m.variables:
                v_rp = m.add_variables(lower=0, name=var_name, coords=[sns, df.index])
            #
            else:
                v_rp = m.variables[var_name]
            #
            # get the variables
            attr = comps[c]
            link_cap = m.variables[f"{c}-{attr}"]
            # dispatch = m.variables[f'{c}-{attr[0]}']
            # reserve = m.variables[var_name]
            #
            rm_class = globals()["rm_load_class"]
            rm_tech = globals()["rm_load_name"]
            load = n.c[rm_class].static[attr[0] + "_set"][rm_tech]
            #
            # add the constraints
            constr_name = "Global-hourly_reserve_requirement-equation"
            m.add_constraints(
                v_rp.sum("name") >= Fa * ((Fb * load + Fc**2) ** (Fd)) - Fe,
                name=constr_name,
            )
            #
            constr_name = f"{c}-reserve_up-limit-available_capacity"
            m.add_constraints(
                link_cap - m.variables[f"{var_name}"] - m.variables[f"{c}-{attr[0]}"]
                >= 0,
                name=constr_name,
                coords=[sns, df.index],
            )
            #
            constr_name = f"{c}-reserve_up-limit-min_pu"
            m.add_constraints(
                v_rp <= df[col] * (1 - df[attr[0] + "_min_pu"]) * link_cap,
                name=constr_name,
                coords=[sns, df.index],
            )
            #
            constr_name = f"{c}-reserve_up-limit-ramp_up"
            m.add_constraints(
                v_rp <= df[col] * df["ramp_limit_up"].fillna(1) * link_cap,
                name=constr_name,
                coords=[sns, df.index],
            )
            #
            """
            if eval(globals()['rm_max_generator']):
                constr_name = f'{c}-sum_of_reserves-largest_unit'
                m.add_constraints(
                    # v_rp.sum('name') >= n_reserve.static.generators.p_nom.max(),
                    c = 'Generator'
                    v_rp.sum('name') >= n.c[c].static.p_nom.max() * 1,
                    name=constr_name)
            """
            #
            # add cost to the objective function
            tech_flow = m.variables[f"{var_name}"]
            m.objective += (tech_flow * n.c[c].static.marginal_cost).sum()
    #
    return None


def limit_operation_of_emergency_technology(
    n: pypsa.Network,
    bigM: float = 1e8,
) -> None:
    """
    Limit the operation of emergency operators (in hours per year).

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
    sns = n.snapshots
    col = "_max_op_hours"
    comps = pypsa.descriptors.nominal_attrs
    #
    # loop through all components
    for c in comps:
        df = n.c[c].static
        #
        if col in df.columns:
            for t, row in df[df[col] > 0].iterrows():
                # the scenario name comes before the technology name
                if n.has_scenarios:
                    t = t[1]
                #
                # create the binary status variable
                var_name = f"{c}-{t}-hourly-opstatus"
                if var_name not in m.variables:
                    status = m.add_variables(name=var_name, binary=True, coords=[sns])
                #
                else:
                    status = m.variables[var_name]
                #
                # get the variables
                op_col = comps[c][0]
                dispatch = m.variables[f"{c}-{op_col}"].loc[:, t]
                #
                constr_name = f"{c}-{t}-is_online_if_in_operation"
                if constr_name not in m.constraints:
                    m.add_constraints(dispatch <= bigM * status, name=constr_name)
                #
                # loop through available years
                for y in sns.to_frame().period.unique():
                    constr_name = f"{c}-{t}-limit_hours_of_op-{y}"
                    if constr_name not in m.constraints and abs(row[col]) != np.inf:
                        m.add_constraints(
                            status.loc[y].sum() <= float(row[col]), name=constr_name
                        )
                        print(f"added {constr_name}")
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
    sns = n.snapshots
    comps = pypsa.descriptors.nominal_attrs
    col = "_strict_binary_op"
    df_list = []
    #
    # loop through all components
    for c in ["Link"]:  # comps:
        df = n.c[c].static
        #
        if (not df.empty) and (col in df.columns):
            df[col] = df[col].astype(str)
            df_copy = df[(df[col] > "") & (df[col] != "nan")].copy()
            df_copy["technology_type"] = c
            df_list.append(df_copy[["technology_type", col]])
    #
    df_concat = pd.concat(df_list, sort=False)
    #
    # get unique shared potential strings
    for idx in df_concat.groupby(col, axis=0).count().iterrows():
        # valid configurations have two entries
        if idx[1][0] != 2:
            print(
                f"warning! configuration of strict binary operation {idx[0]} is not correct"
            )
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
            charge_p = m.variables[f"{c1}-{attr1[0]}"].loc[:, f"{t1}"]
            discharge_p = m.variables[f"{c2}-{attr2[0]}"].loc[:, f"{t2}"]
            #
            var_name = f"{str_idx}-operation1_bin"
            if var_name not in m.variables:
                bin_var = m.add_variables(
                    binary=True,
                    dims=["snapshot"],
                    coords={"snapshot": sns},
                    name=var_name,
                )
            #
            else:
                bin_var = m.variables[var_name]
            #
            var_name = f"{str_idx}-operation2_bin"
            if var_name not in m.variables:
                one = m.add_variables(
                    lower=1.0,
                    upper=1.0,
                    dims=["snapshot"],
                    coords={"snapshot": sns},
                    name=var_name,
                )
            #
            else:
                one = m.variables[var_name]
            #
            constr_name = f"{str_idx}-strict-on-charge"
            m.add_constraints(charge_p <= bigM * bin_var, name=constr_name)
            print(f"x) added constraint {constr_name}")
            constr_name = f"{str_idx}-strict-on-discharge"
            m.add_constraints(discharge_p <= bigM * (one - bin_var), name=constr_name)
            print(f"x) added constraint {constr_name}")
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
    m = n.model
    constr_name = "Kirchhoff-Voltage-Law"
    if constr_name in m.constraints:
        m.remove_constraints(constr_name)
        print(f"removed {constr_name}")
    #
    return None


def consider_retirement_gains(
    n: pypsa.Network,
) -> None:
    """
    Cost reduction by unused equipment. Very simple implementation for now.
    The following column in the individual DataFrames needs to be defined:
        _retirement_gain

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
    col = "_retirement_gain"
    #
    # loop through all components
    for c in comps:
        df = n.c[c].static
        attr = comps[c]
        #
        if (f"{c}-{attr}" in m.variables) and (col in df.columns):
            # only take the candidates where changes are allowed based on the
            # provided data (p_nom, p_nom_min, and p_nom_max are not equal)
            dfs = df[
                (df[f"{attr}"] != df[f"{attr}_min"])
                & (df[f"{attr}"] != df[f"{attr}_max"])
            ]
            #
            if len(dfs) > 0:
                final_cap = m.variables[f"{c}-{attr}"].loc[
                    dfs.index.get_level_values("name").unique()
                ]
                initial_cap = dfs[attr].unique()
                minimum_cap = dfs[f"{attr}_min"].unique()
                retirement_cost = dfs[col].unique()
                #
                if retirement_cost.sum() > 0:
                    # add variable to decide if retirement tool place
                    var_name = f"{c}-retirement"
                    if var_name not in m.variables:
                        retired = m.add_variables(
                            # lower=0,
                            coords=[dfs.index.get_level_values("name").unique()],
                            name=var_name,
                        )
                    #
                    else:
                        retired = m.variables[var_name]
                    #
                    # add constraint to consider potential retirement
                    constr_name = f"{c}-retirement-limit1"
                    if constr_name not in m.constraints:
                        m.add_constraints(
                            retired >= initial_cap - final_cap, name=constr_name
                        )
                        print(f"added {constr_name}")
                    #
                    # add constraint to consider potential retirement
                    constr_name = f"{c}-retirement-limit2"
                    if constr_name not in m.constraints:
                        m.add_constraints(
                            final_cap - minimum_cap >= 0, name=constr_name
                        )
                        print(f"added {constr_name}")
                    #
                    # adjust the objectiv function
                    m.objective += retirement_cost * retired
    #
    return None


def add_maintenance_constraints(
    n: pypsa.Network,
    method: str = "monthly",
) -> None:
    """
    Optimize maintenance start.
    The following column in the individual DataFrames needs to be defined:
        _opt_maint
        _maint_duration

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
    sns = n.snapshots
    comps = pypsa.descriptors.nominal_attrs
    col = "_opt_maint"
    #
    for c in comps:
        if col in n.c[c].static.columns:
            attr = comps[c]
            techs = n.c[c].static.index
            #
            if method == "daily":
                # allow one start per day (midnight)
                start_times = sns[sns.hour == 0]
            elif method == "weekly":
                # allow one start per week (Monday 00:00)
                start_times = sns[(sns.weekday == 0) & (sns.hour == 0)]
            elif method == "monthly":
                # allow one start per month (Day 1, 00:00)
                start_times = sns[(sns.day == 1) & (sns.hour == 0)]
            elif method == "hourly":
                # otherwise allow start every hour
                start_times = sns
            #
            # Binary maintenance start variable
            m.add_variables(
                binary=True, coords=[start_times, techs], name=f"{c}-maintenance_start"
            )
            #
            z = m[f"{c}-maintenance_start"]
            constr_name = f"{c}-one_maintenance_per_option"
            m.add_constraints(z.sum(dim="snapshot") == 1, name=constr_name)
            #
            for t in techs:
                for t_start in start_times:
                    idx_start = sns.get_loc(t_start)
                    affected = sns[
                        idx_start : idx_start + n.c[c].static.at[t, "_maint_duration"]
                    ]
                    #
                    if len(affected) > 0:
                        constr_name = f"{c}-{t}-maintenance_off_{str(t_start).replace('-', '')[0:8]}"
                        m.add_constraints(
                            n.model[f"{c}-{attr[0]}"].loc[affected, t]
                            <= n.c[c].static.at[t, attr] * (1 - z.loc[t_start, t]),
                            name=constr_name,
                        )
    #
    return None


# --------------------------------------------------------------------------- #
# helpers for add_primary_reserves()
# --------------------------------------------------------------------------- #
def _static(n: pypsa.Network, ctype: str) -> pd.DataFrame:
    return n.components[ctype].static


def _as_dense(n: pypsa.Network, ctype: str, attr: str, sns) -> pd.DataFrame:
    c = n.components[ctype]
    if hasattr(c, "as_dynamic"):  # v2 API
        return c.as_dynamic(attr, sns)
    return n.get_switchable_as_dense(ctype, attr, sns)  # legacy fallback


def _to_da(df: pd.DataFrame, sns, idx: pd.Index) -> xr.DataArray:
    return xr.DataArray(
        np.asarray(df.loc[sns, idx], dtype=float),
        coords=[sns, idx],
        dims=["snapshot", "name"],
    )


def _const(s: pd.Series, idx: pd.Index) -> xr.DataArray:
    return xr.DataArray(np.asarray(s[idx], dtype=float), coords=[idx], dims=["name"])


def _param(val, idx: pd.Index, default=None) -> xr.DataArray:
    """Scalar or per-unit mapping -> DataArray over `name`."""
    if isinstance(val, (pd.Series, dict)):
        s = pd.Series(val, dtype=float).reindex(idx)
        if default is not None:
            s = s.fillna(float(default))
        if s.isna().any():
            raise ValueError(f"missing value for {list(idx[s.isna()])}")
    else:
        s = pd.Series(float(val), index=idx)
    return xr.DataArray(s.to_numpy(float), coords=[idx], dims=["name"])


def _sel(var, sns, n, **kw):
    """Select a name subset and, if needed, a snapshot subset of a linopy variable."""
    v = var.sel(**kw)
    if len(sns) != len(n.snapshots):
        v = v.sel(snapshot=sns)
    return v


def _check(n, ctype, idx):
    missing = idx.difference(_static(n, ctype).index)
    if len(missing):
        raise KeyError(f"unknown {ctype}: {list(missing)}")


def add_primary_reserves(
    n: pypsa.Network,
    # TODO move them into the PyPSA network or at least into the Assumption Book
    cost=2.0,  # EUR/MW per snapshot; scalar or per-unit mapping
    dur_min: float = 15.0,  # minutes the reserve must be sustained
    ramp_pu_per_min: float = 0.05,  # generators
    storage_ramp_pu_per_min=0.20,  # storage units; None = no ramp limit
    h_min: float | None = None,  # MWs of required system inertia
    inertia=None,  # pd.Series of H [s], indexed by unit name
    big_m: float | None = None,  # fallback cap for committable+extendable gens
    store_p_nom=None,  # MW rating of Store units (they have none in PyPSA)
) -> None:
    #
    col = "_rm_participation"
    #
    generators = []
    storage_units = []
    stores = []
    #
    if col in n.c["Generator"].static.columns:
        generators = (
            n.c["Generator"].static.index[n.c["Generator"].static[col]].to_list()
        )
    #
    if col in n.c["StorageUnit"].static:
        storage_units = (
            n.c["StorageUnit"].static.index[n.c["StorageUnit"].static[col]].to_list()
        )
    #
    if col in n.c["Store"].static:
        stores = n.c["Store"].static.index[n.c["Store"].static[col]].to_list()
    #
    m = n.model
    sns = n.snapshots
    dur_h = float(dur_min) / 60.0
    obj = 0
    blocks = []  # one entry per component: {"ru": var, "p": expr at risk, "tag": str}
    #
    # ======================================================================= #
    # generators
    # ======================================================================= #
    idx = pd.Index(generators, name="name")
    if len(idx):
        _check(n, "Generator", idx)
        g = _static(n, "Generator").loc[idx]
        ext = g.p_nom_extendable.astype(bool)
        com = (
            g.committable.astype(bool)
            if "committable" in g
            else pd.Series(False, index=idx)
        )
        fix = ~ext
        #
        i_ext, i_com = idx[ext], idx[com]
        i_fix_only = idx[fix & ~com]
        i_com_ext, i_com_fix = idx[com & ext], idx[com & fix]
        #
        cap = g.p_nom.where(fix, g.p_nom_max)  # big-M capacity
        if big_m is not None:
            cap = cap.replace(np.inf, big_m)
        if len(i_com_ext) and not np.isfinite(cap[i_com_ext]).all():
            bad = list(i_com_ext[~np.isfinite(cap[i_com_ext])])
            raise ValueError(
                "committable + extendable generators need a finite p_nom_max "
                f"or an explicit big_m: {bad}"
            )
        #
        k = float(ramp_pu_per_min) * float(dur_min)  # p.u. of p_nom within dur_min
        if k <= 0:
            raise ValueError("ramp_pu_per_min * dur_min must be positive")
        #
        ru = m.add_variables(lower=0, coords=[sns, idx], name="Generator-r_up")
        p = _sel(m.variables["Generator-p"], sns, n, name=idx)
        pmax = _to_da(_as_dense(n, "Generator", "p_max_pu", sns), sns, idx)
        p_nom = m.variables["Generator-p_nom"] if len(i_ext) else None
        st = (
            _sel(m.variables["Generator-status"], sns, n, name=i_com)
            if len(i_com)
            else None
        )
        #
        # headroom: p + r_up <= p_max_pu * p_nom * status
        if len(i_fix_only):
            m.add_constraints(
                p.sel(name=i_fix_only) + ru.sel(name=i_fix_only)
                <= pmax.sel(name=i_fix_only) * _const(g.p_nom, i_fix_only),
                name="Reserve-headroom_fix",
            )
        if len(i_ext):
            m.add_constraints(
                p.sel(name=i_ext)
                + ru.sel(name=i_ext)
                - pmax.sel(name=i_ext) * p_nom.sel(name=i_ext)
                <= 0,
                name="Reserve-headroom_ext",
            )
        if len(i_com):
            m.add_constraints(
                p.sel(name=i_com)
                + ru.sel(name=i_com)
                - (pmax.sel(name=i_com) * _const(cap, i_com)) * st
                <= 0,
                name="Reserve-headroom_status",
            )
        #
        # ramp limit: r_up <= k * p_nom * status
        if len(i_fix_only):
            m.add_constraints(
                ru.sel(name=i_fix_only) <= k * _const(g.p_nom, i_fix_only),
                name="Reserve-ramp_limit_fix",
            )
        if len(i_ext):
            m.add_constraints(
                ru.sel(name=i_ext) - k * p_nom.sel(name=i_ext) <= 0,
                name="Reserve-ramp_limit_ext",
            )
        if len(i_com):
            m.add_constraints(
                ru.sel(name=i_com) - (k * _const(cap, i_com)) * st <= 0,
                name="Reserve-ramp_limit_status",
            )
        #
        obj = obj + (_param(cost, idx) * ru).sum()
        blocks.append({"ru": ru, "p": p, "tag": "gen"})
    #
    # ======================================================================= #
    # storage units  (discharge headroom + backed-off charging, energy limited)
    # ======================================================================= #
    sidx = pd.Index(storage_units, name="name")
    if len(sidx):
        _check(n, "StorageUnit", sidx)
        s = _static(n, "StorageUnit").loc[sidx]
        sext = s.p_nom_extendable.astype(bool)
        i_sext, i_sfix = sidx[sext], sidx[~sext]
        #
        ru_s = m.add_variables(lower=0, coords=[sns, sidx], name="StorageUnit-r_up")
        pdis = _sel(m.variables["StorageUnit-p_dispatch"], sns, n, name=sidx)
        psto = _sel(m.variables["StorageUnit-p_store"], sns, n, name=sidx)
        soc = _sel(m.variables["StorageUnit-state_of_charge"], sns, n, name=sidx)
        pmax_s = _to_da(_as_dense(n, "StorageUnit", "p_max_pu", sns), sns, sidx)
        s_nom = m.variables["StorageUnit-p_nom"] if len(i_sext) else None
        #
        # power: p_dispatch + r_up - p_store <= p_max_pu * p_nom
        # (reserve may be delivered by ramping up discharge *or* stopping charging)
        if len(i_sfix):
            m.add_constraints(
                pdis.sel(name=i_sfix) + ru_s.sel(name=i_sfix) - psto.sel(name=i_sfix)
                <= pmax_s.sel(name=i_sfix) * _const(s.p_nom, i_sfix),
                name="Reserve-su_headroom_fix",
            )
        if len(i_sext):
            m.add_constraints(
                pdis.sel(name=i_sext)
                + ru_s.sel(name=i_sext)
                - psto.sel(name=i_sext)
                - pmax_s.sel(name=i_sext) * s_nom.sel(name=i_sext)
                <= 0,
                name="Reserve-su_headroom_ext",
            )
        #
        # energy: delivering r_up for dur_h hours must be covered by the SOC
        m.add_constraints(
            dur_h * ru_s - _const(s.efficiency_dispatch, sidx) * soc <= 0,
            name="Reserve-su_energy",
        )
        #
        # optional converter ramp limit
        if storage_ramp_pu_per_min is not None:
            ks = _param(storage_ramp_pu_per_min, sidx) * float(dur_min)
            if len(i_sfix):
                m.add_constraints(
                    ru_s.sel(name=i_sfix)
                    <= ks.sel(name=i_sfix) * _const(s.p_nom, i_sfix),
                    name="Reserve-su_ramp_limit_fix",
                )
            if len(i_sext):
                m.add_constraints(
                    ru_s.sel(name=i_sext) - ks.sel(name=i_sext) * s_nom.sel(name=i_sext)
                    <= 0,
                    name="Reserve-su_ramp_limit_ext",
                )
        #
        obj = obj + (_param(cost, sidx) * ru_s).sum()
        blocks.append({"ru": ru_s, "p": pdis - psto, "tag": "su"})
    #
    # ======================================================================= #
    # stores  (no p_nom in PyPSA -> rating must be supplied; energy limited)
    # ======================================================================= #
    eidx = pd.Index(stores, name="name")
    if len(eidx):
        _check(n, "Store", eidx)
        e_s = _static(n, "Store").loc[eidx]
        eext = e_s.e_nom_extendable.astype(bool)
        i_eext, i_efix = eidx[eext], eidx[~eext]

        ru_e = m.add_variables(lower=0, coords=[sns, eidx], name="Store-r_up")
        pe = _sel(m.variables["Store-p"], sns, n, name=eidx)
        en = _sel(m.variables["Store-e"], sns, n, name=eidx)
        emin = _to_da(_as_dense(n, "Store", "e_min_pu", sns), sns, eidx)
        e_nom = m.variables["Store-e_nom"] if len(i_eext) else None

        # power rating (a Store's power is limited by whatever link feeds it)
        if store_p_nom is not None:
            m.add_constraints(
                pe + ru_e <= _param(store_p_nom, eidx), name="Reserve-store_headroom"
            )
        #
        # energy: r_up * dur_h <= e - e_min_pu * e_nom
        if len(i_efix):
            m.add_constraints(
                dur_h * ru_e.sel(name=i_efix) - en.sel(name=i_efix)
                <= -emin.sel(name=i_efix) * _const(e_s.e_nom, i_efix),
                name="Reserve-store_energy_fix",
            )
        if len(i_eext):
            m.add_constraints(
                dur_h * ru_e.sel(name=i_eext)
                - en.sel(name=i_eext)
                + emin.sel(name=i_eext) * e_nom.sel(name=i_eext)
                <= 0,
                name="Reserve-store_energy_ext",
            )
        #
        obj = obj + (_param(cost, eidx) * ru_e).sum()
        blocks.append({"ru": ru_e, "p": pe, "tag": "store"})
    #
    if not blocks:
        raise ValueError("no reserve providers given")
    #
    # ======================================================================= #
    # N-1: the pooled reserve, minus the lost unit's own reserve, covers its output
    # ======================================================================= #
    total = blocks[0]["ru"].sum("name")
    for b in blocks[1:]:
        total = total + b["ru"].sum("name")
    for b in blocks:
        m.add_constraints(total - b["ru"] - b["p"] >= 0, name=f"Reserve-N-1_{b['tag']}")

    # ======================================================================= #
    # inertia:  sum_i H_i * p_nom_i * status_i >= h_min
    # ======================================================================= #
    if h_min is not None:
        if inertia is None:
            raise ValueError("inertia must be given when h_min is set")
        H = pd.Series(inertia, dtype=float)
        terms, offset = [], 0.0

        if len(idx):
            Hg = H.reindex(idx)
            if Hg.isna().any():
                raise ValueError(f"missing inertia for {list(idx[Hg.isna()])}")

            if len(i_fix_only):  # always on, fixed -> constant
                offset += float((Hg[i_fix_only] * g.p_nom[i_fix_only]).sum())

            i_on_ext = idx[ext & ~com]  # always on, extendable
            if len(i_on_ext):
                terms.append(
                    (_const(Hg, i_on_ext) * p_nom.sel(name=i_on_ext)).sum("name")
                )

            if len(i_com_fix):  # committable, fixed
                terms.append(
                    (_const(Hg * g.p_nom, i_com_fix) * st.sel(name=i_com_fix)).sum(
                        "name"
                    )
                )

            if len(i_com_ext):  # bilinear -> McCormick on z
                z = m.add_variables(
                    lower=0, coords=[sns, i_com_ext], name="Generator-p_nom_on"
                )
                pn_ce, st_ce = p_nom.sel(name=i_com_ext), st.sel(name=i_com_ext)
                cap_ce = _const(cap, i_com_ext)
                m.add_constraints(z - pn_ce <= 0, name="Reserve-p_nom_on_cap")
                m.add_constraints(z - cap_ce * st_ce <= 0, name="Reserve-p_nom_on_off")
                m.add_constraints(
                    z - pn_ce - cap_ce * st_ce >= -cap_ce, name="Reserve-p_nom_on_lb"
                )
                terms.append((_const(Hg, i_com_ext) * z).sum("name"))

        # storage units: synthetic/grid-forming inertia, no commitment variable,
        # so the contribution is available whenever the converter capacity exists
        if len(sidx):
            Hs = H.reindex(sidx).fillna(0.0)
            if (Hs[i_sfix] != 0).any():
                offset += float((Hs[i_sfix] * s.p_nom[i_sfix]).sum())
            i_h_ext = i_sext[Hs[i_sext] != 0]
            if len(i_h_ext):
                terms.append(
                    (_const(Hs, i_h_ext) * s_nom.sel(name=i_h_ext)).sum("name")
                )

        if terms:
            lhs = terms[0]
            for t in terms[1:]:
                lhs = lhs + t
            m.add_constraints(lhs >= float(h_min) - offset, name="Reserve-inertia")
        elif offset < float(h_min):
            raise ValueError(
                f"inertia requirement {h_min} MWs exceeds the fixed fleet ({offset} MWs)"
            )

    # ======================================================================= #
    # cost of holding reserve
    # (multiply by n.snapshot_weightings.objective.loc[sns] if you want it weighted)
    # ======================================================================= #
    m.objective += obj


def add_startup_types(
    n: pypsa.Network,
    sns,
    types,
) -> None:
    """
    Add hot and warm startup into PyPSA.
        _maint_duration   ?????

    Parameters
    ----------
    n: pypsa.Network
        PyPSA network to get the details from.

    Returns
    -------
    None
    """
    #
    col = "_rm_participation"
    units = ()
    #
    if col in n.generators.static.columns:
        units = n.generators.static.index[
            n.generators.static["_rm_participation"]
        ].to_list()
    #
    m = n.model
    idx = pd.Index(units, name="name")
    su = m["Generator-start_up"].sel(name=idx)
    st = m["Generator-status"].sel(name=idx)
    warmer, c_cold = [], [c for lab, _, _, c in types if lab == "cold"][0]
    for label, t0, t1, cost in types:
        if label == "cold":
            continue
        d = m.add_variables(
            lower=0, upper=1, coords=[sns, idx], name=f"Generator-start_{label}"
        )
        window = sum(st.shift(snapshot=i) for i in range(t0 + 1, t1 + 1))
        m.add_constraints(d - window <= 0, name=f"Startup-{label}_window")
        warmer.append((d, cost))
    m.add_constraints(sum(d for d, _ in warmer) - su <= 0, name="Startup-exclusive")
    m.objective += (c_cold * su).sum() + sum(
        ((c - c_cold) * d).sum() for d, c in warmer
    )


def extra_functionalities(
    n: pypsa.Network, sns: pd.core.indexes.datetimes.DatetimeIndex
) -> None:
    """
    Adds extra functionalities into the PyPSA model above the features provided
    out of the shelf.

    Parameters
    ----------
    n: pypsa.Network
        PyPSA network to get the details from.

    sns: pd.core.indexes.datetimes.DatetimeIndex
        Snapshots being considered in the current PyPSA model.

    Returns
    -------
    None
    """
    #
    # LP functionalities
    print("add LP constraints if/as needed ...")
    #
    if eval(globals()["remove_kvl_constraints"]):
        remove_KVL_constraints(n)
    #
    background_marginal_cost(n)
    #
    if eval(globals()["do_operational_constraints"]):
        link_operation(n)
        limit_hourly_operation_by_capacity(n)
        add_ramping_costs(n)
        block_constraints(n)
        #
        add_REN_matching(
            n,
            share=globals()["cfe_matching"],
            frequency=globals()["cfe_type"],
            renewable_carriers=globals()["cfe_rens"],
        )
    #
    if eval(globals()["do_investment_constraints"]):
        link_capacities(n)
        shared_technology_potential(n)
        force_technology_capacity(n)
        consider_retirement_gains(n)
        #
        if eval(globals()["rm_activate"]):
            if not n.has_scenarios:
                reserve_constraints(n)
        #
        if eval(globals()["nminus1_activate"]):
            if not n.has_scenarios:
                # equivalent to N-1 from generation perspective
                add_primary_reserves(n)
    #
    if (eval(globals()["run_mga_runs"])) and (globals()["mga_slack"] > 0):
        mga_settings(n)
    #
    # MILP functionalities
    if eval(globals()["do_milp_constraints"]):
        print("add MILP constraints if/as needed ...\n")
        #
        if eval(globals()["do_strict_unsimultaneous_dis+charging"]):
            strict_unsimultaneous_charging_discharging(n)
        #
        if eval(globals()["do_operational_constraints"]):
            minimum_load_if_operates(n)
            limit_operation_of_emergency_technology(n)
        #
        if eval(globals()["do_investment_constraints"]):
            invest_if_installed(n)
            min_capacity_if_installed(n)
        #
        if eval(globals()["do_maintenance_planing"]):
            add_maintenance_constraints(n, globals()["maintenance_mode"])
        #
        # TODO 'statup_types' needs to come from assumption file
        startup_types = [
            ("hot", 1, 3, 200.0),
            ("warm", 3, 8, 800.0),
            ("cold", 8, None, 2500.0),
        ]
        add_startup_types(n, sns, startup_types)
    #
    print("")
    return None


def is_gurobi_configured():
    try:
        import gurobipy as gp
        from gurobipy import GRB

        with gp.Env() as env, gp.Model(env=env) as m:
            env.setParam("OutputFlag", 0)
            env.setParam("LogToConsole", 0)
            env.start()
            # source: https://www.gurobi.com/documentation/9.5/quickstart_mac/cs_example_mip1_py.html
            m = gp.Model("mip1", env=env)
            x = m.addVar(vtype=GRB.BINARY, name="x")
            y = m.addVar(vtype=GRB.BINARY, name="y")
            z = m.addVar(vtype=GRB.BINARY, name="z")
            m.setObjective(x + y + 2 * z, GRB.MAXIMIZE)
            m.addConstr(x + 2 * y + 3 * z <= 4, "c0")
            m.addConstr(x + y >= 1, "c1")
            m.optimize()
            del env, m, x, y, z
            solver_name = "gurobi"
            use_oetc = "False"
    #
    except ValueError:
        solver_name = "highs"
        use_oetc = "False"
    #
    return (solver_name, use_oetc)


def check_oetc_usage(
    kwargs: dict,
) -> dict:
    """
    Check if OETC should be used.

    Parameters
    ----------
    kwargs: dict
        Dictionary of keyword arguments.

    Returns
    -------
    kwargs: dict
        Dictionary of keyword arguments.
    """
    #
    if eval(globals()["use_oetc"]):
        try:
            print("info! try to initiate OETC ...")
            globals()["solver_name"] = "gurobi"
            # set OETC settings
            rndStr = random_string(6)
            oetc = {}
            oetc["name"] = f"PyPSA-X-{rndStr}"
            oetc["authentication_server_url"] = "https://auth.oetcloud.com"
            oetc["orchestrator_server_url"] = "https://orchestrator.oetcloud.com"
            oetc["compute_provider"] = (ComputeProvider.GCP,)
            oetc["cpu_cores"] = 4  # adjust as needed; RAM = #CPUs * 8 [GB]
            oetc["disk_space_gb"] = 20  # adjust as needed [GB]
            oetc["delete_worker_on_error"] = False
            oetc["credentials"] = OetcCredentials(
                email=os.environ["OETC_EMAIL"], password=os.environ["OETC_PASSWORD"]
            )
            oetc["solver"] = globals()["solver_name"]
            oetc["solver_options"] = get_solver_setting()
            oetc_settings = OetcSettings(**oetc)
            oetc_handler = OetcHandler(oetc_settings)
            kwargs["remote"] = oetc_handler
            print("info! initiated OETC successfully")
        #
        except Exception as e:
            print(f"info! not able to initiate OETC\n{e}")
            #
            globals()["solver_name"], globals()["use_oetc"] = is_gurobi_configured()
    #
    else:
        print("info! use local solver")
        #
        globals()["solver_name"], globals()["use_oetc"] = is_gurobi_configured()
    #
    kwargs["solver_options"] = get_solver_setting()
    print(f"info! solver to use: {globals()['solver_name']}\n")
    #
    return kwargs


def pypsa_to_long_format(
    networks: pypsa.Network | dict[str, pypsa.Network],
    include_static: bool = True,
    include_dynamic: bool = True,
    drop_na: bool = True,
) -> pd.DataFrame:
    """
    Melt all static and dynamic components of PyPSA network(s) into a unified long-format DataFrame.

    Parameters
    ----------
    networks : pypsa.Network or dict[str, pypsa.Network]
        A single PyPSA Network object or a dictionary mapping scenario names to Network objects.

    include_static : bool, optional
        Whether to include static attributes (e.g., p_nom, bus, carrier).

    include_dynamic : bool, optional
        Whether to include time-series attributes (e.g., p, p_max_pu).

    drop_na : bool, optional
        Whether to drop rows with missing (NaN) values.

    Returns
    -------
    pd.DataFrame
        ['scenario', 'component', 'component_name', 'attribute', 'snapshot', 'value']
    """
    #
    # Normalize input to a dictionary of scenarios
    if isinstance(networks, pypsa.Network):
        scenarios_dict = {"default": networks}
    #
    else:
        scenarios_dict = networks
    #
    long_frames = []
    for scenario_name, net in scenarios_dict.items():
        for c in n.components:
            comp_name = c.name
            #
            # static DataFrames
            if include_static and not c.static.empty:
                static_df = c.static.copy()
                static_df.index.name = "component_name"
                static_df = static_df.reset_index()
                static_df.rename(columns={"name": "component_name"}, inplace=True)
                #
                static_melted = pd.melt(
                    static_df,
                    id_vars=["component_name"],
                    var_name="attribute",
                    value_name="value",
                )
                static_melted["scenario"] = scenario_name
                static_melted["component"] = comp_name
                static_melted["snapshot"] = pd.NA  # Static data has no timestamp
                long_frames.append(static_melted)
            #
            # dynamic (Time-varying) DataFrames
            if include_dynamic and hasattr(c, "dynamic"):
                # Iterate through dynamic attributes (e.g., 'p', 'p_max_pu', 'status')
                for attr, dynamic_df in c.dynamic.items():
                    if isinstance(dynamic_df, pd.DataFrame) and not dynamic_df.empty:
                        dynamic_df = dynamic_df.copy()
                        dynamic_df.index.name = "snapshot"
                        dynamic_df = dynamic_df.reset_index()
                        dynamic_df.rename(
                            columns={"name": "component_name"}, inplace=True
                        )
                        #
                        dynamic_melted = pd.melt(
                            dynamic_df,
                            id_vars=["snapshot"],
                            var_name="component_name",
                            value_name="value",
                        )
                        dynamic_melted["scenario"] = scenario_name
                        dynamic_melted["component"] = comp_name
                        dynamic_melted["attribute"] = attr
                        long_frames.append(dynamic_melted)
    #
    if not long_frames:
        return pd.DataFrame(
            columns=[
                "scenario",
                "component",
                "component_name",
                "attribute",
                "snapshot",
                "value",
            ]
        )
    #
    # Combine all melted DataFrames
    full_df = pd.concat(long_frames, ignore_index=True)
    #
    # Standardize column order
    cols_order = [
        "scenario",
        "component",
        "component_name",
        "attribute",
        "snapshot",
        "value",
    ]
    full_df = full_df[cols_order]
    #
    if drop_na:
        full_df = full_df.dropna(subset=["value"])
    #
    return full_df


def main() -> None:
    """
    Main function.

    Parameters
    ----------
    None

    Returns
    -------
    n : pypsa.Network
        The last PyPSA network after all runs.

    list_results : list
        List of results from all runs.

    list_supplies : list
        List of supply results from all runs.

    list_balances : list
        List of balance results from all runs.

    list_curtailments : list
        List of curtailment results from all runs.

    df : pd.DataFrame
        DataFrame containing capacity comparison of all runs.
    """
    #
    # read the optimization settings and scenario definitions
    opt_params, df_scens, df_stochs = read_all_params(input_source, input_type)
    # check the OETC usage and settings
    globals()["kwargs"] = {}
    # set solver options
    kwargs = check_oetc_usage(globals()["kwargs"])
    #
    # create and/or get the base network
    read_network_data(
        input_source,
        input_type,
        globals()["target_folder"],
        globals()["temp_file"],
    )
    #
    # validate if all scenario adjustments are in general ok
    all_adjustments_ok = validate_scenario_adjustments(
        f"{globals()['target_folder']}/{globals()['temp_file']}", df_scens
    )
    #
    # get the active scenarios only
    unique_scens = df_scens.sort_values("scenario").scenario.unique()
    print(
        f"{len(unique_scens)} active scenario(s) (ordered by name): {list(unique_scens)}\n"
    )
    print(
        f"{len(df_scens)} active lines for scenario adjustments:\n{df_scens.to_string(index=False)}"
    )
    #
    if eval(globals()["run_stochastic_runs"]):
        print(f"{len(df_stochs)} active lines for stochastic adjustments:\n", df_stochs)
    #
    list_results = []
    list_supplies = []
    list_balances = []
    list_curtailments = []
    #
    # import the just created PyPSA network details and check for consistency
    print("\nload the temporary network model ...")
    n, inv_periods = read_and_update_network(
        f"{globals()['target_folder']}/{globals()['temp_file']}",
        pd.core.frame.DataFrame(),
        df_scens.scenario.unique()[0],
        True,
    )
    #
    if all_adjustments_ok:
        n, list_results, list_supplies, list_balances, list_curtailments, status, tc = (
            do_all_runs(n, df_scens, df_stochs, kwargs)
        )
        #
        if status != "ok":
            print("\nError! check your settings!")
            print(
                f'optimization ended with status "{status}" and termination code "{tc}"\n'
            )
        #
        if len(list_results) > 1:
            df = show_case_comparison(
                list_results, list_supplies, list_balances, list_curtailments
            )
            #
            df.to_excel(
                f"{globals()['target_folder']}/run_comparison.xlsx", engine="openpyxl"
            )
        #
        else:
            df = pd.DataFrame()
    #
    else:
        df = pd.DataFrame()
        print("\nerror! Could not validate all scenario settings")
    #
    return n, list_results, list_supplies, list_balances, list_curtailments, df


# MAIN ------------------------------------------------------------------------

if __name__ == "__main__":
    input_source, input_type = parse_arguments()
    # main()
    n, list_results, list_supplies, list_balances, list_curtailments, df = main()
