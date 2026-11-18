<!--
SPDX-FileCopyrightText: PyPSA-X Contributors
SPDX-License-Identifier: MIT
-->

# PyPSA-X - PyPSA for PtX and micro-grid (µ-grid) projects

**PyPSA-X** is an open-source Python framework for optimizing and simulating **power-to-anything
(PtX) projects** as well as **micro-grid projects**. It builds on top of
[PyPSA](https://github.com/PyPSA/PyPSA) which comes with the following main features:
- **Economic Dispatch** (ED, production cost modelling),
- **Linear Optimal Power Flow** (LOPF),
- **Security-Constrained LOPF** (SCLOPF),
- **Capacity Expansion Planning** (CEP),
- **Pathway Planning**,
- **Stochastic Optimisation**,
- **Modelling-to-Generate-Alternatives** (MGA),
- **Static Power Flow** Analysis, and
- **Sector-Coupling**.

Especially the sector-coupling feature is of interest for **PyPSA-X** which is made for project
developers, and industry needing an easy-to-use and transparent tool
for power-to-X power and energy system analysis.

For further information about <ins>PyPSA</ins> please check its  extensive
[documentation](https://docs.pypsa.org) with tutorials, user guides, examples and an API reference.

## Features
- **marginal background cost**: add marginal costs of operation towards the objective function
without having them as part of the overall investment and operational costs;
- **link technology operation**: link technology operation to ensure proper operation of e.g.,
electrolyzers on green electricity only;
- **limit hourly operation**: limit the hourly operation of several technology options compared
with another technology size (e.g., green and grey power purchase and a transformer station);
- **link of technology capacity**: link the capacity of technologies (e.g., storage charger is
equal to storage discharger);
- **shared technology potential**: limit the expansion of technologies based on a joined limitation
(e.g., different wind turbines with a land limitation);
- **forced technology capacity**:  force the capacity built to be above or below a certain value
(e.g., at least 100 MW of any wind turbine technology);
- **strict un-simultaneous operation**: make sure that 2 technology options can’t operate at the
same time (e.g., dis-/charging);
- **minimum load if in operation**: limits the operation of a technology to a given value as
minimum operation, but no operation is allowed;
- **investment if installed**: consider an investment if a technology option is selected (e.g,
cost for ground preparation);
- **minimum capacity if installed**: limits new installed capacity with a lower value, but does
not force the installation of this capacity;
- **modular representation**: represents each technology option as individual module instead of
collection of multiple units;
- **time series aggregation**: determines typical operation periods or decreases the temporal
resolution to accelerate model optimization or experiment runs.
- **run scenarios**: adjust individual settings between different optimization runs (e.g,.
different investment costs, different technology options);
- **simulation after optimization**: runs a rolling horizon optimization after an successful
optimization for a more accurate operational behavior.
- **consider retirement gains**: allow retirement of technology which decreases the annual
maintenance cost;
- **N-1 operation**: consider a strict N-1 operation for advanced reliability, can be provided
by generators, storage units or stores;
- **limit operating hours**: option to limit the operation of e.g., emergency technology towards a
predefined number of hours (e.g., 3 h/a);
- **cost for changing output**: add additional cost above the marginal cost for changing the
power output;
- **block decision**: limit dispatch decisions to be every x hours instead of every timestep
(e.g. hourly);

### DRAFT:
- **reserve margin**: adds a preliminary version of operational reserve considerations;
- **hourly match**: fullfil given hourly matching constraints by e.g., year, month, week, day,
and hour;
- **"primary reserve**: add constraints to add primary reserve capacity;
- **hot, warm, cold startup**: add hot, warm, and cold startup cost considerations;

### IDEAS:
- **country, bidding zone, reserve area**: distinguish between different region definitions
for different purposes > need to add this columns in all PyPSA classes able to participate
in the different markets; country given by x- and y-coordinate?

## Installation

### Clone the Repository
First of all, clone the **PyPSA-X** repository using the version control system **git** in the
command line or download the ZIP file from the GitHub server manually.

``` bash
git clone https://github.com/gincrement/PyPSA-X
```

### Install Python Dependencies
**PyPSA-X** relies on a set of other Python packages to function. We manage these using **pixi**.
Once [pixi](https://pixi.prefix.dev/latest/installation/) is installed, you can activate the
project environment for your operating system and have access to all the **PyPSA-X** dependencies
from the command line:

``` bash
pixi shell
```

## Usage
`pypsa-x.py` reads a case in one of two ways: from an Excel assumption book (`--xls`) or
from a case folder of YAML and CSV files (`--folder`, the default). Both build the same PyPSA
network; the two options cannot be combined.

### Option 1: Excel assumption book
``` bash
python pypsa-x.py --xls PyPSA_PtX_AB_default.xlsx
```

This executes the **PyPSA-X** script, reads the assumption book 'PyPSA_PtX_AB_default.xlsx'
and follows the configuration within the worksheets
<ins>opt_params</ins>, <ins>scen_params</ins>, and <ins>stoch_params</ins>.
The sheet <ins>opt_params</ins> contains options to guide the PyPSA-X script (e.g., target folder
to store the results; OETC settings).
The sheet <ins>scen_params</ins> contains options of which variables to change between the
optimization of different scenarios.

### Option 2: case folder (YAML + CSV)
``` bash
python pypsa-x.py --folder cases/default
```

With `--folder`, `pypsa-x.py` reads a case folder instead of an Excel file (`cases/default` if
neither `--folder` nor `--xls` is given). All inputs are plain text, so changes can be
reviewed and compared with git.

```
cases/default/
├── case.yaml              settings, assumptions, scenarios, stochastic cases, time series mapping
├── technologies.csv       technology parameters (former AB sheet, technical block)
├── network/               topology: carriers, buses, generators, links, stores, ... (one CSV each)
└── timeseries/
    └── profiles.csv       hourly raw profiles (former raw_data sheet)
```

#### Create a case from an existing assumption book
``` bash
python pypsa-x.py --xls-to-case PyPSA_PtX_AB_default.xlsx cases/my_project
```
The converter uses the values Excel calculated, so the workbook must have been saved in Excel.
It prints every asset value it had to keep as an explicit override (see below). Use `--force`
to overwrite an existing folder.

#### case.yaml
| Section       | Former sheet              | Content |
|---------------|---------------------------|---------|
| `settings`    | opt_params                | optimization settings (debug, solver, segmentation, constraints, MGA, ...) |
| `assumptions` | AB (economic block)       | years, financing, project data, production target |
| `scenarios`   | scen_params               | changes per scenario (`set`, `del`, `active`, `inactive`) |
| `stochastic`  | stoch_params              | changes per stochastic case |
| `timeseries`  | `generators-p_max_pu`, `loads-p_set`, ... | which profile feeds which PyPSA time series, optionally scaled |

Values the assumption book calculated with formulas are derived automatically and are not part
of the file: WACC, annuity factor, equity share, end year, construction period, hourly
production target, solver time limit, investment period (`[y_cod]`), etc.

Example scenario definition:
``` yaml
scenarios:
  cheapPV:
    set:
      Python: {transmission_losses: 0}               # change a script setting
      Generator:
        PV_01: {capital_cost: 8446.58}               # change a component attribute
  noBES02:
    del:
      StorageUnit: [BES_02]                          # remove a component
  noMinPuIfInOp:
    active: false                                    # keep the scenario, but do not run it
    set:
      Link:
        FT_01: {_min_pu_if_in_op: 0}
```

#### technologies.csv and network/*.csv
- `technologies.csv` lists one parameter per row: `component,parameter,value,unit,...`.
  A `capital_cost` of `auto` is calculated as `invest_cost * annuity_factor + fom * invest_cost`.
- Each `network/*.csv` row is one asset. Its `_component` column names the technology in
  `technologies.csv`. **Empty cells are filled from `technologies.csv`** (and the carrier and unit
  from the connected bus); **a filled cell overrides** that value for this asset only.

#### Validation
`case.yaml` is checked against [case.schema.json](case.schema.json) every time it is read.
Typos in keys, invalid choices (e.g. `segmentation.function`), and out-of-range values stop the
run with a list of all problems and their location in the file. Editors with YAML support (e.g.
VS Code with the YAML extension) use the schema for autocompletion and inline checks, through the
first line of `case.yaml`:
``` yaml
# yaml-language-server: $schema=../../case.schema.json
```

## Dependencies
**PyPSA-X** relies heavily on other open-source Python packages. The most important once are:
- [PyPSA](https://github.com/PyPSA/PyPSA) for optimizing and simulating modern power and energy systems;
- [linopy](https://github.com/PyPSA/linopy) for preparing linear optimization problems;
- [pandas](http://github.com/pandas-dev/pandas) for storing data about components and time series.

**PyPSA-X** can be used with different solvers. For instance, the free solvers such as
- [HiGHS](https://highs.dev/) (installed by default),
- [GLPK](https://www.gnu.org/software/glpk/), and
- [CBC](https://github.com/coin-or/Cbc/)

or commercial solvers like
- [Gurobi](http://www.gurobi.com/), and
- [FICO Xpress](https://www.fico.com/en/products/fico-xpress-optimization).

## Contributing and Support
We strongly welcome anyone interested in contributing to this project. If you have any ideas, suggestions
or encounter problems, feel invited to file [issues](https://github.com/gincrement/PyPSA-X/issues) or
make [pull requests](https://github.com/gincrement/PyPSA-X/pulls) on GitHub.

## Licence
Copyright [PyPSA-X Contributors]

PyPSA-X is licensed under the open source [MIT License](LICENSES/MIT.txt)
