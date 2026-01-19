# PyPSA-X - PyPSA for PtX and Microgrid projects

**PyPSA-X** is an open-source Python framework for optimising and simulating **power-to-anything
(PtX) projects** as well as **microgrid projects**. It builds on top of [PyPSA](https://github.com/PyPSA/PyPSA)
which comes with the following main features:
- Economic Dispatch (ED),
- Linear Optimal Power Flow (LOPF),
- Security-Constrained LOPF (SCLOPF),
- Capacity Expansion Planning (CEP),
- Pathway Planning,
- Stochastic Optimisation,
- Modelling-to-Generate-Alternatives (MGA),
- Static Power Flow Analysis, and 
- Sector-Coupling.

Especially the <ins>sector-coupling usage</ins> is of interest for **PyPSA-X** which is made for project 
developers, and industry needing an easy-to-use and transparent tool 
for power-to-X and energy system analysis.

## Features
- **marginal background cost**: add marginal costs of operation towards the objective function
without having them as part of the overall investment and operational costs;
- **link technology operation**: link technology operation to ensure proper operation of e.g., 
electorlyzers on green electricity only;
- **limit hourly operation**: limit the hourly operation of several technology options compared 
with another technology size (e.g., green and grey power purchase and a transformer station);
- **link of technology capacity**: link the capacity of technologies (e.g., storage charger is 
equal to storage discharger);
- **shared technology potential**: limit the expansion of technologies based on a joined limitation
(e.g., different wind turbines with a land limitation);
- **forced technology capacity**:  force the capacity built to be above or below a certain value
(e.g., at least 100 MW of any wind turbine technology);
- **strict unsimultaneous operation**: make sure that 2 technology options can’t operate at the 
same time (e.g., dis-/charging);
- **minimum load if in operation**: limits the operation of a technology to a given value as 
minimum operation, but no operation is allowed;
- **investment if installed**: consider an investment if a technology option is selected (e.g, 
cost for ground preparation);
- **minimum capacity if installed**: limits new installed capacity with a lower value, but does
not force the installation of this capacity.

## Usage
``` py
$ python pypsa-x.py AB_v0.9.1.xlsx
```

This executes the **PyPSA-X** script and reads the assumption book 'AB_v0.9.1.xlsx' and follows
the configuration within the worksheets <ins>opt_params</ins>, and <ins>scen_params</ins>. The sheet
opt_params contains options to guide the PyPSA-X script (e.g., target folder to store the results;
OETC settings). The sheet scen_params contains options of which variables to change between the 
optimization of different scenarios.
A more detailed description will follow soon.

## Dependencies
**PyPSA-X** relies heavily on other open-source Python packages. The most important once are:
- [PyPSA](https://github.com/PyPSA/PyPSA) for optimising and simulating modern power and energy systems;
- [linopy](https://github.com/PyPSA/linopy) for preparing linear optimisation problems;
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
make [pull requests}(https://github.com/gincrement/PyPSA-X/pulls) on GitHub.

## Licence

Copyright [PyPSA-X Contributors]

PyPSA-X is licensed under the open source [MIT License](LICENSES/MIT.txt)
