# SIAEXTRACTLIB

**Version: 0.2.0**

Python library as part of the SIA project. Its goal is to provide
an easy to use interface that connects with oceanic data repositories
to extract subsets of data programaticaly, taking care of the in-between
steps, like network failures and size limitations from the source.

Page of the project: https://sia-information-system.github.io/sia-website

## Table of content

- [Install](#install)
  - [Production mode](#production-mode)
  - [Development mode](#development-mode)
- [Tests](#tests)
- [Documentation](#documentation)
- [Contributions](#contributions)

## Install

### Production mode

You can install this package from PyPI using:

``` bash
pip install siaextractlib
```

To install the package from source code, clone the repo from GitHub and
run the following command in the package root directory 
(where the `pyproject.toml` file is located):

``` bash
pip install .
```

See [local installation](https://pip.pypa.io/en/stable/topics/local-project-installs/) for details.

### Development mode

**NOTE**: For development, it's recommended to use an isolated environment.
You can use tools like `anaconda` / `minionda` or `virtualenv` to create
this kind of environments. We recommend `miniconda`. If you store the
environment in the root directory of the project, name the environment as
`venv` sin this name directory is ignored by git.

To install the package in development mode (--editable), run the following command
in the package root directory (where de `pyproject.toml` file is located):

``` sh
pip install --editable .[dev]
```

To increase the version number, the package `bumpver` is used.
[Read the docs](https://github.com/mbarkhau/bumpver#reference)
to see how to use it.

## Tests

You need to first create and fill the `etc/config.ini` file
before running the tests. See the template `etc/config.ini.example`.
This file contains your credentials for
[Copernicus](https://marine.copernicus.eu/).

Once done, at the test directory, run the following command
to start the testing module:

``` sh
python test_lib_opendap.py
```

Tests are written using the `unittest` framework, so alternative ways of running
the tests are described in its [documentation](https://docs.python.org/3/library/unittest.html).

## Documentation

Read the documentation for this package [here](./docs/README.md).

## Contributions

Rules are:

- First ask to maintainers if the new proposed feature can be added. You can open an issue on GitHub.
- Document every new feature added.
- Add tests for each new feature.
