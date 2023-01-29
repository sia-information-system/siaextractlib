# OMDEPEXTRACTLIB

Python library as part of the OMDEP project. Its goal is to provide
an easy to use interface that connects with some open oceanic and
meteorological data repositories to extract slices of data programaticaly.

## Table of content

- [Dependencies](#dependencies)
- [Install](#install)
  - [Production mode](#production-mode)
  - [Development mode](#development-mode)
- [Tests](#tests)
- [CLI](#cli)

## Dependencies

**NOTE**: If you want to use this repository for development,
create a virtual environment named `venv` in the root directory
and install the required dependencies in there.

- xarray
- netCDF4
- motuclient
- Dask

## Install

The following instructions cover a local installation.
See [local installation](https://pip.pypa.io/en/stable/topics/local-project-installs/) for details.

### Production mode

To install the package in production mode, run the following command in the
package root directory (where de `pyproject.toml` file is located):

```
pip install .
```

### Development mode

To install the package in development mode (--editable), run the following command
in the package root directory (where de `pyproject.toml` file is located):

``` sh
pip install --editable .
```

## Tests

**NOTE:** You need to first create and fill the `etc/config.ini` file
before running the tests. See the template `etc/config.ini.example`.

Once done, at the project root directory, run the following command
to start the testing module:

``` sh
python tests/test_lib_copernicus_marine.py
```

## CLI

This package includes a command line interface to use the capabilities
of the library without implement it in code.

The CLI is named `omdepextract`, so call

``` sh
omdepextract -h
```

to show the manual.

To use it, you must create an specification file with the details of
the data to extract. See the `etc/copernicus_marine.json.example`
for reference. You can store your custom specification files in `etc/cli-conf/`

Example call for the cli:

``` sh
omdepextract etc/cli-conf/copernicus-marine.json -a get-size

omdepextract etc/cli-conf/copernicus-marine.json -a=extract
```
