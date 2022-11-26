# OMDEPEXTRACTLIB

Python library as part of the OMDEP project. Its goal is to provide
an easy to use interface that connects with some open oceanic and
meteorological data repositories to extract slices of data programaticaly.

## Table of content

- [Dependencies](#dependencies)
- [Tests](#tests)

## Dependencies

**NOTE**: If you want to use this repository for development,
create a virtual environment named `venv` in the root directory
and install the required dependencies in there.

- xarray
- netCDF4
- motuclient
- Dask

## Tests

**NOTE:** You need to first create and fill the `etc/config.ini` file
before running the tests. See the template `etc/config.ini.example`.

Once done, at the project root directory, run the following command
to start the testing module:

``` sh
python tests.py
```
