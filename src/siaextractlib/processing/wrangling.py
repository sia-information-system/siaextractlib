# Standard
import re
import sys
# Third party
import xarray as xr
from cftime import num2pydate

def slice_dice(
  dataset: xr.Dataset,
  dim_constraints: dict,
  var: str | list = None
) -> xr.Dataset | xr.DataArray:
  """
  Make a subset by dimension contraints and a selected (and optional)
  list of variables. A single variable name can be passed as a string.
  """
  # Initializing.
  subset = dataset

  # Selecting variables of interest.
  if var is not None:
    subset = dataset[var]
  
  constraints_w_slices = {}
  constraints_w_no_slices = {}
  for dim_name in dim_constraints.keys():
    if type(dim_constraints[dim_name]) is slice:
      constraints_w_slices[dim_name] = dim_constraints[dim_name]
    else:
      constraints_w_no_slices[dim_name] = dim_constraints[dim_name]
  
  # Apply dimension constraints
  subset = subset.sel(constraints_w_no_slices, method = 'nearest').squeeze()
  subset = subset.sel(constraints_w_slices).squeeze()

  return subset

# Standard for time dimension in NetCDF files: https://cfconventions.org/Data/cf-conventions/cf-conventions-1.7/build/ch04s04.html 
def get_time_dim(dataset: xr.Dataset, time_dim_name: str = 'time') -> xr.DataArray | None:
  try:
    return dataset[time_dim_name]
  except:
    return None


def get_time_bound_from_ds(dataset: xr.Dataset, time_dim_name: str = 'time'):
  time_min = None
  time_max = None
  _time_dim = get_time_dim(dataset, time_dim_name=time_dim_name)
  if _time_dim is not None:
    time_min = _time_dim.min().values
    time_max = _time_dim.max().values
  return time_min, time_max


def get_dims(dataset: xr.Dataset) -> list[str]:
  return list(dataset.coords)


def get_vars(dataset: xr.Dataset) -> list[str]:
  return list(dataset.data_vars)


def is_time_dim(dataset: xr.Dataset, dim_name: str):
  dim = dataset.coords[dim_name]
  if 'axis' in dim.attrs:
    if dim.attrs['axis'] == 'T' or dim.attrs['axis'] == 't':
      return True
  if 'units' in dim.attrs:
    if re.search(r'since ([0-9]{4}(-|/)[0-9]{2}(-|/)[0-9]{2})', dim.attrs['units']):
      return True


def get_time_dims(dataset: xr.Dataset):
  time_dim_list = []
  for dim_name in get_dims(dataset=dataset):
    if is_time_dim(dataset=dataset, dim_name=dim_name):
      time_dim_list.append(dim_name)
  return time_dim_list


# TODO: this should open a dataset like the original method, but when fails
# tries to identify if it was a problem with time dimension, if so, it opens the dataset without
# the built-in time decoding feature, then decode the time by itself, adjust the
# axis and return the dataset. This should work for on-disk dataset and remote datasets.
def open_dataset(filename_or_obj, log_stream = sys.stderr, **kwargs):
  try:
    return xr.open_dataset(filename_or_obj, **kwargs)
  except ValueError as err:
    err_msg = str(err)
    print(f'An error occured while opening the dataset: f{err_msg}.', file=log_stream)
    if not re.search(r'unable to decode time units', err_msg):
      raise err.with_traceback(err.__traceback__)
  
  # May need custom time decoding.
  print(f'Trying to open it without the built-in time decoding.', file=log_stream)
  dataset = xr.open_dataset(filename_or_obj, decode_times=False, **kwargs)
  time_dims = get_time_dims(dataset)
  if not time_dims:
    print(f'WARNING: No time dimension detected. Could be this an error?', file=log_stream)
  for time_dim_name in time_dims:
    print(f'Time dim to decode: {time_dim_name}', file=log_stream)
    calendar = 'standard'
    dim = dataset.coords[time_dim_name]
    dim_attrs = dim.attrs.copy()
    if 'calendar' in dim.attrs:
      calendar = dim.attrs['calendar']
      del dim_attrs['calendar']
    del dim_attrs['units']
    decoded_times = num2pydate(dim.data, units=dim.attrs['units'], calendar=calendar)
    dataset = dataset.assign_coords({time_dim_name: (time_dim_name, decoded_times, dim_attrs)})
  return dataset
