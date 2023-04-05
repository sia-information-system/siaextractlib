import xarray as xr

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
