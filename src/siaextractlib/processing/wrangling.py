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
