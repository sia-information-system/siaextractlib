# Standard
import sys
import re
from pathlib import Path
from datetime import datetime
# Thrid party
import numpy as np
import xarray as xr
from webob.exc import HTTPError
# Own
from siaextractlib.extractors.base_extractor import OpendapExtractor
from siaextractlib.utils.auth import SimpleAuth
from siaextractlib.utils.metadata import ExtractionDetails, SizeUnit, FileDetails
from siaextractlib.processing import wrangling
from siaextractlib.utils.exceptions import ExtractionException

class CopernicusOpendapExtractor(OpendapExtractor):
  def __init__(
    self,
    opendap_url: str,
    auth: SimpleAuth = None,
    dim_constraints: dict[str, slice | list] = None,
    requested_vars: list[str] = None,
    log_stream=sys.stderr,
    max_attempts: int = 20,
    verbose: bool = False
  ) -> None:
    super().__init__(
      opendap_url=opendap_url,
      auth=auth,
      dim_constraints=dim_constraints,
      requested_vars=requested_vars,
      log_stream=log_stream,
      verbose=verbose)
    self.max_attempts = max_attempts
    self.tmp_files: list[FileDetails] = []
  

  def sync_extract(self, filepath: Path | str) -> ExtractionDetails:
    self.log('starting extraction process.')
    # Try simple case.
    req_max_size = None
    try:
      self.log('Using straightforward method.')
      return super().sync_extract(filepath=filepath)
    except HTTPError as err:
      # Get the max_allowed size
      self.log(f'Straightforward extraction was not possible.')
      self.log(err.detail)
      self.log('Searching for size limitations in error details.')
      m = re.search(r'max=(([0-9]+)+(.[0-9]+)?)', err.detail)
      if m:
        req_max_size = float(m.group().split('=')[1])
      else:
        self.log('No size limitations was found in error details. This is a critical error.')
        raise ExtractionException(messages=[
          'Unable to perform request split due to max allowed size was not found in error details.',
          err.detail
        ])
    self.log('Using request splitting method.')
    # Needs to split the request
    # First way.
    # advance_factor = total_size / max_allowed
    # days_ahead = (total days in df) * advance_factor
    # use days_ahead to move forward in the dataset
    # while (reference date) + days_ahead <= end date

    # New way (actually implemented): Using blocks of times, since time dimension is an array.
    # n_blocks = ceil(total_size / max_allowed). Use ceil to get an int as n_blocks
    # block_size = dataset.time.length / n_blocks
    # start_index = 0
    # end_index = 0
    # done = False
    # while Not done:
    #  if time_index > dataset.time.length: time_index = dataset.time.length -1; done = True;
    #  end_index = start_index + block_size
    #  exec_straction(start_index, end_index);
    #  start_index = end_index + 1
    
    # Computing parameters.
    subset = wrangling.slice_dice(self.dataset, self.dim_constraints, self.requested_vars)
    time_arr = wrangling.get_time_dim(subset, time_dim_name=self.time_dim_name).values
    request_size = self.get_size(SizeUnit.MEGA_BYTE).size
    n_blocks = np.ceil(request_size / req_max_size)
    dim_time_len = len(time_arr)
    block_size = int(np.ceil(dim_time_len / n_blocks))
    start_index = 0
    end_index = 0
    self.log(f'Split parameters: request_size={request_size}; req_max_size={req_max_size}; n_blocks={n_blocks}; dim_time.length={dim_time_len}; block_size={block_size}.')

    # Loop setup.
    if type(filepath) is str:
      filepath = Path(filepath)
    download_dir = filepath.parent.absolute()
    done = False
    constraints = self.dim_constraints
    block_count = 1
    while not done:
      # Tmp file name
      datetime_str = datetime.now().strftime('%Y-%m-%d_%H:%M:%S.%f')
      tmp_filename = f'tmp_dataset_{datetime_str}.nc'
      # Computing date range
      end_index = start_index + block_size
      if end_index >= dim_time_len:
        end_index = dim_time_len - 1
        done = True # This is the last iteration
        if start_index >= dim_time_len:
          break # Dates out of range.
      # Subsetting
      constraints[self.time_dim_name] = slice(time_arr[start_index], time_arr[end_index])
      subset = wrangling.slice_dice(self.dataset, constraints, self.requested_vars)
      self.log(f'Extracting block: number={block_count}; start_index={start_index}; end_index={end_index}; constraints={constraints}.')
      file_details = self.fetch(subset, Path(download_dir, tmp_filename))
      self.tmp_files.append(file_details)
      # Adjusting time index.
      start_index = end_index + 1
      # Other adjustments.
      block_count += 1
    # Merging files.
    self.log('Extraction done. Merging files.')
    fielpaths = [ f.path for f in self.tmp_files ]
    dataset = xr.open_mfdataset(fielpaths, combine = 'by_coords')
    dataset.to_netcdf(filepath)
    time_min, time_max = wrangling.get_time_bound_from_ds(dataset=dataset, time_dim_name=self.time_dim_name)
    dataset.close()
    # Delete tmp files.
    for f in self.tmp_files:
      f.unlink()
    self.tmp_files = []
    # Return data.
    self.log('Extraction successfully completed.')
    return ExtractionDetails(
      description='extraction_completed.',
      file=FileDetails(description='full_dataset', path=filepath),
      complete=True, time_min=time_min, time_max=time_max)
