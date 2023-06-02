# Standard
import sys
import time
import traceback
from pathlib import Path
from collections.abc import Callable
# Third party
import requests
import numpy as np
import xarray as xr
# Own
from siaextractlib.processing import wrangling
from siaextractlib.utils.auth import SimpleAuth
from siaextractlib.utils.metadata import RequestSize, SizeUnit, FileDetails, ExtractionDetails
from siaextractlib.utils.exceptions import ExtractionException
from siaextractlib.extractors.interfaces import ExtractorInterface
from siaextractlib.processing.parallelism import AsyncRunner, AsyncRunnerManager
from siaextractlib.extractors.base_extractor import BaseExtractor

# Needs Pydap >= 3.3.0
# https://github.com/pydap/pydap
class OpendapExtractor(BaseExtractor):
  def __init__(
    self,
    opendap_url: str,
    auth: SimpleAuth = None,
    dim_constraints: dict[str, slice | list] = None,
    requested_vars: list[str] = None,
    log_stream = sys.stderr,
    max_attempts: int = 5,
    req_max_size: int = 64, # MB
    verbose: bool = False
  ) -> None:
    super().__init__(log_stream=log_stream, verbose=verbose)
    self.opendap_url = opendap_url
    self.auth = auth
    self.dim_constraints = dim_constraints
    self.requested_vars = requested_vars
    self.dataset = None
    self.filepath = None
    self.session = None
    self.time_dim_name = 'time'
    self.max_attempts = max_attempts
    self.tmp_files: list[FileDetails] = []
    self.req_max_size = req_max_size
    # self.__async_connect = AsyncRunner(sync_fn=self.sync_connect)
    self.async_runner_manager.add_runner('connect', AsyncRunner(sync_fn=self.sync_connect))
  

  def verify_safety_for_processing(self):
    if self.dataset is None:
      raise ExtractionException(messages='No dataset has been opened. Execute ".connect(...)" first.')
  

  def forget_tmp_files(self):
    """
    Clean the in-between file list without unlink them.
    """
    self.tmp_files = []
  

  def unlink_tmp_files(self):
    """
    Remove the in-between files generated.
    """
    self.log('Unlinking tmp files.')
    for f in self.tmp_files:
      try:
        f.unlink()
      except BaseException as err:
        self.log(f'{err.__class__.__name__}: {err}')
    # self.tmp_files = []
    self.forget_tmp_files()
    self.log('Unlinking done.')
    

  def connect(self, success_callback: Callable[..., None], failure_callback: Callable[[BaseException], None], **kwargs):
    """
    Asynchronous call of "sync_connect(...)" method.
    """
    runner = self.async_runner_manager.get_runner('connect')
    runner.success_callback = success_callback
    runner.failure_callback = failure_callback
    runner.sync_fn_kwargs = kwargs
    runner.run()


  def sync_connect(self, **kwargs):
    """
    Generates a Pydap connection and open the dataset with it.
    **kwargs are forwarded to `xr.open_dataset` method.
    """
    self.log('Trying to open the remote dataset.')
    self.close()
    try:
      self.session = requests.Session()
      if self.auth:
        self.session.auth = (self.auth.user, self.auth.passwd)
      opendap_conn = xr.backends.PydapDataStore.open(
        self.opendap_url,
        session=self.session)
      self.dataset = wrangling.open_dataset(opendap_conn, log_stream=self.log_stream, **kwargs)
      self.log('Dataset opened.')
      return self
    except BaseException as err:
      self.close()
      raise err.with_traceback(err.__traceback__)
  

  def close(self):
    """
    Closes the connection with the remote dataset.
    """
    if self.session is not None:
      self.session.close()
      self.session = None
      self.dataset = None


  def fetch(self, subset: xr.Dataset, path: Path | str) -> FileDetails:
    """
    Executes the actual download process and writes the data
    into an actual file in disk.
    """
    self.log('Extracting chunk of data. This can take a while.')
    subset.to_netcdf(path)
    subset.close()
    file_details = FileDetails(description='dataset', path=path)
    self.log(f'Extracted chunk: {file_details}')
    return file_details


  def get_size(self, unit: SizeUnit = SizeUnit.BYTE) -> RequestSize:
    """
    Returns the size of the dataset based on the current constraints.
    """
    self.verify_safety_for_processing()
    subset = wrangling.slice_dice(self.dataset, self.dim_constraints, self.requested_vars, squeeze=False)
    rsize = RequestSize()
    if unit == SizeUnit.KILO_BYTE:
      rsize.unit = SizeUnit.KILO_BYTE
      rsize.size = subset.nbytes / 1e3
    elif unit == SizeUnit.MEGA_BYTE:
      rsize.unit = SizeUnit.MEGA_BYTE
      rsize.size = subset.nbytes / 1e6
    elif unit == SizeUnit.GIGA_BYTE:
      rsize.unit = SizeUnit.GIGA_BYTE
      rsize.size = subset.nbytes / 1e9
    elif unit == SizeUnit.BYTE:
      rsize.unit = SizeUnit.BYTE
      rsize.size = subset.nbytes
    else:
      raise KeyError(message=f'Option "{unit}" not valid.')
    return rsize


  def get_dims(self) -> list[str]:
    """
    Returns the dimensions of the dataset.
    """
    self.verify_safety_for_processing()
    return list(self.dataset.coords)


  def get_vars(self) -> list[str]:
    """
    Returns the variables of the dataset.
    """
    self.verify_safety_for_processing()
    return list(self.dataset.data_vars)


  # Not used.
  def sync_extract_straightforward(self, filepath: Path | str) -> ExtractionDetails:
    self.verify_safety_for_processing()
    self.log('Starting extraction process.')
    subset = wrangling.slice_dice(self.dataset, self.dim_constraints, self.requested_vars, squeeze=False)
    file_details = self.fetch(subset, filepath)
    time_min, time_max = wrangling.get_time_bound_from_ds(dataset=subset)
    self.log('Extraction done.')
    return ExtractionDetails(
      description='dataset',
      file=file_details,
      complete=True,
      time_min=time_min,
      time_max=time_max)
  

  # Actually used.
  def sync_extract(self, filepath: Path | str) -> ExtractionDetails:
    """
    Executes the extraction by splitting the request size in blocks of self.req_max_size size.
    Once all blocks has been downloaded, it merges them all in a new single file.
    """
    self.verify_safety_for_processing()
    self.log('starting extraction process.')
    req_max_size = self.req_max_size # MB
    # The use of the straightforward method was omitted due to
    # in some cases the server does not respond (time out).

    # Try simple case.
    # try:
    #   self.log('Using straightforward method.')
    #   return super().sync_extract(filepath=filepath)
    # except HTTPError as err:
    #   # Get the max_allowed size
    #   self.log(f'Straightforward extraction was not possible.')
    #   self.log(err.detail)
    #   self.log('Searching for size limitations in error details.')
    #   m = re.search(r'max=(([0-9]+)+(.[0-9]+)?)', err.detail)
    #   if m:
    #     req_max_size = float(m.group().split('=')[1])
    #   else:
    #     self.log('No size limitations was found in error details. This is a critical error.')
    #     raise ExtractionException(messages=[
    #       'Unable to perform request split due to max allowed size was not found in error details.',
    #       err.detail
    #     ])
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
    subset = wrangling.slice_dice(self.dataset, self.dim_constraints, self.requested_vars, squeeze=False)
    time_dim, self.time_dim_name = wrangling.get_time_dim(subset)
    if time_dim is None:
      raise ExtractionException(messages='No time dimension found in extraction process. Cannot proceed.')
    time_arr = time_dim.values
    request_size = self.get_size(SizeUnit.MEGA_BYTE).size
    n_blocks = int(np.ceil(request_size / req_max_size))
    dim_time_len = len(time_arr)
    block_size = int(np.ceil(dim_time_len / n_blocks))
    n_blocks = int(np.ceil(dim_time_len / block_size)) # Adjustment to reflect the actual number of blocks due to previous rounding.
    start_index = 0
    end_index = 0
    self.log(f'Split parameters: request_size={request_size}; req_max_size={req_max_size}; n_blocks={n_blocks}; dim_time.length={dim_time_len}; block_size={block_size}.')

    # Loop setup.
    if type(filepath) is str:
      filepath = Path(filepath)
    download_dir = filepath.parent.absolute()
    done = False
    constraints = self.dim_constraints
    block_count = 0
    extraction_completed = True
    while not done:
      # Tmp file name
      # datetime_str = datetime.now().strftime('%Y-%m-%d_%H:%M:%S.%f')
      timestamp = time.time()
      tmp_filename = f'tmp_dataset_{timestamp}.nc'
      # Computing date range
      end_index = start_index + (block_size - 1) # range is of size: block_size.
      if end_index >= dim_time_len:
        end_index = dim_time_len - 1
        done = True # This is the last iteration
        if start_index >= dim_time_len:
          break # Dates out of range.
      # Subsetting
      constraints[self.time_dim_name] = slice(time_arr[start_index], time_arr[end_index])
      subset = wrangling.slice_dice(self.dataset, constraints, self.requested_vars, squeeze=False)
      file_details = None
      block_attempt = 1
      block_completed = False
      while block_attempt <= self.max_attempts and not block_completed:
        self.log(f'Extracting block: number={block_count + 1}/{n_blocks}; start_index={start_index}; end_index={end_index}; constraints={constraints}; attempt={block_attempt}/{self.max_attempts}.')
        try:
          file_details = self.fetch(subset, Path(download_dir, tmp_filename))
          block_completed = True
        except Exception as err:
          self.log('An error has occurred while fetching block:')
          traceback.print_exception(err, file=self.log_stream)
          self.log('Retrying.')
          block_attempt += 1
      if block_completed:
        self.tmp_files.append(file_details)
        # Adjusting time index.
        start_index = end_index + 1
        # Other adjustments.
        block_count += 1
      else:
        self.log('Maximum number of attempts was reached for a block extraction. Stopping extraction.')
        self.log(f'Blocks extracted: {block_count}/{n_blocks}.')
        done = True
        extraction_completed = False
    # Merging files.
    self.log('Extraction done.')
    if not len(self.tmp_files):
      raise ExtractionException(messages='Maximum number of attempts was reached for the extraction of the first block. No data was extracted.')
    self.log('Merging blocks.')
    fielpaths = [ f.path for f in self.tmp_files ]
    # dataset = xr.open_mfdataset(fielpaths, combine = 'by_coords')
    dataset = wrangling.open_mfdataset(fielpaths, combine = 'by_coords', log_stream=self.log_stream)
    dataset.to_netcdf(filepath)
    time_min, time_max = wrangling.get_time_bound_from_ds(dataset=dataset)
    dataset.close()
    # Delete tmp files.
    self.unlink_tmp_files()
    # for f in self.tmp_files:
    #   f.unlink()
    # self.tmp_files = []
    # Return data.
    self.log('Extraction successfully completed.')
    return ExtractionDetails(
      description='dataset',
      file=FileDetails(description='dataset', path=filepath),
      complete=extraction_completed, time_min=time_min, time_max=time_max)


  def __del__(self):
    self.close()
