# Standard
import sys
from pathlib import Path
from threading import Thread
from collections.abc import Callable
# Third party
import xarray as xr
import requests
# Own
from siaextractlib.processing import wrangling
from siaextractlib.utils.auth import SimpleAuth
from siaextractlib.utils.metadata import RequestSize, SizeUnit, FileDetails, ExtractionDetails
from siaextractlib.utils.exceptions import ExtractionException
from siaextractlib.extractors.interfaces import ExtractorInterface
from siaextractlib.processing.parallelism import AsyncRunner, AsyncRunnerManager


class BaseExtractor(ExtractorInterface):
  """
  Not intended to be instanced, but derived. This is an abstract class that
  implements some common processes of an standard extractor.
  """
  def __init__(
    self,
    log_stream = sys.stderr,
    verbose: bool = False
  ) -> None:
    self.log_stream = log_stream
    self.verbose = verbose
    # self.__async_extract = AsyncRunner(sync_fn=self.sync_extract)
    self.async_runner_manager = AsyncRunnerManager()
    self.async_runner_manager.add_runner('extract', AsyncRunner(sync_fn=self.sync_extract))
  

  def log(self, *args, **kwargs):
    if self.verbose:
      print(*args, **kwargs, file=self.log_stream)
  

  def extract(self, filepath: Path | str, success_callback: Callable[[ExtractionDetails], None], failure_callback: Callable[[Exception], None]) -> None:
    kwargs = {
      'filepath': filepath
    }
    runner = self.async_runner_manager.get_runner('extract')
    runner.success_callback = success_callback
    runner.failure_callback = failure_callback
    runner.sync_fn_kwargs = kwargs
    runner.run()


  def wait(self, seconds: float = None) -> None:
    self.async_runner_manager.get_runner('extract').wait(seconds=seconds)
  

  def still_working(self) -> bool:
    return self.async_runner_manager.get_runner('extract').still_working()


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
    # self.__async_connect = AsyncRunner(sync_fn=self.sync_connect)
    self.async_runner_manager.add_runner('connect', AsyncRunner(sync_fn=self.sync_connect))
  

  def verify_safety_for_processing(self):
    if self.dataset is None:
      raise ExtractionException(messages='No dataset has been opened. Execute ".connect(...)" first.')
    

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
    Generate a Pydap connection and open the dataset with it.
    **kwargs are forwarded to `xr.open_dataset` method.
    """
    self.log('Trying to open the remote dataset.')
    self.close()
    self.session = requests.Session()
    if self.auth:
      self.session.auth = (self.auth.user, self.auth.passwd)
    opendap_conn = xr.backends.PydapDataStore.open(
      self.opendap_url,
      session=self.session)
    self.dataset = xr.open_dataset(opendap_conn, **kwargs)
    self.log('Dataset opened.')
    return self
  

  def close(self):
    if self.session is not None:
      self.session.close()
      self.session = None
      self.dataset = None


  def fetch(self, subset: xr.Dataset, path: Path | str) -> FileDetails:
    """
    Execute the actual download process and writes the data
    into an actual file in disk.
    """
    self.log('Extracting chunk of data. This can take a while.')
    subset.to_netcdf(path)
    subset.close()
    file_details = FileDetails(description='downloaded_dataset', path=path)
    self.log(f'Extracted chunk: {file_details}')
    return file_details


  def get_size(self, unit: SizeUnit = SizeUnit.BYTE) -> RequestSize:
    self.verify_safety_for_processing()
    subset = wrangling.slice_dice(self.dataset, self.dim_constraints, self.requested_vars)
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
    self.verify_safety_for_processing()
    return list(self.dataset.coords)


  def get_vars(self) -> list[str]:
    self.verify_safety_for_processing()
    return list(self.dataset.data_vars)


  def sync_extract(self, filepath: Path | str) -> ExtractionDetails:
    self.verify_safety_for_processing()
    self.log('Starting extraction process.')
    subset = wrangling.slice_dice(self.dataset, self.dim_constraints, self.requested_vars)
    file_details = self.fetch(subset, filepath)
    time_min, time_max = wrangling.get_time_bound_from_ds(dataset=subset, time_dim_name=self.time_dim_name)
    self.log('Extraction done.')
    return ExtractionDetails(
      description='extraction_completed',
      file=file_details,
      complete=True,
      time_min=time_min,
      time_max=time_max)
  

  def __del__(self):
    self.close()
