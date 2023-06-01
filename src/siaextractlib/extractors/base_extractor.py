# Standard
import sys
from pathlib import Path
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
    """
    Asynchronous call of "sync_extract(...)" method.
    """
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
