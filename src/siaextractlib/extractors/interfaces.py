# Standard
from pathlib import Path
from collections.abc import Callable
# Third party

# Own
from siaextractlib.utils.metadata import RequestSize, SizeUnit, ExtractionDetails


class ExtractorInterface:
  """
  Interface for standard extractors.
  """
  def log(self, *args, **kwargs) -> None:
    """
    Logs to a configurable stream. The reference to the stream must be
    in a member variable.
    """
    raise NotImplementedError(f'{self.__class__.__name__}: This is a virtual method.')
  

  def sync_extract(self, filepath: Path | str) -> ExtractionDetails:
    """
    Manages the complete extraction process sinchronously including: request splitting, chunk merging, etc.
    """
    raise NotImplementedError(f'{self.__class__.__name__}: This is a virtual method.')


  def extract(self, filepath: Path | str, callback: Callable[[ExtractionDetails], None]) -> None:
    """
    Runs in a wrapper the ".sync_extract(...)" method on another thread.
    When the ".sync_extract(...)" method returns, its return values is 
    captured and passed to the callback. This is an asynchronous method.
    """
    raise NotImplementedError(f'{self.__class__.__name__}: This is a virtual method.')
  

  def wait(self, seconds: float = None) -> None:
    """
    Awaits `seconds` seconds if given, or until the asynchronous extraction process terminates otherwise.
    """
    raise NotImplementedError(f'{self.__class__.__name__}: This is a virtual method.')
  

  def still_working(self) -> bool:
    """
    Evaluates if the asynchronous extraction process is still running.
    """
    raise NotImplementedError(f'{self.__class__.__name__}: This is a virtual method.')
  

  def get_size(self, unit: SizeUnit) -> RequestSize:
    """
    Computes the size of the subset generated using the constraints and variables previously set.
    The result is returned in the specified units.
    """
    raise NotImplementedError(f'{self.__class__.__name__}: This is a virtual method.')
  

  def get_dims(self) -> list[str]:
    """
    Returns the list of dimension names in the dataset.
    """
    raise NotImplementedError(f'{self.__class__.__name__}: This is a virtual method.')
  

  def get_vars(self) -> list[str]:
    """
    Returns the list of variable names in the dataset.
    """
    raise NotImplementedError(f'{self.__class__.__name__}: This is a virtual method.')
