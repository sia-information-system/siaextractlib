from enum import Enum
from pathlib import Path
from datetime import datetime


class SizeUnit(Enum):
  BYTE = 0
  KILO_BYTE = 1
  MEGA_BYTE = 2
  GIGA_BYTE = 3


class RequestSize:
  def __init__(
    self,
    size: float = None,
    unit: SizeUnit | str = None,
    max_allowed_size: float = None,
    code: str = None,
    message: str = None
  ):
    self.size = size
    self.unit = unit
    self.max_allowed_size = max_allowed_size
    self.code = code
    self.message = message
  
  
  def __str__(self):
    unit = self.get_unit_name()
    return f'size: {self.size} {unit}. Max allowed: {self.max_allowed_size} {self.unit}. Code: {self.code}. Message: {self.message}.'
  

  def get_unit_name(self) -> str:
    return self.unit.name if type(self.unit) is SizeUnit else self.unit


class FileDetails:
  def __init__(
    self,
    description: str = '',
    path: Path | str = None
  ):
    # , file_size = None
    self.description = description
    self.path = path
    #self.file_size = file_size
  
  def __str__(self) -> str:
    messages = [
      f'Description: {self.description}',
      f'Path: {self.path}'
    ]
    return '<' + ', '.join(messages) + '>'
  

  def unlink(self):
    self.path.unlink()
    self.path = None


class ExtractionDetails:
  def __init__(
    self,
    description: str = '',
    logs: list[str] = [],
    file: FileDetails = None,
    complete: bool = False,
    time_min: datetime | str = None,
    time_max: datetime | str = None
  ):
    self.description = description
    self.file = file
    self.complete = complete
    self.time_min = time_min
    self.time_max =  time_max
    self.logs = logs
  

  def __str__(self):
    return f'Description: {self.description}. Completed: {self.complete}. Time min: {self.time_min}. Time max: {self.time_max}. Logs: {self.logs}.'
