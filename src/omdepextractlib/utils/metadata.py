class FileSize:
  def __init__(self, size, unit, max_allowed_size, code = '', message = ''):
    self.size = size
    self.unit = unit
    self.max_allowed_size = max_allowed_size
    self.code = code
    self.message = message
  
  
  def __str__(self):
    return f'size: {self.size} {self.unit}. Max allowed: {self.max_allowed_size} {self.unit}. Code: {self.code}. Message: {self.message}.'


class GeneratedFile:
  def __init__(self, description = '', path = None, file_size = None):
    self.description = description
    self.path = path
    self.file_size = file_size
  
  def __str__(self) -> str:
    messages = [
      f'Description: {self.description}',
      f'Path: {self.path}'
    ]
    return '<' + ', '.join(messages) + '>'


class ExtractionResult:
  def __init__(
    self,
    description = '',
    logs = [],
    generated_file = None,
    complete = False,
    date_min = '',
    date_max = ''):
      self.description = description
      self.generated_file = generated_file
      self.complete = complete
      self.date_min = date_min
      self.date_max =  date_max
      self.logs = logs
  

  def __str__(self):
    return f'Description: {self.description}. Completed: {self.complete}. Date min: {self.date_min}. Date max: {self.date_max}. Logs: {self.logs}.'