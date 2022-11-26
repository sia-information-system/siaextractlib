# ExtractionException
class ExtractionException(Exception):
  def __init__(self, message = '', source_log = '', generated_files = []):
    self.message = message
    self.source_log = source_log
    self.generated_files = generated_files
  

  def __str__(self):
    return f'ExtractionException: {self.message}.'


# NoMoreExtractionRequiredException
class NoMoreExtractionRequiredException(Exception):
  def __init__(self, message = ''):
    self.message = message
  

  def __str__(self):
    return f'NoMoreExtractionRequiredException: {self.message}.'


# WrongExtractedFileFormatException
class WrongExtractedFileFormatException(ExtractionException):
  def __init__(self, message = '', source_log = '', generated_files = [], original_exception_message = ''):
    ExtractionException.__init__(self, message, source_log, generated_files)
    self.original_exception_message = original_exception_message
  

  def __str__(self):
    return f'WrongExtractedFileFormatException: {self.message}. Original message: {self.original_exception_message}'


# WrongExtractionArgsException
class WrongExtractionArgsException(ExtractionException):
  def __init__(self, message = '', source_log = '', generated_files = []):
    ExtractionException.__init__(self, message, source_log, generated_files)
  

  def __str__(self):
    return f'WrongExtractionArgsException: {self.message}'


# WrongArgsException
class WrongArgsException(Exception):
  def __init__(self, message = '', fields = []):
    self.message = message
    self.fields = fields
  

  def __str__(self):
    return f'WrongArgsException: {self.message}. Fields: {self.fields}'

