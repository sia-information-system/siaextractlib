# ExtractionException
class ExtractionException(Exception):
  def __init__(self, message = '', source_log = '', generated_files = []):
    self.message = message
    self.source_log = source_log
    self.generated_files = generated_files
  

  def __str__(self):
    f_srt_list = []
    for gf in self.generated_files:
      f_srt_list.append(gf.__str__())
    messages = [
      f'Message: {self.message}',
      f'Source log:\n{self.source_log}',
      f'Generated files: [{",".join(f_srt_list)}]'
    ]
    return '\n'.join(messages)


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

