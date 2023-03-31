# ExtractionException
# class ExtractionException(Exception):
#   def __init__(self, message = '', source_log = '', generated_files = []):
#     self.message = message
#     self.source_log = source_log
#     self.generated_files = generated_files
  

#   def __str__(self):
#     f_srt_list = []
#     for gf in self.generated_files:
#       f_srt_list.append(gf.__str__())
#     messages = [
#       f'Message: {self.message}',
#       f'Source log:\n{self.source_log}',
#       f'Generated files: [{",".join(f_srt_list)}]'
#     ]
#     return '\n'.join(messages)

class ExtractionException(Exception):
  def __init__(self, messages: str | list[str] = None, files = [], tb = None):
    if type(messages) is str and messages != '':
      self.messages = [messages]
    elif type(messages) is list:
      self.messages = messages
    else:
      self.messages = []
    if tb is not None:
      self.with_traceback(tb)
    self.files = files
  

  def __str__(self):
    # f_srt_list = []
    # for gf in self.generated_files:
    #   f_srt_list.append(gf.__str__())
    # messages = [
    #   f'Message: {self.message}',
    #   f'Source log:\n{self.source_log}',
    #   f'Generated files: [{",".join(f_srt_list)}]'
    # ]
    # return '\n'.join(messages)
    message = ''
    if self.messages:
      message = '\nMessages:\n'
      for m in self.messages:
        message += f'-> {m}\n'
    # TODO: Add messages for referenced files.
    return message
  

  def add_message(self, message: str):
    self.messages.append(message)


# NoMoreExtractionRequiredException
# EndOfDataException.
class EndOfDataException(ExtractionException):
  def __init__(self, **kwargs):
    super().__init__(**kwargs)


# May be not necessary
# WrongExtractedFileFormatException
# UnexpectedFileStructureException
class UnexpectedFileStructureException(ExtractionException):
  # def __init__(self, message = '', source_log = '', generated_files = [], original_exception_message = ''):
  def __init__(self, expected_format = None, **kwargs):
    super().__init__(**kwargs)
    self.expected_format = expected_format
  

  def __str__(self):
    message = super().__str__()
    if self.expected_format is None or self.expected_format == '':
      return message
    message += f'Expected format: {self.expected_format}\n'
    return message


# WrongExtractionArgsException
class WrongExtractionArgsException(ExtractionException):
  def __init__(self, **kwargs):
    super().__init__(**kwargs)


class AsyncRunnerBusyException(ExtractionException):
  def __init__(self, **kwargs):
    super().__init__(**kwargs)


class DuplicatedAsyncRunnerException(ExtractionException):
  def __init__(self, **kwargs):
    super().__init__(**kwargs)


class AsyncRunnerMissingException(ExtractionException):
  def __init__(self, **kwargs):
    super().__init__(**kwargs)
