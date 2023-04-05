class LogStream:
  def __init__(self, callback = None) -> None:
    self.data = []
    self.callback = callback
  

  def write(self, _data: str):
    if type(_data) is not str:
      return
    if _data == '':
      return
    self.data.append(_data)
    if callable(self.callback):
      self.callback(_data)
  

  def read(self):
    return ''.join(self.data)
  

  def readline(self):
    raise NotImplementedError()
