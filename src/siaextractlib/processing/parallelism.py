# Standard
from collections.abc import Callable
from threading import Thread
# Own
from siaextractlib.utils.exceptions import AsyncRunnerBusyException, DuplicatedAsyncRunnerException, AsyncRunnerMissingException


class AsyncRunner:
  """
  Runs a synchronous function as an asynchronous one on another thread. Its return values are forwared to the callback.
  """
  def __init__(
    self,
    sync_fn: Callable[..., None],
    sync_fn_kwargs: dict[str, any] = {},
    success_callback: Callable[..., None] = None,
    failure_callback: Callable[[Exception], None] = None
  ) -> None:
    self.sync_fn = sync_fn
    self.success_callback = success_callback
    self.failure_callback = failure_callback
    self.sync_fn_kwargs = sync_fn_kwargs
    self.__thread: Thread = None
  

  def validate_safe_execution(self):
    """
    Validates the state of the instance is safe for running the process.
    """
    if not callable(self.sync_fn):
      raise TypeError('Parameter "sync_fn" must be callable.')
    if not callable(self.success_callback):
      raise TypeError('Parameter "success_callback" must be callable and take arguments in the same order and type as the return values of "sync_fn".')
    if not callable(self.failure_callback):
      raise TypeError('Parameter "failure_callback" must be callable and take as argument an instance of a subclass of BaseException')
    if type(self.sync_fn_kwargs) is not dict:
      raise TypeError('Parameter "sync_fn_kwargs" must be a dict[str, any].')
    # Originally placed in "run" method.
    if self.__thread is not None and self.__thread.is_alive():
      raise AsyncRunnerBusyException('An asynchronous process is still running. Just one async process is allowed per instance.')


  def wrapper_fn(self):
    """
    A wrapper that runs the synchronous function. Its return value is captured
    and casted to a tuple if needed in order to forward it to the callback
    as *args.
    """
    try:
      result = self.sync_fn(**self.sync_fn_kwargs)
      if result is None:
        self.success_callback()
        return
      if type(result) is not tuple:
        result = tuple([result])
      self.success_callback(*result)
    except BaseException as e:
      self.failure_callback(e)

  
  def run(self) -> Thread:
    """
    Creates the thread and runs the wrapper function there.
    """
    self.validate_safe_execution()
    th = Thread(target=self.wrapper_fn)
    th.start()
    self.__thread = th
    return th
  

  def wait(self, seconds: float = None) -> None:
    self.__thread.join(timeout=seconds)
  

  def still_working(self) -> bool:
    return self.__thread.is_alive()


class AsyncRunnerManager:
  """
  Manages a set of AsyncRunners.
  """
  def __init__(self) -> None:
    self.__runners: dict[str, AsyncRunner] = {}
  

  def add_runner(self, runner_id: str, runner: AsyncRunner):
    if runner_id in self.__runners:
      raise DuplicatedAsyncRunnerException(messages=f'Id "{runner_id}" already registered. Cannot be duplicated.')
    self.__runners[runner_id] = runner
    return self
  

  def remove_runner(self, runner_id: str):
    if runner_id not in self.__runners:
      raise AsyncRunnerMissingException(messages=f'Id {runner_id} not found.')
    del self.__runners[runner_id]
    return self
  

  def get_runner(self, runner_id: str) -> AsyncRunner:
    if runner_id not in self.__runners:
      raise AsyncRunnerMissingException(messages=f'Id {runner_id} not found.')
    return self.__runners[runner_id]
  

  def get_ids(self) -> list[str]:
    return list(self.__runners.keys())
