# Standard
import unittest
import time
from pathlib import Path
import inspect
# Third party

# Own
from siaextractlib.extractors import OpendapExtractor
from siaextractlib.utils.auth import SimpleAuth
from siaextractlib.utils.log import LogStream
from siaextractlib.utils.exceptions import ExtractionException
from siaextractlib.utils.metadata import SizeUnit, ExtractionDetails

# Custom for testing
from lib import general_utils

DATA_DIR = Path(Path(__file__).parent.absolute(), '..', 'tmp', 'data')

"""
# LogStream
def show_log(data):
  print(data, end='')

log_stream = LogStream(callback=show_log)

# Extractor
def catch_extraction_result(extract_details):
  print(extract_details)

def callback_connect(this_extractor: OpendapExtractor):
  this_extractor.extract('ds.nc', success_callback=catch_extraction_result, failure_callback=lambda e: print(e))
  # extractor.wait()
  print('-- vars:', this_extractor.get_vars())
  print('-- dims:', this_extractor.get_dims())
  print('-- Still working?', this_extractor.still_working())

extractor = OpendapExtractor(
  opendap_url='https://nrt.cmems-du.eu/thredds/dodsC/cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m',
  auth=SimpleAuth(user='amontejo', passwd='MyCopernicusAccount25;'),
  dim_constraints={
    'time': ['2023-04-06'],
    'depth': [0.49],
    'longitude': slice(-87.21394123899096, -86.14119796245421),
    'longitude': slice(20.216928148926932, 21.687290554990795)
  },
  requested_vars=['uo'],
  log_stream=log_stream,
  verbose=True)

extractor.connect(success_callback=callback_connect, failure_callback=lambda e: print(e))
# extractor.extract('ds.nc', catch_extraction_result)
# extractor.wait()
# print('-- vars:', extractor.get_vars())
# print('-- dims:', extractor.get_dims())
# print('-- Still working?', extractor.still_working())

# try:
#   print()
#   extractor.extract('ds.nc', catch_extraction_result)
# except ExtractionException as e:
#   print('--Error:', e)

#print('--After run the extraction.')
#time.sleep(5)
#print('--After await 5 seconds')
print('-- End of script.')

"""

class TestOpendap(unittest.TestCase):
  def test_size_query(self):
    print(f'---- Starting: {inspect.currentframe().f_code.co_name}')
    def show_log(data):
      print(data, end='')
    log_stream = LogStream(callback=show_log)

    extractor = OpendapExtractor(
      opendap_url='https://nrt.cmems-du.eu/thredds/dodsC/cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m',
      auth=SimpleAuth(user='amontejo', passwd='MyCopernicusAccount25;'),
      dim_constraints={
        #'time': ['2023-04-06'],
        'time': slice('2023-03-06', '2023-04-06'),
        'depth': [0.49],
        'longitude': slice(-87.21394123899096, -86.14119796245421),
        'longitude': slice(20.216928148926932, 21.687290554990795)
      },
      requested_vars=['uo'],
      log_stream=log_stream,
      verbose=True)
    request_size = extractor.sync_connect().get_size(SizeUnit.MEGA_BYTE)
    print(request_size)
    extractor.close_connect()
    self.assertIsNotNone(request_size)


  def test_4mb_donwload_sync(self):
    print(f'---- Starting: {inspect.currentframe().f_code.co_name}')
    def show_log(data):
      print(data, end='')
    log_stream = LogStream(callback=show_log)

    extractor = OpendapExtractor(
      opendap_url='https://nrt.cmems-du.eu/thredds/dodsC/cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m',
      auth=SimpleAuth(user='amontejo', passwd='MyCopernicusAccount25;'),
      dim_constraints={
        #'time': ['2023-04-06'],
        'time': slice('2023-03-06', '2023-04-06'),
        'depth': [0.49],
        'longitude': slice(-87.21394123899096, -86.14119796245421),
        'longitude': slice(20.216928148926932, 21.687290554990795)
      },
      requested_vars=['uo'],
      log_stream=log_stream,
      verbose=True)
    extract_details = extractor.sync_connect().sync_extract(Path(DATA_DIR, 'ds_1.nc'))
    print(extract_details)
    extractor.close_connect()
    self.assertIsNotNone(extract_details)


  def test_4mb_donwload_async(self):
    print(f'---- Starting: {inspect.currentframe().f_code.co_name}')
    def show_log(data):
      print(data, end='')
    log_stream = LogStream(callback=show_log)

    extractor = OpendapExtractor(
      opendap_url='https://nrt.cmems-du.eu/thredds/dodsC/cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m',
      auth=SimpleAuth(user='amontejo', passwd='MyCopernicusAccount25;'),
      dim_constraints={
        #'time': ['2023-04-06'],
        'time': slice('2023-03-06', '2023-04-06'),
        'depth': [0.49],
        'longitude': slice(-87.21394123899096, -86.14119796245421),
        'longitude': slice(20.216928148926932, 21.687290554990795)
      },
      requested_vars=['uo'],
      log_stream=log_stream,
      verbose=True)
    # extract_details = extractor.sync_connect().sync_extract('ds.nc')
    
    extractor.connect(
      success_callback=self._4mb_donwload_async_connect_success_handler,
      failure_callback=self._4mb_donwload_failure_handler)
    
    # The runner id is the asynchronous method name.
    # Run the .wait(...) method will block the current thread.
    # In most cases, run this method won't be needed.
    extractor.async_runner_manager.get_runner('connect').wait()


  def _4mb_donwload_async_connect_success_handler(self, this_extractor: OpendapExtractor):
    this_extractor.extract(
      filepath=Path(DATA_DIR, 'ds_2.nc'),
      success_callback=self._4mb_donwload_async_extract_success_handler,
      failure_callback=self._4mb_donwload_failure_handler)
    
    # The runner id is the asynchronous method name.
    # Run the .wait(...) method will block the current thread.
    # In most cases, run this method won't be needed.
    this_extractor.async_runner_manager.get_runner('extract').wait()
    # Alternatively, extractor.wait(...) can be used for extractor.extract(...)
    # as a short cut of the above way.

    # Always close the conexion when the extractor is not needed any more.
    this_extractor.close_connect()
    

  def _4mb_donwload_async_extract_success_handler(self, extraction_details):
    print(extraction_details)
    self.assertIsInstance(extraction_details, ExtractionDetails)


  def _4mb_donwload_failure_handler(self, e: BaseException):
    print(e)
    self.assertFalse( issubclass(type(e), BaseException) )


if __name__ == '__main__':
  general_utils.mkdir_r(DATA_DIR)
  unittest.main()