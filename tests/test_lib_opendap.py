# Standard
import unittest
import time
import sys
import inspect
from pathlib import Path
from datetime import datetime
import traceback

import warnings

# Third party

# Own
from siaextractlib.extractors import OpendapExtractor
from siaextractlib.utils.auth import SimpleAuth
from siaextractlib.utils.log import LogStream
from siaextractlib.utils.exceptions import ExtractionException
from siaextractlib.utils.metadata import SizeUnit, ExtractionDetails
from siaextractlib.processing import wrangling

# Custom for testing
from lib import general_utils

warnings.filterwarnings("ignore")

DATA_DIR = Path(Path(__file__).parent.absolute(), '..', 'tmp', 'data')
general_utils.mkdir_r(DATA_DIR)
config = general_utils.read_config()


class OpendapTestCase(unittest.TestCase):
  def __init__(self, methodName: str = "runTest") -> None:
    super().__init__(methodName)
    warnings.filterwarnings("ignore")
    self.dataset_name: str = '__dataset_name__.nc'
    self.process_ok: bool = False
    self.extraction_details: ExtractionDetails = None
    self.print_dataset_structure: bool = False


  def _connect_success_handler(self, this_extractor: OpendapExtractor):
    # Print the dataset structure
    if self.print_dataset_structure:
      print(this_extractor.dataset, file=sys.stderr)

    # Run the extraction process
    this_extractor.extract(
      filepath=Path(DATA_DIR, self.dataset_name),
      success_callback=self._extract_success_handler,
      failure_callback=self._failure_handler)
    
    # The runner id is the asynchronous method name.
    # Run the .wait(...) method will block the current thread.
    # In most cases, run this method won't be needed.
    this_extractor.async_runner_manager.get_runner('extract').wait()
    # Alternatively, extractor.wait(...) can be used for extractor.extract(...)
    # as a short cut of the above way.

    # Always close the conexion when the extractor is not needed any more.
    this_extractor.close()
  

  def _connect_success_basic_handler(self, this_extractor: OpendapExtractor):
    # Print the dataset structure
    if self.print_dataset_structure:
      print(this_extractor.dataset, file=sys.stderr)
    self.process_ok = True


  def _extract_success_handler(self, extraction_details: ExtractionDetails):
    print(extraction_details, file=sys.stderr)
    self.extraction_details = extraction_details
    self.process_ok = True
    

  def _failure_handler(self, e: BaseException):
    print('---- ERROR HANDLER ----', file=sys.stderr)
    traceback.print_exception(e, file=sys.stderr)
    self.process_ok = False


class TestOpendap(OpendapTestCase):
  def test_size_query(self):
    t_i = time.time()
    _id = f'{self.__class__.__name__}_{inspect.currentframe().f_code.co_name}'
    datetime_str = datetime.now().strftime('%Y-%m-%d_%H:%M:%S.%f')
    _timestamp = "".join(str(datetime.now().timestamp()).split('.'))
    ds_name = f'{_id}_{_timestamp}.nc'
    print(f'---- Starting: {_id} at {datetime_str}', file=sys.stderr)
    
    def show_log(data):
      print(data, end='', file=sys.stderr)
    log_stream = LogStream(callback=show_log)

    extractor = OpendapExtractor(
      opendap_url='https://nrt.cmems-du.eu/thredds/dodsC/cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m',
      auth=SimpleAuth(user=config['DEFAULT']['COPERNICUS_USER'], passwd=config['DEFAULT']['COPERNICUS_PASSWD']),
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
    print(request_size, file=sys.stderr)
    extractor.close()
    self.assertIsNotNone(request_size)
    print(f'Test finished on: {time.time() - t_i}s\n')
  

  def test_access_dims_vars(self):
    t_i = time.time()
    _id = f'{self.__class__.__name__}_{inspect.currentframe().f_code.co_name}'
    datetime_str = datetime.now().strftime('%Y-%m-%d_%H:%M:%S.%f')
    _timestamp = "".join(str(datetime.now().timestamp()).split('.'))
    ds_name = f'{_id}_{_timestamp}.nc'
    print(f'---- Starting: {_id} at {datetime_str}', file=sys.stderr)

    def show_log(data):
      print(data, end='', file=sys.stderr)
    log_stream = LogStream(callback=show_log)

    extractor = OpendapExtractor(
      opendap_url='https://nrt.cmems-du.eu/thredds/dodsC/cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m',
      auth=SimpleAuth(user=config['DEFAULT']['COPERNICUS_USER'], passwd=config['DEFAULT']['COPERNICUS_PASSWD']),
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
    extractor.sync_connect()
    dims = extractor.get_dims()
    print('---- dims:', dims, file=sys.stderr)
    self.assertIsInstance(dims, list)
    vars = extractor.get_vars()
    print('---- vars:', vars, file=sys.stderr)
    self.assertIsInstance(vars, list)
    extractor.close()
    print(f'Test finished on: {time.time() - t_i}s\n')


  def test_4mb_donwload_sync(self):
    t_i = time.time()
    _id = f'{self.__class__.__name__}_{inspect.currentframe().f_code.co_name}'
    datetime_str = datetime.now().strftime('%Y-%m-%d_%H:%M:%S.%f')
    _timestamp = "".join(str(datetime.now().timestamp()).split('.'))
    ds_name = f'{_id}_{_timestamp}.nc'
    print(f'---- Starting: {_id} at {datetime_str}', file=sys.stderr)

    def show_log(data):
      print(data, end='', file=sys.stderr)
    log_stream = LogStream(callback=show_log)

    extractor = OpendapExtractor(
      opendap_url='https://nrt.cmems-du.eu/thredds/dodsC/cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m',
      auth=SimpleAuth(user=config['DEFAULT']['COPERNICUS_USER'], passwd=config['DEFAULT']['COPERNICUS_PASSWD']),
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
    extract_details = extractor.sync_connect().sync_extract(Path(DATA_DIR, ds_name))
    print(extract_details, file=sys.stderr)
    extractor.close()
    self.assertIsNotNone(extract_details)
    print(f'Test finished on: {time.time() - t_i}s\n')


  def test_4mb_donwload_async(self):
    t_i = time.time()
    _id = f'{self.__class__.__name__}_{inspect.currentframe().f_code.co_name}'
    datetime_str = datetime.now().strftime('%Y-%m-%d_%H:%M:%S.%f')
    _timestamp = "".join(str(datetime.now().timestamp()).split('.'))
    ds_name = f'{_id}_{_timestamp}.nc'
    print(f'---- Starting: {_id} at {datetime_str}', file=sys.stderr)

    def show_log(data):
      print(data, end='', file=sys.stderr)
    log_stream = LogStream(callback=show_log)

    extractor = OpendapExtractor(
      opendap_url='https://nrt.cmems-du.eu/thredds/dodsC/cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m',
      auth=SimpleAuth(user=config['DEFAULT']['COPERNICUS_USER'], passwd=config['DEFAULT']['COPERNICUS_PASSWD']),
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

    self.dataset_name = ds_name
    extractor.connect(
      success_callback=self._connect_success_basic_handler,
      failure_callback=self._failure_handler)
    
    # The runner id is the asynchronous method name.
    # Run the .wait(...) method will block the current thread.
    # In most cases, run this method won't be needed.
    extractor.async_runner_manager.get_runner('connect').wait()

    self.assertTrue(self.process_ok)

     # Run the extraction process
    extractor.extract(
      filepath=Path(DATA_DIR, self.dataset_name),
      success_callback=self._extract_success_handler,
      failure_callback=self._failure_handler)
    
    # The runner id is the asynchronous method name.
    # Run the .wait(...) method will block the current thread.
    # In most cases, run this method won't be needed.
    extractor.async_runner_manager.get_runner('extract').wait()
    # Alternatively, extractor.wait(...) can be used for extractor.extract(...)
    # as a short cut of the above way.

    # Always close the conexion when the extractor is not needed any more.
    extractor.close()

    self.assertTrue(self.process_ok)
    print(f'Test finished on: {time.time() - t_i}s\n')


class TestCopernicusOpendap(OpendapTestCase):
  def test_4mb_donwload_sync(self):
    t_i = time.time()
    _id = f'{self.__class__.__name__}_{inspect.currentframe().f_code.co_name}'
    datetime_str = datetime.now().strftime('%Y-%m-%d_%H:%M:%S.%f')
    _timestamp = "".join(str(datetime.now().timestamp()).split('.'))
    ds_name = f'{_id}_{_timestamp}.nc'
    print(f'---- Starting: {_id} at {datetime_str}', file=sys.stderr)

    def show_log(data):
      print(data, end='', file=sys.stderr)
    
    log_stream = LogStream(callback=show_log)

    extractor = OpendapExtractor(
      opendap_url='https://nrt.cmems-du.eu/thredds/dodsC/cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m',
      auth=SimpleAuth(user=config['DEFAULT']['COPERNICUS_USER'], passwd=config['DEFAULT']['COPERNICUS_PASSWD']),
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
    extract_details = extractor.sync_connect().sync_extract(Path(DATA_DIR, ds_name))
    print(extract_details, file=sys.stderr)
    extractor.close()
    self.assertIsNotNone(extract_details)
    print(f'Test finished on: {time.time() - t_i}s\n')
  
  
  def test_1_7_gb_download_sync(self):
    t_i = time.time()
    _id = f'{self.__class__.__name__}_{inspect.currentframe().f_code.co_name}'
    datetime_str = datetime.now().strftime('%Y-%m-%d_%H:%M:%S.%f')
    _timestamp = "".join(str(datetime.now().timestamp()).split('.'))
    ds_name = f'{_id}_{_timestamp}.nc'
    print(f'---- Starting: {_id} at {datetime_str}', file=sys.stderr)

    def show_log(data):
      print(data, end='', file=sys.stderr)
    log_stream = LogStream(callback=show_log)

    extractor = OpendapExtractor(
      opendap_url='https://nrt.cmems-du.eu/thredds/dodsC/cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m',
      auth=SimpleAuth(user=config['DEFAULT']['COPERNICUS_USER'], passwd=config['DEFAULT']['COPERNICUS_PASSWD']),
      dim_constraints={
        'time': slice('2020-11-01', '2020-12-20'),
        'depth': [0.494025]
      },
      requested_vars=['uo'],
      log_stream=log_stream,
      max_attempts=3,
      verbose=True)
    extract_details = extractor.sync_connect().sync_extract(Path(DATA_DIR, ds_name))
    print(extract_details, file=sys.stderr)
    extractor.close()
    self.assertIsNotNone(extract_details)
    print(f'Test finished on: {time.time() - t_i}s\n')
  

  def test_1_7_gb_download_async(self):
    t_i = time.time()
    _id = f'{self.__class__.__name__}_{inspect.currentframe().f_code.co_name}'
    datetime_str = datetime.now().strftime('%Y-%m-%d_%H:%M:%S.%f')
    _timestamp = "".join(str(datetime.now().timestamp()).split('.'))
    ds_name = f'{_id}_{_timestamp}.nc'
    print(f'---- Starting: {_id} at {datetime_str}', file=sys.stderr)
    
    def show_log(data):
      print(data, end='', file=sys.stderr)
    log_stream = LogStream(callback=show_log)

    extractor = OpendapExtractor(
      opendap_url='https://nrt.cmems-du.eu/thredds/dodsC/cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m',
      auth=SimpleAuth(user=config['DEFAULT']['COPERNICUS_USER'], passwd=config['DEFAULT']['COPERNICUS_PASSWD']),
      dim_constraints={
        'time': slice('2020-11-01', '2020-12-20'),
        'depth': [0.494025]
      },
      requested_vars=['uo'],
      log_stream=log_stream,
      max_attempts=3,
      verbose=True)
    
    self.dataset_name = ds_name
    extractor.connect(
      success_callback=self._connect_success_basic_handler,
      failure_callback=self._failure_handler)
    
    # The runner id is the asynchronous method name.
    # Run the .wait(...) method will block the current thread.
    # In most cases, run this method won't be needed.
    extractor.async_runner_manager.get_runner('connect').wait()

    self.assertTrue(self.process_ok)

     # Run the extraction process
    extractor.extract(
      filepath=Path(DATA_DIR, self.dataset_name),
      success_callback=self._extract_success_handler,
      failure_callback=self._failure_handler)
    
    # The runner id is the asynchronous method name.
    # Run the .wait(...) method will block the current thread.
    # In most cases, run this method won't be needed.
    extractor.async_runner_manager.get_runner('extract').wait()
    # Alternatively, extractor.wait(...) can be used for extractor.extract(...)
    # as a short cut of the above way.

    # Always close the conexion when the extractor is not needed any more.
    extractor.close()

    self.assertTrue(self.process_ok)
    print(f'Test finished on: {time.time() - t_i}s\n')


class TestHycomOpendap(OpendapTestCase):
  def test_size_query_and_time_decoding(self):
    t_i = time.time()
    self.process_ok = False
    self.extraction_details = None
    _id = f'{self.__class__.__name__}_{inspect.currentframe().f_code.co_name}'
    datetime_str = datetime.now().strftime('%Y-%m-%d_%H:%M:%S.%f')
    _timestamp = "".join(str(datetime.now().timestamp()).split('.'))
    ds_name = f'{_id}_{_timestamp}.nc'
    print(f'---- Starting: {_id} at {datetime_str}', file=sys.stderr)
    
    def show_log(data):
      print(data, end='', file=sys.stderr)
    log_stream = LogStream(callback=show_log)

    extractor = OpendapExtractor(
      opendap_url='http://tds.hycom.org/thredds/dodsC/GOMu0.04/expt_90.1m000/data/hindcasts/2023',
      dim_constraints={
        'depth': slice(0, 10),
        'lon': slice(-96, -85),
        'lat': slice(15, 30),
        'time': slice('2023-01-01T12:00:00.000000000', '2023-01-01T16:00:00.000000000')
      },
      log_stream=log_stream,
      verbose=True)
    request_size = extractor.sync_connect().get_size(SizeUnit.MEGA_BYTE)
    print('Size:', request_size, file=sys.stderr)
    print(extractor.dataset)
    extractor.close()
    self.assertIsNotNone(request_size)
    print(f'Test finished on: {time.time() - t_i}s\n')
  

  def test_40mb_download(self):
    t_i = time.time()
    self.process_ok = False
    self.extraction_details = None
    _id = f'{self.__class__.__name__}_{inspect.currentframe().f_code.co_name}'
    datetime_str = datetime.now().strftime('%Y-%m-%d_%H:%M:%S.%f')
    _timestamp = "".join(str(datetime.now().timestamp()).split('.'))
    ds_name = f'{_id}_{_timestamp}.nc'
    print(f'---- Starting: {_id} at {datetime_str}', file=sys.stderr)
    
    def show_log(data):
      print(data, end='', file=sys.stderr)
    log_stream = LogStream(callback=show_log)

    extractor = OpendapExtractor(
      opendap_url='http://tds.hycom.org/thredds/dodsC/GOMu0.04/expt_90.1m000/data/hindcasts/2023',
      dim_constraints={
        'depth': slice(0, 10),
        'lon': slice(-96, -85),
        'lat': slice(15, 30),
        'time': slice('2023-01-01T12:00:00.000000000', '2023-01-01T16:00:00.000000000')
      },
      log_stream=log_stream,
      verbose=True)
    self.print_dataset_structure = True
    self.dataset_name = ds_name
    # extractor.connect(success_callback=self._connect_success_handler, failure_callback=self._failure_handler)
    # extractor.async_runner_manager.get_runner('connect').wait()
    # extractor.close()
    extractor.connect(
      success_callback=self._connect_success_basic_handler,
      failure_callback=self._failure_handler)
    
    # The runner id is the asynchronous method name.
    # Run the .wait(...) method will block the current thread.
    # In most cases, run this method won't be needed.
    extractor.async_runner_manager.get_runner('connect').wait()

    self.assertTrue(self.process_ok)

     # Run the extraction process
    extractor.extract(
      filepath=Path(DATA_DIR, self.dataset_name),
      success_callback=self._extract_success_handler,
      failure_callback=self._failure_handler)
    
    # The runner id is the asynchronous method name.
    # Run the .wait(...) method will block the current thread.
    # In most cases, run this method won't be needed.
    extractor.async_runner_manager.get_runner('extract').wait()
    # Alternatively, extractor.wait(...) can be used for extractor.extract(...)
    # as a short cut of the above way.

    # Always close the conexion when the extractor is not needed any more.
    extractor.close()

    self.assertTrue(self.process_ok)

    # Read test
    print('Opening downloaded dataset.', file=sys.stderr)
    ds = wrangling.open_dataset(self.extraction_details.file.path, log_stream=log_stream)
    print(ds, file=sys.stderr)
    self.assertTrue(self.process_ok)
    print(f'Test finished on: {time.time() - t_i}s\n')
  

  def test_xy_0_5kb_download(self):
    t_i = time.time()
    warnings.filterwarnings("ignore")
    self.process_ok = False
    self.extraction_details: ExtractionDetails | None = None
    _id = f'{self.__class__.__name__}_{inspect.currentframe().f_code.co_name}'
    datetime_str = datetime.now().strftime('%Y-%m-%d_%H:%M:%S.%f')
    _timestamp = "".join(str(datetime.now().timestamp()).split('.'))
    ds_name = f'{_id}_{_timestamp}.nc'
    print(f'---- Starting: {_id} at {datetime_str}', file=sys.stderr)
    
    def show_log(data):
      print(data, end='', file=sys.stderr)
    log_stream = LogStream(callback=show_log)

    extractor = OpendapExtractor(
      opendap_url='https://tds.hycom.org/thredds/dodsC/GLBa0.08/expt_91.1',
      dim_constraints={
        'X': slice(1, 2),
        'Y': slice(1, 2),
        'MT': slice('2014-04-05T00:00:00.000000000', '2014-04-06T00:00:00.000000000'),
        'Depth': slice(0.,   10.,)
      },
      log_stream=log_stream,
      verbose=True)
    self.print_dataset_structure = True
    self.dataset_name = ds_name
    # extractor.connect(success_callback=self._connect_success_handler, failure_callback=self._failure_handler)
    # extractor.async_runner_manager.get_runner('connect').wait()
    # extractor.close()
    extractor.connect(
      success_callback=self._connect_success_basic_handler,
      failure_callback=self._failure_handler)
    
    # The runner id is the asynchronous method name.
    # Run the .wait(...) method will block the current thread.
    # In most cases, run this method won't be needed.
    extractor.async_runner_manager.get_runner('connect').wait()

    self.assertTrue(self.process_ok)

     # Run the extraction process
    extractor.extract(
      filepath=Path(DATA_DIR, self.dataset_name),
      success_callback=self._extract_success_handler,
      failure_callback=self._failure_handler)
    
    # The runner id is the asynchronous method name.
    # Run the .wait(...) method will block the current thread.
    # In most cases, run this method won't be needed.
    extractor.async_runner_manager.get_runner('extract').wait()
    # Alternatively, extractor.wait(...) can be used for extractor.extract(...)
    # as a short cut of the above way.

    # Always close the conexion when the extractor is not needed any more.
    extractor.close()

    self.assertTrue(self.process_ok)

    # Read test
    print('Opening downloaded dataset.', file=sys.stderr)
    ds = wrangling.open_dataset(self.extraction_details.file.path, log_stream=log_stream)
    print(ds, file=sys.stderr)
    self.assertTrue(self.process_ok)
    print(f'Test finished on: {time.time() - t_i}s\n')


if __name__ == '__main__':
  # general_utils.mkdir_r(DATA_DIR)
  unittest.main()
