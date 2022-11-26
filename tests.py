import pathlib
from omdepextractlib.sources import copernicus
import time
import sys
import unittest
import tools.general_utils as general_utils


DATA_DIR = pathlib.Path(pathlib.Path(__file__).parent.absolute(), 'tmp', 'data')


class TestCopernicusExtractor(unittest.TestCase):

  def test_4bg_date_range_forward(self):
    print('\n--- Starting 4GB date range forward test. ---', file=sys.stderr)
    time_start = time.time()
    config = general_utils.read_config()
    cop = copernicus.DatasetClient(
      copernicus_user = config['DEFAULT']['COPERNICUS_USER'],
      copernicus_passwd = config['DEFAULT']['COPERNICUS_PASSWD'],
      motu_source = 'https://nrt.cmems-du.eu/motu-web/Motu',
      service = 'GLOBAL_ANALYSIS_FORECAST_PHY_001_024-TDS',
      product = 'global-analysis-forecast-phy-001-024',
      lon = [-90, -80],
      lat = [10, 30],
      depths = [0.494, 5727.917],
      dates = ['2020-01-01 12:00:00', '2020-12-31 12:00:00'],
      vars = ['thetao', 'so', 'vo', 'uo', 'zos'],
      out_dir = DATA_DIR,
      sensing_frequency = 'daily',
      verbose=True)
    # test
    date_min = None
    date_max = None
    prev_date_max = None
    try:
      while date_max != cop.dates[1]:
        print('Generating date range.', file = sys.stderr)
        prev_date_max = date_max
        [date_min, date_max] = cop.get_next_date_range_daily(date_max)
        print(f'--> date_min: {date_min}, date_max: {date_max}.', file=sys.stderr)
    except Exception as err:
      print('--------- ERROR ---------', file=sys.stderr)
      print(err)
    print('Finishing test.', file=sys.stderr)
    time_end = time.time()
    print(f'----> Time elapsed: {time_end - time_start}s.', file=sys.stderr)
    self.assertTrue(date_max == cop.dates[1])


  def test_34kb_download(self):
    print('\n--- Starting 34KB download test. ---', file=sys.stderr)
    time_start = time.time()
    config = general_utils.read_config()
    extraction_result = None
    cop = copernicus.DatasetClient(
      copernicus_user = config['DEFAULT']['COPERNICUS_USER'],
      copernicus_passwd = config['DEFAULT']['COPERNICUS_PASSWD'],
      motu_source = 'https://nrt.cmems-du.eu/motu-web/Motu',
      service = 'GLOBAL_ANALYSIS_FORECAST_PHY_001_024-TDS',
      product = 'global-analysis-forecast-phy-001-024',
      lon = [-90, -80],
      lat = [10, 30],
      depths = [0.494, 5727.917],
      dates = ['2022-10-11 00:00:00', '2022-10-13 12:00:00'],
      vars = ['thetao', 'so', 'vo', 'uo', 'zos'],
      out_dir = DATA_DIR,
      sensing_frequency = 'daily',
      verbose=True)
    # test
    try:
      extraction_result = cop.extract()
      print(extraction_result, file=sys.stderr)
    except Exception as err:
      print('--------- ERROR ---------', file=sys.stderr)
      print(err)
    time_end = time.time()
    print(f'----> Time elapsed: {time_end - time_start} s.', file=sys.stderr)
    extraction_not_none = extraction_result is not None
    self.assertTrue(extraction_not_none)
    if extraction_not_none:
      self.assertTrue(extraction_result.complete)


  def test_4gb_download(self):
    print('\n--- Starting 4GB download test. ---', file=sys.stderr)
    time_start = time.time()
    config = general_utils.read_config()
    extraction_result = None
    cop = copernicus.DatasetClient(
      copernicus_user = config['DEFAULT']['COPERNICUS_USER'],
      copernicus_passwd = config['DEFAULT']['COPERNICUS_PASSWD'],
      motu_source = 'https://nrt.cmems-du.eu/motu-web/Motu',
      service = 'GLOBAL_ANALYSIS_FORECAST_PHY_001_024-TDS',
      product = 'global-analysis-forecast-phy-001-024',
      lon = [-90, -80],
      lat = [10, 30],
      depths = [0.494, 5727.917],
      dates = ['2020-01-01 12:00:00', '2020-12-31 12:00:00'],
      vars = ['thetao', 'so', 'vo', 'uo', 'zos'],
      out_dir = DATA_DIR,
      sensing_frequency = 'daily',
      verbose=True)
    # test
    try:
      extraction_result = cop.extract()
      print(extraction_result, file=sys.stderr)
    except Exception as err:
      print('--------- ERROR ---------', file=sys.stderr)
      print(err)
    time_end = time.time()
    print(f'----> Time elapsed: {time_end - time_start} s.', file=sys.stderr)
    extraction_not_none = extraction_result is not None
    self.assertTrue(extraction_not_none)
    if extraction_not_none:
      self.assertTrue(extraction_result.complete)


if __name__ == '__main__':
  general_utils.mkdir_r(DATA_DIR)
  unittest.main()
