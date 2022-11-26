# NOTE: This is a legacy test file. Its use is not recommended.
# Use the 'tests.py' file at the root directory instead.
# If you still want to use this file, do not run it in this location.
# Run it in the project root directory. It will fail otherwise.

import pathlib
from configparser import ConfigParser
from omdepextractlib.sources import copernicus
import time
import sys
from tools.general_utils import read_config, mkdir_r

# TODO: Implement the library "unittest": https://docs.python.org/3/library/unittest.html#assert-methods

DATA_DIR = pathlib.Path(pathlib.Path(__file__).parent.absolute(), 'tmp', 'data')

# Functions

def test_4bg_date_range_forward():
  print('\n--- Starting 4GB date range forward test. ---', file=sys.stderr)
  time_start = time.time()
  config = read_config()
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


def test_4gb_download():
  print('\n--- Starting 4GB download test. ---', file=sys.stderr)
  time_start = time.time()
  config = read_config()
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
    print(cop.extract(), file=sys.stderr)
  except Exception as err:
    print('--------- ERROR ---------', file=sys.stderr)
    print(err)
  finally:
    time_end = time.time()
    print(f'----> Time elapsed: {time_end - time_start} s.', file=sys.stderr)


def test_34kb_download():
  print('\n--- Starting 34KB download test. ---', file=sys.stderr)
  time_start = time.time()
  config = read_config()
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
    print(cop.extract(), file=sys.stderr)
  except Exception as err:
    print('--------- ERROR ---------', file=sys.stderr)
    print(err)
  time_end = time.time()
  print(f'----> Time elapsed: {time_end - time_start} s.', file=sys.stderr)


def main():
  # config = read_config()
  # --service-id GLOBAL_ANALYSIS_FORECAST_PHY_001_024-TDS
  # --product-id global-analysis-forecast-phy-001-024
  # --longitude-min -90
  # --longitude-max -80 
  # --latitude-min 10
  # --latitude-max 30
  # --date-min "2022-10-11 12:00:00"
  # --date-max "2022-10-11 12:00:00"
  # --depth-min 0.494
  # --depth-max 5727.917
  # --variable thetao
  # --variable so
  # --variable vo
  # --variable uo
  # --variable zos

  # cop = copernicus.DatasetClient(
  #   copernicus_user = config['DEFAULT']['COPERNICUS_USER'],
  #   copernicus_passwd = config['DEFAULT']['COPERNICUS_PASSWD'],
  #   motu_source = 'https://nrt.cmems-du.eu/motu-web/Motu',
  #   service = 'GLOBAL_ANALYSIS_FORECAST_PHY_001_024-TDS',
  #   product = 'global-analysis-forecast-phy-001-024',
  #   lon = [-90, -80],
  #   lat = [10, 30],
  #   depths = [0.494, 5727.917],
  #   dates = ['2022-10-11 00:00:00', '2022-10-13 12:00:00'],
  #   vars = ['thetao', 'so', 'vo', 'uo', 'zos'],
  #   out_dir = f'{pathlib.Path(__file__).parent.absolute()}/../tmp/data',
  #   sensing_frequency = 'daily',
  #   verbose=True)

  # cop = copernicus.DatasetClient(
  #   copernicus_user = config['DEFAULT']['COPERNICUS_USER'],
  #   copernicus_passwd = config['DEFAULT']['COPERNICUS_PASSWD'],
  #   motu_source = 'https://nrt.cmems-du.eu/motu-web/Motu',
  #   service = 'GLOBAL_ANALYSIS_FORECAST_PHY_001_024-TDS',
  #   product = 'global-analysis-forecast-phy-001-024',
  #   lon = [-90, -80],
  #   lat = [10, 30],
  #   depths = [0.494, 5727.917],
  #   dates = ['2020-01-01 12:00:00', '2020-12-31 12:00:00'],
  #   vars = ['thetao', 'so', 'vo', 'uo', 'zos'],
  #   out_dir = f'{pathlib.Path(__file__).parent.absolute()}/../tmp/data')

  # completed_process = cop.exec_fetch(['2022-10-11 00:00:00', '2022-10-11 00:00:00'], 'ttt_test.cf')
  # #completed_process = cop.exec_fetch(['2020-01-01 12:00:00', '2020-12-31 12:00:00'], 'test.cf')
  # print('STDERR:')
  # print(completed_process.stderr)
  # print('STDOUT:')
  # print(completed_process.stdout)
  # print('Completed Process:')
  # print(completed_process)

  test_4bg_date_range_forward()
  test_34kb_download()
  test_4gb_download()


if __name__ == '__main__':
  mkdir_r(DATA_DIR)
  main()
