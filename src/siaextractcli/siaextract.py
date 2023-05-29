import sys
import time
import pathlib
import json
import argparse
from siaextractlib.extractors.copernicus.motu import CopernicusMotuExtractor

# print log
def plog(message, log_type='info', sep = '\n', file = sys.stderr):
  types = { 'info': '[INFO]', 'warning': '[WARNING]', 'error': '[ERROR]' }
  prefix = types[log_type]
  print(f'{prefix}: {message}', sep=sep, file=file)


def setup():
  arg_parser = argparse.ArgumentParser(
    prog = 'siaextract',
    description = 'Executes the SIA extractor engine based on a given specification file.',
    epilog = 'SIA Project.')
  
  arg_parser.add_argument('filename',
    help='Name or path to the specification file.')
  arg_parser.add_argument('-a', '--action',
    choices=['extract', 'get-size'],
    default='extract',
    help='Set the action to perform: extract data or query size. Default: extract.')
  
  args = arg_parser.parse_args()
  return args


def load_conf_file(filename):
  config_file_path = pathlib.Path(filename)
  config_file = open(config_file_path)
  config = json.load(config_file)
  config_file.close()
  return config

def main():
  time_start = time.time()
  args = setup()
  config = load_conf_file(args.filename)
  extraction_result = None
  size_result = None
  
  plog(f'Source -> {config["data_source"]}')
  plog(f'Action -> {args.action}')
  plog('Starting process.')
  if config['data_source'] == 'copernicus-marine':
    copernicus_marine_extractor = CopernicusMotuExtractor(
      copernicus_user = config['user'],
      copernicus_passwd = config['passwd'],
      motu_source = config['motu_source'],
      service = config['service_id'],
      product = config['product_id'],
      lon = [config['lon_min'], config['lon_max']],
      lat = [config['lat_min'], config['lat_max']],
      depths = [config['depth_min'], config['depth_max']],
      dates = [config['date_min'], config['date_max']],
      vars = config['variables'],
      out_dir = config['out_dir'],
      sensing_frequency = 'daily',
      verbose=True)
    if args.action == 'extract':
      try:
        extraction_result = copernicus_marine_extractor.extract()
        plog(extraction_result)
      except Exception as err:
        plog(err, log_type='error')
    elif args.action == 'get-size':
      size_result, _ = copernicus_marine_extractor.get_size()
      plog(size_result)
  else:
    plog(f'{config["data_source"]} is not supported. Skipping.', log_type='warning')

  time_end = time.time()
  plog(f'Time elapsed: {time_end - time_start} s.')


if __name__ == '__main__':
  main()
