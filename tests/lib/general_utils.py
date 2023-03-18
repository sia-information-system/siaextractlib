import pathlib
from configparser import ConfigParser
import os


def read_config():
  config_path = pathlib.Path(
    pathlib.Path(__file__).parent.absolute(),
    '..',
    '..',
    'etc',
    'config.ini')
  config = ConfigParser()
  config.read(config_path)
  return config


def mkdir_r(path):
  os.makedirs(path, exist_ok=True)

