import subprocess
import re
from pathlib import Path
from siaextractlib.utils import exceptions, metadata
import xml.dom.minidom as minidom
from datetime import date, timedelta, datetime
import sys
import xarray
import warnings


# Legacy
# Not a standard implementation.
class CopernicusMotuExtractor:
  def __init__(
    self, 
    copernicus_user = '', 
    copernicus_passwd = '', 
    motu_source = '', 
    service = '', 
    product = '', 
    lon = [], 
    lat = [], 
    depths = [],
    dates = [],
    vars = [],
    out_dir = '',
    increase_factor = 12,
    sensing_frequency = '',
    verbose = False,
    max_attempts_to_compute_date_range = 50):
      warn_message = [
        'This extractor do not implement the standard interface for extractors,',
        'so it may not be compatible with other extractors.',
        'Moreover, it is not fully stable so it may fail in some cases.',
        'Its use is discouraged.'
      ]
      warnings.warn(' '.join(warn_message), DeprecationWarning)
      self.copernicus_user = copernicus_user
      self.copernicus_passwd = copernicus_passwd
      self.motu_source = motu_source
      self.service = service
      self.product = product
      self.lon = lon
      self.lat = lat
      self.depths = depths
      self.dates = dates
      self.vars = vars
      self.out_dir = out_dir
      self.__tmp_files = []
      self.increase_factor = increase_factor
      self.sensing_frequency = sensing_frequency
      self.verbose = verbose
      self.max_attempts_to_compute_date_range = max_attempts_to_compute_date_range
      # Validations
      self.__validate_fields()
  

  def __validate_fields(self):
    sensing_frequency_allowed = ['hourly', 'daily', 'monthly', 'yearly']
    if not (self.sensing_frequency in sensing_frequency_allowed):
      raise exceptions.WrongArgsException(
        message = f'{self.sensing_frequency} is not supported',
        fields = ['sensing_frequency'])


  def __exec_fetch(self, dates, out_name):
    command = [
      'motuclient',
      f'--motu {self.motu_source}',
      f'--service-id {self.service}',
      f'--product-id {self.product}',
      f'--longitude-min {min(self.lon)}',
      f'--longitude-max {max(self.lon)}',
      f'--latitude-min {min(self.lat)}',
      f'--latitude-max {max(self.lat)}',
      f'--date-min \'{dates[0]}\'',
      f'--date-max \'{dates[1]}\'',
      f'--depth-min {min(self.depths)}',
      f'--depth-max {max(self.depths)}',
      f'--out-dir {self.out_dir}',
      f'--out-name {out_name}',
      f'--user {self.copernicus_user}',
      f'--pwd {self.copernicus_passwd}'
    ]

    for v in self.vars:
      command.append(f'--variable {v}')

    completed_process = subprocess.run(command, stderr = subprocess.PIPE, stdout = subprocess.PIPE, universal_newlines = True)
    # completed_process = subprocess.run(command)

    path_dataset = Path(self.out_dir, out_name)

    if self.verbose:
      print(f'File has been stored in: {path_dataset}', file=sys.stderr)
      print('Analysing extraction logs and exit code.', file=sys.stderr)

    # In stdout, a line with:
    # '[ INFO] Done' if OK.
    # '[ERROR] 010-6 : The date range is invalid. No data in date range: [2022-10-11 00:00:00,2022-10-11 00:00:00]' if date range is invalid.
    # '[ERROR]' if non zero return code.
    try:
      if completed_process.returncode != 0:
        if self.verbose:
          print('Process child "motuclient" returned a non-zero exit code.', file = sys.stderr)
          print('Deleting result file if exists.', file = sys.stderr)
        path_dataset.unlink(missing_ok = True)
        raise exceptions.ExtractionException(messages=[
          'Process child "motuclient" returned a non-zero exit code',
          completed_process.stdout])
      re_invalid_dates = re.compile('\[ERROR\] 010-6')
      re_general_error = re.compile('\[ERROR\]')
      for line in completed_process.stdout.splitlines():
        m = re_invalid_dates.search(line)
        if m is not None:
          raise exceptions.WrongExtractionArgsException(messages=[
            m.group(0),
            completed_process.stdout])
        m = re_general_error.search(line)
        if m is not None:
          raise exceptions.ExtractionException(messages=[
            m.group(0),
            completed_process.stdout])
    except Exception as err:
      path_dataset.unlink(missing_ok = True)
      raise err
    if self.verbose:
      print('Partial file completed.', file=sys.stderr)
    return metadata.FileDetails(
      description = 'netcdf_dataset_part',
      path = path_dataset)
  

  def get_next_date_range_daily(self, last_date_max = None):
    return self.__get_next_date_range_daily(last_date_max)


  def __get_next_date_range_daily(self, last_date_max):
    if self.verbose:
      print('Computing date range.', file = sys.stderr)
    next_date_min = None
    next_date_max = None
    hour_part_min = None
    hour_part_max = None
    global_date_max = date.fromisoformat(self.dates[1].split()[0])
    # increase_factor = 12
    if (last_date_max is None) or (last_date_max == ''):
      base_date_splited = self.dates[0].split()
      next_date_min = date.fromisoformat(base_date_splited[0])
      hour_part_min = base_date_splited[1]
      hour_part_max = hour_part_min
    else:
      base_date_splited = last_date_max.split()
      next_date_min = date.fromisoformat(
        base_date_splited[0])
      # TODO: Add 25 or 25 minutes, not 1 day.
      next_date_min = next_date_min + timedelta(days = 1)
      hour_part_min = base_date_splited[1]
      hour_part_max = hour_part_min

    valid_range = False
    xml_path = None
    by_pass_increase_factor = False
    attempt_counter = 0
    while not valid_range:
      if attempt_counter > self.max_attempts_to_compute_date_range:
        raise exceptions.ExtractionException(
          messages=f'Too many attempts: {attempt_counter}. Max allowed: {self.max_attempts_to_compute_date_range}')
      if not by_pass_increase_factor:
        next_date_max = next_date_min + timedelta(days = 30 * self.increase_factor)
      if next_date_max > global_date_max:
        next_date_max = global_date_max
        hour_part_max = self.dates[1].split()[1]
        if next_date_min > next_date_max:
          raise exceptions.EndOfDataException(messages='"date_min" is greater than "date_max". Extraction can be stopped.')
      try:
        attempt_counter += 1
        if self.verbose:
          print(
            'Date range to test:', 
            [f'{str(next_date_min)} {hour_part_min}', f'{str(next_date_max)} {hour_part_max}'],
            file = sys.stderr)
        file_size, xml_path = self.__get_size(
          [f'{str(next_date_min)} {hour_part_min}', f'{str(next_date_max)} {hour_part_max}'],
          remove_file_when_finish = False)
        if self.verbose:
          print(f'Computed file size: {file_size.size} {file_size.get_unit_name()}', file = sys.stderr)
        if file_size.code == '005-0':
          valid_range = True
        else:
          self.increase_factor -= 2
          if self.increase_factor <= 0:
            self.increase_factor = 1
          if self.verbose:
            print(file_size.message, file = sys.stderr)
            print(f'Using increase factor={self.increase_factor}.', file = sys.stderr)
        by_pass_increase_factor = False
        xml_path.unlink(missing_ok = True)
        xml_path = None
      except exceptions.WrongExtractionArgsException as err:
        if self.verbose:
          print('Date range not acceptable.', file = sys.stderr)
          print('Trying to get a correct date range from error details.', file = sys.stderr)
        # print(err.source_log, file = sys.stderr)
        if len(err.files) != 0:
          xml_path = err.files[0].path
        else:
          if self.verbose:
            print('No generated file detected. This is a critical error.', file = sys.stderr)
          raise exceptions.UnexpectedFileStructureException(messages=[
            'Date range could not be determinated because there is not enough information.',
            'File with error messages for bad date range (produced by: UnexpectedFileStructureException) is missing.'])
        xml_file_ref = open(xml_path)
        re_dates = re.compile('(([0123456789]{4})-([0123456789]{2})-([0123456789]{2})) (([0123456789]{2}):([0123456789]{2}):([0123456789]{2})) and values >= (([0123456789]{4})-([0123456789]{2})-([0123456789]{2})) (([0123456789]{2}):([0123456789]{2}):([0123456789]{2}))')
        for line in xml_file_ref:
          date_match = re_dates.search(line)
          if date_match is not None:
            # 2022-10-10 12:00:00 and values >= 2022-10-11 12:00:00
            # ('2022-10-10', '2022', '10', '10', '12:00:00', '12', '00', '00', '2022-10-11', '2022', '10', '11', '12:00:00', '12', '00', '00')
            # print(date_match.groups(), file = sys.stderr)
            # Get the upper limit
            next_date_max = date.fromisoformat(date_match.group(9))
            hour_part_max = date_match.group(13)
            by_pass_increase_factor = True
            break
        xml_file_ref.close()
      except exceptions.ExtractionException as err:
        self.increase_factor -= 2
        if self.increase_factor <= 0:
          self.increase_factor = 1
      finally:
        if xml_path is not None:
          xml_path.unlink(missing_ok = True)
          xml_path = None
    if self.verbose:
      print('Range accepted.', file = sys.stderr)
    return [f'{str(next_date_min)} {hour_part_min}', f'{str(next_date_max)} {hour_part_max}']
  

  def extract(self):
    date_min = None
    date_max = None
    prev_date_max = None
    subset_paths = []
    extraction_result = None
    if self.verbose:
      print('Starting extraction.', file = sys.stderr)
    while date_max != self.dates[1]:
      if self.verbose:
        print('Generating date range.', file = sys.stderr)
      prev_date_max = date_max
      if date_max == self.dates[1]:
        break
      if self.sensing_frequency == 'hourly':
        raise NotImplementedError(
          f'sensing_frequency="{self.sensing_frequency}" is not implemented yet')
      elif self.sensing_frequency == 'daily':
        [date_min, date_max] = self.__get_next_date_range_daily(date_max)
      elif self.sensing_frequency == 'monthly':
        raise NotImplementedError(
          f'sensing_frequency="{self.sensing_frequency}" is not implemented yet')
      elif self.sensing_frequency == 'yearly':
        raise NotImplementedError(
          f'sensing_frequency="{self.sensing_frequency}" is not implemented yet')
      else:
        raise NotImplementedError(
          f'sensing_frequency="{self.sensing_frequency}" is not supported')
      now = datetime.now()
      curr_date_str = now.strftime('%Y-%m-%d') 
      curr_time_srd = now.strftime('%Hh-%Mm-%Ss-%fms')
      out_name = f'partial_{self.product}-{self.service}-date-{curr_date_str}-time-{curr_time_srd}.nc'
      # If ok, continue with the extraction. If not, try to recover. 
      # If not working after many tries, rise an error.
      try:
        if self.verbose:
          print('Executing fetch. This can take a while.', file = sys.stderr)
        generated_file = self.__exec_fetch([date_min, date_max], out_name)
        subset_paths.append(generated_file.path)
        if self.verbose:
          print('Fetch successfuly completed.', file = sys.stderr)
      except exceptions.ExtractionException as err:
        # WrongExtractionArgsException
        extraction_result = metadata.ExtractionDetails(
          description = 'copernicus_subset',
          # 'Generated date range not valid. This is a critical error',
          logs = err.messages,
          date_min = self.dates[0],
          date_max = prev_date_max)
        break
      except Exception as err:
        extraction_result = metadata.ExtractionDetails(
          description = 'copernicus_subset',
          logs = [
            'Unexpected extraction error',
            str(err)
          ],
          date_min = self.dates[0],
          date_max = prev_date_max)
        break
    n_files_downloaded = len(subset_paths)
    if self.verbose:
      print(f'Starting file fusion. Partial files downloaded: {n_files_downloaded}.', file=sys.stderr)
    # Generating filename and path.
    now = datetime.now()
    curr_date_str = now.strftime('%Y-%m-%d') 
    curr_time_srd = now.strftime('%Hh-%Mm-%Ss-%fms')
    out_name = f'{self.product}-{self.service}-date-{curr_date_str}-time-{curr_time_srd}.nc'
    dataset_file = metadata.FileDetails(
      description=f'{self.product}-{self.service}',
      path=Path(self.out_dir, out_name))
    # Mergin and writing dataset partials.
    dataset = None
    if n_files_downloaded > 0:
      dataset = xarray.open_mfdataset(subset_paths, combine = 'by_coords')
      dataset.to_netcdf(dataset_file.path)
      dataset.close()
    # Generating extraction result objet.
    # If none, the extraction was successful.
    if extraction_result is None:
      if self.verbose:
        print('Extraction successfuly done.')
      extraction_result = metadata.ExtractionDetails(
        description=f'{self.product}-{self.service}',
        file=dataset_file if n_files_downloaded > 0 else None,
        complete=True,
        time_min=self.dates[0],
        time_max=self.dates[1])
    # Else, there was an error y just return what was downloaded.
    else:
      if self.verbose:
        print('Some errors ocurred when executing extraction. Using partials extracted')
      extraction_result.file = dataset_file if n_files_downloaded > 0 else None
    # Deleting partial files.
    for file_path in subset_paths:
      file_path.unlink(missing_ok = True)
    # Returning result.
    return extraction_result


  def get_size(self, remove_file_when_finish = True):
    return self.__get_size(
      self.dates,
      remove_file_when_finish = remove_file_when_finish)
  

  def __decode_size_unit(self, size_unit):
    if size_unit == 'b' or size_unit == 'B':
      return metadata.SizeUnit.BYTE
    elif size_unit == 'kB':
      return metadata.SizeUnit.KILO_BYTE
    elif size_unit == 'mB':
      return metadata.SizeUnit.MEGA_BYTE
    elif size_unit == 'gB':
      return metadata.SizeUnit.GIGA_BYTE
    return size_unit


  def __get_size(self, dates, remove_file_when_finish = True):
    if self.verbose:
      print(f'Querying file size.', file=sys.stderr)
    out_name = "".join(str(datetime.now().timestamp()).split('.'))
    command = [
      'motuclient',
      f'--motu {self.motu_source}',
      f'--service-id {self.service}',
      f'--product-id {self.product}',
      f'--longitude-min {min(self.lon)}',
      f'--longitude-max {max(self.lon)}',
      f'--latitude-min {min(self.lat)}',
      f'--latitude-max {max(self.lat)}',
      f'--date-min \'{dates[0]}\'',
      f'--date-max \'{dates[1]}\'',
      f'--depth-min {min(self.depths)}',
      f'--depth-max {max(self.depths)}',
      f'--out-dir {self.out_dir}',
      f'--out-name {out_name}',
      f'--user {self.copernicus_user}',
      f'--pwd {self.copernicus_passwd}',
      '--size'
    ]

    for v in self.vars:
      command.append(f'--variable {v}')

    completed_process = subprocess.run(command, stderr = subprocess.PIPE, stdout = subprocess.PIPE, universal_newlines = True)
    #completed_process = subprocess.run(command)

    # In STDOUT (using file mode):
    # '[WARNING] File size: unknown' means it was unable to determine size. Maybe wrong value parameters (like date range).
    # '[ INFO] Done' if everything is ok.

    # Read the XML if the request was ok, and return the size.
    path_xml = Path(self.out_dir, out_name)

    if self.verbose:
      print(f'Query result has been stored in: {path_xml}', file=sys.stderr)
      print('Analysing extraction logs, exit code and result.', file=sys.stderr)

    if completed_process.returncode != 0:
      # remove xml file.
      if remove_file_when_finish:
        path_xml.unlink(missing_ok = True)
      # rise exception with stdout.
      raise exceptions.ExtractionException(messages=[
        'Motuclient retured with a non-zero exit code', 
        completed_process.stdout],
        files = [
          metadata.FileDetails(
            description = 'file_size_request',
            path = path_xml)
        ]) # ExtractionException

    # Verify the request status. If something is wrong, delete the XML and rise an exception.
    re_unknown_size = re.compile('(\[WARNING\] File size: unknown){1}')
    for line in completed_process.stdout.splitlines():
      if re_unknown_size.search(line) is not None:
        if remove_file_when_finish:
          path_xml.unlink(missing_ok = True)
        raise exceptions.WrongExtractionArgsException(
          messages = [
            'File size unknown. Some value parameters (like dates) may be wrong.',
            completed_process.stdout
          ],
          files = [
            metadata.FileDetails(
              description = 'file_size_request',
              path = path_xml)
          ])

    # Read the XML file, parse it and return the value.
    # If something is wrong, delete the XML and rise an exception.
    try:
      xml_file = minidom.parse(str(path_xml))
      request_size = xml_file.getElementsByTagName('requestSize')
      if self.verbose:
        print('Analysis done. Returning result.', file=sys.stderr)
      return metadata.RequestSize(
        size = request_size[0].attributes['size'].value,
        unit = self.__decode_size_unit(request_size[0].attributes['unit'].value),
        #unit = request_size[0].attributes['unit'].value,
        max_allowed_size = request_size[0].attributes['maxAllowedSize'].value,
        code  = request_size[0].attributes['code'].value,
        message = request_size[0].attributes['msg'].value), path_xml
    except Exception as err:
      raise exceptions.UnexpectedFileStructureException(
        messages = [
          'Unable to access the file size data. XML file may not have the appropriate structure.',
          completed_process.stdout,
          str(err)
        ],
        files = [
          metadata.FileDetails(
            description = 'file_size_request',
            path = path_xml)
        ])
    finally:
      if remove_file_when_finish:
        path_xml.unlink(missing_ok = True)
      xml_file.unlink()

  
  def exec_fetch(self, dates, out_name):
    return self.__exec_fetch(dates, out_name)

