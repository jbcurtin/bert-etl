#!/usr/bin/env python
'''
In this exmaple of how to use bert, we'll create a pipeline that'll download Messier Objects from a copy of the Messier
Catelog on wikipedia.
'''

# https://github.com/mdeff/fma#coverage
# http://www.audiocontentanalysis.org/data-sets/
# https://data.vision.ee.ethz.ch/cvl/ae_dataset/

import os
import typing

from bert import binding, constants, utils

ENCODING: str = 'utf-8'
OUTPUT_DIR: str = os.path.join(os.getcwd(), 'sounds')
HEADERS: typing.Dict[str, str] = {
  'User-Agent': 'https://github.com/jbcurtin/bert'
}

@binding.follow('noop', pipeline_type=constants.PipelineType.BOTTLE)
def sync_sounds():
  '''
  comm_binder takes the function you're currently writing as an argument and returns two queues backed by Redis.
  - The first Queue is a work queue, named after a the concept of accepting work
  - The second Queue is a done queue, named after the concept of offering work because you're done with it in this step
  - ologger is a python logger instance, with the associated process-id in the name that you're executing within
  '''
  work_queue, done_queue, ologger = utils.comm_binders(sync_sounds)

  '''
  The rationale put putting imports inside the fuctions is based aruond how the jobs are executed. When DEBUG is True,
  everything is synchronous. When DEBUG is False, the multiprocessing module is utilized to spawn daemons and manage
  subprocesses executing copies of the fuction you're working inside.
  '''
  import audioread
  import csv
  import librosa
  import os
  import requests
  import typing
  import zipfile
  '''
  Repeatability or idempotent functions go a long way with reducing your debug loop. In the commandline function of
  bert, you're provided with the option to flush the Redis database on every job start. This allows you to create
  artifacts while processing data. Binding the functions together, allowing you to execute the functions in order
  without having to recompute the resutls
  '''
  if not os.path.exists(OUTPUT_DIR):
    ologger.info(f'Creating Directory[{OUTPUT_DIR}]')
    os.makedirs(OUTPUT_DIR)

  '''
  Sometimes repeatability requires code-duplication. When opening an archive, it could be a few gigabytes in size. If the archive isn't
  open, the following code will open it to disk. If this function is ran a second time, it'll skip opening the files to disk and add the
  files to queue.
  ''' 
  archive_path: str = os.path.join(OUTPUT_DIR, 'ae_dataset.zip')
  if not os.path.exists(archive_path):
    # full dataset
    #  https://data.vision.ee.ethz.ch/cvl/ae_dataset/ae_dataset.zip
    url: str = f'https://github.com/jbcurtin/bert/blob/d4a0e0792f093fd86708074c2ff14ef0040aa1d1/data/ae_dataset-subset.zip?raw=true'
    ologger.info(f'Downloading Sounds[{url}]')
    response = requests.get(url, stream=True, headers=HEADERS)
    with open(archive_path, 'wb') as stream:
      for chunk in response.iter_content(chunk_size=1024):
        stream.write(chunk)
  
  archive_output_path: str = os.path.join(OUTPUT_DIR, 'ae_dataset')
  if not os.path.exists(archive_output_path):
    ologger.info(f'Opening Archive[{archive_output_path}]')
    with zipfile.ZipFile(archive_path, 'r') as compressed_stream:
      for name in compressed_stream.namelist():
        info: zipfile.ZipInfo = compressed_stream.getinfo(name)
        infopath: str = os.path.join(archive_output_path, info.filename.strip('/'))
        if info.is_dir():
          if not os.path.exists(infopath):
            os.makedirs(infopath)
          continue

        with open(infopath, 'wb') as output_stream:
          output_stream.write(compressed_stream.read(name))

        try:
          wave, samplerate = librosa.core.load(infopath, mono=True, sr=None)
        except audioread.NoBackendError:
          ologger.info(f'Unable to load WaveFile[{infopath}]')
          continue

        done_queue.put({
          'class': os.path.dirname(infopath).rsplit('/', 1)[1],
          'wave_path': infopath,
          'samplerate': samplerate,
          'duration': librosa.core.get_duration(wave),
        })

  else:
    ologger.info(f'Scanning Archive[{archive_output_path}]')
    with zipfile.ZipFile(archive_path, 'r') as compressed_stream:
      for name in compressed_stream.namelist():
        info: zipfile.ZipInfo = compressed_stream.getinfo(name)
        infopath: str = os.path.join(archive_output_path, info.filename.strip('/'))
        if info.is_dir():
          continue

        try:
          wave, samplerate = librosa.core.load(infopath, mono=True, sr=None)
        except audioread.NoBackendError:
          ologger.info(f'Unable to load WaveFile[{infopath}]')
          continue

        done_queue.put({
          'class': os.path.dirname(infopath).rsplit('/', 1)[1],
          'wave_path': infopath,
          'samplerate': samplerate,
          'duration': librosa.core.get_duration(wave),
        })


@binding.follow(sync_sounds, pipeline_type=constants.PipelineType.BOTTLE)
def analyse_sound_files():
  work_queue, done_queue, ologger = utils.comm_binders(analyse_sound_files)
  import librosa
  import numpy as np

  samples: typing.Dict[str, typing.List[np.array]] = []
  for details in work_queue:
    if details['class'] == 'test':
      continue

    # Begin work here
    ologger.info('New to Bert? Check out the tutorial[pending]')
    import sys; sys.exit(0)

