#!/usr/bin/env python

import os
import sys

from bert import factory, utils

if __name__ in ['__main__']:
  sys.path.append(os.getcwd())
  options = factory.capture_options()
  factory.start_service(options)

