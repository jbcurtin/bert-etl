#!/usr/bin/env python3

import os
import sys

from bert.deploy import factory

if __name__ in ['__main__']:
  sys.path.append(os.getcwd())
  options = factory.capture_options()
  factory.deploy_service(options)

