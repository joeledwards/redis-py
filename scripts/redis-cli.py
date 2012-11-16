#!/usr/bin/env python
import os
import random
import re
import readline
from redis import config
import sys
import time

cfg = config.from_file(config.select_config(config.find_configs()))

try:
    command = "redis-cli -h %(host)s -p %(port)s -a %(auth)s" % cfg
    sys.exit(os.system(command))
except KeyError, ex:
    print "Malformed config key '%s' in %s" % (str(ex), config_file)
    sys.exit(1)
except Exception, ex:
    print "Could not connect to database. Details: %s" % str(ex)
    sys.exit(1)

