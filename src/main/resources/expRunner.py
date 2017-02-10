#!/usr/bin/env python
import sys, traceback
import os
import time

from threaded_ssh import ThreadedClients
from observer import *
from exp_config import *
from setup import *


import logging

if 'threading' in sys.modules:
    del sys.modules['threading']
    import gevent
    import gevent.socket
    import gevent.monkey
    gevent.monkey.patch_all()

logging.basicConfig()

if __name__ == "__main__":
    # stop
    logging
    stopSetup()
    # setup
    clients = startSetup()
    # run
    print "experiments with some settings"
    # kill running services
    for c in clients:
        c.kill()