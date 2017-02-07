#!/usr/bin/env python
import sys
import signal
import os
import time

from threaded_ssh import ThreadedClients
from observer import *
from exp_config import *

from functools import partial

import logging
import gevent.monkey; gevent.monkey.patch_thread()

# if 'threading' in sys.modules:
#         del sys.modules['threading']
#         import gevent
#         import gevent.socket
#         import gevent.monkey
#         gevent.monkey.patch_all()

logging.basicConfig()

def confZk():
    deleteClient = ThreadedClients([Config.server], "rm -rf {0}".format(Config.zk_dir), root=True)
    deleteClient.start()
    deleteClient.join()

def startZk():
    observer = Observer("binding to port")
    zk_cmd = '{0}/bin/zookeeper-server-start.sh {0}/config/{1}'.format(Config.exec_path, Config.zk_config)
    zkClient = ThreadedClients([Config.server], zk_cmd, root=True, observers=[observer])
    print zk_cmd
    zkClient.start()
    # observer.waitFor(1)
    return zkClient

def startKafka():
    zkClient = startZk()
    # start kafka
    # create topics
    return [zkClient]

def exitGracefully(storageClients, signal, frame):
    print ""
    print "\033[1;31mShutting down Storage\033[0m"
    for client in storageClients:
        client.kill()
    #unmount_memfs()
    os.exit(0)

if __name__ == "__main__":
    expClients = startKafka()
    # signal.signal(signal.SIGINT, partial(exitGracefully, expClients))
    # print "\033[1;31mExp started\033[0m"
    # print "\033[1;31mHit Ctrl-C to shut it down\033[0m"
    for client in expClients:
        client.join()