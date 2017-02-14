#!/usr/bin/env python
import sys, traceback
import os
import time

from threaded_ssh import ThreadedClients
from observer import *
from exp_config import *
from setup import *
from argparse import ArgumentParser

import logging

if 'threading' in sys.modules:
    del sys.modules['threading']
    import gevent
    import gevent.socket
    import gevent.monkey
    gevent.monkey.patch_all()

logging.basicConfig()

javaCmd = "java -jar {0}/{1}.jar {2}"
doneStr = "----- {0} DONE -----"

def populate():
   checkTopics()
   params = "kafka={0}:9092 zk={0}:2181 missing={1} sf={2} tuples={3} pcompletion={4} psuccess={5} bid_ratio={6} > {7}.{1}.log"
   aPa = params.format(Config.server, Config.missing, Config.tuples, Config.pcompletion, Config.psuccess, Config.bratio, "auctionProducer")
   bPa = params.format(Config.server, Config.missing, Config.tuples, Config.pcompletion, Config.psuccess, Config.bratio, "bidProducer")

   # populating auctions
   aC = ThreadedClients([Config.server], javaCmd.format(Config.jarpath, Config.aproducer, aPa))
   aC.start()
   aC.join()

   # populating bids
   bC = ThreadedClients([Config.server], javaCmd.format(Config.jarpath, Config.bproducer, bPa))
   bC.start()
   bC.join()
   print doneStr.format("POPULATION")
   time.sleep(3)

def execApp(approach):
    jParams = "kafka={0}:9092 zk={0}:2181 missing={1} > {2}.{1}.log"
    jPa = jParams.format(Config.server, Config.missing, approach)
    jC = ThreadedClients([Config.server], javaCmd.format(Config.jarpath, Config.pjoin, jPa))
    jC.start()
    jC.join()
    print doneStr.format(approach)

def runExperiments(approaches):
    for a in approaches:
        print "###############################"
        print "######### " + a + " ###########"
        print "###############################"
        # setup
        clients = startSetup()
        time.sleep(2)
        # run
        print "experiments with some settings"
        populate()
        execApp(a)
        # kill running services
        for c in clients:
            c.kill()
        # stop
        stopSetup()
        

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-l", dest='l', help="Location", default='l')
    parser.add_argument("-a", dest='approaches', type=str, nargs='*', help="Approaches")
    args = parser.parse_args()
    if (args.l == 'h'):
        Config.exec_path = Config.exec_path_h
    runExperiments(args.approaches)
