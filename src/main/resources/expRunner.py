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

def populate(nMiss):
   checkTopics()
   print doneStr.format("CREATION")
   time.sleep(3)

   #params = "kafka={0}:9092 zk={0}:2181 missing={1} sf={2} tuples={3} pcompletion={4} psuccess={5} bid_ratio={6} > {7}.{1}.log"
   params = "kafka={0}:9092 zk={0}:2181 missing={1} sf={2} tuples={3} pcompletion={4} psuccess={5} bid_ratio={6}"
   #aPa = params.format(Config.server, Config.missing, Config.sf, Config.tuples, Config.pcompletion, Config.psuccess, Config.bratio, "auctionProducer")
   aPa = params.format(Config.server, nMiss, Config.sf, Config.tuples, Config.pcompletion, Config.psuccess, Config.bratio)
   #bPa = params.format(Config.server, Config.missing, Config.sf, Config.tuples, Config.pcompletion, Config.psuccess, Config.bratio, "bidProducer")
   bPa = params.format(Config.server, nMiss, Config.sf, Config.tuples, Config.pcompletion, Config.psuccess, Config.bratio)

   # populating auctions
   aC = ThreadedClients([Config.server], javaCmd.format(Config.jarpath, Config.aproducer, aPa))
   aC.start()
   aC.join()
   time.sleep(3)

   # populating bids
   bC = ThreadedClients([Config.server], javaCmd.format(Config.jarpath, Config.bproducer, bPa))
   bC.start()
   bC.join()
   print doneStr.format("POPULATION")
   time.sleep(3)

def execApp(approach, nMiss):
    #jParams = "kafka={0}:9092 zk={0}:2181 missing={1} > {2}.{1}.log"
    jParams = "kafka={0}:9092 zk={0}:2181 missing={1}"
     # loop for all operations
    jPa = jParams.format(Config.server, nMiss)
    print javaCmd.format(Config.jarpath, approach, jPa)
    jC = ThreadedClients([Config.server], javaCmd.format(Config.jarpath, approach, jPa))
    jC.start()
    jC.join()
    time.sleep(3)
    print doneStr.format(approach + "-" + str(nMiss))

def runExperiments(approaches):
    for a in approaches:
        print "###############################"
        print "######### " + a + " ###########"
        print "###############################"
        nMiss = 0
        while nMiss <= Config.partitions:
            # setup
            clients = startSetup()
            time.sleep(2)
            # run
            populate(nMiss)
            execApp(a, nMiss)
            # kill running services
            for c in clients:
                c.kill()
            # stop
            stopSetup()
            if nMiss == 0:
               nMiss = 1
            else:
              nMiss = nMiss * 2
        print doneStr.format(a)

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-l", dest='l', help="Location", default='l')
    parser.add_argument("-a", dest='approaches', type=str, nargs='*', help="Approaches")
    args = parser.parse_args()
    if (args.l == 'h'):
        Config.exec_path = Config.exec_path_h
    runExperiments(args.approaches)
