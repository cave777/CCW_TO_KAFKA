#!/apps/dslab/anaconda/python3/bin/python
# -*- coding: utf-8 -*-
import os
import pdb
import socket
import re
import sys
import json
import subprocess

def app_kafka_monitor(address, port):
    # Create a TCP socket
    s = socket.socket()
    #print "Attempting to connect to %s on port %s" % (address, port)
    response = {}
    response['STATUS'] = 'SUCCESS'

    try:
        s.connect((address, port))
        print ("Connected to %s on port %s" % (address, port))
    except Exception as e:
        print ("Connection to %s on port %s failed: %s" % (address, port, e))
        response['STATUS'] = 'FAILURE'

    return response
