#!/usr/bin/env python

"""

terminate_this_ec2_instance.py
(c) Copyright 2011 Joe Masters Emison
Licensed under the Mozilla Public License (MPL) v1.1
Full license terms available at http://www.mozilla.org/MPL/MPL-1.1.html

terminate_this_ec2_instance provides an easy way to terminate a running EC2
instance from the instance itself, using the AWS API.  
This is preferable to running "shutdown -h now" or "halt" because both of
those will only stop an EBS-backed instance by default, instead of killing
it altogether

"""

#######################################################################
### Imports
#######################################################################
import boto
import subprocess

#######################################################################
### Get current instance id
#######################################################################
proc = subprocess.Popen(["wget -q -O - http://169.254.169.254/latest/meta-data/instance-id"], stdout=subprocess.PIPE, shell=True)
(out, err) = proc.communicate()

#######################################################################
### Connect to AWS API with boto, and kill the instance
#######################################################################
if out is not None and out != "":
﻿  conn = boto.connect_ec2()
﻿  instances = conn.terminate_instances([out])

