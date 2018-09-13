#!/usr/bin/env python

"""

multi_cloud_mirror.py
(c) Copyright 2011 Joe Masters Emison
Licensed under the Mozilla Public License (MPL) v1.1
Full license terms available at http://www.mozilla.org/MPL/MPL-1.1.html

multi_cloud_mirror provides an easy, multi-processing way of synchronizing
a bucket at Amazon S3 to a container at Rackspace Cloud Files, or vice
versa.

"""

#######################################################################
### Imports
#######################################################################
import boto3
import boto
import cloudfiles
import os
import io
import sys
import time
import datetime
import argparse
import multiprocessing
from boto.exception import S3ResponseError, S3PermissionsError, S3CopyError
from cloudfiles.errors import (ResponseError, NoSuchContainer, InvalidContainerName, InvalidUrl,
                               ContainerNotPublic, AuthenticationFailed, AuthenticationError,
                               NoSuchObject, InvalidObjectName, InvalidMetaName, InvalidMetaValue,
                               InvalidObjectSize, IncompleteSend)
import ConfigParser
from ConfigParser import NoSectionError, NoOptionError, MissingSectionHeaderError, ParsingError
from subprocess import Popen, PIPE

#######################################################################
### Global Functions
#######################################################################
def connectToClouds():
   """
   Open connections to S3 and Cloud Files
   """
   s3Conn = None
   cfConn = None
   try:
      ## boto reads from ~/.aws/credentials profiles
      session = boto3.Session(profile_name='jwp-webteam')
      s3Conn = session.resource('s3')
      ## the cloud files library doesn't automatically read from a file, so we handle that here:
      cfConfig = ConfigParser.ConfigParser()
      cfConfig.read('/Users/sarahgray/git/multi-cloud-mirror/rackspace.cfg')
      cfConn = cloudfiles.get_connection(cfConfig.get('CREDENTIALS','username'), cfConfig.get('CREDENTIALS','api_key'))
   except (NoSectionError, NoOptionError, MissingSectionHeaderError, ParsingError), err:
      raise MultiCloudMirrorException("Error in reading Cloud Files configuration file (/etc/cloudfiles.cfg): %s" % (err))
   except (S3ResponseError, S3PermissionsError), err:
      raise MultiCloudMirrorException("Error in connecting to S3: [%d] %s" % (err.status, err.reason))
   except (ResponseError, InvalidUrl, AuthenticationFailed, AuthenticationError), err:
      raise MultiCloudMirrorException("Error in connecting to CF: %s" % (err))
   return (s3Conn, cfConn)


#######################################################################
### Copy functions
#######################################################################
def copyToS3(srcBucketName, myKeyName, destBucketName):
   """
   Copy files to S3 from CF, given a source container and key,
   and a destination bucket via a byte stream.
   """
   (s3Conn, cfConn) = connectToClouds()
   file = cfConn.get_container(srcBucketName).get_object(myKeyName)
   nf = io.BytesIO(file.read())
   bucket = s3Conn.Bucket(destBucketName)
   s3KeyName = srcBucketName + '/' + myKeyName
   object_upload = bucket.Object(s3KeyName)
   object_upload.upload_fileobj(nf)


#######################################################################
### MultiCloudMirror Classes
#######################################################################
class MultiCloudMirrorException(Exception):
   pass

class MultiCloudMirror:
   LOG_CRIT=3
   LOG_WARN=2
   LOG_INFO=1
   LOG_DEBUG=0
   CF_MAX_OBJECTS_IN_LIST=10000

   def __init__(self, sync=None, numProcesses=10, maxFileSize=5368709120,
                 debug=0):
      # initialize variables
      self.debug              = debug
      self.sync               = sync
      self.maxFileSize        = maxFileSize
      self.s3Conn             = None
      self.cfConn             = None
      self.pool               = multiprocessing.Pool(numProcesses)
      self.jobs               = []
      self.jobCount           = 0
      self.srcList            = []
      self.destList           = []
      self.filesAtSource      = {}
      self.filesAtDestination = {}
      self.syncCount          = 0
      self.copyCount          = 0

   def logItem(self, msg, level):
      """
      Log function for MultiCloudMirror class: email and printing to screen
      """
      if level >= 0:
         if self.debug: print msg
         if level >= 1:
            self.emailMsg += msg + "\n"


   def getScenarioDetails(self,scenario):
      """
      Take a scenario input and break it into component pieces; log error
      """
      [fromBucket, toBucket] = scenario.split('->')
      srcService = fromBucket[:2].lower()
      destService = toBucket[:2].lower()
      srcBucketName  = fromBucket[5:]
      destBucketName = toBucket[5:]
      serviceError = None
      # Validate Inputs
      if srcService not in ['cf','s3']:
         serviceError ="Source service not recognized."
      elif destService not in ['cf','s3']:
         serviceError ="Destination service not recognized."
      elif srcService == destService:
         serviceError ="Same-cloud mirroring not supported."
      self.logItem("\nScenario: %s; (from: %s in %s , to: %s in %s)" % (scenario, srcBucketName, srcService, destBucketName, destService), self.LOG_INFO)
      return(srcService, srcBucketName, destService, destBucketName, serviceError)

   def get_all_objects(self, cfBucketName, cfList, marker=None):
      """Returns a list of file objects from a given container, recursively collected"""
      objects = self.cfConn.get_container(cfBucketName).get_objects(limit=self.CF_MAX_OBJECTS_IN_LIST, marker=marker)
      cfList.extend(objects)
      if len(objects) == self.CF_MAX_OBJECTS_IN_LIST:
         return self.get_all_objects(cfBucketName, cfList, cfList[-1].name)
      else:
         return cfList

   def connectToBuckets(self, srcService, srcBucketName, destBucketName):
      """
      Open connections and list files in buckets/containers
      """
      # There's a limit in Cloud Files per listing of objects, so we get the Cloud Files list here
      # (to maximize code reuse)
      cfBucketName = destBucketName if srcService == 's3' else srcBucketName
      # Because the cloudfiles.ObjectResults class can't easily be appended to, we make a new list

      cfList = self.get_all_objects(cfBucketName, [])
      #no swf files please
      cfList = [obj for obj in cfList if obj.name.split('.')[-1] !='swf']

      # Now assign bucket/container lists to class lists
      if srcService == 's3':
         self.srcList        = self.s3Conn.Bucket(srcBucketName).objects.all()
         self.destList       = cfList
      elif srcService == 'cf':
         self.srcList        = cfList
         self.destList       = self.s3Conn.Bucket(destBucketName).objects.all()
      # Loop through the files at the destination
      for dKey in self.destList:
         myKeyName = getattr(dKey, 'key', dKey.key).split('/')[-1]
         self.filesAtDestination[myKeyName] = dKey.e_tag.replace('"','')

   def checkAndCopy(self, sKey, srcService, srcBucketName, destService, destBucketName):
      """
      Check to see if this file should be copied, and, if so, queue it
      """
      myKeyName = getattr(sKey, 'key', sKey.name)
      self.filesAtSource[myKeyName] = sKey.etag.replace('"','')

      # skip S3 "folders", since Cloud Files doesn't support them, and skip files that are too large
      self.logItem("Found %s at source" % (myKeyName), self.LOG_DEBUG)
      if myKeyName[-1] == '/':
         self.logItem("Skipping %s because it is a 'folder'" % (myKeyName), self.LOG_DEBUG)
         return
      if (sKey.size > self.maxFileSize):
         self.logItem("Skipping %s because it is too large (%d bytes)" % (myKeyName, sKey.size), self.LOG_WARN)
         return
      # Copy if MD5 (e_tag) values are different, or if file does not exist at destination
      doCopy = False;
      try:
         if self.filesAtDestination[myKeyName] != sKey.etag.replace('"',''):
            # the file is at the destination, but the md5sums do not match, so overwrite
            doCopy = True
            self.logItem("...Found at destination, but md5sums did not match, so it will be copied", self.LOG_DEBUG)
      except KeyError:
         doCopy = True
         self.logItem("...Not found at destination, so it will be copied", self.LOG_DEBUG)
      if doCopy:
         # add copy job to pool
         self.jobCount += 1
         job = None

         job = self.pool.apply_async(copyToS3, (srcBucketName, myKeyName, destBucketName))
         job_dict = dict(job=job, task="copy", myKeyName=myKeyName, srcService=srcService, srcBucketName=srcBucketName, destBucketName=destBucketName, destService=destService)
         self.jobs.append(job_dict)
         self.copyCount = self.copyCount + 1
      else:
         # if we did not need to copy the file, log it:
         self.logItem("...Found at destination and md5sums match, so it will not be copied", self.LOG_DEBUG)
         self.syncCount = self.syncCount + 1

   def waitForJobstoFinish(self):
      """
      Loop through jobs, waiting for them to end
      """
      allFinished = False
      while not allFinished:
         # Check the status of the jobs.
         if self.jobs:
            self.logItem("Checking status of %d remaining tasks at %s" % (len(self.jobs), str(datetime.datetime.now())), self.LOG_DEBUG)
            for job_dict in self.jobs:
               job = job_dict['job']
               if job.ready():
                  # If the job finished but failed, note the exception
                  if not job.successful():
                     try:
                        job.get() # This will re-raise the exception.
                     except (S3ResponseError, S3PermissionsError, S3CopyError) as err:
                        self.logItem("Error in %s %s to/from S3 bucket %s: [%d] %s" % (job_dict['task'], job_dict['myKeyName'], job_dict['s3BucketName'], err.status, err.reason), self.LOG_WARN)
                        self.jobs.remove(job_dict)
                     except (ResponseError, NoSuchContainer, InvalidContainerName, InvalidUrl, ContainerNotPublic, AuthenticationFailed, AuthenticationError,
                             NoSuchObject, InvalidObjectName, InvalidMetaName, InvalidMetaValue, InvalidObjectSize, IncompleteSend), err:
                        self.logItem("Error in %s %s to/from to CF container %s: %s" % (job_dict['task'], job_dict['myKeyName'], job_dict['srcBucketName'], err), self.LOG_WARN)
                        self.jobs.remove(job_dict)
                     except MultiCloudMirrorException as err:
                        self.logItem("MultiCloudMirror error in %s %s: %s" % (job_dict['task'], job_dict['myKeyName'], str(err)), self.LOG_WARN)
                        self.jobs.remove(job_dict)
                     except Exception as err:
                        # even if we have an unknown error, we still want to forget about the job
                        self.logItem("Unknown error in %s %s: %s (%s)" % (job_dict['task'], job_dict['myKeyName'], str(err), str(err.args)), self.LOG_WARN)
                        self.jobs.remove(job_dict)
                  else:
                     self.logItem("%s %s [on/to %s]\n" % (job_dict['task'], job_dict['myKeyName'], job_dict['destService']), self.LOG_INFO)
                     self.jobs.remove(job_dict)
         # Exit when there are no jobs left.
         if not self.jobs:
            allFinished = True
         else:
            time.sleep(5)

   def get_non_empty_containers(self, containers, non_empty_containers_list):
      """
      Returns a list of names of non-empty containers, recursively collected
      """
      [non_empty_containers_list.append(c['name']) for c in containers if c['bytes'] != 0]
      if len(containers) == self.CF_MAX_OBJECTS_IN_LIST:
         next_batch_containers = self.cfConn.list_containers_info(marker=containers[-1]['name'], limit=self.CF_MAX_OBJECTS_IN_LIST)
         return self.get_non_empty_containers(next_batch_containers, non_empty_containers_list)
      else:
         return non_empty_containers_list

   def run(self):
      """
      Run multi-cloud-mirroring
      """
      self.logItem("Multi-Cloud Mirror Script started at %s" % (str(datetime.datetime.now())), self.LOG_INFO)
      try:
         (self.s3Conn, self.cfConn) = connectToClouds()
      except MultiCloudMirrorException as err:
         self.logItem("MultiCloudMirror error on connect: %s" % (err), self.LOG_CRIT)
         raise
      # Cycle Through Requested Synchronizations
      for scenario in self.sync:
         [srcService, srcBucketName, destService, destBucketName, serviceError] = self.getScenarioDetails(scenario)

         #collect all the non-empty containers
         first_batch_containers = self.cfConn.list_containers_info()
         non_empty_containers = self.get_non_empty_containers(first_batch_containers, [])[97:]

         for srcBucketName in non_empty_containers:
         #override the passed bucket name and pick up all buckets from rackspace
         # reestablish connection here to avoid the timeout?
            (self.s3Conn, self.cfConn) = connectToClouds()
            self.logItem("transferring bucket " + srcBucketName, self.LOG_INFO)
            if serviceError is not None:
               self.logItem(serviceError, self.LOG_WARN)
               continue
            # Connect to the proper buckets and retrieve file lists
            try:
               self.connectToBuckets(srcService, srcBucketName, destBucketName)
            except (S3ResponseError, S3PermissionsError), err:
               self.logItem("Error in connecting to S3 bucket: [%d] %s" % (err.status, err.reason), self.LOG_WARN)
               continue
            except (ResponseError, NoSuchContainer, InvalidContainerName, InvalidUrl, ContainerNotPublic, AuthenticationFailed, AuthenticationError), err:
               self.logItem("Error in connecting to CF container: %s" % (err), self.LOG_WARN)
               continue
            # Iterate through files at the source to see which ones to copy, and put them on the multiprocessing queue:
            for sKey in self.srcList:
               self.checkAndCopy(sKey, srcService, srcBucketName, destService, destBucketName)

            self.waitForJobstoFinish()
            self.logItem("\n\n%s Files were previously mirrored %s Files Copied" % (self.syncCount, self.copyCount), self.LOG_INFO)
            self.logItem("\n\nMulti-Cloud Mirror Script ended at %s" % (str(datetime.datetime.now())), self.LOG_INFO)




#######################################################################
### Run from the commandline
#######################################################################
if __name__ == '__main__':
   try:
      parser = argparse.ArgumentParser(description='Multi-Cloud Mirror Script')
      parser.add_argument('--process', dest='numProcesses',type=int, default=4,
                          help='number of simultaneous file upload threads to run')
      parser.add_argument('--maxsize', dest='maxFileSize',type=int, default=5368709120,
                          help='maximium file size to sync, in bytes (files larger than this size will be skipped)')
      parser.add_argument('--debug', dest='debug', default=False, help='turn on debug output')
      parser.add_argument('sync', metavar='"s3://bucket->cf://container"', nargs='+',
                          help='a synchronization scenario, of the form "s3://bucket->cf://container" or "cf://container->s3://bucket"')
      args = parser.parse_args()
      mcm = MultiCloudMirror(args.sync, args.numProcesses, args.maxFileSize, args.debug)
      mcm.run()
   except MultiCloudMirrorException as err:
      print "Error from MultiCloudMirror: %s" % (str(err))
