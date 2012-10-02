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
import boto
import cloudfiles
import smtplib
import os
import sys
import time
import datetime
import argparse
import ConfigParser
import multiprocessing
from boto.exception import S3ResponseError, S3PermissionsError, S3CopyError
from cloudfiles.errors import (ResponseError, NoSuchContainer, InvalidContainerName, InvalidUrl,
                               ContainerNotPublic, AuthenticationFailed, AuthenticationError,
                               NoSuchObject, InvalidObjectName, InvalidMetaName, InvalidMetaValue,
                               InvalidObjectSize, IncompleteSend)
from ConfigParser import NoSectionError, NoOptionError, MissingSectionHeaderError, ParsingError

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
      ## boto reads from /etc/boto.cfg (or ~/boto.cfg)
      s3Conn = boto.connect_s3()
      ## the cloud files library doesn't automatically read from a file, so we handle that here:
      cfConfig = ConfigParser.ConfigParser()
      cfConfig.read('/etc/cloudfiles.cfg')
      cfConn = cloudfiles.get_connection(cfConfig.get('Credentials','username'), cfConfig.get('Credentials','api_key'))
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
def copyToS3(srcBucketName, myKeyName, destBucketName,tmpFile):
   """
   Copy files to S3 from CF, given a source container and key, 
   and a destination bucket and temporary file for local storage
   """
   # because of the way S3 and boto work, we have to save to a local file first, then upload to Cloud Files
   # note that maximum file size (as of this writing) for Cloud Files is 5GB, and we expect 6+GB free on the drive
   (s3Conn, cfConn) = connectToClouds()
   cfConn.get_container(srcBucketName).get_object(myKeyName).save_to_filename(tmpFile)
   destBucket = s3Conn.get_bucket(destBucketName)
   newObj = None
   try:
      newObj = destBucket.new_key(myKeyName)
   except S3ResponseError:
      # key may exist; just get it instead:
      newObj = destBucket.get_key(myKeyName)
   newObj.set_contents_from_filename(tmpFile,replace=True)
   os.remove(tmpFile)


def copyToCF(srcBucketName, myKeyName, destBucketName):
   """
   Copy files to CF from S3, given a source bucket and key,
   and a destination container
   """
   # we can stream from S3 to Cloud Files, saving us from having to write to disk
   (s3Conn, cfConn) = connectToClouds()
   srcBucket  = s3Conn.get_bucket(srcBucketName)
   destBucket = cfConn.get_container(destBucketName)
   #with S3, we must request the key singly to get its metadata:
   fullKey = srcBucket.get_key(myKeyName)
   #initialize new object at Cloud Files
   newObj = destBucket.create_object(myKeyName)
   newObj.content_type = fullKey.content_type or "application/octet-stream"
   newObj.size = fullKey.size
   #stream the file from S3 to Cloud Files
   newObj.send(fullKey)


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

   def __init__(self, sync=None, numProcesses=4, maxFileSize=5368709120, emailDest='', emailSrc='',
                emailSubj="[Multi-Cloud Mirror] Script Run at %s" % (str(datetime.datetime.now())),
                tmpFile='/tmp/tmpfile', debug=0):
      # initialize variables
      self.debug              = debug
      self.emailMsg           = ""
      self.sync               = sync
      self.maxFileSize        = maxFileSize
      self.emailDest          = emailDest
      self.emailSrc           = emailSrc
      self.emailSubj          = emailSubj
      self.tmpFile            = tmpFile
      self.s3Conn             = None
      self.cfConn             = None
      self.pool               = multiprocessing.Pool(numProcesses)
      self.jobs               = []
      self.jobCount           = 0
      self.srcList            = []
      self.destList           = []
      self.filesAtDestination = {}


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


   def connectToBuckets(self, srcService, srcBucketName, destBucketName):
      """
      Open connections and list files in buckets/containers
      """
      # There's a limit in Cloud Files per listing of objects, so we get the Cloud Files list here
      # (to maximize code reuse)
      cfBucketName = destBucketName if srcService == 's3' else srcBucketName
      # Because the cloudfiles.ObjectResults class can't easily be appended to, we make a new list
      cfList = []
      cfList.extend(self.cfConn.get_container(cfBucketName).get_objects())
      lastLen = len(cfList)
      while lastLen == self.CF_MAX_OBJECTS_IN_LIST:
         cfList.extend(self.cfConn.get_container(cfBucketName).get_objects(marker=cfList[-1].name))
         lastLen = len(cfList) - lastLen
      # Now assign bucket/container lists to class lists
      if srcService == 's3':
         self.srcList        = self.s3Conn.get_bucket(srcBucketName).list()
         self.destList       = cfList
      elif srcService == 'cf':
         self.srcList        = cfList
         self.destList       = self.s3Conn.get_bucket(destBucketName).list()
      # Loop through the files at the destination
      for dKey in self.destList:
         myKeyName = getattr(dKey, 'key', dKey.name)
         self.filesAtDestination[myKeyName] = dKey.etag.replace('"','')


   def checkAndCopy(self, sKey, srcService, srcBucketName, destBucketName):
      """
      Check to see if this file should be copied, and, if so, queue it
      """
      myKeyName = getattr(sKey, 'key', sKey.name)
      # skip S3 "folders", since Cloud Files doesn't support them, and skip files that are too large
      self.logItem("Found %s at source" % (myKeyName), self.LOG_DEBUG)
      if myKeyName[-1] == '/':
         self.logItem("Skipping %s because it is a 'folder'" % (myKeyName), self.LOG_DEBUG)
         return
      if (sKey.size > self.maxFileSize):
         self.logItem("Skipping %s because it is too large (%d bytes)" % (myKeyName, sKey.size), self.LOG_WARN)
         return
      # Copy if MD5 (etag) values are different, or if file does not exist at destination
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
         if srcService == "s3":
            job = self.pool.apply_async(copyToCF, (srcBucketName, myKeyName, destBucketName))
         elif srcService == "cf":
            job = self.pool.apply_async(copyToS3, (srcBucketName, myKeyName, destBucketName, str(self.tmpFile + str(self.jobCount))))
         job_dict = dict(job=job, myKeyName=myKeyName, srcService=srcService, srcBucketName=srcBucketName, destBucketName=destBucketName)
         self.jobs.append(job_dict)
      else:
         # if we did not need to copy the file, log it:
         self.logItem("...Found at destination and md5sums match, so it will not be copied", self.LOG_DEBUG)


   def waitForJobstoFinish(self):
      """
      Loop through jobs, waiting for them to end
      """
      allFinished = False
      while not allFinished:
         # Check the status of the jobs.
         if self.jobs:
            self.logItem("Checking status of %d remaining copy tasks at %s" % (len(self.jobs), str(datetime.datetime.now())), self.LOG_DEBUG)
            for job_dict in self.jobs:
               job = job_dict['job']
               if job.ready():
                  # If the job finished but failed, note the exception
                  if not job.successful():
                     try:
                        job.get() # This will re-raise the exception.
                     except (S3ResponseError, S3PermissionsError, S3CopyError) as err:
                        self.logItem("Error in copying %s to/from S3 bucket %s: [%d] %s" % (job_dict['myKeyName'], job_dict['s3BucketName'], err.status, err.reason), self.LOG_WARN)
                        self.jobs.remove(job_dict)
                     except (ResponseError, NoSuchContainer, InvalidContainerName, InvalidUrl, ContainerNotPublic, AuthenticationFailed, AuthenticationError,
                             NoSuchObject, InvalidObjectName, InvalidMetaName, InvalidMetaValue, InvalidObjectSize, IncompleteSend), err:
                        self.logItem("Error in copying %s to/from to CF container %s: %s" % (job_dict['myKeyName'], job_dict['cfBucketName'], err), self.LOG_WARN)
                        self.jobs.remove(job_dict)
                     except MultiCloudMirrorException as err:
                        self.logItem("MultiCloudMirror error in copying %s: %s" % (job_dict['myKeyName'], str(err)), self.LOG_WARN)
                        self.jobs.remove(job_dict)
                     except Exception as err:
                        # even if we have an unknown error, we still want to forget about the job
                        self.logItem("Unknown error in copying %s: %s (%s)" % (job_dict['myKeyName'], str(err), str(err.args)), self.LOG_WARN)
                        self.jobs.remove(job_dict)
                  else:
                     self.logItem("Copied %s to destination\n" % (job_dict['myKeyName']), self.LOG_INFO)
                     self.jobs.remove(job_dict)
         # Exit when there are no jobs left.
         if not self.jobs:
            allFinished = True
         else:
            time.sleep(5)


   def sendStatusEmail(self):
      """
      Send status email if we have a from and to email address
      """
      if (self.emailDest is not None and self.emailSrc is not None):
         s = smtplib.SMTP('localhost')
         s.sendmail(self.emailSrc, self.emailDest.split(','), "From: %s\nTo: %s\nSubject: %s\n\n%s" %
                    (self.emailSrc, self.emailDest, self.emailSubj, self.emailMsg))
         s.quit()
         self.logItem("\nReport emailed to %s (from %s):\n----------------------\n%s\n----------------------\n"
                      % (self.emailDest, self.emailSrc, self.emailMsg), self.LOG_DEBUG)


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
            self.checkAndCopy(sKey, srcService, srcBucketName, destBucketName)
      self.waitForJobstoFinish()
      self.logItem("\n\nMulti-Cloud Mirror Script ended at %s" % (str(datetime.datetime.now())), self.LOG_INFO)
      self.sendStatusEmail()



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
      parser.add_argument('--from', help='email address from which to send the status email; must be specified to receive message', dest='emailDest')
      parser.add_argument('--to', dest='emailSrc',
                          help='email address(es) (comma-separated) to which to send the status email; must be specificed to recieve message')
      parser.add_argument('--subject', help='subject of the status email', dest='emailSubj',
                          default="[Multi-Cloud Mirror] Script Run at %s" % (str(datetime.datetime.now())))
      parser.add_argument('--tmpfile', dest='tmpFile',
                          help='temporary file used for writing when sending from cf to s3', default='/mnt/cloudfile')
      parser.add_argument('--debug', dest='debug', default=False, help='turn on debug output')
      parser.add_argument('sync', metavar='"s3://bucket->cf://container"', nargs='+',
                          help='a synchronization scenario, of the form "s3://bucket->cf://container" or "cf://container->s3://bucket"')
      args = parser.parse_args()
      mcm = MultiCloudMirror(args.sync, args.numProcesses, args.maxFileSize, args.emailDest, args.emailSrc, args.emailSubj, args.tmpFile, args.debug)
      mcm.run()
   except MultiCloudMirrorException as err:
      print "Error from MultiCloudMirror: %s" % (str(err))
