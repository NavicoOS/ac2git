#!/usr/bin/python3

# ################################################################################################ #
# AccuRev to Git conversion script                                                                 #
# Author: Lazar Sumar                                                                              #
# Date:   06/11/2014                                                                               #
#                                                                                                  #
# This script is intended to convert an entire AccuRev depot into a git repository converting      #
# workspaces and streams into branches and respecting merges.                                      #
# ################################################################################################ #

import sys
import argparse
import os
import os.path
import shutil
import subprocess
import logging
import warnings
import xml.etree.ElementTree as ElementTree
from datetime import datetime, timedelta
import time
import re
import types
import copy
import codecs
import json
import pytz
import tempfile

from collections import OrderedDict

import accurev
import git

logger = None

ignored_transaction_types = [ "archive", "compress", "defcomp", "dispatch", "unarchive" ]

# Taken from this StackOverflow answer: http://stackoverflow.com/a/19238551
# Compulsary quote: https://twitter.com/codinghorror/status/712467615780708352
def utc2local(utc):
    epoch = time.mktime(utc.timetuple())
    offset = datetime.fromtimestamp (epoch) - datetime.utcfromtimestamp (epoch)
    return utc + offset

# This function calls the provided function func, only with arguments that were
# not None.
def CallOnNonNoneArgs(func, *args):
    return func(a for a in args if a is not None)

# ################################################################################################ #
# Script Classes                                                                                   #
# ################################################################################################ #
class Config(object): 
    class AccuRev(object):
        @classmethod
        def fromxmlelement(cls, xmlElement):
            if xmlElement is not None and xmlElement.tag == 'accurev':
                depot    = xmlElement.attrib.get('depot')
                username = xmlElement.attrib.get('username')
                password = xmlElement.attrib.get('password')
                startTransaction = xmlElement.attrib.get('start-transaction')
                endTransaction   = xmlElement.attrib.get('end-transaction')
                commandCacheFilename = xmlElement.attrib.get('command-cache-filename')
                
                excludeStreamTypes = None
                streamMap = None
                streamListElement = xmlElement.find('stream-list')
                if streamListElement is not None:
                    excludeStreamTypes = streamListElement.attrib.get("exclude-types")
                    if excludeStreamTypes is not None:
                        excludeStreamTypes = [x.strip() for x in excludeStreamTypes.split(',') if len(x.strip()) > 0]
                    streamMap = OrderedDict()
                    streamElementList = streamListElement.findall('stream')
                    for streamElement in streamElementList:
                        streamName = streamElement.text
                        branchName = streamElement.attrib.get("branch-name")
                        if branchName is None:
                            branchName = streamName

                        streamMap[streamName] = branchName
                
                return cls(depot, username, password, startTransaction, endTransaction, streamMap, commandCacheFilename, excludeStreamTypes)
            else:
                return None
            
        def __init__(self, depot = None, username = None, password = None, startTransaction = None, endTransaction = None, streamMap = None, commandCacheFilename = None, excludeStreamTypes = None):
            self.depot    = depot
            self.username = username
            self.password = password
            self.startTransaction = startTransaction
            self.endTransaction   = endTransaction
            self.streamMap = streamMap
            self.commandCacheFilename = commandCacheFilename
            self.excludeStreamTypes = excludeStreamTypes
    
        def __repr__(self):
            str = "Config.AccuRev(depot=" + repr(self.depot)
            str += ", username="          + repr(self.username)
            str += ", password="          + repr(self.password)
            str += ", startTransaction="  + repr(self.startTransaction)
            str += ", endTransaction="    + repr(self.endTransaction)
            if self.streamMap is not None:
                str += ", streamMap="    + repr(self.streamMap)
            if self.commandCacheFilename is not None:
                str += ", commandCacheFilename=" + repr(self.commandCacheFilename)
            if self.excludeStreamTypes is not None:
                str += ", excludeStreamTypes=" + repr(self.excludeStreamTypes)
            str += ")"
            
            return str

        def UseCommandCache(self):
            return self.commandCacheFilename is not None
            
    class Git(object):
        @classmethod
        def fromxmlelement(cls, xmlElement):
            if xmlElement is not None and xmlElement.tag == 'git':
                repoPath     = xmlElement.attrib.get('repo-path')
                messageStyle = xmlElement.attrib.get('message-style')
                messageKey   = xmlElement.attrib.get('message-key')
                authorIsCommitter = xmlElement.attrib.get('author-is-committer')
                emptyChildStreamAction  = xmlElement.attrib.get('empty-child-stream-action')
                sourceStreamFastForward = xmlElement.attrib.get('source-stream-fast-forward')
                newBasisIsFirstParent = xmlElement.attrib.get('new-basis-is-first-parent')

                remoteMap = OrderedDict()
                remoteElementList = xmlElement.findall('remote')
                for remoteElement in remoteElementList:
                    remoteName     = remoteElement.attrib.get("name")
                    remoteUrl      = remoteElement.attrib.get("url")
                    remotePushUrl  = remoteElement.attrib.get("push-url")
                    
                    remoteMap[remoteName] = git.GitRemoteListItem(name=remoteName, url=remoteUrl, pushUrl=remotePushUrl)

                return cls(repoPath=repoPath, messageStyle=messageStyle, messageKey=messageKey, authorIsCommitter=authorIsCommitter, remoteMap=remoteMap, emptyChildStreamAction=emptyChildStreamAction, sourceStreamFastForward=sourceStreamFastForward, newBasisIsFirstParent=newBasisIsFirstParent)
            else:
                return None
            
        def __init__(self, repoPath, messageStyle=None, messageKey=None, authorIsCommitter=None, remoteMap=None, emptyChildStreamAction=None, sourceStreamFastForward=None, newBasisIsFirstParent=None):
            self.repoPath               = repoPath
            self.messageStyle           = messageStyle
            self.messageKey             = messageKey
            self.remoteMap              = remoteMap

            if authorIsCommitter is not None:
                authorIsCommitter = authorIsCommitter.lower()
                if authorIsCommitter not in [ "true", "false" ]:
                    raise Exception("The author-is-committer attribute only accepts true or false but was set to '{v}'.".format(v=authorIsCommitter))
                authorIsCommitter = (authorIsCommitter == "true")
            else:
                authroIsCommitter = True
            self.authorIsCommitter = authorIsCommitter

            if emptyChildStreamAction is not None:
                if emptyChildStreamAction not in [ "merge", "cherry-pick" ]:
                    raise Exception("Error, the empty-child-stream-action attribute only accepts merge or cherry-pick options but got: {0}".format(emptyChildStreamAction))
                self.emptyChildStreamAction = emptyChildStreamAction
            else:
                self.emptyChildStreamAction = "cherry-pick"

            if sourceStreamFastForward is not None:
                sourceStreamFastForward = sourceStreamFastForward.lower()
                if sourceStreamFastForward not in [ "true", "false" ]:
                    raise Exception("Error, the source-stream-fast-forward attribute only accepts true or false options but got: {0}".format(sourceStreamFastForward))
                self.sourceStreamFastForward = (sourceStreamFastForward == "true")
            else:
                self.sourceStreamFastForward = False
            
            if newBasisIsFirstParent is not None:
                newBasisIsFirstParent = newBasisIsFirstParent.lower()
                if newBasisIsFirstParent not in [ "true", "false" ]:
                    raise Exception("Error, the new-basis-is-first-parent attribute only accepts true or false options but got: {0}".format(sourceStreamFastForward))
                self.newBasisIsFirstParent = (newBasisIsFirstParent == "true")
            else:
                self.newBasisIsFirstParent = True

        def __repr__(self):
            str = "Config.Git(repoPath=" + repr(self.repoPath)
            if self.messageStyle is not None:
                str += ", messageStyle=" + repr(self.messageStyle)
            if self.messageKey is not None:
                str += ", messageKey=" + repr(self.messageKey)
            if self.remoteMap is not None:
                str += ", remoteMap="    + repr(self.remoteMap)
            if self.authorIsCommitter is not None:
                str += ", authorIsCommitter="    + repr(self.authorIsCommitter)
            if self.newBasisIsFirstParent is not None:
                str += ", newBasisIsFirstParent=" + repr(self.newBasisIsFirstParent)
            str += ")"
            
            return str
            
    class UserMap(object):
        @classmethod
        def fromxmlelement(cls, xmlElement):
            if xmlElement is not None and xmlElement.tag == 'map-user':
                accurevUsername = None
                gitName         = None
                gitEmail        = None
                timezone        = None
                
                accurevElement = xmlElement.find('accurev')
                if accurevElement is not None:
                    accurevUsername = accurevElement.attrib.get('username')
                gitElement = xmlElement.find('git')
                if gitElement is not None:
                    gitName  = gitElement.attrib.get('name')
                    gitEmail = gitElement.attrib.get('email')
                    timezone = gitElement.attrib.get('timezone')
                
                return cls(accurevUsername=accurevUsername, gitName=gitName, gitEmail=gitEmail, timezone=timezone)
            else:
                return None
            
        def __init__(self, accurevUsername, gitName, gitEmail, timezone=None):
            self.accurevUsername = accurevUsername
            self.gitName         = gitName
            self.gitEmail        = gitEmail
            self.timezone        = timezone
    
        def __repr__(self):
            str = "Config.UserMap(accurevUsername=" + repr(self.accurevUsername)
            str += ", gitName="                     + repr(self.gitName)
            str += ", gitEmail="                    + repr(self.gitEmail)
            str += ", timezone="                    + repr(self.timezone)
            str += ")"
            
            return str

    @staticmethod
    def FilenameFromScriptName(scriptName):
        (root, ext) = os.path.splitext(scriptName)
        return root + '.config.xml'

    @ staticmethod
    def GetBooleanAttribute(xmlElement, attribute):
        if xmlElement is None or attribute is None:
            return None
        value = xmlElement.attrib.get(attribute)
        if value is not None:
            if value.lower() == "true":
                value = True
            elif value.lower() == "false":
                value = False
            else:
                Exception("Error, could not parse {attr} attribute of tag {tag}. Expected 'true' or 'false', but got '{value}'.".format(attr=attribute, tag=xmlElement.tag, value=value))

        return value

    @staticmethod
    def GetAbsoluteUsermapsFilename(filename, includedFilename):
        if includedFilename is None:
            return None
        if os.path.isabs(includedFilename):
            return includedFilename
        if filename is None:
            return None
        drive, path = os.path.splitdrive(filename)
        head, tail = os.path.split(path)
        if len(head) > 0 and head != '/' and head != '\\': # For an absolute path the starting slash isn't removed from head.
            return os.path.abspath(os.path.join(head, includedFilename))
        return os.path.abspath(includedFilename)

    @staticmethod
    def GetUsermapsFromXmlElement(usermapsElem):
        usermaps = []
        if usermapsElem is not None and usermapsElem.tag == 'usermaps':
            for usermapElem in usermapsElem.findall('map-user'):
                usermaps.append(Config.UserMap.fromxmlelement(usermapElem))
        return usermaps

    @staticmethod
    def GetUsermapsFromFile(filename, ignoreFiles=None):
        usermaps = []
        knownAccurevUsers = set()
        directCount, indirectCount = 0, 0
        if filename is not None:
            if os.path.exists(filename):
                with codecs.open(filename) as f:
                    mapXmlString = f.read()
                    mapXmlRoot = ElementTree.fromstring(mapXmlString)
                    if mapXmlRoot is not None:
                        userMapElements = []
                        if mapXmlRoot.tag == "usermaps":
                            userMapElements.append(mapXmlRoot)
                        else:
                            for userMapElem in mapXmlRoot.findall('usermaps'):
                                userMapElements.append(userMapElem)

                        fileList = [] # the linked files are processed after direct usermaps so that the direct usermaps override the same users in the linked files...
                        for userMapElem in userMapElements:
                            directUsermaps = Config.GetUsermapsFromXmlElement(userMapElem)
                            directCount += len(directUsermaps)
                            for user in directUsermaps:
                                if user.accurevUsername not in knownAccurevUsers:
                                    usermaps.append(user)
                                    knownAccurevUsers.add(user.accurevUsername)
                                else:
                                    #print("Ignoring duplicated user:", user.accurevUsername)
                                    pass

                            mapFile = userMapElem.attrib.get('filename')
                            if mapFile is not None:
                                fileList.append(mapFile)
                        for mapFile in fileList:
                            if ignoreFiles is None:
                                ignoreFiles = set()
                            mapFile = Config.GetAbsoluteUsermapsFilename(filename, mapFile) # Prevent circular loads.

                            if mapFile not in ignoreFiles:
                                ignoreFiles.add(mapFile)
                                includedUsermaps = Config.GetUsermapsFromFile(mapFile, ignoreFiles=ignoreFiles)
                                indirectCount += len(includedUsermaps)
                                for user in includedUsermaps:
                                    if user.accurevUsername not in knownAccurevUsers:
                                        usermaps.append(user)
                                        knownAccurevUsers.add(user.accurevUsername)
                                    else:
                                        #print("Ignoring duplicated user:", user.accurevUsername)
                                        pass
                            else:
                                print("Circular usermaps inclusion detected at file,", mapFile, "which was already processed.", file=sys.stderr)
            print("usermaps: filename", filename, "direct", directCount, "included", indirectCount)
        return usermaps


    @classmethod
    def fromxmlstring(cls, xmlString, filename=None):
        # Load the XML
        xmlRoot = ElementTree.fromstring(xmlString)
        
        if xmlRoot is not None and xmlRoot.tag == "accurev2git":
            accurev = Config.AccuRev.fromxmlelement(xmlRoot.find('accurev'))
            git     = Config.Git.fromxmlelement(xmlRoot.find('git'))
            
            method = "diff" # Defaults to diff
            methodElem = xmlRoot.find('method')
            if methodElem is not None:
                method = methodElem.text

            mergeStrategy = "normal" # Defaults to normal
            mergeStrategyElem = xmlRoot.find('merge-strategy')
            if mergeStrategyElem is not None:
                mergeStrategy = mergeStrategyElem.text

            logFilename = None
            logFileElem = xmlRoot.find('logfile')
            if logFileElem is not None:
                logFilename = logFileElem.text

            usermaps = []
            userMapsElem = xmlRoot.find('usermaps')
            if userMapsElem is not None:
                usermaps = Config.GetUsermapsFromXmlElement(userMapsElem)
                knownAccurevUsers = set([x.accurevUsername for x in usermaps])
                # Check if we need to load extra usermaps from a file.
                mapFilename = userMapsElem.attrib.get("filename")
                if mapFilename is not None:
                    if filename is not None:
                        mapFilename = Config.GetAbsoluteUsermapsFilename(filename, mapFilename) # Prevent circular loads.
                    includedUsermaps = Config.GetUsermapsFromFile(mapFilename)
                    for user in includedUsermaps:
                        if user.accurevUsername not in knownAccurevUsers:
                            usermaps.append(user)
                        else:
                            #print("Known user:", user.accurevUsername)
                            pass
            
            return cls(accurev=accurev, git=git, usermaps=usermaps, method=method, mergeStrategy=mergeStrategy, logFilename=logFilename)
        else:
            # Invalid XML for an accurev2git configuration file.
            return None

    @staticmethod
    def fromfile(filename):
        config = None
        if os.path.exists(filename):
            with codecs.open(filename) as f:
                configXml = f.read()
                config = Config.fromxmlstring(configXml, filename=filename)
        return config

    def __init__(self, accurev=None, git=None, usermaps=None, method=None, mergeStrategy=None, logFilename=None):
        self.accurev       = accurev
        self.git           = git
        self.usermaps      = usermaps
        self.method        = method
        self.mergeStrategy = mergeStrategy
        self.logFilename   = logFilename
        
    def __repr__(self):
        str = "Config(accurev="   + repr(self.accurev)
        str += ", git="           + repr(self.git)
        str += ", usermaps="      + repr(self.usermaps)
        str += ", method="        + repr(self.method)
        str += ", mergeStrategy=" + repr(self.mergeStrategy)
        str += ", logFilename="   + repr(self.logFilename)
        str += ")"
        
        return str

# Prescribed recepie:
# - Get the list of tracked streams from the config file.
# - For each stream in the list
#   + If this stream is new (there is no data in git for it yet)
#     * Create the git branch for the stream
#     * Get the stream create (mkstream) transaction number and set it to be the start-transaction. Note: The first stream in the depot has no mkstream transaction.
#   + otherwise
#     * Get the last processed transaction number and set that to be the start-transaction.
#     * Obtain a diff from accurev listing all of the files that have changed and delete them all.
#   + Get the end-transaction from the user or from accurev's highest/now keyword for the hist command.
#   + For all transactions between the start-transaction and end-transaction
#     * Checkout the git branch at latest (or just checkout if no-commits yet).
#     * Populate the retrieved transaction with the recursive option but without the overwrite option (quick).
#     * Preserve empty directories by adding .gitignore files.
#     * Commit the current state of the directory but don't respect the .gitignore file contents. (in case it was added to accurev in the past).
#     * Increment the transaction number by one
#     * Obtain a diff from accurev listing all of the files that have changed and delete them all.
class AccuRev2Git(object):
    gitRefsNamespace = 'refs/ac2git/'
    gitNotesRef_state = 'ac2git'
    gitNotesRef_accurevInfo = 'accurev'

    commandFailureRetryCount = 3
    commandFailureSleepSeconds = 3

    def __init__(self, config):
        self.config = config
        self.cwd = None
        self.gitRepo = None

    # Returns True if the path was deleted, otherwise false
    def DeletePath(self, path):
        if os.path.lexists(path):
            if os.path.islink(path):
                os.unlink(path)
            elif os.path.isfile(path):
                os.remove(path)
            elif os.path.isdir(path):
                shutil.rmtree(path)
            
        return not os.path.lexists(path)
   
    def ClearGitRepo(self):
        # Delete everything except the .git folder from the destination (git repo)
        logger.debug( "Clear git repo." )
        for root, dirs, files in os.walk(self.gitRepo.path, topdown=False):
            for name in files:
                path = os.path.join(root, name)
                if git.GetGitDirPrefix(path) is None:
                    self.DeletePath(path)
            for name in dirs:
                path = os.path.join(root, name)
                if git.GetGitDirPrefix(path) is None:
                    self.DeletePath(path)

    def PreserveEmptyDirs(self):
        preservedDirs = []
        for root, dirs, files in os.walk(self.gitRepo.path, topdown=True):
            for name in dirs:
                path = ToUnixPath(os.path.join(root, name))
                # Preserve empty directories that are not under the .git/ directory.
                if git.GetGitDirPrefix(path) is None and len(os.listdir(path)) == 0:
                    filename = os.path.join(path, '.gitignore')
                    with codecs.open(filename, 'w', 'utf-8') as file:
                        #file.write('# accurev2git.py preserve empty dirs\n')
                        preservedDirs.append(filename)
                    if not os.path.exists(filename):
                        logger.error("Failed to preserve directory. Couldn't create '{0}'.".format(filename))
        return preservedDirs

    def DeleteEmptyDirs(self):
        deletedDirs = []
        for root, dirs, files in os.walk(self.gitRepo.path, topdown=True):
            for name in dirs:
                path = ToUnixPath(os.path.join(root, name))
                # Delete empty directories that are not under the .git/ directory.
                if git.GetGitDirPrefix(path) is None:
                    dirlist = os.listdir(path)
                    count = len(dirlist)
                    delete = (len(dirlist) == 0)
                    if len(dirlist) == 1 and '.gitignore' in dirlist:
                        with codecs.open(os.path.join(path, '.gitignore')) as gi:
                            contents = gi.read().strip()
                            delete = (len(contents) == 0)
                    if delete:
                        if not self.DeletePath(path):
                            logger.error("Failed to delete empty directory '{0}'.".format(path))
                            raise Exception("Failed to delete '{0}'".format(path))
                        else:
                            deletedDirs.append(path)
        return deletedDirs

    def GetGitUserFromAccuRevUser(self, accurevUsername):
        if accurevUsername is not None:
            for usermap in self.config.usermaps:
                if usermap.accurevUsername == accurevUsername:
                    return (usermap.gitName, usermap.gitEmail)
        logger.error("Cannot find git details for accurev username {0}".format(accurevUsername))
        return (accurevUsername, None)

    def GetGitTimezoneFromDelta(self, time_delta):
        seconds = time_delta.total_seconds()
        absSec = abs(seconds)
        offset = (int(absSec / 3600) * 100) + (int(absSec / 60) % 60)
        if seconds < 0:
            offset = -offset
        return offset

    def GetDeltaFromGitTimezone(self, timezone):
        # Git timezone strings follow the +0100 format
        tz = int(timezone)
        tzAbs = abs(tz)
        tzdelta = timedelta(seconds=((int(tzAbs / 100) * 3600) + ((tzAbs % 100) * 60)))
        return tzdelta

    def GetGitDatetime(self, accurevUsername, accurevDatetime):
        usertime = accurevDatetime
        tz = None
        if accurevUsername is not None:
            for usermap in self.config.usermaps:
                if usermap.accurevUsername == accurevUsername:
                    tz = usermap.timezone
                    break

        if tz is None:
            # Take the following default times 48 hours from Epoch as reference to compute local time.
            refTimestamp = 172800
            utcRefTime = datetime.utcfromtimestamp(refTimestamp)
            refTime = datetime.fromtimestamp(refTimestamp)

            tzdelta = (refTime - utcRefTime)
            usertime = accurevDatetime + tzdelta
            
            tz = self.GetGitTimezoneFromDelta(tzdelta)
        else:
            match = re.match(r'^[+-][0-9]{4}$', tz)
            if match:
                # This is the git style format
                tzdelta = self.GetDeltaFromGitTimezone(tz)
                usertime = accurevDatetime + tzdelta
                tz = int(tz)
            else:
                # Assuming it is an Olson timezone format
                userTz = pytz.timezone(tz)
                usertime = userTz.localize(accurevDatetime)
                tzdelta = usertime.utcoffset() # We need two aware times to get the datetime.timedelta.
                usertime = accurevDatetime + tzdelta # Adjust the time by the timezone since localize din't.
                tz = self.GetGitTimezoneFromDelta(tzdelta)

        return usertime, tz

    def GetFirstTransaction(self, depot, streamName, startTransaction=None, endTransaction=None, useCache=False):
        invalidRetVal = (None, None)
        # Get the stream creation transaction (mkstream). Note: The first stream in the depot doesn't have an mkstream transaction.
        tr = accurev.ext.get_mkstream_transaction(stream=streamName, depot=depot, useCache=useCache)
        if tr is None:
            raise Exception("Failed to find the mkstream transaction for stream {s}".format(s=streamName))

        hist, histXml = self.TryHist(depot=depot, timeSpec=tr.id) # Make the first transaction be the mkstream transaction.

        if startTransaction is not None:
            startTrHist, startTrXml = self.TryHist(depot=depot, timeSpec=startTransaction)
            if startTrHist is None:
                return invalidRetVal

            startTr = startTrHist.transactions[0]
            if tr.id < startTr.id:
                logger.info( "The first transaction (#{0}) for stream {1} is earlier than the conversion start transaction (#{2}).".format(tr.id, streamName, startTr.id) )
                tr = startTr
                hist = startTrHist
                histXml = startTrXml

        if endTransaction is not None:
            endTrHist, endTrHistXml = self.TryHist(depot=depot, timeSpec=endTransaction)
            if endTrHist is None:
                return invalidRetVal

            endTr = endTrHist.transactions[0]
            if endTr.id < tr.id:
                logger.info( "The first transaction (#{0}) for stream {1} is later than the conversion end transaction (#{2}).".format(tr.id, streamName, startTr.id) )
                tr = None
                return invalidRetVal

        return hist, histXml

    def TryGitCommand(self, cmd, allowEmptyString=False, retry=True):
        rv = None
        for i in range(0, AccuRev2Git.commandFailureRetryCount):
            rv = self.gitRepo.raw_cmd(cmd)
            if not retry:
                break
            if rv is not None:
                rv = rv.strip()
                if not allowEmptyString and len(rv) == 0:
                    rv = None
                else:
                    break
            time.sleep(AccuRev2Git.commandFailureSleepSeconds)
        return rv

    def GetLastCommitHash(self, branchName=None, ref=None, retry=True):
        cmd = []
        commitHash = None
        if ref is not None:
            cmd = [ u'git', u'show-ref', u'--hash', ref ]
        else:
            cmd = [u'git', u'log', u'-1', u'--format=format:%H']
            if branchName is not None:
                cmd.append(branchName)

        commitHash = self.TryGitCommand(cmd=cmd, retry=retry)

        if commitHash is None:
            logger.error("Failed to retrieve last git commit hash. Command `{0}` failed.".format(' '.join(cmd)))

        return commitHash

    def GetTreeFromRef(self, ref):
        treeHash = None
        cmd = [u'git', u'log', u'-1', u'--format=format:%T']
        if ref is not None:
            cmd.append(ref)
        treeHash = self.TryGitCommand(cmd=cmd)

        if treeHash is None:
            logger.error("Failed to retrieve tree hash. Command `{0}` failed.".format(' '.join(cmd)))

        return treeHash

    def UpdateAndCheckoutRef(self, ref, commitHash, checkout=True):
        if ref is not None and commitHash is not None and len(ref) > 0 and len(commitHash) > 0:
            # refs/heads are branches which are updated automatically when you commit to them (provided we have them checked out).
            # so at least raise a warning for the user.

            # If we were asked to update a ref, not updating it is considered a failure to commit.
            if self.gitRepo.raw_cmd([ u'git', u'update-ref', ref, commitHash ]) is None:
                logger.error( "Failed to update ref {ref} to commit {hash}".format(ref=ref, hash=commitHash) )
                return False
            if checkout and ref != 'HEAD' and self.gitRepo.checkout(branchName=ref) is None: # no point in checking out HEAD if that's what we've updated!
                logger.error( "Failed to checkout ref {ref} to commit {hash}".format(ref=ref, hash=commitHash) )
                return False

            return True

        return None

    def SafeCheckout(self, ref, doReset=False, doClean=False):
        status = self.gitRepo.status()
        if doReset:
            logger.debug( "Reset current branch - '{br}'".format(br=status.branch) )
            self.gitRepo.reset(isHard=True)
        if doClean:
            logger.debug( "Clean current branch - '{br}'".format(br=status.branch) )
            self.gitRepo.clean(directories=True, force=True, forceSubmodules=True, includeIgnored=True)
            pass
        if ref is not None and status.branch != ref:
            logger.debug( "Checkout {ref}".format(ref=ref) )
            self.gitRepo.checkout(branchName=ref)
            status = self.gitRepo.status()
            logger.debug( "On branch {branch} - {staged} staged, {changed} changed, {untracked} untracked files{initial_commit}.".format(branch=status.branch, staged=len(status.staged), changed=len(status.changed), untracked=len(status.untracked), initial_commit=', initial commit' if status.initial_commit else '') )
            if status is None:
                raise Exception("Invalid initial state! The status command return is invalid.")
            if status.branch is None or status.branch != ref:
                # The parser for the status isn't very smart and git doesn't necessarily report the name of the ref that you have checked out. So, check if the current HEAD points to the desired ref by comparing hashes.
                headHash = self.gitRepo.raw_cmd(['git', 'log', '--format=%H', 'HEAD', '-1'])
                refHash = self.gitRepo.raw_cmd(['git', 'log', '--format=%H', ref, '-1'])
                if headHash is None:
                    raise Exception("Failed to determine the hash of the HEAD commit!")
                elif refHash is None:
                    raise Exception("Failed to determine the hash of the {ref} commit!".format(ref=ref))
                elif refHash != headHash:
                    raise Exception("Invalid initial state! The status command returned an invalid name for current branch. Expected {ref} but got {statusBranch}.".format(ref=ref, statusBranch=status.branch))
            if len(status.staged) != 0 or len(status.changed) != 0 or len(status.untracked) != 0:
                raise Exception("Invalid initial state! There are changes in the tracking repository. Staged {staged}, changed {changed}, untracked {untracked}.".format(staged=status.staged, changed=status.changed, untracked=status.untracked))

    def Commit(self, transaction=None, allowEmptyCommit=False, messageOverride=None, parents=None, treeHash=None, ref=None, checkout=True, authorIsCommitter=None):
        usePlumbing = (parents is not None or treeHash is not None)

        if authorIsCommitter is None:
            authorIsCommitter = self.config.git.authorIsCommitter

        # Custom messages for when we have a transaction.
        trMessage, forTrMessage = '', ''
        if transaction is not None:
            trMessage = ' transaction {0}'.format(transaction.id)
            forTrMessage = ' for{0}'.format(trMessage)

        # Begin the commit processing.
        if treeHash is None:
            self.PreserveEmptyDirs()

            # Add all of the files to the index
            self.gitRepo.add(force=True, all=True, git_opts=[u'-c', u'core.autocrlf=false'])

        # Create temporary file for the commit message.
        messageFilePath = None
        with tempfile.NamedTemporaryFile(mode='w+', prefix='ac2git_commit_', encoding='utf-8', delete=False) as messageFile:
            messageFilePath = messageFile.name
            emptyMessage = True
            if messageOverride is not None:
                if len(messageOverride) > 0:
                    messageFile.write(messageOverride)
                    emptyMessage = False
            elif transaction is not None and transaction.comment is not None and len(transaction.comment) > 0:
                # In git the # at the start of the line indicate that this line is a comment inside the message and will not be added.
                # So we will just add a space to the start of all the lines starting with a # in order to preserve them.
                messageFile.write(transaction.comment)
                emptyMessage = False

            if emptyMessage:
                # `git commit` and `git commit-tree` commands, when given an empty file for the commit message, seem to revert to
                # trying to read the commit message from the STDIN. This is annoying since we don't want to be opening a pipe to
                # the spawned process all the time just to write an EOF character so instead we will just add a single space as the
                # message and hope the user doesn't notice.
                # For the `git commit` command it's not as bad since white-space is always stripped from commit messages. See the 
                # `git commit --cleanup` option for details.
                messageFile.write(' ')
        
        if messageFilePath is None:
            logger.error("Failed to create temporary file for commit message{0}".format(forTrMessage))
            return None

        # Get the author's and committer's name, email and timezone information.
        authorName, authorEmail, authorDate, authorTimezone = None, None, None, None
        if transaction is not None:
            authorName, authorEmail = self.GetGitUserFromAccuRevUser(transaction.user)
            authorDate, authorTimezone = self.GetGitDatetime(accurevUsername=transaction.user, accurevDatetime=transaction.time)

        # If the author-is-committer flag is set to true make the committer the same as the author.
        committerName, committerEmail, committerDate, committerTimezone = None, None, None, None
        if authorIsCommitter:
            committerName, committerEmail, committerDate, committerTimezone = authorName, authorEmail, authorDate, authorTimezone

        lastCommitHash = None
        if parents is None:
            lastCommitHash = self.GetLastCommitHash(ref=ref) # If ref is None, it will get the last commit hash from the HEAD ref.
            if lastCommitHash is None:
                parents = []
            else:
                parents = [ lastCommitHash ]
        elif len(parents) != 0:
            lastCommitHash = parents[0]

        # Make the commit.
        commitHash = None
        if usePlumbing:
            if treeHash is None:
                treeHash = self.gitRepo.write_tree()
            if treeHash is not None and len(treeHash.strip()) > 0:
                treeHash = treeHash.strip()
                commitHash = self.gitRepo.commit_tree(tree=treeHash, parents=parents, message_file=messageFilePath, committer_name=committerName, committer_email=committerEmail, committer_date=committerDate, committer_tz=committerTimezone, author_name=committerName, author_email=committerEmail, author_date=committerDate, author_tz=committerTimezone, allow_empty=allowEmptyCommit, git_opts=[u'-c', u'core.autocrlf=false'])
                if commitHash is None:
                    logger.error( "Failed to commit tree {0}{1}. Error:\n{2}".format(treeHash, forTrMessage, self.gitRepo.lastStderr) )
                else:
                    commitHash = commitHash.strip()
            else:
                logger.error( "Failed to write tree{0}. Error:\n{1}".format(forTrMessage, self.gitRepo.lastStderr) )
        else:
            commitResult = self.gitRepo.commit(message_file=messageFilePath, committer_name=committerName, committer_email=committerEmail, committer_date=committerDate, committer_tz=committerTimezone, author_name=committerName, author_email=committerEmail, author_date=committerDate, author_tz=committerTimezone, allow_empty_message=True, allow_empty=allowEmptyCommit, cleanup='whitespace', git_opts=[u'-c', u'core.autocrlf=false'])
            if commitResult is not None:
                commitHash = commitResult.shortHash
                if commitHash is None:
                    commitHash = self.GetLastCommitHash()
            elif "nothing to commit" in self.gitRepo.lastStdout:
                logger.debug( "nothing to commit{0}...?".format(forTrMessage) )
            else:
                logger.error( "Failed to commit".format(trMessage) )
                logger.error( "\n{0}\n{1}\n".format(self.gitRepo.lastStdout, self.gitRepo.lastStderr) )

        # For detached head states (which occur when you're updating a ref and not a branch, even if checked out) we need to make sure to update the HEAD. Either way it doesn't hurt to
        # do this step whether we are using plumbing or not...
        if commitHash is not None:
            if ref is None:
                ref = 'HEAD'
            if self.UpdateAndCheckoutRef(ref=ref, commitHash=commitHash, checkout=(checkout and ref != 'HEAD')) != True:
                logger.error( "Failed to update ref {ref} with commit {h}{forTr}".format(ref=ref, h=commitHash, forTr=forTrMessage) )
                commitHash = None

        os.remove(messageFilePath)

        if commitHash is not None:
            if lastCommitHash == commitHash:
                logger.error("Commit command returned True when nothing was committed...? Last commit hash {0} didn't change after the commit command executed.".format(lastCommitHash))
                commitHash = None # Invalidate return value
        else:
            logger.error("Failed to commit{tr}.".format(tr=trMessage))

        return commitHash

    def GetStreamMap(self):
        streamMap = self.config.accurev.streamMap

        if streamMap is None:
            streamMap = OrderedDict()

        if len(streamMap) == 0:
            # When the stream map is missing or empty we intend to process all streams
            streams = accurev.show.streams(depot=self.config.accurev.depot)
            for stream in streams.streams:
                if self.config.accurev.excludeStreamTypes is not None and stream.Type in self.config.accurev.excludeStreamTypes:
                    logger.debug("Excluded stream '{0}' of type '{1}'".format(stream.name, stream.Type))
                    continue
                streamMap[stream.name] = self.SanitizeBranchName(stream.name)

        return streamMap

    def FindNextChangeTransaction(self, streamName, startTrNumber, endTrNumber, deepHist=None):
        # Iterate over transactions in order using accurev diff -a -i -v streamName -V streamName -t <lastProcessed>-<current iterator>
        if self.config.method == "diff":
            nextTr = startTrNumber + 1
            diff, diffXml = self.TryDiff(streamName=streamName, firstTrNumber=startTrNumber, secondTrNumber=nextTr)
            if diff is None:
                return (None, None)
    
            # Note: This is likely to be a hot path. However, it cannot be optimized since a revert of a transaction would not show up in the diff even though the
            #       state of the stream was changed during that period in time. Hence to be correct we must iterate over the transactions one by one unless we have
            #       explicit knowlege of all the transactions which could affect us via some sort of deep history option...
            while nextTr <= endTrNumber and len(diff.elements) == 0:
                nextTr += 1
                diff, diffXml = self.TryDiff(streamName=streamName, firstTrNumber=startTrNumber, secondTrNumber=nextTr)
                if diff is None:
                    return (None, None)
        
            logger.debug("FindNextChangeTransaction diff: {0}".format(nextTr))
            return (nextTr, diff)
        elif self.config.method == "deep-hist":
            if deepHist is None:
                raise Exception("Script error! deepHist argument cannot be none when running a deep-hist method.")
            # Find the next transaction
            for tr in deepHist:
                if tr.id > startTrNumber:
                    if tr.Type in ignored_transaction_types:
                        logger.debug("Ignoring transaction #{id} - {Type} (transaction type is in ignored_transaction_types list)".format(id=tr.id, Type=tr.Type))
                    else:
                        diff, diffXml = self.TryDiff(streamName=streamName, firstTrNumber=startTrNumber, secondTrNumber=tr.id)
                        if diff is None:
                            return (None, None)
                        elif len(diff.elements) > 0:
                            logger.debug("FindNextChangeTransaction deep-hist: {0}".format(tr.id))
                            return (tr.id, diff)
                        else:
                            logger.debug("FindNextChangeTransaction deep-hist skipping: {0}, diff was empty...".format(tr.id))

            diff, diffXml = self.TryDiff(streamName=streamName, firstTrNumber=startTrNumber, secondTrNumber=endTrNumber)
            return (endTrNumber + 1, diff) # The end transaction number is inclusive. We need to return the one after it.
        elif self.config.method == "pop":
            logger.debug("FindNextChangeTransaction pop: {0}".format(startTrNumber + 1))
            return (startTrNumber + 1, None)
        else:
            logger.error("Method is unrecognized, allowed values are 'pop', 'diff' and 'deep-hist'")
            raise Exception("Invalid configuration, method unrecognized!")

    def DeleteDiffItemsFromRepo(self, diff):
        # Delete all of the files which are even mentioned in the diff so that we can do a quick populate (wouth the overwrite option)
        deletedPathList = []
        for element in diff.elements:
            for change in element.changes:
                for stream in [ change.stream1, change.stream2 ]:
                    if stream is not None and stream.name is not None:
                        name = stream.name
                        if name.startswith('\\.\\') or name.startswith('/./'):
                            # Replace the accurev depot relative path start with a normal relative path.
                            name = name[3:]
                        if os.path.isabs(name):
                            # For os.path.join() to work we need a non absolute path so turn the absolute path (minus any drive letter or UNC path part) into a relative path w.r.t. the git repo.
                            name = os.path.splitdrive(name)[1][1:]
                        path = os.path.abspath(os.path.join(self.gitRepo.path, name))

                        # Ensure we restrict the deletion to the git repository and that we don't delete the git repository itself.
                        doClearAll = False
                        relPath = os.path.relpath(path, self.gitRepo.path)
                        relPathDirs = SplitPath(relPath)
                        if relPath.startswith('..'):
                            logger.error("Trying to delete path outside the worktree! Deleting worktree instead. git path: {gp}, depot path: {dp}".format(gp=path, dp=stream.name))
                            doClearAll = True
                        elif relPathDirs[0] == '.git':
                            logger.error("Trying to delete git directory! Ignored... git path: {gp}, depot path: {dp}".format(gp=path, dp=stream.name))
                        elif relPath == '.':
                            logger.error("Deleting the entire worktree due to diff with bad '..' elements! git path: {gp}, depot path: {dp}".format(gp=path, dp=stream.name))
                            doClearAll = True

                        if doClearAll:
                            self.ClearGitRepo()
                            return [ self.gitRepo.path ]

                        if os.path.lexists(path): # Ensure that broken links are also deleted!
                            if not self.DeletePath(path):
                                logger.error("Failed to delete '{0}'.".format(path))
                                raise Exception("Failed to delete '{0}'".format(path))
                            else:
                                deletedPathList.append(path)

        return deletedPathList

    def TryDiff(self, streamName, firstTrNumber, secondTrNumber):
        for i in range(0, AccuRev2Git.commandFailureRetryCount):
            diffXml = accurev.raw.diff(all=True, informationOnly=True, verSpec1=streamName, verSpec2=streamName, transactionRange="{0}-{1}".format(firstTrNumber, secondTrNumber), isXmlOutput=True, useCache=self.config.accurev.UseCommandCache())
            if diffXml is not None:
                diff = accurev.obj.Diff.fromxmlstring(diffXml)
                if diff is not None:
                    break
        if diff is None:
            logger.error( "accurev diff failed! stream: {0} time-spec: {1}-{2}".format(streamName, firstTrNumber, secondTrNumber) )
        return diff, diffXml

    def TryHist(self, depot, timeSpec, streamName=None, transactionKind=None):
        trHist = None
        for i in range(0, AccuRev2Git.commandFailureRetryCount):
            trHistXml = accurev.raw.hist(depot=depot, stream=streamName, timeSpec=timeSpec, transactionKind=transactionKind, useCache=self.config.accurev.UseCommandCache(), isXmlOutput=True, expandedMode=True, verboseMode=True)
            if trHistXml is not None:
                trHist = accurev.obj.History.fromxmlstring(trHistXml)
                if trHist is not None:
                    break
        return trHist, trHistXml

    def TryPop(self, streamName, transaction, overwrite=False):
        for i in range(0, AccuRev2Git.commandFailureRetryCount):
            popResult = accurev.pop(verSpec=streamName, location=self.gitRepo.path, isRecursive=True, isOverride=overwrite, timeSpec=transaction.id, elementList='.')
            if popResult:
                break
            else:
                logger.error("accurev pop failed:")
                for message in popResult.messages:
                    if message.error is not None and message.error:
                        logger.error("  {0}".format(message.text))
                    else:
                        logger.info("  {0}".format(message.text))
        
        return popResult

    def TryStreams(self, depot, timeSpec, stream=None):
        streams = None
        for i in range(0, AccuRev2Git.commandFailureRetryCount):
            streamsXml = accurev.raw.show.streams(depot=depot, timeSpec=timeSpec, stream=stream, isXmlOutput=True, includeDeactivatedItems=True, includeHasDefaultGroupAttribute=True, useCache=self.config.accurev.UseCommandCache())
            if streamsXml is not None:
                streams = accurev.obj.Show.Streams.fromxmlstring(streamsXml)
                if streams is not None:
                    break
        return streams, streamsXml

    def TryDepots(self):
        depots = None
        for i in range(0, AccuRev2Git.commandFailureRetryCount):
            depotsXml = accurev.raw.show.depots(isXmlOutput=True, includeDeactivatedItems=True)
            if depotsXml is not None:
                depots = accurev.obj.Show.Depots.fromxmlstring(depotsXml)
                if depots is not None:
                    break
        return depots, depotsXml

    def NormalizeAccurevXml(self, xml):
        xmlNormalized = re.sub('TaskId="[0-9]+"', 'TaskId="0"', xml)
        xmlDecoded = git.decode_proc_output(xmlNormalized)
        return xmlDecoded

    def WriteInfoFiles(self, path, depot, transaction, streamsXml=None, histXml=None, streamName=None, diffXml=None, useCommandCache=False):
        streams = None
        hist = None
        diff = None

        if streamsXml is not None:
            streams = accurev.obj.Show.Streams.fromxmlstring(streamsXml)
        
        if streams is None or streamsXml is None:
            streams, streamsXml = self.TryStreams(depot=depot, timeSpec=transaction)
            if streams is None or streamsXml is None:
                return (None, None, None)

        if histXml is not None:
            hist = accurev.obj.History.fromxmlstring(histXml)
        if hist is None or histXml is None:
            hist, histXml = self.TryHist(depot=depot, timeSpec=transaction)
            if hist is None or histXml is None:
                return (None, None)

        tr = hist.transactions[0]
        if tr.id > 1 and tr.Type != "mkstream":
            if diffXml is not None:
                diff = accurev.obj.Diff.fromxmlstring(streamsXml)
            
            if diff is None or diffXml is None:
                if streamName is not None:
                    diff, diffXml = self.TryDiff(streamName=streamName, firstTrNumber=tr.id, secondTrNumber=(tr.id - 1))
                    if diff is None or diffXml is None:
                        return (None, None)
                else:
                    return (None, None)

            diffFilePath = os.path.join(self.gitRepo.path, 'diff.xml')
            with codecs.open(diffFilePath, mode='w', encoding='utf-8') as f:
                f.write(self.NormalizeAccurevXml(diffXml))

        streamsFilePath = os.path.join(path, 'streams.xml')
        with codecs.open(streamsFilePath, mode='w', encoding='utf-8') as f:
            f.write(self.NormalizeAccurevXml(streamsXml))
        
        histFilePath = os.path.join(path, 'hist.xml')
        with codecs.open(histFilePath, mode='w', encoding='utf-8') as f:
            f.write(self.NormalizeAccurevXml(histXml))

    # GetDepotRefsNamespace
    # When depot is None it returns the git ref namespace where all depots are under.
    # When depot is not None it queries the stored depots for the depot name or number and returns the git ref namespace for that depot.
    # If the depot name or number is not None and does not correspont to a depot in Accurev this function returns None.
    def GetDepotRefsNamespace(self, depot=None):
        depotsNS = '{refsNS}depots/'.format(refsNS=AccuRev2Git.gitRefsNamespace)
        if depot is not None:
            d = self.GetDepot(depot)
            if d is not None:
                depotNS = '{depotsNS}{depotNumber}/'.format(depotsNS=depotsNS, depotNumber=d.number)
                return depotNS
            return None # Invalid depot, no refs allowed.
        return depotsNS

    def ParseDepotRef(self, ref):
        depotNumber, remainder = None, None
        if ref is not None and isinstance(ref, str):
            depotsNS = self.GetDepotRefsNamespace()
            # Extract the depot number.
            match = re.match(r'^{depotsNS}(\d+)/(.*)$'.format(depotsNS=depotsNS), ref)
            if match is not None:
                depotNumber = int(match.group(1))
                remainder = match.group(2)
        return depotNumber, remainder

    def GetDepot(self, depot):
        depotNumber = None
        depotName = None
        try:
            depotNumber = int(depot)
        except:
            if isinstance(depot, str):
                depotName = depot
        depotsRef = '{depotsNS}info'.format(depotsNS=self.GetDepotRefsNamespace())

        # Check if the ref exists!
        commitHash = self.GetLastCommitHash(self, ref=depotsRef)
        haveCommitted = False
        if commitHash is None:
            # It doesn't exist, we can create it.        
            logger.debug( "Ref '{br}' doesn't exist.".format(br=depotsRef) )

            # Delete everything in the index and working directory.
            self.gitRepo.rm(fileList=['.'], force=True, recursive=True)
            self.ClearGitRepo()

            depots, depotsXml = self.TryDepots()
            if depots is None or depotsXml is None:
                return None

            depotsFilePath = os.path.join(self.gitRepo.path, 'depots.xml')
            with codecs.open(depotsFilePath, 'w') as f:
                f.write(re.sub('TaskId="[0-9]+"', 'TaskId="0"', depotsXml))

            commitHash = self.Commit(transaction=None, messageOverride="depots at ac2git invocation.", parents=[], ref=depotsRef)
            if commitHash is None:
                logger.debug( "First commit on the depots ref ({ref}) has failed. Aborting!".format(ref=depotsRef) )
                return None
            else:
                logger.info( "Depots ref updated {ref} -> commit {hash}".format(hash=self.ShortHash(commitHash), ref=depotsRef) )
                haveCommitted = True
        else:
            depotsXml, depots = self.GetDepotsInfo(ref=commitHash)

        # Try and find the depot in the list of existing depots.
        for d in depots.depots:
            if depotName is not None and d.name == depotName:
                return d
            elif depotNumber is not None and d.number == depotNumber:
                return d

        if haveCommitted:
            logger.info( "Failed to find depot {d} on depots ref {r} at commit {h}".format(d=depot, h=self.ShortHash(commitHash), r=depotsRef) )
            return None

        # We haven't committed anything yet so a depot might have been renamed since we started. Run the depots command again and commit it if there have been any changes.

        self.gitRepo.checkout(branchName=depotsRef)

        # Delete everything in the index and working directory.
        self.gitRepo.rm(fileList=['.'], force=True, recursive=True)
        self.ClearGitRepo()

        depots, depotsXml = self.TryDepots()
        if depots is None or depotsXml is None:
            return None

        depotsFilePath = os.path.join(self.gitRepo.path, 'depots.xml')
        with codecs.open(depotsFilePath, 'w') as f:
            f.write(re.sub('TaskId="[0-9]+"', 'TaskId="0"', depotsXml))

        commitHash = self.Commit(transaction=None, messageOverride="depots at ac2git invocation.".format(trId=tr.id), ref=depotsRef)
        if commitHash is None:
            logger.debug( "Commit on the depots ref ({ref}) has failed. Couldn't find the depot {d}. Aborting!".format(ref=depotsRef, d=depot) )
            return None
        else:
            logger.info( "Depots ref updated {ref} -> commit {hash}".format(hash=self.ShortHash(commitHash), ref=depotsRef) )
            haveCommitted = True

            # Try and find the depot in the list of existing depots.
            for d in depots.depots:
                if depotName is not None and d.name == depotName:
                    return d
                elif depotNumber is not None and d.number == depotNumber:
                    return d

        return None

    def GetStreamRefsNamespace(self, depot, streamNumber=None):
        depotNS = self.GetDepotRefsNamespace(depot=depot)
        if depotNS is not None:
            streamsNS = '{depotNS}streams/'.format(depotNS=depotNS)
            if streamNumber is not None:
                if not isinstance(streamNumber, int):
                    streamNumber = int(streamNumber)
                streamNS = '{streamsNS}{streamNumber}'.format(streamsNS=streamsNS, streamNumber=streamNumber)
                return streamNS
            return streamsNS
        return None

    def ParseStreamRef(self, ref):
        depotNumber, streamNumber, remainder = None, None, None
        if ref is not None and isinstance(ref, str):
            depotNumber, depotRemainder = self.ParseDepotRef(ref=ref)
            if depotNumber is not None:
                streamsNS = self.GetStreamRefsNamespace(depot=depotNumber)
                if streamsNS is not None:
                    # Extract the stream number.
                    match = re.match(r'^{streamsNS}(\d+)/(.*)$'.format(streamsNS=streamsNS), ref)
                    if match is not None:
                        streamNumber = int(match.group(1))
                        remainder = match.group(2)
        return (depotNumber, streamNumber, remainder)

    def GetStreamRefs(self, depot, streamNumber):
        stateRef, dataRef, hwmRef = None, None, None
        streamNS = self.GetStreamRefsNamespace(depot, streamNumber=streamNumber)
        if streamNS is not None:
            dataRef  = '{streamNS}/data'.format(streamNS=streamNS)
            stateRef = '{streamNS}/info'.format(streamNS=streamNS)
            hwmRef = '{streamNS}/hwm'.format(streamNS=streamNS) # High-water mark ref.
        return (stateRef, dataRef, hwmRef)

    # Gets the diff.xml contents and parsed accurev.obj.Diff object from the given \a ref (git ref or hash).
    def GetDiffInfo(self, ref):
        # Get the diff information. (if any)
        diff = None
        diffXml = self.gitRepo.raw_cmd(['git', 'show', '{hash}:diff.xml'.format(hash=ref)]) # Doesn't exist for the mkstream transaction (first commit)
        if diffXml is not None:
            diff = accurev.obj.Diff.fromxmlstring(diffXml)
        elif len(diffXml) == 0:
            raise Exception("Command failed! git show {hash}:diff.xml".format(hash=ref))
        return (diffXml, diff)

    # Gets the hist.xml contents and parsed accurev.obj.History object from the given \a ref (git ref or hash).
    def GetHistInfo(self, ref):
        # Get the hist information.
        hist = None
        histXml = self.gitRepo.raw_cmd(['git', 'show', '{hash}:hist.xml'.format(hash=ref)])
        if histXml is not None and len(histXml) != 0:
            hist = accurev.obj.History.fromxmlstring(histXml)
        else:
            raise Exception("Command failed! git show {hash}:hist.xml".format(hash=ref))
        return (histXml, hist)

    # Gets the streams.xml contents and parsed accurev.obj.Show.Streams object from the given \a ref (git ref or hash).
    def GetStreamsInfo(self, ref):
        # Get the stream information.
        streams = None
        streamsXml = self.gitRepo.raw_cmd(['git', 'show', '{hash}:streams.xml'.format(hash=ref)])
        if streamsXml is not None and len(streamsXml) != 0:
            streams = accurev.obj.Show.Streams.fromxmlstring(streamsXml)
        else:
            raise Exception("Command failed! git show {hash}:streams.xml".format(hash=ref))
        return (streamsXml, streams)

    # Gets the depots.xml contents and parsed accurev.obj.Show.Streams object from the given \a ref (git ref or hash).
    def GetDepotsInfo(self, ref):
        # Get the stream information.
        depots = None
        depotsXml = self.gitRepo.raw_cmd(['git', 'show', '{hash}:depots.xml'.format(hash=ref)])
        if depotsXml is not None and len(depotsXml) != 0:
            depots = accurev.obj.Show.Depots.fromxmlstring(depotsXml)
        else:
            raise Exception("Command failed! git show {hash}:depots.xml".format(hash=ref))
        return (depotsXml, depots)

    def RetrieveStreamInfo(self, depot, stream, stateRef, startTransaction, endTransaction):
        logger.info( "Processing Accurev state for {0} : {1} - {2}".format(stream.name, startTransaction, endTransaction) )

        # Check if the ref exists!
        stateRefObj = self.gitRepo.raw_cmd(['git', 'show-ref', stateRef])
        assert stateRefObj is None or len(stateRefObj) != 0, "Invariant error! Expected non-empty string returned by git show-ref, but got '{s}'".format(s=stateRefObj)

        # Either checkout last state or make the initial commit for a new stateRef.
        tr = None
        commitHash = None
        doInitialCheckout = False
        if stateRefObj is not None:
            # This means that the ref already exists so we should switch to it.
            doInitialCheckout = True
            histXml, hist = self.GetHistInfo(ref=stateRef)
            tr = hist.transactions[0]
        else:
            logger.debug( "Ref '{br}' doesn't exist.".format(br=stateRef) )
            # We are tracking a new stream
            firstHist, firstHistXml = self.GetFirstTransaction(depot=depot, streamName=stream.name, startTransaction=startTransaction, endTransaction=endTransaction)
            if firstHist is not None and len(firstHist.transactions) > 0:
                tr = firstHist.transactions[0]
                try:
                    destStream = self.GetDestinationStreamName(history=hist, depot=None)
                except:
                    destStream = None

                # Delete everything in the index and working directory.
                self.gitRepo.rm(fileList=['.'], force=True, recursive=True)
                self.ClearGitRepo()

                self.WriteInfoFiles(path=self.gitRepo.path, depot=depot, streamName=stream.name, transaction=tr.id, useCommandCache=self.config.accurev.UseCommandCache())

                commitHash = self.Commit(transaction=tr, messageOverride="transaction {trId}".format(trId=tr.id), parents=[], ref=stateRef, authorIsCommitter=True)
                if commitHash is None:
                    logger.debug( "{0} first commit has failed. Is it an empty commit? Aborting!".format(stream.name) )
                    return (None, None)
                else:
                    logger.info( "stream {streamName}: tr. #{trId} {trType} -> commit {hash} on {ref}".format(streamName=stream.name, trId=tr.id, trType=tr.Type, hash=self.ShortHash(commitHash), ref=stateRef) )
            else:
                logger.info( "Failed to get the first transaction for {0} from accurev. Won't retrieve any further.".format(stream.name) )
                return (None, None)

        # Get the end transaction.
        endTrHist, endTrHistXml = self.TryHist(depot=depot, timeSpec=endTransaction)
        if endTrHist is None:
            logger.debug("accurev hist -p {0} -t {1}.1 failed.".format(depot, endTransaction))
            return (None, None)
        endTr = endTrHist.transactions[0]
        logger.info("{0}: retrieving transaction range #{1} - #{2}".format(stream.name, tr.id, endTr.id))

        if tr.id > endTr.id:
            logger.info("{0}: nothing to do, last processed transaction {1} is greater than the end transaction {2}.".format(stream.name, tr.id, endTr.id))
            return (tr, self.GetLastCommitHash(ref=stateRef))

        # Iterate over all of the transactions that affect the stream we are interested in and maybe the "chstream" transactions (which affect the streams.xml).
        deepHist = None
        if self.config.method == "deep-hist":
            ignoreTimelocks=False # The code for the timelocks is not tested fully yet. Once tested setting this to false should make the resulting set of transactions smaller
                                 # at the cost of slightly larger number of upfront accurev commands called.
            logger.debug("accurev.ext.deep_hist(depot={0}, stream={1}, timeSpec='{2}-{3}', ignoreTimelocks={4})".format(depot, stream.name, tr.id, endTr.id, ignoreTimelocks))
            deepHist = accurev.ext.deep_hist(depot=depot, stream=stream.name, timeSpec="{0}-{1}".format(tr.id, endTr.id), ignoreTimelocks=ignoreTimelocks, useCache=self.config.accurev.UseCommandCache())
            logger.info("Deep-hist returned {count} transactions to process.".format(count=len(deepHist)))
            if deepHist is None:
                raise Exception("accurev.ext.deep_hist() failed to return a result!")
            elif len(deepHist) == 0:
                return (None, None)
        while True:
            nextTr, diff = self.FindNextChangeTransaction(streamName=stream.name, startTrNumber=tr.id, endTrNumber=endTr.id, deepHist=deepHist)
            if nextTr is None:
                logger.debug( "FindNextChangeTransaction(streamName='{0}', startTrNumber={1}, endTrNumber={2}, deepHist={3}) failed!".format(stream.name, tr.id, endTr.id, deepHist) )
                return (None, None)

            logger.debug( "{0}: next transaction {1} (end tr. {2})".format(stream.name, nextTr, endTr.id) )
            if nextTr <= endTr.id:
                if doInitialCheckout:
                    # A postponed initialization of state. If there's nothing to do we should skip this checkout because
                    # it can be expensive. So only do it once and only when we will need to use it.
                    self.SafeCheckout(ref=stateRef, doReset=True, doClean=True)
                    doInitialCheckout = False

                # Right now nextTr is an integer representation of our next transaction.
                # Delete all of the files which are even mentioned in the diff so that we can do a quick populate (wouth the overwrite option)
                if self.config.method == "pop":
                    self.ClearGitRepo()
                else:
                    if diff is None:
                        return (None, None)

                # The accurev hist command here must be used with the depot option since the transaction that has affected us may not
                # be a promotion into the stream we are looking at but into one of its parent streams. Hence we must query the history
                # of the depot and not the stream itself.
                hist, histXml = self.TryHist(depot=depot, timeSpec=nextTr)
                if hist is None:
                    logger.debug("accurev hist -p {0} -t {1}.1 failed.".format(depot, endTransaction))
                    return (None, None)
                tr = hist.transactions[0]
                stream = accurev.show.streams(depot=depot, stream=stream.streamNumber, timeSpec=tr.id, useCache=self.config.accurev.UseCommandCache()).streams[0]

                self.WriteInfoFiles(path=self.gitRepo.path, depot=depot, streamName=stream.name, transaction=tr.id, useCommandCache=self.config.accurev.UseCommandCache())
                    
                # Commit
                commitHash = self.Commit(transaction=tr, messageOverride="transaction {trId}".format(trId=tr.id), ref=stateRef, authorIsCommitter=True)
                if commitHash is None:
                    if "nothing to commit" in self.gitRepo.lastStdout:
                        logger.info("stream {streamName}: tr. #{trId} is a no-op. Potential but unlikely error. Continuing.".format(streamName=stream.name, trId=tr.id))
                    else:
                        break # Early return from processing this stream. Restarting should clean everything up.
                else:
                    if self.UpdateAndCheckoutRef(ref=stateRef, commitHash=commitHash) != True:
                        return (None, None)
                    logger.info( "stream {streamName}: tr. #{trId} {trType} -> commit {hash} on {ref}".format(streamName=stream.name, trId=tr.id, trType=tr.Type, hash=self.ShortHash(commitHash), ref=stateRef) )
            else:
                logger.info( "Reached end transaction #{trId} for {streamName} -> {ref}".format(trId=endTr.id, streamName=stream.name, ref=stateRef) )
                break

        return (tr, commitHash)

    def GetHashForTransaction(self, ref, trNum):
        # Find the commit hash on our ref that corresponds to the provided transaction number.
        cmd = ['git', 'log', '--format=%H', '--grep', '^transaction {trId}$'.format(trId=trNum), ref]
        lastCommitHash = self.gitRepo.raw_cmd(cmd)
        if lastCommitHash is None:
            raise Exception("Couldn't query {ref} for Accurev state information at transaction {trId}. {cmd}".format(ref=ref, trId=trNum, cmd=' '.join(cmd)))
        lastCommitHash = lastCommitHash.strip()

        if len(lastCommitHash) == 0:
            logger.error( "Failed to load transaction ({trId}) from ref {ref}. '{cmd}' returned empty.".format(trId=trNum, ref=ref, cmd=' '.join(cmd)) )
            return None
        return lastCommitHash

    def GetTransactionForRef(self, ref):
        # Find the last transaction number that we processed.
        lastCommitInfo = self.gitRepo.raw_cmd(['git', 'log', '--pretty=oneline', ref, '-1'])
        if lastCommitInfo is None:
            raise Exception("Couldn't load last transaction for ref: {ref}".format(ref=ref))
        lastCommitInfo = lastCommitInfo.strip()
        if len(lastCommitInfo) == 0:
            raise Exception("Couldn't load last transaction for ref: {ref} (empty result)".format(ref=ref))
        lastCommitInfo = lastCommitInfo.split(' ')
        if len(lastCommitInfo) != 3:
            raise Exception("Unexpected format for last commit message! Expected 3 space separated fields but read: {info}".format(info=' '.join(lastCommitInfo)))
        return int(lastCommitInfo[2])

    def GetGitLogList(self, ref, afterCommitHash=None, gitLogFormat=None):
        # Get the list of new hashes that have been committed to the stateRef but we haven't processed on the ref just yet.
        cmd = ['git', 'log']
        if gitLogFormat is not None:
            cmd.append('--format={f}'.format(f=gitLogFormat))
        cmd.append(ref)
        if afterCommitHash is not None:
            cmd.append('^{lastHash}'.format(lastHash=afterCommitHash))

        hashList = self.gitRepo.raw_cmd(cmd)
        if hashList is None:
            logger.debug("Couldn't get the commit hash list from the ref {ref}. '{cmd}'".format(ref=ref, cmd=' '.join(cmd)))
            return None

        hashList = hashList.strip()
        if len(hashList) == 0:
            return []

        return hashList.split('\n')

    # Uses the stateRef information to fetch the contents of the stream for each transaction that whose information was committed to the stateRef and commits it to the dataRef.
    def RetrieveStreamData(self, stream, dataRef, stateRef):
        # Check if the ref exists!
        dataRefObj = self.gitRepo.raw_cmd(['git', 'show-ref', dataRef])
        assert dataRefObj is None or len(dataRefObj) != 0, "Invariant error! Expected non-empty string returned by git show-ref, but got '{str}'".format(s=dataRefObj)

        # Either checkout last state or make the initial commit for a new dataRef.
        lastTrId = None
        stateHashList = None
        if dataRefObj is not None:
            # Find the last transaction number that we processed on the dataRef.
            lastTrId = self.GetTransactionForRef(ref=dataRef)

            # Find the commit hash on our stateRef that corresponds to our last transaction number.
            lastStateCommitHash = self.GetHashForTransaction(ref=stateRef, trNum=lastTrId)
            if lastStateCommitHash is None:
                logger.error( "{dataRef} is pointing to transaction {trId} which wasn't found on the state ref {stateRef}.".format(trId=lastTrId, dataRef=dataRef, stateRef=stateRef) )
                return (None, None)

            # Get the list of new hashes that have been committed to the stateRef but we haven't processed on the dataRef just yet.
            stateHashList = self.GetGitLogList(ref=stateRef, afterCommitHash=lastStateCommitHash, gitLogFormat='%H')
            if stateHashList is None:
                logger.error("Couldn't get the commit hash list to process from the Accurev state ref {stateRef}.".format(stateRef=stateRef))
                return (None, None)
            elif len(stateHashList) == 0:
                logger.error( "{dataRef} is upto date. Couldn't load any more transactions after tr. ({trId}) from Accurev state ref {stateRef}.".format(trId=lastTrId, dataRef=dataRef, stateRef=stateRef, lastHash=lastStateCommitHash) )

                # Get the first transaction that we are about to process.
                trHistXml, trHist = self.GetHistInfo(ref=lastStateCommitHash)
                tr = trHist.transactions[0]

                commitHash = self.GetHashForTransaction(ref=dataRef, trNum=tr.id)

                return (tr, commitHash)

            # This means that the ref already exists so we should switch to it.
            # We shouldn't do this earlier since if there's nothing to do we can skip this expensive operation.
            self.SafeCheckout(ref=dataRef, doReset=True, doClean=True)

        else:
            # Get all the hashes from the stateRef since we need to process them all.
            stateHashList = self.GetGitLogList(ref=stateRef, gitLogFormat='%H')
            if stateHashList is None:
                raise Exception("Couldn't get the commit hash list to process from the Accurev state ref {stateRef}.".format(stateRef=stateRef))

            if len(stateHashList) == 0:
                logger.error( "{dataRef} is upto date. No transactions available in Accurev state ref {stateRef}. git log {stateRef} returned empty.".format(dataRef=dataRef, stateRef=stateRef) )
                return (None, None)

            # Remove the first hash (last item) from the processing list and process it immediately.
            stateHash = stateHashList.pop()
            assert stateHash is not None and len(stateHash) != 0, "Invariant error! We shouldn't have empty strings in the stateHashList"

            logger.info( "No {dr} found. Processing {h} on {sr} first.".format(dr=dataRef, h=self.ShortHash(stateHash), sr=stateRef) )

            # Get the first transaction that we are about to process.
            trHistXml, trHist = self.GetHistInfo(ref=stateHash)
            tr = trHist.transactions[0]

            lastTrId = tr.id

            # Delete everything in the index and working directory.
            self.gitRepo.rm(fileList=['.'], force=True, recursive=True)
            self.ClearGitRepo()

            # Populate the stream contents from accurev
            popResult = self.TryPop(streamName=stream.name, transaction=tr, overwrite=True)
            if not popResult:
                logger.error( "accurev pop failed for {trId} on {dataRef}".format(trId=tr.id, dataRef=dataRef) )
                return (None, None)

            # Make first commit.
            commitHash = self.Commit(transaction=tr, allowEmptyCommit=True, messageOverride="transaction {trId}".format(trId=tr.id), parents=[], ref=dataRef, authorIsCommitter=True)
            if commitHash is None:
                # The first streams mkstream transaction will be empty so we may end up with an empty commit.
                logger.debug( "{0} first commit has failed.".format(stream.name) )
                return (None, None)
            else:
                if self.gitRepo.checkout(branchName=dataRef) is None:
                    logger.debug( "{0} failed to checkout data ref {1}. Aborting!".format(stream.name, dataRef) )
                    return (None, None)

                logger.info( "stream {streamName}: tr. #{trId} {trType} -> commit {hash} on {ref}".format(streamName=stream.name, trId=tr.id, trType=tr.Type, hash=self.ShortHash(commitHash), ref=dataRef) )

        # Find the last transaction number that we processed on the dataRef.
        lastStateTrId = self.GetTransactionForRef(ref=stateRef)
        if lastStateTrId is None:
            logger.error( "Failed to get last transaction processed on the {ref}.".format(ref=stateRef) )
            return (None, None)
        # Notify the user what we are processing.
        logger.info( "Processing stream data for {0} : {1} - {2}".format(stream.name, lastTrId, lastStateTrId) )

        # Process all the hashes in the list
        for stateHash in reversed(stateHashList):
            assert stateHash is not None, "Invariant error! Hashes in the stateHashList cannot be none here!"
            assert len(stateHash) != 0, "Invariant error! Excess new lines returned by `git log`? Probably safe to skip but shouldn't happen."

            # Get the diff information. (if any)
            diffXml, diff = self.GetDiffInfo(ref=stateHash)

            # Get the hist information.
            histXml, hist = self.GetHistInfo(ref=stateHash)

            # Get the stream information.
            streamsXml, streams = self.GetStreamsInfo(ref=stateHash)

            popOverwrite = (self.config.method == "pop")
            deletedPathList = None
            if self.config.method == "pop":
                self.ClearGitRepo()
            else:
                if diff is None:
                    logger.error( "No diff available for {trId} on {dataRef}".format(trId=tr.id, dataRef=dataRef) )
                    return (None, None)
 
                try:
                    deletedPathList = self.DeleteDiffItemsFromRepo(diff=diff)
                except:
                    popOverwrite = True
                    logger.info("Error trying to delete changed elements. Fatal, aborting!")
                    # This might be ok only in the case when the files/directories were changed but not in the case when there
                    # was a deletion that occurred. Abort and be safe!
                    # TODO: This must be solved somehow since this could hinder this script from continuing at all!
                    return (None, None)

                # Remove all the empty directories (this includes directories which contain an empty .gitignore file since that's what we is done to preserve them)
                try:
                    self.DeleteEmptyDirs()
                except:
                    popOverwrite = True
                    logger.info("Error trying to delete empty directories. Fatal, aborting!")
                    # This might be ok only in the case when the files/directories were changed but not in the case when there
                    # was a deletion that occurred. Abort and be safe!
                    # TODO: This must be solved somehow since this could hinder this script from continuing at all!
                    return (None, None)

            tr = hist.transactions[0]
            streamAtTr = streams.getStream(stream.streamNumber)
            if streamAtTr is None:
                raise Exception("Failed to find stream {name} ({num}) in {list}".format(name=stream.name, num=stream.streamNumber, list=[(s.name, s.streamNumber) for s in streams]))
            else:
                stream = streamAtTr

            # Work out the source and destination streams for the promote (for the purposes of the commit message info).
            destStreamName, destStreamNumber = hist.toStream()
            destStream = None
            if destStreamNumber is not None:
                destStream = streams.getStream(destStreamNumber)
                if destStream is None:
                    raise Exception("Failed to find stream {name} ({num}) in {list}".format(name=destStreamName, num=destStreamNumber, list=[(s.name, s.streamNumber) for s in streams]))

            srcStream = None
            try:
                srcStreamName, srcStreamNumber = hist.fromStream()
                if srcStreamNumber is not None:
                    srcStream = streams.getStream(srcStreamNumber)
                    if srcStream is None:
                        raise Exception("Failed to find stream {name} ({num}) in {list}".format(name=srcStreamName, num=srcStreamNumber, list=[(s.name, s.streamNumber) for s in streams]))
            except:
                srcStreamName, srcStreamNumber = None, None

            # Populate
            logger.debug( "{0} pop: {1} {2}{3}".format(stream.name, tr.Type, tr.id, " to {0}".format(destStreamName) if destStreamName is not None else "") )
            popResult = self.TryPop(streamName=stream.name, transaction=tr, overwrite=popOverwrite)
            if not popResult:
                logger.error( "accurev pop failed for {trId} on {dataRef}".format(trId=tr.id, dataRef=dataRef) )
                return (None, None)

            # Make the commit. Empty commits are allowed so that we match the state ref exactly (transaction for transaction).
            # Reasoning: Empty commits are cheap and since these are not intended to be seen by the user anyway so we may as well make them to have a simpler mapping.
            commitHash = self.Commit(transaction=tr, allowEmptyCommit=True, messageOverride="transaction {trId}".format(trId=tr.id), ref=dataRef, authorIsCommitter=True)
            if commitHash is None:
                logger.error( "Commit failed for {trId} on {dataRef}".format(trId=tr.id, dataRef=dataRef) )
                return (None, None)
            else:
                logger.info( "stream {streamName}: tr. #{trId} {trType} -> commit {hash} on {ref} (end tr. {endTrId})".format(streamName=stream.name, trId=tr.id, trType=tr.Type, hash=self.ShortHash(commitHash), ref=dataRef, endTrId=lastStateTrId) )

        return (tr, commitHash)

    # Retrieves all of the stream information from accurev, needed for later processing, and stores it in git using the \a dataRef and \a stateRef.
    # The retrieval and processing of the accurev information is separated in order to optimize processing of subsets of streams in a depot. For example,
    # if we have processed 7 streams in a depot and now wish to add an 8th we would have to start processing from the beginning because the merge points
    # between branches will now most likely need to be reconsidered. If the retrieval of information from accurev is a part of the processing step then we
    # have to redo a lot of the work that we have already done for the 7 streams. Instead we have the two steps decoupled so that all we need to do is
    # download the 8th stream information from accurev (which we don't yet have) and do the reprocessing by only looking for information already in git.
    def RetrieveStream(self, depot, stream, dataRef, stateRef, hwmRef, startTransaction, endTransaction):
        prevHwm = None
        if hwmRef is not None:
            hwmRefText = self.ReadFileRef(ref=hwmRef)
            if hwmRefText is not None and len(hwmRefText) > 0:
                prevHwmMetadata = json.loads(hwmRefText)
                prevHwm = prevHwmMetadata.get("high-water-mark")
                startTransaction = CallOnNonNoneArgs(max, int(startTransaction), prevHwm) # make sure we start from the transaction we last processed.

        logger.info( "Retrieving stream {0} info from Accurev for transaction range : {1} - {2}".format(stream.name, startTransaction, endTransaction) )
        stateTr, stateHash = self.RetrieveStreamInfo(depot=depot, stream=stream, stateRef=stateRef, startTransaction=startTransaction, endTransaction=endTransaction)
        logger.info( "Retrieving stream {0} data from Accurev for transaction range : {1} - {2}".format(stream.name, startTransaction if prevHwm is None else prevHwm, endTransaction) )
        dataTr,  dataHash  = self.RetrieveStreamData(stream=stream, dataRef=dataRef, stateRef=stateRef) # Note: In case the last retrieval was interrupted, we will retrieve those transactions first.

        if stateTr is not None and dataTr is not None:
            newHwm = CallOnNonNoneArgs(max, dataTr.id, prevHwm)
            if stateTr.id != dataTr.id:
                logger.error( "Missmatch while retrieving stream {streamName} (id: streamId), the data ref ({dataRef}) is on tr. {dataTr} while the state ref ({stateRef}) is on tr. {stateTr}.".format(streamName=stream.name, streamId=stream.streamNumber, dataTr=dataTr.id, stateTr=stateTr.id, dataRef=dataRef, stateRef=stateRef) )
            else:
                newHwm = CallOnNonNoneArgs(max, int(endTransaction), newHwm)

            # Success! Update the high water mark for the stream.
            if hwmRef is not None:
                metadata = { "high-water-mark": newHwm }
                if self.WriteFileRef(ref=hwmRef, text=json.dumps(metadata)) != True:
                    logger.error( "Failed to write the high-water-mark to ref {ref}".format(ref=hwmRef) )
                else:
                    logger.info( "Updated the high-water-mark to ref {ref} as {trId}".format(ref=hwmRef, trId=newHwm) )
        elif stateTr is not None and dataTr is None:
            logger.error( "Missmatch while retrieving stream {streamName} (id: streamId), the state ref ({stateRef}) is on tr. {stateTr} but the data ref ({dataRef}) wasn't retrieved.".format(streamName=stream.name, streamId=stream.streamNumber, stateTr=stateTr.id, dataRef=dataRef, stateRef=stateRef) )
        elif stateTr is None:
            logger.error( "While retrieving stream {streamName} (id: streamId), the state ref ({stateRef}) failed.".format(streamName=stream.name, streamId=stream.streamNumber, dataRef=dataRef, stateRef=stateRef) )

        return dataTr, dataHash

    def RetrieveStreams(self):
        if self.config.accurev.commandCacheFilename is not None:
            accurev.ext.enable_command_cache(self.config.accurev.commandCacheFilename)
        
        streamMap = self.GetStreamMap()

        depot  = self.config.accurev.depot
        endTrHist = accurev.hist(depot=depot, timeSpec=self.config.accurev.endTransaction)
        if endTrHist is None or endTrHist.transactions is None or len(endTrHist.transactions) == 0:
            logger.error( "Failed to get end transaction for depot {0}. `accurev hist -p {0} -t {1}` returned no transactions. Please make sure the depot name is spelled correctly and that the transaction number/keyword is valid.".format(depot, self.config.accurev.endTransaction) )
            return
        endTr = endTrHist.transactions[0]

        # Retrieve stream information from Accurev and store it inside git.
        for stream in streamMap:
            streamInfo = None
            try:
                streamInfo = accurev.show.streams(depot=depot, stream=stream, useCache=self.config.accurev.UseCommandCache()).streams[0]
            except IndexError:
                logger.error( "Failed to get stream information. `accurev show streams -p {0} -s {1}` returned no streams".format(depot, stream) )
                return
            except AttributeError:
                logger.error( "Failed to get stream information. `accurev show streams -p {0} -s {1}` returned None".format(depot, stream) )
                return

            if depot is None or len(depot) == 0:
                depot = streamInfo.depotName

            stateRef, dataRef, hwmRef  = self.GetStreamRefs(depot=depot, streamNumber=streamInfo.streamNumber)
            assert stateRef is not None and dataRef is not None and len(stateRef) != 0 and len(dataRef) != 0, "Invariant error! The state ({sr}) and data ({dr}) refs must not be None!".format(sr=stateRef, dr=dataRef)
            tr, commitHash = self.RetrieveStream(depot=depot, stream=streamInfo, dataRef=dataRef, stateRef=stateRef, hwmRef=hwmRef, startTransaction=self.config.accurev.startTransaction, endTransaction=endTr.id)

            if self.config.git.remoteMap is not None:
                refspec = "{dataRef}:{dataRef} {stateRef}:{stateRef}".format(dataRef=dataRef, stateRef=stateRef)
                for remoteName in self.config.git.remoteMap:
                    pushOutput = None
                    logger.info("Pushing '{refspec}' to '{remote}'...".format(remote=remoteName, refspec=refspec))
                    try:
                        pushCmd = "git push {remote} {refspec}".format(remote=remoteName, refspec=refspec)
                        pushOutput = subprocess.check_output(pushCmd.split(), stderr=subprocess.STDOUT).decode('utf-8')
                        logger.info("Push to '{remote}' succeeded:".format(remote=remoteName))
                        logger.info(pushOutput)
                    except subprocess.CalledProcessError as e:
                        logger.error("Push to '{remote}' failed!".format(remote=remoteName))
                        logger.error("'{cmd}', returned {returncode} and failed with:".format(cmd="' '".join(e.cmd), returncode=e.returncode))
                        logger.error("{output}".format(output=e.output.decode('utf-8')))
        
        if self.config.accurev.commandCacheFilename is not None:
            accurev.ext.disable_command_cache()

    # Lists the .git/... directory that contains all the stream refs and returns the file list as its result
    def GetAllKnownStreamRefs(self, depot):
        refsPrefix = self.GetDepotRefsNamespace() # Search all depots

        cmd = [ 'git', 'show-ref', '--' ]
        cmdResult = self.gitRepo.raw_cmd(cmd)
        if cmdResult is None:
            raise Exception("Failed to execute 'git show-ref --'!")
        lines = cmdResult.strip().split('\n')

        if len(lines) == 0:
            raise Exception("The 'git show-ref --' command output was empty!")

        rv = []
        for line in lines:
            columns = line.split(' ')
            commitHash, ref = columns[0], columns[1]
            depotNumber, streamNumber, remainder = self.ParseStreamRef(ref=ref)
            if None not in [ depotNumber, streamNumber ]:
                rv.append(ref)
        return rv

    # Tries to get the stream name from the data that we have stored in git.
    def GetStreamByName(self, depot, streamName):
        depot = self.GetDepot(depot)

        streamNamesRefspec = u'{refsNS}cache/depots/{depotNumber}/stream_names'.format(refsNS=AccuRev2Git.gitRefsNamespace, depotNumber=depot.number)
        streamNames = {} # This is so we cache the stream name to stream number mapping which can take about 10 seconds to compute in a large-ish repo...
        streamNamesText = self.ReadFileRef(ref=streamNamesRefspec)
        if streamNamesText is not None:
            streamNames = json.loads(streamNamesText)

        if streamName in streamNames:
            commitHash = streamNames[streamName]
            if commitHash is not None:
                streamsXml, streams = self.GetStreamsInfo(ref=commitHash)
                for s in streams.streams:
                    if s.name == streamName:
                        logger.debug("Loaded cached stream '{name}' by name.".format(name=streamName))
                        return s # Found it!

        logger.debug("Searching for stream '{name}' by name.".format(name=streamName))

        refsPrefix = self.GetStreamRefsNamespace(depot.number)

        refList = self.GetAllKnownStreamRefs(depot.number)
        if refList is None:
            refList = []
        # The stream with the lowest number is most likely to have a transaction with a streams.xml that contains
        # our stream name. Only if we are really unlucky will we have to search more than the lowest numbered stream.
        # So, parse and extract the number from the ..._info refs, and remove the ..._data refs.
        infoRefList = []
        for ref in refList:
            depotNumber, streamNumber, remainder = self.ParseStreamRef(ref=ref)
            if streamNumber is not None and remainder == "info":
                infoRefList.append( (streamNumber, ref) ) # The stream number is extracted and put as the first element for sorting.
        infoRefList.sort()

        if len(infoRefList) == 0:
            logger.warning("The refs from which we search for stream information seem to be missing...")

        for streamNumber, ref in infoRefList:
            # Execute a `git log -S` command with the pickaxe option to find the stream name in the streams.xml
            cmd = [ 'git', 'log', '--format=%H', '-Sname="{n}"'.format(n=streamName), ref, '--', 'streams.xml' ]
            hashList = self.gitRepo.raw_cmd(cmd)
            if hashList is not None:
                hashList = hashList.strip()
                if len(hashList) != 0:
                    hashList = hashList.split()
                    # If there is more than one element then the stream has probably been renamed so we will take the earliest commit in which
                    # the stream name appears.
                    commitHash = hashList[-1]
                    streamsXml, streams = self.GetStreamsInfo(ref=commitHash)
                    for s in streams.streams:
                        if s.name == streamName:
                            streamNames[streamName] = commitHash # Write the commit hash where we found the stream name in the cache.
                            self.WriteFileRef(ref=streamNamesRefspec, text=json.dumps(streamNames)) # Do it for each stream since this is cheaper than searching.
                            return s
                    assert False, "Invariant error! We successfully found that the hash {h} on ref {r} mentions the stream {sn} but couldn't match it?!".format(h=commitHash, r=ref, sn=streamName)
        return None

    def GetRefMap(self, ref, mapType, afterCommitHash=None):
        allowedMapTypes = [ "commit2tr", "tr2commit" ]
        if ref is None or mapType is None:
            raise Exception("None type arguments not allowed! ref: {ref}, mapType: {mapType}".format(ref=ref, mapType=mapType))
        elif mapType not in allowedMapTypes:
            raise Exception("mapType must be one of {types}".format(types=', '.join(allowedMapTypes)))

        cmd = [ 'git', 'log', '--pretty=oneline', ref ]
        if afterCommitHash is not None:
            cmd.append( '^{lastHash}'.format(lastHash=afterCommitHash) )
        cmdResult = self.gitRepo.raw_cmd(cmd)
        strList = None
        if cmdResult is not None:
            cmdResult = cmdResult.strip()
            if len(cmdResult) > 0:
                strList = cmdResult.split('\n')
            else:
                logger.debug("GetRefMap(ref={ref}, mapType={t}) - command result is empty. Cmd: {cmd}".format(ref=ref, t=mapType, cmd=' '.join(cmd)))
                return None
        else:
            logger.debug("GetRefMap(ref={ref}, mapType={t}) - command result was None. Cmd: {cmd}, Err: {err}".format(ref=ref, t=mapType, cmd=' '.join(cmd), err=self.gitRepo.lastStderr))
            return None
 
        refMap = OrderedDict()
        if strList is not None:
            for s in strList:
                columns = s.split(' ')
                if mapType == "commit2tr":
                    refMap[columns[0]] = int(columns[2])
                elif mapType == "tr2commit":
                    refMap[int(columns[2])] = columns[0]

        return refMap

    def ShortHash(self, commitHash):
        if commitHash is None:
            return None
        if not isinstance(commitHash, str):
            return commitHash
        return commitHash[:8]

    def AddNote(self, transaction, commitHash, ref, note, committerName=None, committerEmail=None, committerDate=None, committerTimezone=None):
        notesFilePath = None
        if note is not None:
            with tempfile.NamedTemporaryFile(mode='w+', prefix='ac2git_note_', encoding='utf-8', delete=False) as notesFile:
                notesFilePath = notesFile.name
                notesFile.write(note)

        # Get the author's and committer's name, email and timezone information.
        if transaction is not None:
            committerName, committerEmail = self.GetGitUserFromAccuRevUser(transaction.user)
            committerDate, committerTimezone = self.GetGitDatetime(accurevUsername=transaction.user, accurevDatetime=transaction.time)

        if notesFilePath is not None:
            rv = self.gitRepo.notes.add(messageFile=notesFilePath, obj=commitHash, ref=ref, force=True, committerName=committerName, committerEmail=committerEmail, committerDate=committerDate, committerTimezone=committerTimezone, authorName=committerName, authorEmail=committerEmail, authorDate=committerDate, authorTimezone=committerTimezone)
            os.remove(notesFilePath)

            if rv is not None:
                logger.debug( "Added{ref} note for {hash}.".format(ref='' if ref is None else ' '+str(ref), hash=self.ShortHash(commitHash)) )
            else:
                logger.error( "Failed to add{ref} note for {hash}{trStr}".format(ref='' if ref is None else ' '+str(ref), hash=commitHash, trStr='' if transaction is None else ', tr. ' + str(transaction.id)) )
                logger.error(self.gitRepo.lastStderr)
            
            return rv
        else:
            logger.error( "Failed to create temporary file for script state note for {0}, tr. {1}".format(commitHash, transaction.id) )
        
        return None

    def ProcessStream(self, stream, branchName, startTrId=None, endTrId=None, streamMap=None):
        if stream is not None:
            stateRef, dataRef, hwmRef = self.GetStreamRefs(depot=stream.depotName, streamNumber=stream.streamNumber)
            assert stateRef is not None and dataRef is not None and len(stateRef) != 0 and len(dataRef) != 0, "Invariant error! The state ({sr}) and data ({dr}) refs must not be None!".format(sr=stateRef, dr=dataRef)

            if branchName is None:
                branchName = stream.name

            branchRef = 'refs/heads/{branchName}'.format(branchName=branchName)
            sanitizedRef = self.SanitizeRefName(branchRef)
            if branchRef != sanitizedRef:
                logger.warning("Branch name '{0}' is not allowed, renamed to '{1}'.".format(branchRef[len("refs/heads/"):], sanitizedRef[len("refs/heads/"):]))
                branchRef = sanitizedRef

            branchList = self.gitRepo.branch_list()
            if branchList is None:
                return None

            commitHash = None
            lastDataCommitHash = None
            if branchName in [ br.name if br is not None else None for br in branchList ]:
                commitHash = self.GetLastCommitHash(branchName=branchName)

                streamHistoryRef = self.GetStreamCommitHistoryRef(stream.depotName, stream.streamNumber)
                cmd = [ u'git', u'log', u'-1', u'--format=format:%s', streamHistoryRef ]
                trString = self.TryGitCommand(cmd=cmd)
                if trString is None or len(trString) == 0:
                    logger.error("Branch {br} exists but no previous state was found for this branch. Cannot process branch, skipping...".format(br=branchName))
                    return None

                # Here we know that the state must exist and be good!
                lastTrId = int(trString.strip().split()[1])

                # Find the commit hash on our dataRef that corresponds to our last transaction number.
                lastDataCommitHash = self.GetHashForTransaction(ref=dataRef, trNum=lastTrId)

                if lastDataCommitHash is None:
                    logger.error("Branch {br} exists and its last transaction was {lastTrId}. No new accurev data found, continuing...".format(br=branchName, lastTrId=lastTrId))
                    return None

            # Get the list of new hashes that have been committed to the dataRef but we haven't processed on the dataRef just yet.
            dataHashList = self.GetGitLogList(ref=dataRef, afterCommitHash=lastDataCommitHash, gitLogFormat='%H %s %T')
            if dataHashList is None:
                raise Exception("Couldn't get the commit hash list to process from the Accurev data ref {dataRef}.".format(dataRef=dataRef))
            elif len(dataHashList) == 0:
                logger.error( "{b} is upto date. Couldn't load any more transactions after tr. ({trId}).".format(trId=lastTrId, b=branchName) )

                return self.GetLastCommitHash(branchName=branchName)

            # Get the stateRef map of transaction numbers to commit hashes.
            stateMap = self.GetRefMap(ref=stateRef, mapType="tr2commit")
            assert stateMap is not None, "Invariant error! If the dataMap is not None then neither should the stateMap be!"

            # Commit the new data with the correct commit messages.
            for line in reversed(dataHashList):
                columns = line.split(' ')
                trId, treeHash = int(columns[2]), columns[3]
                if endTrId is not None and endTrId < trId:
                    logger.debug( "ProcessStream(stream='{s}', branchName='{b}', endTrId='{endTrId}') - next tr. is {trId}, stopping.".format(s=stream.name, b=branchName, trId=trId, endTrId=endTrId) )
                    break
                if startTrId is not None and trId < startTrId:
                    logger.debug( "ProcessStream(stream='{s}', branchName='{b}', startTrId='{startTrId}') - tr. {trId} is earlier than the start transaction, skipping.".format(s=stream.name, b=branchName, trId=trId, startTrId=startTrId) )
                    continue

                # Get the transaction info.
                stateHash = stateMap[trId]
                if stateHash is None:
                    raise Exception("Failed to retrieve state information for tr. {trId}".format(trId))
                trHistXml, trHist = self.GetHistInfo(ref=stateHash)
                tr = trHist.transactions[0]

                streamsXml, streams = self.GetStreamsInfo(ref=stateHash)
                dstStreamName, dstStreamNumber = trHist.toStream()
                dstStream = streams.getStream(dstStreamNumber)
                srcStreamName, srcStreamNumber = trHist.fromStream()
                srcStream = streams.getStream(srcStreamNumber)

                parents = []
                if commitHash is not None:
                    parents = [ commitHash ]
                else:
                    # Invariant: The commitHash being None implies that this is
                    # the first commit for this stream and hence this must be a
                    # mkstream transaction.

                    # At the time the stream was created the current basis or time lock may not have
                    # been in effect. Hence we need the streams state at the given transaction.
                    streamAtTr = streams.getStream(stream.streamNumber)

                    # Note: if the streamMap is None the basisBranchName, basisCommitHash and streamTime will all be None and only the basisStream will be returned, hence this argument
                    #       serves a dual purpose and can be used to control if this function attaches the processed branch to its basis. If you want an orphan branch pass in the streamMap
                    #       as None.
                    basisStream, basisBranchName, basisCommitHash, streamTime = self.GetBasisCommitHash(streamAtTr.name, streamAtTr.streamNumber, streamAtTr.basisStreamNumber, streamAtTr.time, streams, streamMap, None, streamAtTr.startTime)
                    if basisBranchName is not None and basisCommitHash is None:
                        # The basis stream we found is tracked but there isn't a commit for it? This means that we are being processed first even though we should have processed the basis first...
                        self.ProcessStream(stream=basisStream, branchName=branchName, startTrId=startTrId, endTrId=endTrId, streamMap=streamMap)
                        # Try again, but this time we don't care if it fails since that must mean that we can't do anything about it.
                        basisStream, basisBranchName, basisCommitHash, streamTime = self.GetBasisCommitHash(streamAtTr.name, streamAtTr.streamNumber, streamAtTr.basisStreamNumber, streamAtTr.time, streams, streamMap, None, streamAtTr.startTime)

                    if basisCommitHash is None:
                        logger.info( "Creating orphan branch {branchName}.".format(branchName=branchName) )
                    else:
                        logger.info( "Creating branch {branchName} based on {basisBranchName} at {basisHash}".format(branchName=branchName, basisBranchName=basisBranchName, basisHash=basisCommitHash) )
                        parents = [ basisCommitHash ]

                commitHash = self.CommitTransaction(tr=tr, stream=stream, parents=parents, treeHash=treeHash, branchName=branchName, srcStream=srcStream, dstStream=dstStream)
                logger.info("Committed transaction {trId} to {br}. Commit {h}".format(trId=tr.id, br=branchName, h=self.ShortHash(commitHash)))

            return True
        return None

    def ProcessStreams(self, orderByStreamNumber=False):
        depot  = self.config.accurev.depot

        # Get the stream information for the configured streams from accurev (this is because stream names can change and accurev doesn't care about this while we do).
        processingList = []
        streamMap = self.GetStreamMap()
        for stream in streamMap:
            streamInfo = self.GetStreamByName(depot=depot, streamName=stream)
            if depot is None or len(depot) == 0:
                depot = streamInfo.depotName
            elif depot != streamInfo.depotName:
                logger.info("Stream {name} (id: {id}) is in depot {streamDepot} which is different than the configured depot {depot}. Ignoring...".format(name=streamInfo.name, id=streamInfo.streamNumber, streamDepot=streamInfo.depotName, depot=depot))

            processingList.append( (streamInfo.streamNumber, streamInfo, streamMap[stream]) )

        if orderByStreamNumber:
            processingList.sort()

        for streamNumber, stream, branchName in processingList:
            oldCommitHash = self.GetLastCommitHash(branchName=branchName, retry=False)

            self.ProcessStream(stream=stream, branchName=branchName)

            newCommitHash = self.GetLastCommitHash(branchName=branchName)

            # If a remote is configured and we have made a commit on this branch then do a push.
            if self.config.git.remoteMap is not None and oldCommitHash != newCommitHash:
                formatOptions = { "accurevNotes": AccuRev2Git.gitNotesRef_accurevInfo, "ac2gitNotes": AccuRev2Git.gitNotesRef_state, "branchName": branchName }
                refspec = "{branchName}".format(**formatOptions)
                if self.gitRepo.raw_cmd(['git', 'show-ref', '--hash', 'refs/notes/{accurevNotes}'.format(**formatOptions)]) is not None:
                    refspec += " refs/notes/{accurevNotes}:refs/notes/{accurevNotes}".format(**formatOptions)
                if self.gitRepo.raw_cmd(['git', 'show-ref', '--hash', 'refs/notes/{ac2gitNotes}'.format(**formatOptions)]) is not None:
                    refspec += " refs/notes/{ac2gitNotes}:refs/notes/{ac2gitNotes}".format(**formatOptions)
                for remoteName in self.config.git.remoteMap:
                    pushOutput = None
                    try:
                        pushCmd = "git push {remote} {refspec}".format(remote=remoteName, refspec=refspec)
                        pushOutput = subprocess.check_output(pushCmd.split(), stderr=subprocess.STDOUT).decode('utf-8')
                        logger.info("Push to '{remote}' succeeded:".format(remote=remoteName))
                        logger.info(pushOutput)
                    except subprocess.CalledProcessError as e:
                        logger.error("Push to '{remote}' failed!".format(remote=remoteName))
                        logger.debug("'{cmd}', returned {returncode} and failed with:".format(cmd="' '".join(e.cmd), returncode=e.returncode))
                        logger.debug("{output}".format(output=e.output.decode('utf-8')))
        
    def AppendCommitMessageSuffixStreamInfo(self, suffixList, linePrefix, stream):
        if stream is not None:
            suffixList.append( ('{linePrefix}:'.format(linePrefix=linePrefix), '{name} (id: {id}; type: {Type})'.format(id=stream.streamNumber, name=stream.name, Type=stream.Type)) )
            if stream.prevName is not None:
                suffixList.append( ('{linePrefix}-prev-name:'.format(linePrefix=linePrefix), '{name}'.format(name=stream.prevName)) )
            if stream.basis is not None:
                suffixList.append( ('{linePrefix}-basis:'.format(linePrefix=linePrefix), '{name} (id: {id})'.format(name=stream.basis, id=stream.basisStreamNumber)) )
            if stream.prevBasis is not None and len(stream.prevBasis) > 0:
                suffixList.append( ('{linePrefix}-prev-basis:'.format(linePrefix=linePrefix), '{name} (id: {id})'.format(name=stream.prevBasis, id=stream.prevBasisStreamNumber)) )
            if stream.time is not None and accurev.GetTimestamp(stream.time) != 0:
                suffixList.append( ('{linePrefix}-timelock:'.format(linePrefix=linePrefix), '{time} (UTC)'.format(time=stream.time)) )
            if stream.prevTime is not None and accurev.GetTimestamp(stream.prevTime) != 0:
                suffixList.append( ('{linePrefix}-prev-timelock:'.format(linePrefix=linePrefix), '{prevTime} (UTC)'.format(prevTime=stream.prevTime)) )

    def GenerateCommitMessageSuffix(self, transaction, stream=None, dstStream=None, srcStream=None, friendlyMessage=None):
        suffixList = []

        if friendlyMessage is not None:
            suffixList.append(friendlyMessage)

        suffixList.append( ('Accurev-transaction:', '{id} (type: {Type})'.format(id=transaction.id, Type=transaction.Type)) )
        if stream is not None:
            self.AppendCommitMessageSuffixStreamInfo(suffixList=suffixList, linePrefix='Accurev-stream', stream=stream)
        if dstStream is not None:
            self.AppendCommitMessageSuffixStreamInfo(suffixList=suffixList, linePrefix='Accurev-dst-stream', stream=dstStream)
        if srcStream is not None:
            self.AppendCommitMessageSuffixStreamInfo(suffixList=suffixList, linePrefix='Accurev-src-stream', stream=srcStream)
        
        # Ensure that all the items are nicely column aligned by padding the titles with spaces after the colon.
        longestSuffixTitle = 0
        for suffix in suffixList:
            if longestSuffixTitle < len(suffix[0]):
                longestSuffixTitle = len(suffix[0])
        suffixFormat = '{suffix: <' + str(longestSuffixTitle) + '} {info}'
        lineList = []
        for suffix in suffixList:
            lineList.append(suffixFormat.format(suffix=suffix[0], info=suffix[1]))
            
        return '\n'.join(lineList)

    def GenerateCommitMessage(self, transaction, stream=None, dstStream=None, srcStream=None, title=None, friendlyMessage=None, cherryPickSrcHash=None):
        messageSections = []
        
        # The optional transaction key tag can be added to the footer or the header of the comment before anything else is done.
        trComment = transaction.comment
        messageKey = None
        if self.config.git.messageKey is not None:
            messageKey = self.config.git.messageKey.lower()

            trKey = '{stream}/{transaction}'.format(stream=transaction.affectedStream()[0], transaction=transaction.id)

            if trComment is None:
                trComment = trKey
            else:
                if messageKey == "footer":
                    trComment = '\n\n'.join([trComment, trKey])
                elif messageKey == "header":
                    trComment = '\n\n'.join([trKey, trComment])
                else:
                    raise Exception("Unrecognized value '{v}' for message-key attribute of the git configuration file element.".format(v=self.config.git.messageKey))

        # The messageStyle option determines additional information that is far more detailed than the simple transaction key and is processed here.
        style = "notes"
        if self.config.git.messageStyle is not None:
            style = self.config.git.messageStyle.lower()

        notes = None
        if style == "clean":
            messageSections.append(trComment)
        elif style in [ "normal", "notes" ]:
            if title is not None:
                messageSections.append(title)
            if trComment is not None:
                messageSections.append(trComment)
            
            suffix = self.GenerateCommitMessageSuffix(transaction=transaction, stream=stream, dstStream=dstStream, srcStream=srcStream, friendlyMessage=friendlyMessage)
            if suffix is not None:
                if style == "normal":
                    messageSections.append(suffix)
                elif style == "notes":
                    notes = suffix
        else:
            raise Exception("Unrecognized git message style '{s}'".format(s=style))

        if cherryPickSrcHash is not None:
            if len(messageSections) > 0:
                messageSections[0] = "(CP) {0}".format(messageSections[0])
            messageSections.append("(cherry picked from commit {hash})".format(hash=cherryPickSrcHash))

        return ('\n\n'.join(messageSections), notes)

    def SanitizeRefComponent(self, component):
        if component is None or len(component) == 0:
            return component
        # If it starts with a dot, remove the dot
        if component[0] == '.':
            component = component[1:]
        # If it ends with .lock, remove the .lock
        if component.endswith(".lock"):
            component = component[:-len(".lock")]
        return component

    def SanitizeRefName(self, name):
        if name is None or len(name) == 0:
            return name

        while "//" in name:
            name = name.replace("//", "/")

        illegalSequence = {
            "..": "__",
            "?": "_",
            "*": "_",
            "[": "_",
            "\\": "/",
            "@{": "_",
            " ": "_"
        }
        for s in illegalSequence:
            name = name.replace(s, illegalSequence[s])
        
        # Remove control characters
        nonControl = ""
        for ch in name:
            if ord(ch) <= 40:
                nonControl += '_'
            else:
                nonControl += ch
        name = nonControl

        # Sanitize components
        name = "/".join([self.SanitizeRefComponent(x) for x in name.split('/')])

        illegalEnding = {
            ".": "",
            "/": "/_"
        }
        for e in illegalEnding:
            if name[-1] == e:
                name = name[:-1] + illegalEnding[e]

        if name == "@":
            return "_"

        return name

    def SanitizeBranchName(self, name):
        if name is None or len(name) == 0:
            return name
        sanitized = self.SanitizeRefName("refs/heads/{0}".format(name))
        if sanitized is None or len(sanitized) == 0:
            return sanitized
        return sanitized[len("refs/heads/"):]

    def BuildStreamTree(self, streams):
        rv = {}
        for s in streams:
            rv[s.streamNumber] = { "parent": s.basisStreamNumber, "children": [], "self": s }
        for s in streams:
            if s.basisStreamNumber is None:
                continue
            if s.basisStreamNumber not in rv:
                raise Exception("Incomplete set of streams given! Stream {s} is missing from the streams list, cannot build tree!".format(s=s.basisStreamNumber))
            rv[s.basisStreamNumber]["children"].append(s.streamNumber)
        return rv

    def PruneStreamTree(self, streamTree, keepList):
        rv = None
        if streamTree is not None:
            if keepList is None:
                return streamTree
            elif len(keepList) == 1:
                return { keepList[0]: { "parent": None, "children": [], "self": streamTree[keepList[0]]["self"] } }
            rv = streamTree.copy()
            # Remove all the streams that are not in the keepList and take their children and add them to the parent stream.
            for s in streamTree:
                if s not in keepList:
                    # Find the next parent that we are keeping.
                    p = streamTree[s]["parent"]
                    while p is not None and p not in keepList:
                        p = streamTree[p]["parent"]
                    # If we found the parent then append our children to his/hers.
                    if p is not None:
                        c = streamTree[s]["children"]
                        rv[p]["children"].extend(c)
                    del rv[s]
                    # Set the parent for all the children to either None or the actual parent.
                    for c in streamTree[s]["children"]:
                        if c in rv:
                            rv[c]["parent"] = p
            # Remove all the streams that are not in the keepList from each streams children list.
            for s in rv:
                children = []
                for c in rv[s]["children"]:
                    if c in keepList:
                        children.append(c)  # subset of children
                rv[s]["children"] = children

        return rv

    def GetStreamCommitHistoryRef(self, depot, streamNumber):
        depotObj = self.GetDepot(depot)
        if depotObj is None:
            raise Exception("Failed to get depot {depot}!".format(depot=depot))
        depot = depotObj
        if isinstance(streamNumber, int):
            return u'{refsNS}state/depots/{depotNumber}/streams/{streamNumber}/commit_history'.format(refsNS=AccuRev2Git.gitRefsNamespace, depotNumber=depot.number, streamNumber=streamNumber)
        return None

    def LogBranchState(self, stream, tr, commitHash):
        assert stream is not None and commitHash is not None and tr is not None, "LogBranchState(stream={s}, tr={t}, commitHash={h}) does not accept None arguments.".format(s=stream, t=tr, h=commitHash)

        # Log this commit at this transaction in the state refs that keep track of this stream's history over time.
        streamStateRefspec = self.GetStreamCommitHistoryRef(stream.depotName, stream.streamNumber)
        if streamStateRefspec is None:
            raise Exception("Failed to get hidden ref for stream {streamName} (id: {streamNumber}) depot {depotName}".format(streamName=stream.name, streamNumber=stream.streamNumber, depotName=stream.depotName))

        # Write the empty tree to the git repository to ensure there is one.
        emptyTree = self.gitRepo.empty_tree(write=True)
        if emptyTree is None or len(emptyTree) == 0:
            raise Exception("Failed to write empty tree to git repository!")

        # Get the last known state
        lastStateCommitHash = self.GetLastCommitHash(ref=streamStateRefspec)
        if lastStateCommitHash is None:
            # Since we will use git log --first-parent a lot we need to make sure we have a parentless commit to start off with.
            lastStateCommitHash = self.Commit(transaction=tr, allowEmptyCommit=True, messageOverride='transaction {trId}'.format(trId=tr.id), parents=[], treeHash=emptyTree, ref=streamStateRefspec, checkout=False, authorIsCommitter=True)
            if lastStateCommitHash is None:
                raise Exception("Failed to add empty state commit for stream {streamName} (id: {streamNumber})".format(streamName=stream.name, streamNumber=stream.streamNumber))
            logger.debug("Created state branch for stream {streamName} as {ref} - tr. {trType} {trId} - commit {h}".format(trType=tr.Type, trId=tr.id, streamName=stream.name, ref=streamStateRefspec, h=self.ShortHash(lastStateCommitHash)))
        stateCommitHash = self.Commit(transaction=tr, allowEmptyCommit=True, messageOverride='transaction {trId}'.format(trId=tr.id), parents=[ lastStateCommitHash, commitHash ], treeHash=emptyTree, ref=streamStateRefspec, checkout=False, authorIscommitter=True)
        if stateCommitHash is None:
            raise Exception("Failed to commit {Type} {tr} to hidden state ref {ref} with commit {h}".format(Type=tr.Type, tr=tr.id, ref=streamStateRefspec, h=self.ShortHash(commitHash)))
        logger.debug("Committed stream state for {streamName} to {ref} - tr. {trType} {trId} - commit {h}".format(trType=tr.Type, trId=tr.id, streamName=stream.name, ref=streamStateRefspec, h=self.ShortHash(stateCommitHash)))

    def TagTransaction(self, tagName, objHash, tr, stream, title=None, friendlyMessage=None, force=False):
        tagMessage, notes = self.GenerateCommitMessage(transaction=tr, stream=stream, title=title, friendlyMessage=friendlyMessage)
        
        # Create temporary file for the commit message.
        messageFilePath = None
        with tempfile.NamedTemporaryFile(mode='w+', prefix='ac2git_tag_', encoding='utf-8', delete=False) as messageFile:
            messageFilePath = messageFile.name
            emptyMessage = True
            if tagMessage is not None:
                if len(tagMessage) > 0:
                    messageFile.write(tagMessage)
                    emptyMessage = False
            elif tr is not None and tr.comment is not None and len(tr.comment) > 0:
                # In git the # at the start of the line indicate that this line is a comment inside the message and will not be added.
                # So we will just add a space to the start of all the lines starting with a # in order to preserve them.
                messageFile.write(tr.comment)
                emptyMessage = False

            if emptyMessage:
                # `git commit` and `git commit-tree` commands, when given an empty file for the commit message, seem to revert to
                # trying to read the commit message from the STDIN. This is annoying since we don't want to be opening a pipe to
                # the spawned process all the time just to write an EOF character so instead we will just add a single space as the
                # message and hope the user doesn't notice.
                # For the `git commit` command it's not as bad since white-space is always stripped from commit messages. See the 
                # `git commit --cleanup` option for details.
                messageFile.write(' ')
        
        if messageFilePath is None:
            logger.error("Failed to create temporary file for tag message. Transaction {trType} {trId}".format(trType=tr.Type, trId=tr.id))
            return None

        # Get the author's and committer's name, email and timezone information.
        taggerName, taggerEmail, taggerDate, taggerTimezone = None, None, None, None
        if tr is not None:
            taggerName, taggerEmail = self.GetGitUserFromAccuRevUser(tr.user)
            taggerDate, taggerTimezone = self.GetGitDatetime(accurevUsername=tr.user, accurevDatetime=tr.time)

        rv = self.gitRepo.create_tag(name=tagName, obj=objHash, annotated=True, message_file=messageFilePath, tagger_name=taggerName, tagger_email=taggerEmail, tagger_date=taggerDate, tagger_tz=taggerTimezone, cleanup='whitespace')
        os.remove(messageFilePath)
        
        if rv is None:
            # Depending on the version of Git we can't trust the return value of the `git tag` command.
            # Hence we use `git log refs/tags/<tag name>` instead of peeling back the tag to ensure that
            # it was correctly created.
            commitHash = self.GetLastCommitHash(branchName="refs/tags/{0}".format(tagName), retry=True)
            if commitHash != objHash:
                # Note: This assumes that we are ONLY taggint commit objects. If this ever changes then
                # we will need to properly peel back the tag and get the commit hash to which it points
                # to so that we can directly compare it to the objHash.
                logger.error("Failed to tag {trType} {trId}. Tag points to {commitHash} instead of {objHash}".format(trType=tr.Type, trId=tr.id, commitHash=commitHash, objHash=objHash))
                return False

        return True
        
    def CommitTransaction(self, tr, stream, parents=None, treeHash=None, branchName=None, title=None, srcStream=None, dstStream=None, friendlyMessage=None, cherryPickSrcHash=None, refNamespace='refs/heads/'):
        assert branchName is not None, "Error: CommitTransaction() is a helper for ProcessTransaction() and doesn't accept branchName as None."

        branchRef = None
        branchRef = '{ns}{branch}'.format(ns=refNamespace, branch=branchName)
        checkout = (branchName is None)

        commitMessage, notes = self.GenerateCommitMessage(transaction=tr, stream=stream, title=title, friendlyMessage=friendlyMessage, srcStream=srcStream, dstStream=dstStream, cherryPickSrcHash=cherryPickSrcHash)
        commitHash = self.Commit(transaction=tr, allowEmptyCommit=True, messageOverride=commitMessage, parents=parents, treeHash=treeHash, ref=branchRef, checkout=checkout)
        if commitHash is None:
            raise Exception("Failed to commit {Type} {tr}".format(Type=tr.Type, tr=tr.id))
        if notes is not None and self.AddNote(transaction=tr, commitHash=commitHash, ref=AccuRev2Git.gitNotesRef_accurevInfo, note=notes) is None:
            raise Exception("Failed to add note for commit {h} (transaction {trId}) to {br}.".format(trId=tr.id, br=branchName, h=commitHash))

        assert stream is not None, "Error: CommitTransaction() is a helper for ProcessTransaction() and doesn't accept stream as None."

        self.LogBranchState(stream=stream, tr=tr, commitHash=commitHash)

        return commitHash

    def GitRevParse(self, ref):
        if ref is not None:
            commitHash = self.gitRepo.rev_parse(args=[str(ref)], verify=True)
            if commitHash is None:
                raise Exception("Failed to parse git revision {ref}. Err: {err}.".format(ref=ref, err=self.gitRepo.lastStderr))
            return commitHash.strip()
        return None
    
    def GitDiff(self, ref1, ref2):
        # The `git diff --stat` and `git diff` commands have different behavior w.r.t. .git/info/attributes file:
        # http://stackoverflow.com/questions/10415100/want-to-exclude-file-from-git-diff#comment29471399_10421385
        # therefore ensure not to use the `--stat` flag.
        diff = self.gitRepo.diff(refs=[ref1, ref2], stat=False)
        if diff is None:
            raise Exception("Failed to diff {r1} to {r2}! Cmd: {cmd}, Err: {err}".format(r1=ref1, r2=ref2, cmd=' '.join(cmd), err=self.gitRepo.lastStderr))
        return diff.strip()
    
    def GitMergeBase(self, refs=[], isAncestor=False):
        assert None not in refs, "None is not an accepted value for a ref. Given refs are {refs}".format(refs=refs)
        hashes = []
        for ref in refs:
            hashes.append(self.GitRevParse(ref))
        return self.gitRepo.merge_base(commits=hashes, is_ancestor=isAncestor)
            
    def MergeIntoChildren(self, tr, streamTree, streamMap, affectedStreamMap, streams, streamNumber=None):
        srcStream, dstStream = None, None
        dstStreamName, dstStreamNumber = tr.affectedStream()
        if dstStreamNumber is not None:
            dstStream = streams.getStream(dstStreamNumber)
        srcStreamName, srcStreamNumber = tr.fromStream()
        if srcStreamNumber is not None:
            srcStream = streams.getStream(srcStreamNumber)

        if streamNumber is None:
            for sn in streamTree:
                if streamTree[sn]["parent"] is None:
                    stream, branchName, streamData, treeHash = self.UnpackStreamDetails(streams=streams, streamMap=streamMap, affectedStreamMap=affectedStreamMap, streamNumber=sn)
                    
                    parents = None # If left as none it will become a cherry-pick.
                    if stream is None:
                        raise Exception("Couldn't get the stream from its number {n}".format(n=sn))
                    elif treeHash is None:
                        raise Exception("Couldn't get tree hash from stream {s} (branch {b}). tr {trId} {trType}".format(s=stream.name, b=branchName, trId=tr.id, trType=tr.Type))

                    commitHash = self.CommitTransaction(tr=tr, stream=stream, parents=parents, treeHash=treeHash, branchName=branchName, srcStream=srcStream, dstStream=dstStream, cherryPickSrcHash=None)
                    logger.info("{Type} {trId}. cherry-picked to {branch} {h}. Untracked parent stream {ps}.".format(Type=tr.Type, trId=tr.id, branch=branchName, h=self.ShortHash(commitHash), ps=dstStreamName))

                    # Recurse down into children.
                    self.MergeIntoChildren(tr=tr, streamTree=streamTree, streamMap=streamMap, affectedStreamMap=affectedStreamMap, streams=streams, streamNumber=sn)
        else:
            stream, branchName, streamData, treeHash = self.UnpackStreamDetails(streams=streams, streamMap=streamMap, affectedStreamMap=affectedStreamMap, streamNumber=streamNumber)
            if stream is None:
                raise Exception("Couldn't get the stream from its number {n}".format(n=sn))
            elif streamNumber not in streamTree:
                raise Exception("Requested stream {s} (branch {b}) is not in the supplied tree {tree}. tr {trId} {trType}".format(s=stream.name, b=branchName, trId=tr.id, trType=tr.Type, tree=streamTree))
            
            lastCommitHash = self.GetLastCommitHash(branchName=branchName)
            s = streamTree[streamNumber]
            for c in s["children"]:
                assert c is not None, "Invariant error! Invalid dictionary structure. Data: {d1}, from: {d2}".format(d1=s, d2=streamTree)

                childStream, childBranchName, childStreamData, childTreeHash = self.UnpackStreamDetails(streams=streams, streamMap=streamMap, affectedStreamMap=affectedStreamMap, streamNumber=c)
                if childStream is None:
                    raise Exception("Couldn't get the stream from its number {n}".format(n=c))
                elif childTreeHash is None:
                    raise Exception("Couldn't get tree hash from stream {s}".format(s=childStream.name))

                if childStream.time is not None and accurev.GetTimestamp(childStream.time) != 0:
                    logger.info("{trType} {trId}. Child stream {s} is timelocked to {t}. Skipping affected child stream.".format(trType=tr.Type, trId=tr.id, s=childBranchName, t=childStream.time))
                    continue

                lastChildCommitHash = self.GetLastCommitHash(branchName=childBranchName)
                if lastChildCommitHash is None:
                    lastChildCommitHash = lastCommitHash
                assert lastChildCommitHash is not None, "No last commit hash for branch {br}".format(br=childBranchName)

                # Do a diff
                parents = None # Used to decide if we need to perform the commit. If None, don't commit, otherwise we manually set the parent chain.
                diff = self.GitDiff(lastCommitHash, childStreamData["data_hash"])

                if len(diff) == 0:
                    if self.GitMergeBase(refs=[ lastChildCommitHash, lastCommitHash ], isAncestor=True):
                        # Fast-forward the child branch to here.
                        if self.UpdateAndCheckoutRef(ref='refs/heads/{branch}'.format(branch=childBranchName), commitHash=lastCommitHash, checkout=False) != True:
                            raise Exception("Failed to fast-forward {branch} to {hash} (latest commit on {parentBranch}.".format(branch=childBranchName, hash=self.ShortHash(lastCommitHash), parentBranch=branchName))
                        logger.info("{trType} {trId}. Fast-forward {b} to {dst} {h} (affected child stream). Was at {ch}.".format(trType=tr.Type, trId=tr.id, b=childBranchName, dst=branchName, h=self.ShortHash(lastCommitHash), ch=self.ShortHash(lastChildCommitHash)))
                        self.LogBranchState(stream=childStream, tr=tr, commitHash=lastCommitHash) # Since we are not committing we need to manually store the ref state at this time.
                    else:
                        if self.config.git.emptyChildStreamAction == "merge":
                            # Merge by specifying the parent commits.
                            parents = [ lastChildCommitHash , lastCommitHash ] # Make this commit a merge of the parent stream into the child stream.
                            logger.info("{trType} {trId}. Merge {dst} into {b} {h} (affected child stream). {ch} was not an ancestor of {h}.".format(trType=tr.Type, trId=tr.id, b=childBranchName, dst=branchName, h=self.ShortHash(lastCommitHash), ch=self.ShortHash(lastChildCommitHash)))
                        elif self.config.git.emptyChildStreamAction == "cherry-pick":
                            parents = [ lastChildCommitHash ] # Make this commit a cherry-pick of the parent stream into the child stream.
                            logger.info("{trType} {trId}. Cherry pick {dst} into {b} {h} (affected child stream). {ch} was not an ancestor of {h}.".format(trType=tr.Type, trId=tr.id, b=childBranchName, dst=branchName, h=self.ShortHash(lastCommitHash), ch=self.ShortHash(lastChildCommitHash)))
                        else:
                            raise Exception("Unhandled option for self.config.git.emptyChildStreamAction. Option was set to: {0}".format(self.config.git.emptyChildStreamAction))
                else:
                    parents = [ lastChildCommitHash ] # Make this commit a cherry-pick with no relationship to the parent stream.
                    logger.info("{trType} {trId}. Cherry pick {dst} {dstHash} into {b} - diff between {h1} and {dstHash} was not empty! (affected child stream)".format(trType=tr.Type, trId=tr.id, b=childBranchName, dst=branchName, dstHash=self.ShortHash(lastCommitHash), h1=self.ShortHash(childStreamData["data_hash"])))

                if parents is not None:
                    assert None not in parents, "Invariant error! Either the source hash {sh} or the destination hash {dh} was none!".format(sh=parents[1], dh=parents[0])
                    cherryPickSrcHash = None
                    if len(parents) == 1 and parents[0] == lastChildCommitHash:
                        cherryPickSrcHash = lastCommitHash
                    commitHash = self.CommitTransaction(tr=tr, stream=childStream, treeHash=childTreeHash, parents=parents, branchName=childBranchName, srcStream=srcStream, dstStream=dstStream, cherryPickSrcHash=cherryPickSrcHash)
                    if commitHash is None:
                        raise Exception("Failed to commit transaction {trId} to branch {branchName}.".format(trId=tr.id, branchName=childBranchName))

                # Recurse into each child and do the same for its children.
                self.MergeIntoChildren(tr=tr, streamTree=streamTree, streamMap=streamMap, affectedStreamMap=affectedStreamMap, streams=streams, streamNumber=c)

    def UnpackStreamDetails(self, streams, streamMap, affectedStreamMap, streamNumber):
        if streamNumber is not None and not isinstance(streamNumber, int):
            streamNumber = int(streamNumber)

        # Get the information for the stream on which this transaction had occurred.
        stream, branchName, streamData, treeHash = None, None, None, None
        if streamNumber is not None:
            # Check if the destination stream is a part of our processing.
            if streamMap is not None and str(streamNumber) in streamMap:
                branchName = streamMap[str(streamNumber)]["branch"]
                if affectedStreamMap is not None and streamNumber in affectedStreamMap:
                    streamData = affectedStreamMap[streamNumber]
                    treeHash = streamData["data_tree_hash"]
                    if treeHash is None:
                        raise Exception("Couldn't get tree hash from stream {s}".format(s=streamName))

            # Get the deserialized stream object.
            stream = streams.getStream(streamNumber)

        return stream, branchName, streamData, treeHash

    def GetTimestampForCommit(self, commitHash):
        cmd = [u'git', u'log', u'-1', u'--format=format:%at', commitHash]
        timestamp = self.TryGitCommand(cmd=cmd)
        if timestamp is not None:
            return int(timestamp)
        return None

    def GetOrphanCommit(self, ref, customFormat='%H'):
        cmd = [u'git', u'log', u'-1', u'--format=format:{format}'.format(format=customFormat), u'--first-parent', u'--max-parents=0', ref]
        return self.TryGitCommand(cmd=cmd)

    def GetBasisCommitHash(self, streamName, streamNumber, streamBasisNumber, streamTime, streams, streamMap, affectedStreamMap, streamCreationTime):
        # Get the current/new basis stream
        basisStream, basisBranchName, basisStreamData, basisTreeHash = self.UnpackStreamDetails(streams=streams, streamMap=streamMap, affectedStreamMap=affectedStreamMap, streamNumber=streamBasisNumber)
        minTimestamp = None if streamTime is None or accurev.GetTimestamp(streamTime) == 0 else accurev.GetTimestamp(streamTime)
        while basisStream is not None and basisBranchName is None: # Find the first tracked basis stream.
            # Since this is an untracked basis take the earlier timestamp between ours and its timestamp.
            if basisStream.time is not None:
                basisTimestamp = accurev.GetTimestamp(basisStream.time)
                if basisTimestamp != 0:
                    minTimestamp = CallOnNonNoneArgs(min, minTimestamp, basisTimestamp)
            # Make the basis streams basis our basis and try again...
            basisStream, basisBranchName, basisStreamData, basisTreeHash = self.UnpackStreamDetails(streams=streams, streamMap=streamMap, affectedStreamMap=affectedStreamMap, streamNumber=basisStream.basisStreamNumber)

        # Update the stream time to be what we expect.
        minTime = accurev.UTCDateTimeOrNone(minTimestamp)

        if minTimestamp is not None and minTime != streamTime:
            logger.debug("GetBasisCommitHash: One of the parent streams had an earlier timelock.")
            logger.debug("  - Stream timelock:          {ts} ({t})".format(ts=accurev.GetTimestamp(streamTime), t=streamTime))
            logger.debug("  - Earliest parent timelock: {ts} ({t})".format(ts=minTimestamp, t=accurev.UTCDateTimeOrNone(minTimestamp)))

        if streamCreationTime is not None:
            streamCreationTimestamp = accurev.GetTimestamp(streamCreationTime)
            if minTimestamp is None:
                logger.debug("GetBasisCommitHash: streamCreationTime specified but no timelock found.")
                logger.debug("  - streamCreationTime will replace the timelock in further processing.")
                minTime, minTimestamp = streamCreationTime, streamCreationTimestamp
            elif streamCreationTimestamp < minTimestamp:
                logger.warning("GetBasisCommitHash: streamCreationTime is earlier than the timelock!")
                logger.warning("  - streamCreationTime will replace the timelock in further processing.")
                minTime, minTimestamp = streamCreationTime, streamCreationTimestamp

        if basisBranchName is not None:
            basisBranchHistoryRef = self.GetStreamCommitHistoryRef(basisStream.depotName, basisStream.streamNumber)

            timelockMessage = ''
            timelockISO8601Str = None
            if minTime is not None and accurev.GetTimestamp(minTime) != 0: # A timestamp of 0 indicates that a timelock was removed.
                timelockISO8601Str = "{datetime}Z".format(datetime=minTime.isoformat('T')) # The time is in UTC and ISO8601 requires us to specify Z for UTC.
                timelockMessage = ", before {s}".format(s=timelockISO8601Str)

            parentHashes = None
            earliestAllowedTimestamp = self.GetOrphanCommit(ref=basisBranchHistoryRef, customFormat='%at')
            if earliestAllowedTimestamp is None:
                logger.error("Failed to retrieve first commit hash for {ref}".format(ref=basisBranchHistoryRef))
                return None, None, None, None

            cmd = []
            if minTime is not None and (accurev.GetTimestamp(minTime) < int(earliestAllowedTimestamp)):
                # The timelock has been created before the creation date of the stream. We cannot return its
                # state before this time so we must return its first known/possible state.
                cmd = [u'git', u'log', u'-1', u'--format=format:%P', u'--reverse', u'--min-parents=1', u'--first-parent', basisBranchHistoryRef]
                parentHashes = self.TryGitCommand(cmd=cmd)
                logger.warning("Currently processed transaction requested its basis commit hash before its basis existed.")
                logger.warning("  - Earliest time available: {t}.".format(t=accurev.UTCDateTimeOrNone(earliestAllowedTimestamp)))
                logger.warning("  - Time requested:          {t}.".format(t=minTime))
                logger.warning(" Returning the earliest time available instead. TODO: What does Accurev actually do here? Should we look at the next basis in the chain?")
            else:
                cmd = [u'git', u'log', u'-1', u'--format=format:%P', u'--first-parent']
                if timelockISO8601Str is not None:
                    cmd.append(u'--before={before}'.format(before=timelockISO8601Str))
                cmd.append(basisBranchHistoryRef)
                parentHashes = self.TryGitCommand(cmd=cmd)

            if parentHashes is None:
                logger.error("Failed to retrieve last git commit hash. Command `{0}` failed.".format(' '.join(cmd)))
                return None, None, None, None

            parents = parentHashes.split()

            logger.debug("GetBasisCommitHash: Basis stream {basisName} (id: {basisSN}) at commit hash {h} is the basis for stream {name} (id: {sn}){timelockMsg}. (Retrieved from {ref})".format(name=streamName, sn=streamNumber, basisName=basisStream.name, basisSN=basisStream.streamNumber, ref=basisBranchHistoryRef, timelockMsg=timelockMessage, h=self.ShortHash(parents[1])))

            logger.debug("GetBasisCommitHash: Commit hash for stream {name} (id: {sn}) was not found.".format(name=streamName, sn=streamNumber))
            return basisStream, basisBranchName, parents[1], minTime

        logger.debug("GetBasisCommitHash: Commit hash for stream {name} (id: {sn}) was not found.".format(name=streamName, sn=streamNumber))
        return None, None, None, None

    # Processes a single transaction whose id is the trId (int) and which has been recorded against the streams outlined in the affectedStreamMap.
    # affectedStreamMap is a dictionary with the following format { <key:stream_num_str>: { "state_hash": <val:state_ref_commit_hash>, "data_hash": <val:data_ref_commit_hash> } }
    # The streamMap is used so that we can translate streams and their basis into branch names { <key:stream_num_str>: { "stream": <val:config_strem_name>, "branch": <val:config_branch_name> } }
    def ProcessTransaction(self, streamMap, trId, affectedStreamMap, prevAffectedStreamMap):
        # For all affected streams the streams.xml and hist.xml contents should be the same for the same transaction id so get it from any one of them.
        arbitraryStreamNumberStr = next(iter(affectedStreamMap))
        arbitraryStreamData = affectedStreamMap[arbitraryStreamNumberStr]
        streamsXml, streams = self.GetStreamsInfo(ref=arbitraryStreamData["state_hash"])
        if streams is None:
            raise Exception("Couldn't get streams for transaction {tr}. Aborting!".format(tr=trId))

        # Get the transaction information.
        trHistXml, trHist = self.GetHistInfo(ref=arbitraryStreamData["state_hash"])
        if trHist is None or len(trHist.transactions) == 0 is None:
            raise Exception("Couldn't get history for transaction {tr}. Aborting!".format(tr=trId))
        tr = trHist.transactions[0]

        # Get the name and number of the stream on which this transaction had occurred.
        streamName, streamNumber = tr.affectedStream()

        logger.debug( "Transaction #{tr} - {Type} by {user} to {stream} at {localTime} local time ({utcTime} UTC)".format(tr=tr.id, Type=tr.Type, utcTime=tr.time, localTime=utc2local(tr.time), user=tr.user, stream=streamName) )

        # Get the information for the stream on which this transaction had occurred.
        stream, branchName, streamData, treeHash = self.UnpackStreamDetails(streams=streams, streamMap=streamMap, affectedStreamMap=affectedStreamMap, streamNumber=streamNumber)

        # Process the transaction based on type.
        if tr.Type in ignored_transaction_types: # Ignored transactions.
            logger.info("Ignoring transaction #{id} - {Type} (transaction type is in ignored_transaction_types list)".format(id=tr.id, Type=tr.Type))

        elif tr.Type in [ "mkstream", "chstream" ]:
            parents = None
            lastCommitHash = None
            title = None
            targetStreams = []
            prevBasisCommitHash = None
            refNamespace = 'refs/heads/'
            createTag = False
            if tr.Type == "mkstream":
                # Old versions of accurev don't tell you the name of the stream that was created in the mkstream transaction.
                # The only way to find out what stream was created is to diff the output of the `accurev show streams` command
                # between the mkstream transaction and the one that preceedes it. However, the mkstream transaction will only
                # affect one stream so by the virtue of our datastructure the arbitraryStreamData should be the onlyone in our list
                # and we already have its "streamNumber".
                assert len(affectedStreamMap) == 1, "Invariant error! There is no way to know for what stream this mkstream transaction was made!"

                stream, branchName, streamData, treeHash = self.UnpackStreamDetails(streams=streams, streamMap=streamMap, affectedStreamMap=affectedStreamMap, streamNumber=int(arbitraryStreamNumberStr))
                parents = [] # First, orphaned, commit is denoted with an empty parents list.
                title = 'Created {name}'.format(name=branchName)
                targetStreams.append( (stream, branchName, streamData, treeHash, parents) )

                if stream.Type == "snapshot":
                    refNamespace = 'refs/tags/'
                    createTag = True
            elif tr.Type == "chstream":
                assert tr.stream is not None, "Invariant error! Can't handle not having a stream in the accurev hist XML output for a chstream transaction..."

                stream, branchName, streamData, treeHash = self.UnpackStreamDetails(streams=streams, streamMap=streamMap, affectedStreamMap=affectedStreamMap, streamNumber=tr.stream.streamNumber)
                stream = tr.stream
                if branchName is not None: # Only if this stream is being processed should we look it up.
                    title = 'Rebased {name}'.format(name=branchName)
                    lastCommitHash = self.GetLastCommitHash(branchName=branchName)
                    if lastCommitHash is None:
                        raise Exception("Error! Failed to get the last commit hash for branch {b} (stream: {s}), transaction {trType} {trId}!".format(trType=tr.Type, trId=tr.id, b=branchName, s=stream.name))
                    parents = [ lastCommitHash ]
                    targetStreams.append( (stream, branchName, streamData, treeHash, parents) )
                else:
                    allStreamTree = self.BuildStreamTree(streams=streams.streams)
                    keepList = list(set([ sn for sn in affectedStreamMap ]))
                    assert tr.stream.streamNumber not in keepList, "The stream must be tracked otherwise we would be in the if clause."
                    keepList.append(tr.stream.streamNumber)
                    affectedStreamTree = self.PruneStreamTree(streamTree=allStreamTree, keepList=keepList)
                    streamNode = affectedStreamTree[stream.streamNumber]
                    for sn in streamNode["children"]:
                        stream, branchName, streamData, treeHash = self.UnpackStreamDetails(streams=streams, streamMap=streamMap, affectedStreamMap=affectedStreamMap, streamNumber=sn)
                        lastCommitHash = self.GetLastCommitHash(branchName=branchName)
                        if lastCommitHash is None:
                            raise Exception("Error! Failed to get the last commit hash for branch {b} (stream: {s}), transaction {trType} {trId}!".format(trType=tr.Type, trId=tr.id, b=branchName, s=stream.name))
                        parents = [ lastCommitHash ]
                        targetStreams.append( (stream, branchName, streamData, treeHash, parents) )

                # Get the previous commit hash off which we would have been based at the time of the previous processed transaction.
                prevArbitraryStreamNumberStr = next(iter(prevAffectedStreamMap))
                prevArbitraryStreamData = prevAffectedStreamMap[prevArbitraryStreamNumberStr]
                prevStreamsXml, prevStreams = self.GetStreamsInfo(ref=prevArbitraryStreamData["state_hash"])
                if prevStreams is None:
                    raise Exception("Couldn't get streams for previous transaction (current transaction {tr}). Aborting!".format(tr=trId))

                prevBasisStreamNumber = tr.stream.prevBasisStreamNumber
                if prevBasisStreamNumber is None:
                    prevBasisStreamNumber = tr.stream.basisStreamNumber
                prevTime = tr.stream.prevTime
                if prevTime is None:
                    prevTime = tr.stream.time

                prevBasisStream, prevBasisBranchName, prevBasisCommitHash, prevStreamTime = self.GetBasisCommitHash(tr.stream.name, tr.stream.streamNumber, prevBasisStreamNumber, prevTime, prevStreams, streamMap, affectedStreamMap, None)
                if prevBasisBranchName is not None and prevBasisCommitHash is None:
                    raise Exception("Couldn't determine the last basis commit hash.")

                # Stream renames can be looked up in the stream.prevName value here.
                if tr.stream.prevName is not None and len(tr.stream.prevName.strip()) > 0:
                    # if the stream has been renamed, use its new name from now on.
                    logger.info("Stream renamed from {oldName} to {newName}. Branch name is {branch}, ignoring.".format(oldName=tr.stream.prevName, newName=tr.stream.name, branch=branchName))
            else:
                raise Exception("Not yet implemented! {trId} {trType}, unrecognized transaction type.".format(trId=tr.id, trType=tr.Type))

            # Get the commit hash off which we should be based off from this chstream transaction forward.
            basisStream, basisBranchName, basisCommitHash, streamTime = self.GetBasisCommitHash(stream.name, stream.streamNumber, stream.basisStreamNumber, stream.time, streams, streamMap, affectedStreamMap, None)
            if basisCommitHash is None:
                title = "{title} orphaned branch.".format(title=title)
            else:
                title = "{title} based on {basisBranchName} at {h}".format(title=title, basisBranchName=basisBranchName, h=self.ShortHash(basisCommitHash))

            assert len(targetStreams) != 0, "Invariant error! There should be at least one targetStreams item in the list!"

            for stream, branchName, streamData, treeHash, parents in targetStreams:
                assert branchName is not None, "Invariant error! branchName cannot be None here!"

                if prevBasisCommitHash != basisCommitHash:
                    amMergedIntoPrevBasis = ( len(parents) > 0 and prevBasisCommitHash is not None and self.GitMergeBase(refs=[ parents[0], prevBasisCommitHash ], isAncestor=True) )
                    if None in [ amMergedIntoPrevBasis ]:
                        raise Exception("Error! The git merge-base command failed!")
                    elif amMergedIntoPrevBasis:
                        # Fast-forward the timelocked stream branch to the correct commit.
                        assert createTag == False, "Invariant error! We only create tags for snapshots and we can't move a snapshot!"
                        if self.UpdateAndCheckoutRef(ref='{ns}{branch}'.format(ns=refNamespace, branch=branchName), commitHash=basisCommitHash, checkout=False) != True:
                            raise Exception("Failed to fast-forward {branch} to {hash} (latest commit on {parentBranch}). Old basis {oldHash} on {oldParentBranch}. Title: {title}".format(branch=branchName, hash=self.ShortHash(basisCommitHash), parentBranch=basisBranchName, oldHash=self.ShortHash(prevBasisCommitHash), oldParentBranch=prevBasisBranchName, title=title))
                        parents = None # Do not commit!
                        logger.info("{trType} {trId}. Fast-forward {dst} to {b} {h}.".format(trType=tr.Type, trId=tr.id, b=basisBranchName, h=self.ShortHash(basisCommitHash), dst=branchName))
                        self.LogBranchState(stream=stream, tr=tr, commitHash=basisCommitHash) # Since we are not committing we need to manually store the ref state at this time.
                    else:
                        # Merge by specifying the parent commits.
                        if self.config.git.newBasisIsFirstParent:
                            parents.insert(0, basisCommitHash) # Make this commit a merge of the parent stream into the child stream.
                        else:
                            parents.append(basisCommitHash)
                        assert None not in parents, "Invariant error! Either the source hash or the destination hash in {p} was none!".format(p=parents)

                        trInfoMsg="{trType} {trId}.".format(trType=tr.Type, trId=tr.id)
                        if len(parents) == 1:
                            basisTreeHash = self.GetTreeFromRef(ref=basisCommitHash)
                            if basisTreeHash == treeHash:
                                # Fast-forward the created stream branch to the correct commit.
                                if createTag:
                                    if self.TagTransaction(tagName=branchName, objHash=basisCommitHash, tr=tr, stream=stream, title=title) != True:
                                        raise Exception("Failed to create tag {branch} at {hash} (latest commit on {parentBranch}).".format(branch=branchName, hash=self.ShortHash(basisCommitHash), parentBranch=basisBranchName))
                                elif self.UpdateAndCheckoutRef(ref='{ns}{branch}'.format(ns=refNamespace, branch=branchName), commitHash=basisCommitHash, checkout=False) != True:
                                    raise Exception("Failed to fast-forward {branch} to {hash} (latest commit on {parentBranch}).".format(branch=branchName, hash=self.ShortHash(basisCommitHash), parentBranch=basisBranchName))
                                parents = None # Don't commit this mkstream since it doesn't introduce anything new.
                                logger.info("{trInfo} Created {dst} on {b} at {h}".format(trInfo=trInfoMsg, b=basisBranchName, h=self.ShortHash(basisCommitHash), dst=branchName))
                                self.LogBranchState(stream=stream, tr=tr, commitHash=basisCommitHash) # Since we are not committing we need to manually store the ref state at this time.
                            else:
                                logger.info("{trInfo} Created {dst} based on {b} at {h} (tree was not the same)".format(trInfo=trInfoMsg, b=basisBranchName, h=self.ShortHash(basisCommitHash), dst=branchName))
                        else:
                            logger.info("{trInfo} Merging {b} {h} as first parent into {dst}.".format(trInfo=trInfoMsg, b=basisBranchName, h=self.ShortHash(basisCommitHash), dst=branchName))
            
                if parents is not None:
                    if treeHash is None:
                        if branchName is None:
                            raise Exception("Couldn't get tree for {trType} {trId} on untracked stream {s}. Message: {m}".format(trType=tr.Type, trId=tr.id, s=stream.name, m=title))
                        logger.warning("No associated data commit found! Assumption: The {trType} {trId} didn't actually change the stream. An empty commit will be generated on branch {b}. Continuing...".format(trType=tr.Type, trId=tr.id, b=branchName))
                        treeHash = self.GetTreeFromRef(ref=branchName)
                        if treeHash is None:
                            raise Exception("Couldn't get last tree for {trType} {trId} on untracked stream {s}. Message: {m}".format(trType=tr.Type, trId=tr.id, s=stream.name, m=title))

                    commitHash = self.CommitTransaction(tr=tr, stream=stream, treeHash=treeHash, parents=parents, branchName=branchName, title=title, refNamespace=refNamespace)
                    if commitHash is None:
                        raise Exception("Failed to commit chstream {trId}".format(trId=tr.id))
                    logger.info("{Type} {tr}. committed to {branch} {h}. {title}".format(Type=tr.Type, tr=tr.id, branch=branchName, h=self.ShortHash(commitHash), title=title))
                    if createTag:
                        if self.TagTransaction(tagName=branchName, objHash=commitHash, tr=tr, stream=stream, title=title) != True:
                            raise Exception("Failed to create tag {branch} at {hash}.".format(branch=branchName, hash=self.ShortHash(commitHash)))
                        logger.warning("{Type} {tr}. creating tag {branch} for branch {branch} at {h}. {title}".format(Type=tr.Type, tr=tr.id, branch=branchName, h=self.ShortHash(commitHash), title=title))
                else:
                    logger.debug("{Type} {tr}. skiping commit to {branch}. (fast-forwarded to {h}) {title}".format(Type=tr.Type, tr=tr.id, branch=branchName, h=self.ShortHash(basisCommitHash), title=title))


                # Process all affected streams.
                allStreamTree = self.BuildStreamTree(streams=streams.streams)
                keepList = [ sn for sn in affectedStreamMap ]
                if branchName is not None:
                    keepList.append(stream.streamNumber) # The stream on which the chstream transaction occurred will never be affected so we have to keep it in there explicitly for the MergeIntoChildren() algorithm (provided it is being processed).
                keepList = list(set(keepList)) # Keep only unique values
                affectedStreamTree = self.PruneStreamTree(streamTree=allStreamTree, keepList=keepList)
                self.MergeIntoChildren(tr=tr, streamTree=affectedStreamTree, streamMap=streamMap, affectedStreamMap=affectedStreamMap, streams=streams, streamNumber=stream.streamNumber if branchName is not None else None)

        else:
            if branchName is not None and treeHash is None:
                raise Exception("Couldn't retrieve data for {trType} {trId} from stream {s}, branch {b}".format(trType=tr.Type, trId=tr.id, s=stream.name, b=branchName))

            # The rest of the transactions can be processed by stream type. Normal streams that have children need to try and merge down while workspaces which don't have children can skip this step.
            if stream.Type in [ "workspace" ]:
                # Workspaces don't have child streams 
                if tr.Type not in [ "add", "keep", "co", "move" ]:
                    logger.warning("Unexpected transaction {Type} {tr}. occurred in workspace {w}.".format(Type=tr.Type, tr=tr.id, w=stream.name))

                if branchName is not None:
                    commitHash = self.CommitTransaction(tr=tr, stream=stream, treeHash=treeHash, branchName=branchName)
                    logger.info("{Type} {tr}. committed to {branch} {h}.".format(Type=tr.Type, tr=tr.id, branch=branchName, h=self.ShortHash(commitHash)))

            elif stream.Type in [ "normal" ]:
                if tr.Type not in [ "promote", "defunct", "purge" ]:
                    logger.warning("Unexpected transaction {Type} {tr}. occurred in stream {s} of type {sType}.".format(Type=tr.Type, tr=tr.id, s=stream.name, sType=stream.Type))

                # Promotes can be thought of as merges or cherry-picks in git and deciding which one we are dealing with
                # is the key to having a good conversion.
                # There are 4 situations that we should consider:
                #   1. A promote from a child stream to a parent stream that promotes everything from that stream.
                #      This trivial case is the easiest to reason about and is obviously a merge.
                #   2. A promote from a child stream to a parent stream that promotes only some of the things from that
                #      stream. (i.e. one of 2 transactions is promoted up, or a subset of files).
                #      This is slightly trickier to reason about since the transactions could have been promoted in order
                #      (from earliest to latest) in which case it is a sequence of merges or in any other case it should be
                #      a cherry-pick.
                #   3. A promote from either an indirect descendant stream to this stream (a.k.a. cross-promote).
                #      This case can be considered as either a merge or a cherry-pick, but we will endevour to make it a merge.
                #   4. A promote from either a non-descendant stream to this stream (a.k.a. cross-promote).
                #      This case is most obviously a cherry-pick.

                if streamNumber is not None:
                    assert stream is not None, "Invariant error! How is it possible that at a promote transaction we don't have the destination stream? streams.xml must be invalid or incomplete!"
                else:
                    raise Exception("Error! Could not determine the destination stream for promote {tr}.".format(tr=tr.id))

                # Determine the stream from which the files in this this transaction were promoted.
                srcStreamName, srcStreamNumber = trHist.fromStream()
                srcStream, srcBranchName, srcStreamData, srcTreeHash = self.UnpackStreamDetails(streams=streams, streamMap=streamMap, affectedStreamMap=affectedStreamMap, streamNumber=srcStreamNumber)

                lastSrcBranchHash = None
                if srcBranchName is not None:
                    lastSrcBranchHash = self.GetLastCommitHash(branchName=srcBranchName)

                commitHash = None
                if srcBranchName is not None and branchName is not None:
                    # Do a git diff between the two data commits that we will be merging.
                    diff = self.GitDiff(streamData["data_hash"], lastSrcBranchHash)

                    if len(diff) == 0:
                        parents = [ self.GetLastCommitHash(branchName=branchName) ]
                        isAncestor = self.GitMergeBase(refs=[ lastSrcBranchHash, parents[0] ], isAncestor=True)
                        assert isAncestor is not None, "Invariant error! Failed to determine merge base between {c1} and {c2}!".format(c1=lastSrcBranchHash, c2=parents[0])

                        if not isAncestor:
                            parents.append(lastSrcBranchHash) # Make this commit a merge of the last commit on the srcStreamBranch into the branchName.

                        assert None not in parents, "Invariant error! Either the source hash {sh} or the destination hash {dh} was none!".format(sh=parents[1], dh=parents[0])
                        
                        commitHash = self.CommitTransaction(tr=tr, stream=stream, parents=parents, treeHash=treeHash, branchName=branchName, srcStream=srcStream, dstStream=stream)

                        infoMessage = "{trType} {tr}. Merged {src} {srcHash} into {dst} {dstHash}.".format(tr=tr.id, trType=tr.Type, src=srcBranchName, dst=branchName, srcHash=self.ShortHash(lastSrcBranchHash), dstHash=self.ShortHash(commitHash))
                        if self.config.git.sourceStreamFastForward:
                            # This is a manual merge and the srcBranchName should be fastforwarded to this commit since its contents now matches the parent stream.
                            if self.UpdateAndCheckoutRef(ref='refs/heads/{branch}'.format(branch=srcBranchName), commitHash=commitHash, checkout=False) != True:
                                raise Exception("Failed to update source {branch} to {hash} latest commit.".format(branch=srcBranchName, hash=self.ShortHash(commitHash)))
                            infoMessage = "{msg} Fast-forward {src} from {srcHash} to {dst} at {dstHash}.".format(msg=infoMessage, src=srcBranchName, dst=branchName, srcHash=self.ShortHash(lastSrcBranchHash), dstHash=self.ShortHash(commitHash))
                            self.LogBranchState(stream=srcStream, tr=tr, commitHash=commitHash) # Since we are not committing we need to manually store the ref state at this time.
                        logger.info(infoMessage)
                    else:
                        commitHash = self.CommitTransaction(tr=tr, stream=stream, treeHash=treeHash, branchName=branchName, srcStream=None, dstStream=stream, cherryPickSrcHash=lastSrcBranchHash)
                        msg = "{trType} {tr}. Cherry pick {src} {srcHash} into {dst} {dstHash}. Diff {dataHash} to {srcHash} was not empty.".format(tr=tr.id, trType=tr.Type, src=srcBranchName, dst=branchName, dataHash=self.ShortHash(streamData["data_hash"]), srcHash=self.ShortHash(lastSrcBranchHash), dstHash=self.ShortHash(commitHash))
                        logger.info(msg)
                elif branchName is not None:
                    # Cherry pick onto destination and merge into all the children.
                    commitHash = self.CommitTransaction(tr=tr, stream=stream, treeHash=treeHash, branchName=branchName, srcStream=None, dstStream=stream)
                    msgSuffix = ''
                    if srcStreamNumber is None:
                        msgSuffix = "Accurev 'from stream' information missing."
                    else:
                        msgSuffix = "Source stream {name} (id: {number}) is not tracked.".format(name=srcStreamName, number=srcStreamNumber)
                    logger.info("{trType} {tr}. Cherry pick into {dst} {dstHash}. {suffix}".format(trType=tr.Type, tr=tr.id, dst=branchName, dstHash=self.ShortHash(commitHash), suffix=msgSuffix))
                else:
                    logger.info("{trType} {tr}. destination stream {dst} (id: {num}) is not tracked.".format(trType=tr.Type, tr=tr.id, dst=streamName, num=streamNumber))

                # TODO: Fix issue '#51 - Make purges into merges' here
                # ====================================================
                # In the case that this transaction/commit makes the child stream have the same contents as the parent stream (i.e. after a `purge` transaction that deleted everything from the stream)
                # We should either merge this stream into its parent or rebase it onto its parent (neither of which is ideal).
                #   - If we make a merge commit on the parent we are effectively lying, since no transaction actually occurred on the parent stream. Additionally, we probably have to propagate it to all
                #     of our sibling streams that are direct ancestors of our parent and don't have timelocks.
                #   - If we rebase the branch onto the parent we will need to label the commit so that we don't lose that history and are at the same time making the life of anyone who is
                #     trying to figure out what happened in the past more difficult. Accurev shows the purge as a transaction in the stream and so should we.
                #
                # Aside: The more I think about it the more Accurev seems to follow the cherry-picking workflow which might explain why it feels so broken. Trying to infer merges from it is also
                #        quite tricky...
                # ----------------------------------------------------

                # Process all affected streams (which are generally the child streams of this stream).
                allStreamTree = self.BuildStreamTree(streams=streams.streams)
                keepList = list(set([ sn for sn in affectedStreamMap ]))
                if srcStreamNumber is not None and srcStreamNumber in keepList:
                    keepList.remove(srcStreamNumber) # The source stream should never be in the affected streams list.
                    logger.warning("{trType} {tr}. dst stream {dst}, src stream {src}. The src stream was found in the affected child streams list which shouldn't be possible. Removing from affected child streams.".format(trType=tr.Type, tr=tr.id, dst=streamName, src=srcStreamName))
                affectedStreamTree = self.PruneStreamTree(streamTree=allStreamTree, keepList=keepList)
                self.MergeIntoChildren(tr=tr, streamTree=affectedStreamTree, streamMap=streamMap, affectedStreamMap=affectedStreamMap, streams=streams, streamNumber=(None if commitHash is None else streamNumber))

            else:
                raise Exception("Not yet implemented! Unrecognized stream type {Type}. Stream {name}".format(Type=stream.Type, name=stream.name))

    def ReadFileRef(self, ref):
        rv = None
        for i in range(0, AccuRev2Git.commandFailureRetryCount):
            rv = self.gitRepo.raw_cmd([u'git', u'show', ref])
            if rv is None:
                return None # Non-zero return code means that the ref likely doesn't exist.
            elif len(rv) > 0: # The processes sometimes return empty strings via Popen.communicate()... Need to retry.
                return rv
        return rv

    def WriteFileRef(self, ref, text):
        if ref is not None and text is not None and len(text) > 0:
            filePath = None
            with tempfile.NamedTemporaryFile(mode='w+', prefix='ac2git_ref_file_', encoding='utf-8', delete=False) as f:
                filePath = f.name
                f.write(text)
            if filePath is not None:
                cmd = [ u'git', u'hash-object', u'-w', u'{0}'.format(filePath) ]
                objHash = ''
                tryCount = 0
                while objHash is not None and len(objHash) == 0 and tryCount < AccuRev2Git.commandFailureRetryCount:
                    objHash = self.gitRepo.raw_cmd(cmd)
                    objHash = objHash.strip()
                    tryCount += 1
                os.remove(filePath)
                updateRefRetr = None
                if objHash is not None:
                    cmd = [ u'git', u'update-ref', ref, objHash ]
                    updateRefRetr = self.gitRepo.raw_cmd(cmd)
                if objHash is None or updateRefRetr is None:
                    logger.debug("Error! Command {cmd}".format(cmd=' '.join(str(x) for x in cmd)))
                    logger.debug("  Failed with: {err}".format(err=self.gitRepo.lastStderr))
                    logger.error("Failed to record text for ref {r}, aborting!".format(r=ref))
                    raise Exception("Error! Failed to record text for ref {r}, aborting!".format(r=ref))
            else:
                logger.error("Failed to create temporary file for writing text to {r}".format(r=ref))
                raise Exception("Error! Failed to record current state, aborting!")
            return True
        return False

    def GetDepotHighWaterMark(self, depot):
        streamRefs = self.GetAllKnownStreamRefs(depot)
        lowestHwm = None
        for sRef in streamRefs:
            depotNumber, streamNumber, remainder = self.ParseStreamRef(sRef)
            if streamNumber is not None and remainder == "hwm":
                text = self.ReadFileRef(ref=sRef)
                if text is None:
                    logger.error("Failed to read ref {r}!".format(r=sRef))
                hwm = json.loads(text)
                if lowestHwm is None or hwm["high-water-mark"] < lowestHwm:
                    lowestHwm = hwm["high-water-mark"]
        return lowestHwm

    def ProcessTransactions(self):
        depot = self.GetDepot(self.config.accurev.depot)

        if depot is None:
            raise Exception("Failed to get depot {depot}!".format(depot=self.config.accurev.depot))

        # Git refspec for the state ref in which we will store a blob.
        stateRefspec = u'{refsNS}state/depots/{depotNumber}/last'.format(refsNS=AccuRev2Git.gitRefsNamespace, depotNumber=depot.number)

        # Load the streamMap from the current configuration file.
        streamMap = OrderedDict()
        configStreamMap = self.GetStreamMap()
        for configStream in configStreamMap:
            branchName = configStreamMap[configStream]

            logger.info("Getting stream information for stream '{name}' which will be committed to branch '{branch}'.".format(name=configStream, branch=branchName))
            stream = self.GetStreamByName(depot.number, configStream)
            if stream is None:
                raise Exception("Failed to get stream information for {s}".format(s=configStream))
            # Since we will be storing this state in JSON we need to make sure that we don't have
            # numeric indices for dictionaries...
            streamMap[str(stream.streamNumber)] = { "stream": configStream, "branch": branchName }

        # Load the last known state of the conversion repository.
        stateText = self.ReadFileRef(ref=stateRefspec)
        if stateText is not None:
            state = json.loads(stateText)
            # Restore the last known git repository state. We could have been interrupted in the middle of merges or other things so we need to be
            # able to restore all branches.
            if state["branch_list"] is not None and len(state["branch_list"]) > 0:
                # Restore all branches to the last saved state but do the branch that was current at the time last.
                currentBranch = None
                for br in state["branch_list"]:
                    if not br["is_current"]:
                        logger.debug( "Restore branch {branchName} at commit {commit}".format(branchName=br["name"], commit=br["commit"]) )
                        if self.UpdateAndCheckoutRef(ref='refs/heads/{branch}'.format(branch=br["name"]), commitHash=br["commit"], checkout=False) != True:
                            raise Exception("Failed to restore last state for branch {br} at {c}.".format(br=br["name"], c=br["commit"]))
                    else:
                        currentBranch = br
                if currentBranch is not None:
                    logger.debug( "Checkout last processed transaction #{tr} on branch {branchName} at commit {commit}".format(tr=state["last_transaction"], branchName=currentBranch["name"], commit=currentBranch["commit"]) )
                    result = self.gitRepo.raw_cmd([u'git', u'checkout', u'-B', currentBranch["name"], currentBranch["commit"]])
                    if result is None:
                        raise Exception("Failed to restore last state. git checkout -B {br} {c}; failed.".format(br=currentBranch["name"], c=currentBranch["commit"]))

            # Check for branches that exist in the git repository but that we will be creating later.
            streamBranchList = [ state["stream_map"][s]["branch"] for s in state["stream_map"] ] # Get the list of all branches that we will create.
            loadedBranchList = [ b["name"] for b in state["branch_list"] ] # Get the list of all branches that we will create.
            branchList = self.gitRepo.branch_list()
            branchNameSet = set([ b.name for b in branchList ])
            for b in branchList:
                if b.name in streamBranchList and (state["branch_list"] is None or b.name not in loadedBranchList): # state["branch_list"] is a list of the branches that we have already created.
                    logger.warning("Branch {branch} exists in the repo but will need to be created later.".format(branch=b.name))
                    backupNumber = 1
                    while self.gitRepo.raw_cmd(['git', 'checkout', '-b', 'backup/{branch}_{number}'.format(branch=b.name, number=backupNumber)]) is None:
                        # Make a backup of the branch.
                        backupNumber += 1
                    if self.gitRepo.raw_cmd(['git', 'branch', '-D', b.name]) is None: # Delete the branch even if not merged.
                        raise Exception("Failed to delete branch {branch}!".format(branch=b.name))
                    logger.warning("Branch {branch} has been renamed to backup/{branch}_{number}.".format(branch=b.name, number=backupNumber))
            for missingBranch in (set(loadedBranchList) - branchNameSet):
                logger.warning("Branch {branch} is missing from the repo!".format(branch=missingBranch))

            # Check for added/deleted/renamed streams w.r.t. the new config file and last known conversion reporitory state.
            newSet = set([x for x in streamMap])
            oldSet = set([x for x in state["stream_map"]])
            removedSet = oldSet - newSet
            addedSet = newSet - oldSet
            changedSet = newSet & oldSet # intersect

            # Rename each of the branches that have changed names.
            for streamNumberStr in changedSet:
                newBranchName = streamMap[streamNumberStr]["branch"]
                oldBranchName = state["stream_map"][streamNumberStr]["branch"]

                if newBranchName != oldBranchName:
                    msg = "(no-op)"
                    if oldBranchName in branchNameSet:
                        cmd = [ u'git', u'branch', u'-m', oldBranchName, newBranchName ]
                        if self.TryGitCommand(cmd, allowEmptyString=True) is None:
                            raise Exception("Failed to rename branch {old} to {new}.\nErr: {err}".format(old=oldBranchName, new=newBranchName, err=self.gitRepo.lastStderr))
                        msg = "(done)"
                    logger.info("renamed: {streamName} (id: {streamNumber}) -> {branchName} (from {oldBranchName}) {msg}".format(streamNumber=streamNumberStr, streamName=streamMap[streamNumberStr]["stream"], branchName=newBranchName, oldBranchName=oldBranchName, msg=msg))

            # Delete the branches for the streams that were removed from the config file.
            for streamNumberStr in removedSet:
                branchName = state["stream_map"][streamNumberStr]["branch"]
                msg = "(no-op)"
                if branchName in branchNameSet:
                    cmd = [ u'git', u'branch', u'-D', branchName ]
                    if self.TryGitCommand(cmd) is None:
                        raise Exception("Failed to delete branch {br}".format(br=branchName))
                    msg = "(done)"
                logger.info("removed: {streamName} (id: {streamNumber}) -> {branchName}".format(streamNumber=streamNumberStr, streamName=state["stream_map"][streamNumberStr]["stream"], branchName=branchName, msg=msg))

            # Cherry pick all transactions from the creation of the stream until the last processed transaction for each added stream.
            for streamNumberStr in addedSet:
                streamName = streamMap[streamNumberStr]["stream"]
                branchName = streamMap[streamNumberStr]["branch"]

                logger.info("adding: {streamName} (id: {streamNumber}) -> {branchName}".format(streamNumber=streamNumberStr, streamName=streamName, branchName=branchName))
                stream = self.GetStreamByName(depot.number, streamName)
                if self.ProcessStream(stream=stream, branchName=branchName, startTrId=int(self.config.accurev.startTransaction), endTrId=state["last_transaction"], streamMap=streamMap) is not None:
                    logger.warning("Merge information prior to transaction {trId} will not be available for the newly added stream {streamName} (id: {streamNumber}) tracked by branch {branchName}.".format(trId=state["last_transaction"], streamNumber=streamNumberStr, streamName=streamName, branchName=branchName))
                logger.info("added: {streamName} (id: {streamNumber}) -> {branchName}".format(streamNumber=streamNumberStr, streamName=streamName, branchName=branchName))

            # After all of the added/removed/renamed branches are handled we can continue with the new stream map.
            state["stream_map"] = streamMap

        else:
            logger.info("No last state in {ref}, starting new conversion.".format(ref=stateRefspec))

            # Default state
            state = { "depot_number": depot.number,
                      "stream_map": streamMap,
                      "last_transaction": (int(self.config.accurev.startTransaction) - 1),
                      "branch_list": None }

        # Get the list of transactions that we are processing, and build a list of known branch names for maintaining their states between processing stages.
        transactionsMap = {} # is a dictionary with the following format { <key:tr_num>: { <key:stream_num>: { "state_hash": <val:commit_hash>, "data_hash": <val:data_hash> } } }
        for streamNumberStr in state["stream_map"]:
            streamNumber = int(streamNumberStr)

            # Initialize the state that we load every time.
            stateRef, dataRef, hwmRef = self.GetStreamRefs(depot=state["depot_number"], streamNumber=streamNumber)

            # Get the state ref's known transactions list.
            logger.info("Getting transaction to info commit mapping for stream number {s}. Ref: {ref}".format(s=streamNumber, ref=stateRef))
            stateMap = self.GetRefMap(ref=stateRef, mapType="tr2commit")
            if stateMap is None:
                raise Exception("Failed to retrieve the state map for stream {s} (id: {id}).".format(s=state["stream_map"][streamNumberStr]["stream"], id=streamNumber))

            logger.info("Merging transaction to info commit mapping for stream number {s} with previous mappings. Ref: {ref}".format(s=streamNumber, ref=stateRef))
            for tr in reversed(stateMap):
                if tr not in transactionsMap:
                    transactionsMap[tr] = {}
                assert streamNumber not in transactionsMap[tr], "Invariant error! This should be the first time we are adding the stream {s} (id: {id})!".format(s=state["stream_map"][streamNumberStr]["stream"], id=streamNumber)
                transactionsMap[tr][streamNumber] = { "state_hash": stateMap[tr] }
            del stateMap # Make sure we free this, it could get big...

            # Get the data ref's known transactions list.
            logger.info("Getting transaction to data commit mapping for stream number {s}. Ref: {ref}".format(s=streamNumber, ref=stateRef))
            dataMap = None
            dataHashList = self.GetGitLogList(ref=dataRef, gitLogFormat='%H %s %T')
            if dataHashList is None:
                raise Exception("Couldn't get the commit hash list to process from the Accurev data ref {dataRef}.".format(dataRef=dataRef))
            else:
                dataMap = OrderedDict()
                for line in reversed(dataHashList):
                    columns = line.split(' ')
                    trId, commitHash, treeHash = int(columns[2]), columns[0], columns[3]
                    dataMap[trId] = { "data_hash": commitHash, "data_tree_hash": treeHash }

            if dataMap is None:
                raise Exception("Failed to retrieve the data map for stream {s} (id: {id}).".format(s=state["stream_map"][streamNumberStr], id=streamNumber))

            logger.info("Merging transaction to data commit mapping for stream number {s} with previous mappings. Ref: {ref}".format(s=streamNumber, ref=stateRef))
            for tr in reversed(dataMap):
                assert tr in transactionsMap and streamNumber in transactionsMap[tr], "Invariant error! The data ref should contain a subset of the state ref information, not a superset!"
                transactionsMap[tr][streamNumber]["data_hash"] = dataMap[tr]["data_hash"]
                transactionsMap[tr][streamNumber]["data_tree_hash"] = dataMap[tr]["data_tree_hash"]
            del dataMap # Make sure we free this, it could get big...
                
        # Other state variables
        endTransaction = self.GetDepotHighWaterMark(self.config.accurev.depot)
        logger.info("{depot} depot high-water mark is {hwm}.".format(depot=self.config.accurev.depot, hwm=endTransaction))
        try:
            endTransaction = min(int(endTransaction), int(self.config.accurev.endTransaction))
        except:
            pass # keywords highest, now or date time are ignored. We only read the config in case
                 # that the configured end transaction is lower than the lowest high-water-mark we
                 # have for the depot.

        logger.info("Processing transactions for {depot} depot.".format(depot=self.config.accurev.depot))
        knownBranchSet = set([ state["stream_map"][x]["branch"] for x in state["stream_map"] ]) # Get the list of all branches that we will create.
        prevAffectedStreamMap = None
        for tr in sorted(transactionsMap):
            if tr <= state["last_transaction"]:
                prevAffectedStreamMap = transactionsMap[tr]
                del transactionsMap[tr] # ok since sorted returns a sorted list by copy.
                continue
            elif tr > endTransaction:
                break

            # Process the transaction!
            self.ProcessTransaction(streamMap=state["stream_map"], trId=tr, affectedStreamMap=transactionsMap[tr], prevAffectedStreamMap=prevAffectedStreamMap)

            # Store the state of the branches in the repo at this point in time so that we can restore it on next restart.
            state["branch_list"] = []
            for br in self.gitRepo.branch_list():
                if br is None:
                    logger.error("Error: git.py failed to parse a branch name! Please ensure that the git.repo.branch_list() returns a list with no None items. Non-fatal, continuing.")
                    continue
                elif br.name in knownBranchSet:
                    # We only care about the branches that we are processing, i.e. the branches that are in the streamMap.
                    brHash = OrderedDict()
                    brHash["name"] = br.name
                    brHash["commit"] = br.shortHash
                    brHash["is_current"] = br.isCurrent
                    state["branch_list"].append(brHash)

            state["last_transaction"] = tr
            if self.WriteFileRef(ref=stateRefspec, text=json.dumps(state)) != True:
                raise Exception("Failed to write state to {ref}.".format(ref=stateRefspec))

            prevAffectedStreamMap = transactionsMap[tr]
        return True

            
    def InitGitRepo(self, gitRepoPath):
        gitRootDir, gitRepoDir = os.path.split(gitRepoPath)
        if os.path.isdir(gitRootDir):
            if git.isRepo(gitRepoPath):
                # Found an existing repo, just use that.
                logger.info( "Using existing git repository." )
                return True
        
            logger.info( "Creating new git repository" )
            
            # Create an empty first commit so that we can create branches as we please.
            if git.init(path=gitRepoPath) is not None:
                logger.info( "Created a new git repository." )
            else:
                logger.error( "Failed to create a new git repository." )
                sys.exit(1)
                
            return True
        else:
            logger.error("{0} not found.\n".format(gitRootDir))
            
        return False

    # Returns a string representing the name of the stream on which a transaction was performed.
    # If the history (an accurev.obj.History object) is given then it is attempted to retrieve it from the stream list first and
    # should this fail then the history object's transaction's virtual version specs are used.
    # If the transaction (an accurev.obj.Transaction object) is given it is attempted to retrieve the name of the stream from the
    # virtual version spec.
    # The `depot` argument is used both for the accurev.show.streams() command and to control its use. If it is None then the
    # command isn't used at all which could mean a quicker conversion. When specified it indicates that the name of the stream
    # from the time of the transaction should be retrieved. Otherwise the current name of the stream is returned (assumint it was
    # renamed at some point).
    def GetDestinationStreamName(self, history=None, transaction=None, depot=None):
        # depot given as None indicates that accurev.show.streams() command is not to be run.
        if history is not None:
            if depot is None and len(history.streams) == 1:
                return history.streams[0].name
            elif len(history.transactions) > 0:
                rv = self.GetDestinationStreamName(history=None, transaction=history.transactions[0], depot=depot)
                if rv is not None:
                    return rv

        if transaction is not None:
            streamName, streamNumber = transaction.affectedStream()
            if streamNumber is not None and depot is not None:
                try:
                    stream = accurev.show.streams(depot=depot, stream=streamNumber, timeSpec=transaction.id, useCache=self.config.accurev.UseCommandCache()).streams[0] # could be expensive
                    if stream is not None and stream.name is not None:
                        return stream.name
                except:
                    pass
            return streamName
        return None

    # Start
    #   Begins a new AccuRev to Git conversion process discarding the old repository (if any).
    def Start(self, isRestart=False, isSoftRestart=False):
        global maxTransactions

        if not os.path.exists(self.config.git.repoPath):
            logger.error( "git repository directory '{0}' doesn't exist.".format(self.config.git.repoPath) )
            logger.error( "Please create the directory and re-run the script.".format(self.config.git.repoPath) )
            return 1
        
        if isRestart:
            logger.info( "Restarting the conversion operation." )
            logger.info( "Deleting old git repository." )
            git.delete(self.config.git.repoPath)

        # From here on we will operate from the git repository.
        if self.config.accurev.commandCacheFilename is not None:
            self.config.accurev.commandCacheFilename = os.path.abspath(self.config.accurev.commandCacheFilename)
        self.cwd = os.getcwd()
        os.chdir(self.config.git.repoPath)
        
        # This try/catch/finally block is here to ensure that we change directory back to self.cwd in order
        # to allow other scripts to safely call into this method.
        if self.InitGitRepo(self.config.git.repoPath):
            self.gitRepo = git.open(self.config.git.repoPath)
            status = self.gitRepo.status()
            if status is None:
                raise Exception("git state failed. Aborting! err: {err}".format(err=self.gitRepo.lastStderr))
            elif status.initial_commit:
                logger.debug( "New git repository. Initial commit on branch {br}".format(br=status.branch) )
            else:
                logger.debug( "Opened git repository on branch {br}".format(br=status.branch) )
 
            # Configure the remotes
            if self.config.git.remoteMap is not None and len(self.config.git.remoteMap) > 0:
                remoteList = self.gitRepo.remote_list()
                remoteAddList = [x for x in self.config.git.remoteMap.keys()]
                for remote in remoteList:
                    if remote.name in self.config.git.remoteMap:
                        r = self.config.git.remoteMap[remote.name]
                        pushUrl1 = r.url if r.pushUrl is None else r.pushUrl
                        pushUrl2 = remote.url if remote.pushUrl is None else remote.pushUrl
                        if r.url != remote.url or pushUrl1 != pushUrl2:
                            raise Exception("Configured remote {r}'s urls don't match.\nExpected:\n{r1}\nGot:\n{r2}".format(r=remote.name, r1=r, r2=remote))
                        remoteAddList.remove(remote.name)
                    else:
                        logger.debug( "Unspecified remote {remote} ({url}) found. Ignoring...".format(remote=remote.name, url=remote.url) )
                for remote in remoteAddList:
                    r = self.config.git.remoteMap[remote]
                    if self.gitRepo.remote_add(name=r.name, url=r.url) is None:
                        raise Exception("Failed to add remote {remote} ({url})!".format(remote=r.name, url=r.url))
                    logger.info( "Added remote: {remote} ({url}).".format(remote=r.name, url=r.url) )
                    if r.pushUrl is not None and r.url != r.pushUrl:
                        if self.gitRepo.remote_set_url(name=r.name, url=r.pushUrl, isPushUrl=True) is None:
                            raise Exception("Failed to set push url {url} for {remote}!".format(url=r.pushUrl, remote=r.name))
                        logger.info( "Added push url: {remote} ({url}).".format(remote=r.name, url=r.pushUrl) )

            doLogout = False
            if self.config.method != 'skip':
                acInfo = accurev.info()
                isLoggedIn = False
                if self.config.accurev.username is None:
                    # When a username isn't specified we will use any logged in user for the conversion.
                    isLoggedIn = accurev.ext.is_loggedin(infoObj=acInfo)
                else:
                    # When a username is specified that specific user must be logged in.
                    isLoggedIn = (acInfo.principal == self.config.accurev.username)

                if not isLoggedIn:
                    # Login the requested user
                    if accurev.ext.is_loggedin(infoObj=acInfo):
                        # Different username, logout the other user first.
                        logoutSuccess = accurev.logout()
                        logger.info("Accurev logout for '{0}' {1}".format(acInfo.principal, 'succeeded' if logoutSuccess else 'failed'))

                    loginResult = accurev.login(self.config.accurev.username, self.config.accurev.password)
                    if loginResult:
                        logger.info("Accurev login for '{0}' succeeded.".format(self.config.accurev.username))
                    else:
                        logger.error("AccuRev login for '{0}' failed.\n".format(self.config.accurev.username))
                        logger.error("AccuRev message:\n{0}".format(loginResult.errorMessage))
                        return 1

                    doLogout = True
                else:
                    logger.info("Accurev user '{0}', already logged in.".format(acInfo.principal))
                
                # If this script is being run on a replica then ensure that it is up-to-date before processing the streams.
                accurev.replica.sync()

            self.gitRepo.raw_cmd([u'git', u'config', u'--local', u'gc.auto', u'0'])

            if self.config.method in [ "deep-hist", "diff", "pop" ]:
                logger.info("Retrieveing stream information from Accurev into hidden refs.")
                self.RetrieveStreams()
            elif self.config.method in [ "skip" ]:
                logger.info("Skipping retrieval of stream information from Accurev.")
            else:
                raise Exception("Unrecognized method '{method}'".format(method=self.config.method))

            if not isRestart and isSoftRestart:
                logger.info( "Restarting the processing operation." )
                if self.gitRepo.raw_cmd([ u'git', u'checkout', u'--orphan', u'__ac2git_temp__' ]) is None:
                    raise Exception("Failed to checkout empty branch.")
                if self.gitRepo.raw_cmd([ u'git', u'read-tree', u'--empty' ]) is None:
                    raise Exception("Failed to clear the index.")
                if self.gitRepo.raw_cmd([ u'git', u'clean', u'-dfx' ]) is None:
                    raise Exception("Failed to remove untracked files.")
                refOutput = self.gitRepo.raw_cmd([ u'git', u'show-ref' ])
                if refOutput is None:
                    raise Exception("Failed to retrieve refs.")

                # Delete all the branches and refs that we won't need any more.
                streamMap = self.GetStreamMap()
                branchList = [streamMap[x] for x in streamMap]
                deleteCmd = [ u'git', u'update-ref', u'-d' ]
                for refEntry in refOutput.strip().split('\n'):
                    refEntry = refEntry.strip()
                    ref = refEntry.strip().split()[1]
                    delete = False
                    if ref.startswith('refs/heads/'):
                        # Find out if it is a tracked branch that we should delete.
                        branchName = ref[len('refs/heads/'):]
                        if branchName in branchList:
                            delete = True
                    elif ref.startswith('refs/ac2git/state/') or ref in [ 'refs/notes/ac2git', 'refs/notes/accurev' ]:
                        delete = True

                    if delete:
                        if self.gitRepo.raw_cmd( deleteCmd + [ ref ] ) is None:
                            raise Exception("Failed to delete ref {r}.".format(ref))
                        logger.debug("Deleting ref {r}".format(r=ref))
                    else:
                        #logger.debug("Skipping ref {r}".format(r=ref))
                        pass
                # Checkout the master branch or an empty master branch if it doesn't exist.
                if self.gitRepo.raw_cmd([ u'git', u'checkout', u'--orphan', u'master' ]) is None:
                    if self.gitRepo.raw_cmd([ u'git', u'checkout', u'master' ]) is None:
                        raise Exception("Failed to checkout master branch.")

            if self.config.mergeStrategy in [ "normal" ]:
                logger.info("Processing transactions from hidden refs. Merge strategy '{strategy}'.".format(strategy=self.config.mergeStrategy))
                self.ProcessTransactions()
            elif self.config.mergeStrategy in [ "orphanage" ]:
                logger.info("Processing streams from hidden refs. Merge strategy '{strategy}'.".format(strategy=self.config.mergeStrategy))
                self.ProcessStreams(orderByStreamNumber=False)
            elif self.config.mergeStrategy in [ "skip", None ]:
                logger.info("Skipping processing of Accurev data. No git branches will be generated/updated. Merge strategy '{strategy}'.".format(strategy=self.config.mergeStrategy))
                pass # Skip the merge step.
            else:
                raise Exception("Unrecognized merge strategy '{strategy}'".format(strategy=self.config.mergeStrategy))

            self.gitRepo.raw_cmd([u'git', u'config', u'--local', u'--unset-all', u'gc.auto'])
              
            if doLogout:
                if accurev.logout():
                    logger.info( "Accurev logout successful." )
                else:
                    logger.error("Accurev logout failed.\n")
                    return 1
        else:
            logger.error( "Could not create git repository." )

        # Restore the working directory.
        os.chdir(self.cwd)
        
        return 0
            
# ################################################################################################ #
# Script Functions                                                                                 #
# ################################################################################################ #
def DumpExampleConfigFile(outputFilename):
    with codecs.open(outputFilename, 'w') as file:
        file.write("""<accurev2git>
    <!-- AccuRev details:
            username:             The username that will be used to log into AccuRev and retrieve and populate the history. This is optional and if it isn't provided you will need to login before running this script.
            password:             The password for the given username. Note that you can pass this in as an argument which is safer and preferred! This too is optional. You can login before running this script and it will work.
            depot:                The depot in which the stream/s we are converting are located
            start-transaction:    The conversion will start at this transaction. If interrupted the next time it starts it will continue from where it stopped.
            end-transaction:      Stop at this transaction. This can be the keword "now" if you want it to convert the repo up to the latest transaction.
            command-cache-filename: The filename which will be given to the accurev.py script to use as a local command result cache for the accurev hist, accurev diff and accurev show streams commands.
    -->
    <accurev 
        username="joe_bloggs" 
        password="joanna" 
        depot="Trunk" 
        start-transaction="1" 
        end-transaction="now" 
        command-cache-filename="command_cache.sqlite3" >
        <!-- The stream-list is optional. If not given all streams are processed
                exclude-types:   A comma separated list of stream types that are to be excluded from being automatically added. Doesn't exclude streams that were explicitly specified.
                                 The stream types have to match the stream types returned by Accurev in its command line client's XML output. Example list: "normal, workspace, snapshot".
         -->
        <!-- The branch-name attribute is also optional for each stream element. If provided it specifies the git branch name to which the stream will be mapped. -->
        <stream-list exclude-types="workspace">
            <stream branch-name="some_branch">some_stream</stream>
            <stream>some_other_stream</stream>
        </stream-list>
    </accurev>

    <!-- Git details:
            repo-path:     The system path where you want the git repo to be populated. Note: this folder should already exist. 
            message-style: [ "normal", "clean", "notes" ] - When set to "normal" accurev transaction information is included
                           at the end (in the footer) of each commit message. When set to "clean" the transaction comment is the commit message without any
                           additional information. When set to "notes" a note is added to each commit in the "accurev" namespace (to show them use `git log -notes=accurev`),
                           with the same accurev information that would have been shown in the commit message footer in the "normal" mode.
            message-key:   [ "footer", "header" ] - can be either "footer", "header" or omitted and adds a simple key with the destination-stream/transaction-number format either
                           before (header) or after (footer) the commit message which can be used to quickly go back to accurev and figure out where this transaction came from.
            author-is-committer: [ "true", "false" ] - controls if the configured git user or the transaction user is used as the committer for the conversion. Setting
                                 it to "false" makes the conversion use the configured git user to perform the commits while the author information will be taken from the Accurev transaction.
                                 Setting it to "true" will make the conversion set the Accurev transaction user as both the author and the committer.
            empty-child-stream-action: [ "merge", "cherry-pick" ] - controls whether the child streams that are affected by a transaction to its parent stream make a "merge" commit (merging the
                                       parent branch into the child branch) or a "cherry-pick" commit that does not contain that linkage. The "merge" commit is only made if the child stream's
                                       contents at this transaction matches the parent streams contents exactly (git diff between the two branches at this transaction would be the same).
            source-stream-fast-forward: [ "true", "false" ] - when a promote is made and both the source and destination streams are known a merge commit is made on the destination stream. If
                                        this option is set to "true" then the source stream's branch is moved up to the destination branch after the commit is made, otherwise it is left where
                                        it was before.
            new-basis-is-first-parent: [ "true", "false" ] - If set to true, for a chstream transaction, the new basis transaction will be made the corresponding commit's first parent, while
                                                             the previous transaction made in the stream will be the second parent. If set to false the order of the two parents is reversed.
    -->
    <git 
        repo-path="/put/the/git/repo/here" 
        message-style="notes" 
        message-key="footer" 
        author-is-committer="true" 
        empty-child-stream-action="merge" 
        source-stream-fast-forward="false"
        new-basis-is-first-parent="true" > 
        <!-- Optional: You can add remote elements to specify the remotes to which the converted branches will be pushed. The push-url attribute is optional. -->
        <remote name="origin" url="https://github.com/orao/ac2git.git" push-url="https://github.com/orao/ac2git.git" /> 
        <remote name="backup" url="https://github.com/orao/ac2git.git" />
    </git>
    <method>deep-hist</method> <!-- The method specifies what approach is taken to retrieve information from Accurev. Allowed values are 'deep-hist', 'diff', 'pop' and 'skip'.
                                     - deep-hist: Works by using the accurev.ext.deep_hist() function to return a list of transactions that could have affected the stream.
                                                  It then performs a diff between the transactions and only populates the files that have changed like the 'diff' method.
                                                  It is the quickest method but is only as reliable as the information that accurev.ext.deep_hist() provides.
                                     - diff: This method's first commit performs a full `accurev pop` command on either the streams `mkstream` transaction or the start
                                             transaction (whichever is highest). Subsequently it increments the transaction number by one and performs an
                                             `accurev diff -a -i -v <stream> -V <stream>` to find all changed files. If not files have changed it takes the next transaction
                                             and performs the diff again. Otherwise, any files returned by the diff are deleted and an `accurev pop -R` performed which only
                                             downloads the changed files. This is slower than the 'deep-hist' method but faster than the 'pop' method by a large margin.
                                             It's reliability is directly dependent on the reliability of the `accurev diff` command.
                                     - pop: This is the naive method which doesn't care about changes and always performs a full deletion of the whole tree and a complete
                                            `accurev pop` command. It is a lot slower than the other methods for streams with a lot of files but should work even with older
                                            accurev releases. This is the method originally implemented by Ryan LaNeve in his https://github.com/rlaneve/accurev2git repo.
                                     - skip: This will skip the querying of the Accurev server for information about the streams. It makes sense in an already converted repo
                                             for which you only want to reprocess the already retrieved information without getting anything new.
                               -->
    <merge-strategy>normal</merge-strategy> <!-- The merge-strategy specified how the information downloaded from the streams in accurev is processed to form git branches.
                                                 It can be one of the following options ["skip", "normal", "orphanage"]:
                                             'skip' - Skips the processing step. The git repo won't have any visible git branches but will have hidden internal state which
                                                      tracks the accurev depot. When a merge strategy is next set to something other than 'skip' already retrieved information
                                                      won't be redownloaded from accurev and will be processed without executing any accurev commands (won't query the accurev server).
                                              'normal' - Performs merges using a straightforward but imprefect algorithm. The algorithm has the preferred balance between performance
                                                         the resulting merges in git.
                                              'orphanage' - Performs no merges but adds orphaned git branches which track the accurev streams. This is the old conversion method and is
                                                            here for legacy reasons. If streams are added later the resulting git repository commit hashes do not change but it will be
                                                            difficult to merge the branches in git at a later stage.
                               -->
    <logfile>accurev2git.log</logfile>
    <!-- The user maps are used to convert users from AccuRev into git. Please spend the time to fill them in properly. -->
    <usermaps filename="usermaps.config.xml">
        <!-- The filename attribute is optional and if included the provided file is opened and the usermaps from that file are used to complement
             the usermaps provided below (only accurev users that haven't been specified below are loaded from the file). The file can have one or
             more usermaps elements like this one, each of which can point to another file of its own. -->

         <!-- The timezone attribute is optional. All times are retrieved in UTC from AccuRev and will converted to the local timezone by default.
             If you want to override this behavior then set the timezone to either an Olson timezone string (e.g. Europe/Belgrade) or a git style
             timezone string (e.g. +0100, sign and 4 digits required). -->
        <map-user><accurev username="joe_bloggs" /><git name="Joe Bloggs" email="joe@bloggs.com" timezone="Europe/Belgrade" /></map-user>
        <map-user><accurev username="joanna_bloggs" /><git name="Joanna Bloggs" email="joanna@bloggs.com" timezone="+0500" /></map-user>
        <map-user><accurev username="joey_bloggs" /><git name="Joey Bloggs" email="joey@bloggs.com" /></map-user>
    </usermaps>
</accurev2git>
        """)
        return 0
    return 1

def AutoConfigFile(filename, args, preserveConfig=False):
    if os.path.exists(filename):
        # Backup the file
        backupNumber = 1
        backupFilename = "{0}.{1}".format(filename, backupNumber)
        while os.path.exists(backupFilename):
            backupNumber += 1
            backupFilename = "{0}.{1}".format(filename, backupNumber)

        shutil.copy2(filename, backupFilename)

    config = Config.fromfile(filename=args.configFilename)
    
    if config is None:
        config = Config(accurev=Config.AccuRev(), git=Config.Git(), usermaps=[], logFilename=None)
    elif not preserveConfig:
        # preserve only the accurev username and passowrd
        arUsername = config.accurev.username
        arPassword = config.accurev.password
        
        # reset config
        config = Config(accurev=Config.AccuRev(), git=Config.Git(repoPath=None), usermaps=[], logFilename=None)

        config.accurev.username = arUsername
        config.accurev.password = arPassword


    SetConfigFromArgs(config, args)
    if config.accurev.username is None:
        if config.accurev.username is None:
            logger.error("No accurev username provided for auto-configuration.")
        return 1
    else:
        info = accurev.info()
        if info.principal != config.accurev.username:
            if config.accurev.password is None:
                logger.error("No accurev password provided for auto-configuration. You can either provide one on the command line, in the config file or just login to accurev before running the script.")
                return 1
            if not accurev.login(config.accurev.username, config.accurev.password):
                logger.error("accurev login for '{0}' failed.".format(config.accurev.username))
                return 1
        elif config.accurev.password is None:
            config.accurev.password = ''

    if config.accurev.depot is None:
        depots = accurev.show.depots()
        if depots is not None and depots.depots is not None and len(depots.depots) > 0:
            config.accurev.depot = depots.depots[0].name
            logger.info("No depot specified. Selecting first depot available: {0}.".format(config.accurev.depot))
        else:
            logger.error("Failed to find an accurev depot. You can specify one on the command line to resolve the error.")
            return 1

    if config.git.repoPath is None:
        config.git.repoPath = './{0}'.format(config.accurev.depot)

    if config.logFilename is None:
        config.logFilename = 'ac2git.log'

    with codecs.open(filename, 'w') as file:
        file.write("""<accurev2git>
    <!-- AccuRev details:
            username:             The username that will be used to log into AccuRev and retrieve and populate the history
            password:             The password for the given username. Note that you can pass this in as an argument which is safer and preferred!
            depot:                The depot in which the stream/s we are converting are located
            start-transaction:    The conversion will start at this transaction. If interrupted the next time it starts it will continue from where it stopped.
            end-transaction:      Stop at this transaction. This can be the keword "now" if you want it to convert the repo up to the latest transaction.
            command-cache-filename: The filename which will be given to the accurev.py script to use as a local command result cache for the accurev hist, accurev diff and accurev show streams commands.
    -->
    <accurev 
        username="{accurev_username}" 
        password="{accurev_password}" 
        depot="{accurev_depot}" 
        start-transaction="{start_transaction}" 
        end-transaction="{end_transaction}" 
        command-cache-filename="command_cache.sqlite3" >
        <!-- The stream-list is optional. If not given all streams are processed -->
        <!-- The branch-name attribute is also optional for each stream element. If provided it specifies the git branch name to which the stream will be mapped. -->
        <stream-list{exclude_types}>""".format(accurev_username=config.accurev.username,
                                               accurev_password=config.accurev.password,
                                               accurev_depot=config.accurev.depot,
                                               start_transaction=1, end_transaction="now",
                                               exclude_types="" if config.excludeStreamTypes is None else " {0}".format(", ".join(config.excludeStreamTypes))))

        if preserveConfig:
            for stream in config.accurev.streamMap:
                file.write("""
            <stream branch-name="{branch_name}">{stream_name}</stream>""".format(stream_name=stream, branch_name=config.accurev.streamMap[stream]))

        streams = accurev.show.streams(depot=config.accurev.depot, useCache=self.config.accurev.UseCommandCache())
        if streams is not None and streams.streams is not None:
            for stream in streams.streams:
                if not (preserveConfig and stream in config.accurev.streamMap):
                    file.write("""
            <stream branch-name="accurev/{stream_name}">{stream_name}</stream>""".format(stream_name=stream.name))
                    # TODO: Add depot and start/end transaction overrides for each stream...

        file.write("""
        </stream-list>
    </accurev>

    <!-- Git details:
            repo-path:     The system path where you want the git repo to be populated. Note: this folder should already exist. 
            message-style: [ "normal", "clean", "notes" ] - When set to "normal" accurev transaction information is included
                           at the end (in the footer) of each commit message. When set to "clean" the transaction comment is the commit message without any
                           additional information. When set to "notes" a note is added to each commit in the "accurev" namespace (to show them use `git log -notes=accurev`),
                           with the same accurev information that would have been shown in the commit message footer in the "normal" mode.
            message-key:   [ "footer", "header" ] - can be either "footer", "header" or omitted and adds a simple key with the destination-stream/transaction-number format either
                           before (header) or after (footer) the commit message which can be used to quickly go back to accurev and figure out where this transaction came from.
            author-is-committer: [ "true", "false" ] - controls if the configured git user or the transaction user is used as the committer for the conversion. Setting
                                 it to "false" makes the conversion use the configured git user to perform the commits while the author information will be taken from the Accurev transaction.
                                 Setting it to "true" will make the conversion set the Accurev transaction user as both the author and the committer.
            empty-child-stream-action: [ "merge", "cherry-pick" ] - controls whether the child streams that are affected by a transaction to its parent stream make a "merge" commit (merging the
                                       parent branch into the child branch) or a "cherry-pick" commit that does not contain that linkage. The "merge" commit is only made if the child stream's
                                       contents at this transaction matches the parent streams contents exactly (git diff between the two branches at this transaction would be the same).
            source-stream-fast-forward: [ "true", "false" ] - when a promote is made and both the source and destination streams are known a merge commit is made on the destination stream. If
                                        this option is set to "true" then the source stream's branch is moved up to the destination branch after the commit is made, otherwise it is left where
                                        it was before.
            new-basis-is-first-parent: [ "true", "false" ] - If set to true, for a chstream transaction, the new basis transaction will be made the corresponding commit's first parent, while
                                                             the previous transaction made in the stream will be the second parent. If set to false the order of the two parents is reversed.
    -->
    <git 
        repo-path="{git_repo_path}" 
        message-style="{message_style}" 
        message-key="{message_key}"  
        author-is-committer="{author_is_committer}"
        empty-child-stream-action="{empty_child_stream_action}" 
        source-stream-fast-forward="{source_stream_fast_forward}"
        new-basis-is-first-parent="{new_basis_is_first_parent}" >""".format(git_repo_path=config.git.repoPath,
                                                                            message_style=config.git.messageStyle if config.git.messageStyle is not None else 'notes',
                                                                            message_key=config.git.messageKey if config.git.messageKey is not None else 'footer',
                                                                            author_is_committer="true" if config.git.authorIsCommitter else "false",
                                                                            empty_child_stream_action=config.git.emptyChildStreamAction,
                                                                            source_stream_fast_forward="true" if config.git.sourceStreamFastForward else "false",
                                                                            new_basis_is_first_parent="true" if config.git.newBasisIsFirstParent else "false"))
        if config.git.remoteMap is not None:
            for remoteName in remoteMap:
                remote = remoteMap[remoteName]
                file.write("""        <remote name="{name}" url="{url}"{push_url_string} />""".format(name=remote.name, url=name.url, push_url_string='' if name.pushUrl is None else ' push-url="{url}"'.format(url=name.pushUrl)))
        
        file.write("""    </git>
    <method>{method}</method>
    <merge-strategy>{merge_strategy}</merge-strategy>
    <logfile>{log_filename}<logfile>
    <!-- The user maps are used to convert users from AccuRev into git. Please spend the time to fill them in properly. -->""".format(method=config.method, merge_strategy=config.mergeStrategy, log_filename=config.logFilename))
        file.write("""
    <usermaps filename="usermaps.config.xml">
        <!-- The filename attribute is optional and if included the provided file is opened and the usermaps from that file are used to complement
             the usermaps provided below (only accurev users that haven't been specified below are loaded from the file). The file can have one or
             more usermaps elements like this one, each of which can point to another file of its own. -->

        <!-- The timezone attribute is optional. All times are retrieved in UTC from AccuRev and will converted to the local timezone by default.
             If you want to override this behavior then set the timezone to either an Olson timezone string (e.g. Europe/Belgrade) or a git style
             timezone string (e.g. +0100, sign and 4 digits required). -->
        <!-- e.g.
        <map-user><accurev username="joe_bloggs" /><git name="Joe Bloggs" email="joe@bloggs.com" timezone="Europe/Belgrade" /></map-user>
        <map-user><accurev username="joanna_bloggs" /><git name="Joanna Bloggs" email="joanna@bloggs.com" timezone="+0500" /></map-user>
        <map-user><accurev username="joey_bloggs" /><git name="Joey Bloggs" email="joey@bloggs.com" /></map-user>
        -->""")

        if preserveConfig:
            for usermap in config.usermaps:
                file.write("""
        <map-user><accurev username="{accurev_username}" /><git name="{git_name}" email="{git_email}"{timezone_tag} /></map-user>""".format(accurev_username=usermap.accurevUsername, git_name=usermap.gitName, git_email=usermap.gitEmail, timezone_tag="" if usermap.timezone is None else ' timezone="{0}"'.format(usermap.timezone)))


        users = accurev.show.users()
        if users is not None and users.users is not None:
            for user in users.users:
                if not (preserveConfig and user.name in [x.accurevUsername for x in config.usermaps]):
                    file.write("""
        <map-user><accurev username="{accurev_username}" /><git name="{accurev_username}" email="" /></map-user>""".format(accurev_username=user.name))

        file.write("""
    </usermaps>
</accurev2git>
        """)
        return 0
    return 1

def ToUnixPath(path):
    rv = SplitPath(path)
    if rv is not None:
        if rv[0] == '/':
            rv = '/' + '/'.join(rv[1:])
        else:
            rv = '/'.join(rv)
    return rv

def SplitPath(path):
    rv = None
    if path is not None:
        path = str(path)
        rv = []
        drive, path = os.path.splitdrive(path)
        head, tail = os.path.split(path)
        while len(head) > 0 and head != '/' and head != '\\': # For an absolute path the starting slash isn't removed from head.
            rv.append(tail)
            head, tail = os.path.split(head)
        if len(tail) > 0:
            rv.append(tail)
        if len(head) > 0: # For absolute paths.
            rv.append(head)
        if len(drive) > 0:
            rv.append(drive)
        rv.reverse()
    return rv

def TryGetAccurevUserlist(username, password):
    try:
        info = accurev.info()

        isLoggedIn = False
        if username is not None and info.principal != username:
            if password is not None:
                isLoggedIn = accurev.login(username, password)
        else:
            isLoggedIn = accurev.ext.is_loggedin()

        userList = None
        if isLoggedIn:
            users = accurev.show.users()
            if users is not None:
                userList = []
                for user in users.users:
                    userList.append(user.name)

        return userList
    except:
        return None

def GetMissingUsers(config):
    # Try and validate accurev usernames
    userList = TryGetAccurevUserlist(config.accurev.username, config.accurev.password)
    missingList = None

    if config is not None and config.usermaps is not None:
        missingList = []
        if userList is not None and len(userList) > 0:
            for user in userList:
                found = False
                for usermap in config.usermaps:
                    if user == usermap.accurevUsername:
                        found = True
                        break
                if not found:
                    missingList.append(user)

    return missingList

def PrintMissingUsers(config):
    missingUsers = GetMissingUsers(config)
    if missingUsers is not None:
        if len(missingUsers) > 0:
            missingUsers.sort()
            logger.info("Unmapped accurev users:")
            for user in missingUsers:
                logger.info("    {0}".format(user))
            return True
    return False

def SetConfigFromArgs(config, args):
    if args.accurevUsername is not None:
        config.accurev.username = args.accurevUsername
    if args.accurevPassword is not None:
        config.accurev.password = args.accurevPassword
    if args.accurevDepot is not None:
        config.accurev.depot    = args.accurevDepot
    if args.gitRepoPath is not None:
        config.git.repoPath     = args.gitRepoPath
    if args.emptyChildStreamAction is not None:
        config.git.emptyChildStreamAction = args.emptyChildStreamAction
    if args.sourceStreamFastForward is not None:
        config.git.sourceStreamFastForward = (args.sourceStreamFastForward == "true")
    if args.conversionMethod is not None:
        config.method = args.conversionMethod
    if args.mergeStrategy is not None:
        config.mergeStrategy = args.mergeStrategy
    if args.logFile is not None:
        config.logFilename      = args.logFile

def ValidateConfig(config):
    # Validate the program args and configuration up to this point.
    isValid = True
    if config.accurev.depot is None:
        logger.error("No AccuRev depot specified.\n")
        isValid = False
    if config.git.repoPath is None:
        logger.error("No Git repository specified.\n")
        isValid = False

    return isValid

def InitializeLogging(filename, level):
    global logger
    if logger is None:
        logger = logging.getLogger('ac2git')
        logger.setLevel(level)

        consoleHandler = logging.StreamHandler()
        consoleHandler.setLevel(level)

        consoleFormatter = logging.Formatter('%(message)s')
        consoleHandler.setFormatter(consoleFormatter)

        logger.addHandler(consoleHandler)

        if filename is not None:
            fileHandler = logging.FileHandler(filename=filename)
            fileHandler.setLevel(level)

            fileFormatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            fileHandler.setFormatter(fileFormatter)

            logger.addHandler(fileHandler)

        return True
    return False

def PrintConfigSummary(config, filename):
    if config is not None:
        logger.info('Config info:')
        logger.info('  now: {0}'.format(datetime.now()))
        logger.info('  filename: {0}'.format(filename))
        logger.info('  git:')
        logger.info('    repo path: {0}'.format(config.git.repoPath))
        logger.info('    message style: {0}'.format(config.git.messageStyle))
        logger.info('    message key: {0}'.format(config.git.messageKey))
        logger.info('    author is committer: {0}'.format(config.git.authorIsCommitter))
        logger.info('    empty child stream action: {0}'.format(config.git.emptyChildStreamAction))
        logger.info('    source stream fast forward: {0}'.format(config.git.sourceStreamFastForward))
        logger.info('    new basis is first parent: {0}'.format(config.git.newBasisIsFirstParent))
        if config.git.remoteMap is not None:
            for remoteName in config.git.remoteMap:
                remote = config.git.remoteMap[remoteName]
                logger.info('    remote: {name} {url}{push_url}'.format(name=remote.name, url=remote.url, push_url = '' if remote.pushUrl is None or remote.url == remote.pushUrl else ' (push:{push_url})'.format(push_url=remote.pushUrl)))
                
        logger.info('  accurev:')
        logger.info('    depot: {0}'.format(config.accurev.depot))
        if config.accurev.streamMap is not None:
            logger.info('    stream list:')
            for stream in config.accurev.streamMap:
                logger.info('      - {0} -> {1}'.format(stream, config.accurev.streamMap[stream]))
        else:
            logger.info('    stream list: all included')
        logger.info('    start tran.: #{0}'.format(config.accurev.startTransaction))
        logger.info('    end tran.:   #{0}'.format(config.accurev.endTransaction))
        logger.info('    username: {0}'.format(config.accurev.username))
        logger.info('    command cache: {0}'.format(config.accurev.commandCacheFilename))
        logger.info('    ignored transaction types (hard-coded): {0}'.format(", ".join(ignored_transaction_types)))
        if config.accurev.excludeStreamTypes is not None:
            logger.info('    excluded stream types: {0}'.format(", ".join(config.accurev.excludeStreamTypes)))
        logger.info('  method: {0}'.format(config.method))
        logger.info('  merge strategy: {0}'.format(config.mergeStrategy))
        logger.info('  usermaps: {0}'.format(len(config.usermaps)))
        logger.info('  log file: {0}'.format(config.logFilename))
        logger.info('  verbose:  {0}'.format( (logger.getEffectiveLevel() == logging.DEBUG) ))

def PrintRunningTime(referenceTime):
    outMessage = ''
    # Custom formatting of the timestamp
    m, s = divmod((datetime.now() - referenceTime).total_seconds(), 60)
    h, m = divmod(m, 60)
    d, h = divmod(h, 24)
    
    if d > 0:
        outMessage += "{d: >2d}d, ".format(d=int(d))
    
    outMessage += "{h: >2d}:{m:0>2d}:{s:0>5.2f}".format(h=int(h), m=int(m), s=s)

    logger.info("Running time was {timeStr}".format(timeStr=outMessage))

# ################################################################################################ #
# Script Main                                                                                      #
# ################################################################################################ #
def AccuRev2GitMain(argv):
    global state
    
    configFilename = Config.FilenameFromScriptName(argv[0])
    defaultExampleConfigFilename = '{0}.example.xml'.format(configFilename)
    
    # Set-up and parse the command line arguments. Examples from https://docs.python.org/dev/library/argparse.html
    parser = argparse.ArgumentParser(description="Conversion tool for migrating AccuRev repositories into Git. Configuration of the script is done with a configuration file whose filename is `{0}` by default. The filename can be overridden by providing the `-c` option described below. Command line arguments, if given, override the equivalent options in the configuration file.".format(configFilename))
    parser.add_argument('-c', '--config', dest='configFilename', default=configFilename, metavar='<config-filename>', help="The XML configuration file for this script. This file is required for the script to operate. By default this filename is set to be `{0}`.".format(configFilename))
    parser.add_argument('-u', '--accurev-username',  dest='accurevUsername', metavar='<accurev-username>',  help="The username which will be used to retrieve and populate the history from AccuRev.")
    parser.add_argument('-p', '--accurev-password',  dest='accurevPassword', metavar='<accurev-password>',  help="The password for the provided accurev username.")
    parser.add_argument('-d', '--accurev-depot', dest='accurevDepot',        metavar='<accurev-depot>',     help="The AccuRev depot in which the streams that are being converted are located. This script currently assumes only one depot is being converted at a time.")
    parser.add_argument('-g', '--git-repo-path', dest='gitRepoPath',         metavar='<git-repo-path>',     help="The system path to an existing folder where the git repository will be created.")
    parser.add_argument('-M', '--method', dest='conversionMethod', choices=['skip', 'pop', 'diff', 'deep-hist'], metavar='<conversion-method>', help="Specifies the method which is used to perform the conversion. Can be either 'pop', 'diff' or 'deep-hist'. 'pop' specifies that every transaction is populated in full. 'diff' specifies that only the differences are populated but transactions are iterated one at a time. 'deep-hist' specifies that only the differences are populated and that only transactions that could have affected this stream are iterated.")
    parser.add_argument('-S', '--merge-strategy', dest='mergeStrategy', choices=['skip', 'normal', 'orphanage'], metavar='<merge-strategy>', help="Sets the merge strategy which dictates how the git repository branches are generated. Depending on the value chosen the branches can be orphan branches ('orphanage' strategy) or have merges where promotes have occurred with the 'normal' strategy. The 'skip' strategy forces the script to skip making the git branches and will cause it to only do the retrieving of information from accurev for use with some strategy at a later date.")
    parser.add_argument('-E', '--empty-child-stream-action', dest='emptyChildStreamAction', choices=['merge', 'cherry-pick'], metavar='<empty-child-stream-action>', help="When a promote to a parent stream affects the child stream and the result of the two commits on the two branches in git results in a git diff operation returning empty then it could be said that this was in-fact a merge (of sorts). This option controlls whether such situations are treated as cherry-picks or merges in git.")
    parser.add_argument('-K', '--source-stream-fast-forward', dest='sourceStreamFastForward', choices=['true', 'false'], metavar='<source-stream-fast-forward>', help="When both the source and destination streams are known this flag controlls whether the source branch is moved to the resulting merge commit (the destination branch is always updated/moved to this commit). This has an effect of making the history look like the letter K where the promotes come in and then branch from the merge commit instead of the previous commit which occured on the branch.")
    parser.add_argument('-R', '--restart',    dest='restart', action='store_const', const=True, help="Discard any existing conversion and start over.")
    parser.add_argument('-r', '--soft-restart',    dest='softRestart', action='store_const', const=True, help="Discard any existing processed branches and start the processing from the downloaded accurev data anew.")
    parser.add_argument('-v', '--verbose',    dest='debug',   action='store_const', const=True, help="Print the script debug information. Makes the script more verbose.")
    parser.add_argument('-L', '--log-file',   dest='logFile', metavar='<log-filename>',         help="Sets the filename to which all console output will be logged (console output is still printed).")
    parser.add_argument('-q', '--no-log-file', dest='disableLogFile',  action='store_const', const=True, help="Do not log info to the log file. Alternatively achieved by not specifying a log file filename in the configuration file.")
    parser.add_argument('-l', '--reset-log-file', dest='resetLogFile', action='store_const', const=True, help="Instead of appending new log info to the file truncate it instead and start over.")
    parser.add_argument('--example-config', nargs='?', dest='exampleConfigFilename', const=defaultExampleConfigFilename, default=None, metavar='<example-config-filename>', help="Generates an example configuration file and exits. If the filename isn't specified a default filename '{0}' is used. Commandline arguments, if given, override all options in the configuration file.".format(defaultExampleConfigFilename, configFilename))
    parser.add_argument('-m', '--check-missing-users', dest='checkMissingUsers', choices=['warn', 'strict', 'ignore'], default='strict', help="When 'ignore' is used it disables the missing user check. When either 'warn' or 'strict' are used a list of usernames that are in accurev but were not found in the configured usermap is printed. Using 'strict' will cause the script to abort the conversion process if there are any missing users while using 'warn' will not.")
    parser.add_argument('--auto-config', nargs='?', dest='autoConfigFilename', const=configFilename, default=None, metavar='<config-filename>', help="Auto-generate the configuration file from known AccuRev information. It is required that an accurev username and password are provided either in an existing config file or via the -u and -p options. If there is an existing config file it is backed up and only the accurev username and password will be copied to the new configuration file. If you wish to preserve the config but add more information to it then it is recommended that you use the --fixup-config option instead.")
    parser.add_argument('--fixup-config', nargs='?', dest='fixupConfigFilename', const=configFilename, default=None, metavar='<config-filename>', help="Fixup the configuration file by adding updated AccuRev information. It is the same as the --auto-config option but the existing configuration file options are preserved. Other command line arguments that are provided will override the existing configuration file options for the new configuration file.")
    parser.add_argument('-T', '--track',    dest='track', action='store_const', const=True, help="Tracking mode. Sets the 'tracking' flag which makes the script run continuously in a loop. The configuration file is reloaded on each iteration so changes are picked up. Only makes sense for when you want this script to continuously track the accurev depot's newest transactions (i.e. you're using 'highest' or 'now' as your end transactions).")
    parser.add_argument('-I', '--tracking-intermission', nargs='?', dest='intermission', type=int, const=300, default=0, metavar='<intermission-sec>', help="Sets the intermission (in seconds) between consecutive iterations of the script in 'tracking' mode. The script sleeps for <intermission-sec> seconds before continuing the next conversion. This is useless if the --track option is not used.")
    
    args = parser.parse_args()
    
    # Dump example config if specified
    doEarlyReturn = False
    earlyReturnCode = 0
    if args.exampleConfigFilename is not None:
        earlyReturnCode = DumpExampleConfigFile(args.exampleConfigFilename)
        doEarlyReturn = True

    if args.autoConfigFilename is not None:
        earlyReturnCode = AutoConfigFile(filename=args.autoConfigFilename, args=args, preserveConfig=False)
        doEarlyReturn = True

    if args.fixupConfigFilename is not None:
        earlyReturnCode = AutoConfigFile(filename=args.fixupConfigFilename, args=args, preserveConfig=True)
        doEarlyReturn = True

    if doEarlyReturn:
        return earlyReturnCode
    
    loggerConfig = None
    while True:
        try:
            startTime = datetime.now() # used to compute the running time of the script.

            # Load the config file
            config = Config.fromfile(filename=args.configFilename)
            if config is None:
                sys.stderr.write("Config file '{0}' not found.\n".format(args.configFilename))
                return 1
            elif config.git is not None:
                if not os.path.isabs(config.git.repoPath):
                    config.git.repoPath = os.path.abspath(config.git.repoPath)

            # Set the overrides for in the configuration from the arguments
            SetConfigFromArgs(config=config, args=args)
            
            if not ValidateConfig(config):
                return 1

            # Configure logging, but do it only once.
            if logger is None:
                loggingLevel = logging.DEBUG if args.debug else logging.INFO
                if not InitializeLogging(config.logFilename, loggingLevel):
                    sys.stderr.write("Failed to initialize logging. Exiting.\n")
                    return 1

            # Start the script
            state = AccuRev2Git(config)

            PrintConfigSummary(state.config, args.configFilename)
            if args.checkMissingUsers in [ "warn", "strict" ]:
                if PrintMissingUsers(state.config) and args.checkMissingUsers == "strict":
                    sys.stderr.write("Found missing users. Exiting.\n")
                    return 1
            logger.info("Restart:" if args.restart else "Soft restart:" if args.softRestart else "Start:")
            rv = state.Start(isRestart=args.restart, isSoftRestart=args.softRestart)
            PrintRunningTime(referenceTime=startTime)
            if not args.track:
                break
            elif args.intermission is not None:
                print("Tracking mode enabled: sleep for {0} seconds.".format(args.intermission))
                time.sleep(args.intermission)
            print("Tracking mode enabled: Continuing conversion.")
        except:
            if logger is not None:
                PrintRunningTime(referenceTime=startTime)
                logger.exception("The script has encountered an exception, aborting!")
            raise

    return rv
        
# ################################################################################################ #
# Script Start                                                                                     #
# ################################################################################################ #
if __name__ == "__main__":
    AccuRev2GitMain(sys.argv)

