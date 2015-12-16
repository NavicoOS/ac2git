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
import git_stitch

# ################################################################################################ #
# Script Classes                                                                                   #
# ################################################################################################ #
class Config(object):
    class Logger(object):
        def __init__(self):
            self.referenceTime = None
            self.isDbgEnabled = False
            self.isInfoEnabled = True
            self.isErrEnabled = True

            self.logFile = None
            self.logFileDbgEnabled = False
            self.logFileInfoEnabled = True
            self.logFileErrorEnabled = True
        
        def _FormatMessage(self, messages):
            outMessage = ""
            if self.referenceTime is not None:
                # Custom formatting of the timestamp
                m, s = divmod((datetime.now() - self.referenceTime).total_seconds(), 60)
                h, m = divmod(m, 60)
                d, h = divmod(h, 24)
                
                if d > 0:
                    outMessage += "{d: >2d}d, ".format(d=int(d))
                
                outMessage += "{h: >2d}:{m:0>2d}:{s:0>5.2f}# ".format(h=int(h), m=int(m), s=s)
            
            outMessage += " ".join([str(x) for x in messages])
            
            return outMessage
        
        def info(self, *message):
            if self.isInfoEnabled:
                print(self._FormatMessage(message))

            if self.logFile is not None and self.logFileInfoEnabled:
                self.logFile.write(self._FormatMessage(message))
                self.logFile.write("\n")

        def dbg(self, *message):
            if self.isDbgEnabled:
                print(self._FormatMessage(message))

            if self.logFile is not None and self.logFileDbgEnabled:
                self.logFile.write(self._FormatMessage(message))
                self.logFile.write("\n")
        
        def error(self, *message):
            if self.isErrEnabled:
                sys.stderr.write(self._FormatMessage(message))
                sys.stderr.write("\n")

            if self.logFile is not None and self.logFileErrorEnabled:
                self.logFile.write(self._FormatMessage(message))
                self.logFile.write("\n")
        
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
                
                streamMap = None
                streamListElement = xmlElement.find('stream-list')
                if streamListElement is not None:
                    streamMap = OrderedDict()
                    streamElementList = streamListElement.findall('stream')
                    for streamElement in streamElementList:
                        streamName = streamElement.text
                        branchName = streamElement.attrib.get("branch-name")
                        if branchName is None:
                            branchName = streamName

                        streamMap[streamName] = branchName
                
                return cls(depot, username, password, startTransaction, endTransaction, streamMap, commandCacheFilename)
            else:
                return None
            
        def __init__(self, depot = None, username = None, password = None, startTransaction = None, endTransaction = None, streamMap = None, commandCacheFilename = None):
            self.depot    = depot
            self.username = username
            self.password = password
            self.startTransaction = startTransaction
            self.endTransaction   = endTransaction
            self.streamMap = streamMap
            self.commandCacheFilename = commandCacheFilename
    
        def __repr__(self):
            str = "Config.AccuRev(depot=" + repr(self.depot)
            str += ", username="          + repr(self.username)
            str += ", password="          + repr(self.password)
            str += ", startTransaction="  + repr(self.startTransaction)
            str += ", endTransaction="    + repr(self.endTransaction)
            if streamMap is not None:
                str += ", streamMap="    + repr(self.streamMap)
            str += ")"
            
            return str

        def UseCommandCache(self):
            return self.commandCacheFilename is not None
            
    class Git(object):
        @classmethod
        def fromxmlelement(cls, xmlElement):
            if xmlElement is not None and xmlElement.tag == 'git':
                repoPath = xmlElement.attrib.get('repo-path')
                finalize = xmlElement.attrib.get('finalize')
                if finalize is not None:
                    if finalize.lower() == "true":
                        finalize = True
                    elif finalize.lower() == "false":
                        finalize = False
                    else:
                        Exception("Error, could not parse finalize attribute '{0}'. Valid values are 'true' and 'false'.".format(finalize))
                
                return cls(repoPath=repoPath, finalize=finalize)
            else:
                return None
            
        def __init__(self, repoPath, finalize=None):
            self.repoPath = repoPath
            self.finalize = finalize

        def __repr__(self):
            str = "Config.Git(repoPath=" + repr(self.repoPath)
            str += ", finalize="         + repr(self.finalize)
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

    @classmethod
    def fromxmlstring(cls, xmlString):
        # Load the XML
        xmlRoot = ElementTree.fromstring(xmlString)
        
        if xmlRoot is not None and xmlRoot.tag == "accurev2git":
            accurev = Config.AccuRev.fromxmlelement(xmlRoot.find('accurev'))
            git     = Config.Git.fromxmlelement(xmlRoot.find('git'))
            
            method = "diff" # Defaults to diff
            methodElem = xmlRoot.find('method')
            if methodElem is not None:
                method = methodElem.text

            logFilename = None
            logFileElem = xmlRoot.find('logfile')
            if logFileElem is not None:
                logFilename = logFileElem.text

            usermaps = []
            userMapsElem = xmlRoot.find('usermaps')
            if userMapsElem is not None:
                for userMapElem in userMapsElem.findall('map-user'):
                    usermaps.append(Config.UserMap.fromxmlelement(userMapElem))
            
            return cls(accurev=accurev, git=git, usermaps=usermaps, method=method, logFilename=logFilename)
        else:
            # Invalid XML for an accurev2git configuration file.
            return None

    @staticmethod
    def fromfile(filename):
        config = None
        if os.path.exists(filename):
            with codecs.open(filename) as f:
                configXml = f.read()
                config = Config.fromxmlstring(configXml)
        
        return config

    def __init__(self, accurev = None, git = None, usermaps = None, method = None, logFilename = None):
        self.accurev     = accurev
        self.git         = git
        self.usermaps    = usermaps
        self.method      = method
        self.logFilename = logFilename
        self.logger      = Config.Logger()
        
    def __repr__(self):
        str = "Config(accurev=" + repr(self.accurev)
        str += ", git="         + repr(self.git)
        str += ", usermaps="    + repr(self.usermaps)
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
    gitNotesRef_AccurevHistXml = 'accurev/xml/hist'
    gitNotesRef_AccurevHist    = 'accurev/hist'

    commandFailureRetryCount = 3

    def __init__(self, config):
        self.config = config
        self.cwd = None
        self.gitRepo = None
        self.gitBranchList = None

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
                path = os.path.join(root, name).replace('\\','/')
                # Preserve empty directories that are not under the .git/ directory.
                if git.GetGitDirPrefix(path) is None and len(os.listdir(path)) == 0:
                    filename = os.path.join(path, '.gitignore')
                    with codecs.open(filename, 'w', 'utf-8') as file:
                        #file.write('# accurev2git.py preserve empty dirs\n')
                        preservedDirs.append(filename)
                    if not os.path.exists(filename):
                        self.config.logger.error("Failed to preserve directory. Couldn't create '{0}'.".format(filename))
        return preservedDirs

    def DeleteEmptyDirs(self):
        deletedDirs = []
        for root, dirs, files in os.walk(self.gitRepo.path, topdown=True):
            for name in dirs:
                path = os.path.join(root, name).replace('\\','/')
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
                            self.config.logger.error("Failed to delete empty directory '{0}'.".format(path))
                            raise Exception("Failed to delete '{0}'".format(path))
                        else:
                            deletedDirs.append(path)
        return deletedDirs

    def GetGitUserFromAccuRevUser(self, accurevUsername):
        if accurevUsername is not None:
            for usermap in self.config.usermaps:
                if usermap.accurevUsername == accurevUsername:
                    return "{0} <{1}>".format(usermap.gitName, usermap.gitEmail)
        state.config.logger.error("Cannot find git details for accurev username {0}".format(accurevUsername))
        return accurevUsername

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
    
    def GetGitDatetimeStr(self, accurevUsername, accurevDatetime):
        usertime, tz = self.GetGitDatetime(accurevUsername=accurevUsername, accurevDatetime=accurevDatetime)

        gitDatetimeStr = None
        if usertime is not None:
            gitDatetimeStr = "{0}".format(usertime.isoformat())
            if tz is not None:
                gitDatetimeStr = "{0} {1:+05}".format(gitDatetimeStr, tz)
        return gitDatetimeStr

    # Adds a JSON string respresentation of `stateDict` to the given commit using `git notes add`.
    def AddScriptStateNote(self, depotName, stream, transaction, commitHash, ref, committer=None, committerDate=None, committerTimezone=None):
        stateDict = { "depot": depotName, "stream": stream.name, "stream_number": stream.streamNumber, "transaction_number": transaction.id, "transaction_kind": transaction.Type }
        notesFilePath = None
        with tempfile.NamedTemporaryFile(mode='w+', prefix='ac2git_note_', delete=False) as notesFile:
            notesFilePath = notesFile.name
            notesFile.write(json.dumps(stateDict))


        if notesFilePath is not None:
            rv = self.gitRepo.notes.add(messageFile=notesFilePath, obj=commitHash, ref=ref, force=True, committer=committer, committerDate=committerDate, committerTimezone=committerTimezone, author=committer, authorDate=committerDate, authorTimezone=committerTimezone)
            os.remove(notesFilePath)

            if rv is not None:
                self.config.logger.dbg( "Added script state note for {0}.".format(commitHash) )
            else:
                self.config.logger.error( "Failed to add script state note for {0}, tr. {1}".format(commitHash, transaction.id) )
                self.config.logger.error(self.gitRepo.lastStderr)
            
            return rv
        else:
            self.config.logger.error( "Failed to create temporary file for script state note for {0}, tr. {1}".format(commitHash, transaction.id) )
        
        return None

    def AddAccurevHistNote(self, commitHash, ref, depot, transaction, committer=None, committerDate=None, committerTimezone=None, isXml=False):
        # Write the commit notes consisting of the accurev hist xml output for the given transaction.
        # Note: It is important to use the depot instead of the stream option for the accurev hist command since if the transaction
        #       did not occur on that stream we will get the closest transaction that was a promote into the specified stream instead of an error!
        arHistXml = accurev.raw.hist(depot=depot, timeSpec="{0}.1".format(transaction.id), isXmlOutput=isXml)
        notesFilePath = None
        with tempfile.NamedTemporaryFile(mode='w+', prefix='ac2git_note_', delete=False) as notesFile:
            notesFilePath = notesFile.name
            if arHistXml is None or len(arHistXml) == 0:
                self.config.logger.error('accurev hist returned an empty xml for transaction {0} (commit {1})'.format(transaction.id, commitHash))
                return False
            else:
                notesFile.write(arHistXml)
        
        if notesFilePath is not None:        
            rv = self.gitRepo.notes.add(messageFile=notesFilePath, obj=commitHash, ref=ref, force=True, committer=committer, committerDate=committerDate, committerTimezone=committerTimezone, author=committer, authorDate=committerDate, authorTimezone=committerTimezone)
            os.remove(notesFilePath)
        
            if rv is not None:
                self.config.logger.dbg( "Added accurev hist{0} note for {1}.".format(' xml' if isXml else '', commitHash) )
            else:
                self.config.logger.error( "Failed to add accurev hist{0} note for {1}".format(' xml' if isXml else '', commitHash) )
                self.config.logger.error(self.gitRepo.lastStderr)
            
            return rv
        else:
            self.config.logger.error( "Failed to create temporary file for accurev hist{0} note for {1}".format(' xml' if isXml else '', commitHash) )

        return None

    def GetFirstTransaction(self, depot, streamName, startTransaction=None, endTransaction=None):
        # Get the stream creation transaction (mkstream). Note: The first stream in the depot doesn't have an mkstream transaction.
        for i in range(0, AccuRev2Git.commandFailureRetryCount):
            mkstream = accurev.hist(stream=streamName, transactionKind="mkstream", timeSpec="now")
            if mkstream is not None:
                break
        if mkstream is None:
            return None

        tr = None
        if len(mkstream.transactions) == 0:
            self.config.logger.info( "The root stream has no mkstream transaction. Starting at transaction 1." )
            # the assumption is that the depot name matches the root stream name (for which there is no mkstream transaction)
            firstTr = self.TryHist(depot=depot, trNum="1")
            if firstTr is None or len(firstTr.transactions) == 0:
                raise Exception("Error: assumption that the root stream has the same name as the depot doesn't hold. Aborting...")
            tr = firstTr.transactions[0]
        else:
            tr = mkstream.transactions[0]
            if len(mkstream.transactions) != 1:
                self.config.logger.error( "There seem to be multiple mkstream transactions for this stream... Using {0}".format(tr.id) )

        if startTransaction is not None:
            startTrHist = self.TryHist(depot=depot, trNum=startTransaction)
            if startTrHist is None:
                return None

            startTr = startTrHist.transactions[0]
            if tr.id < startTr.id:
                self.config.logger.info( "The first transaction (#{0}) for stream {1} is earlier than the conversion start transaction (#{2}).".format(tr.id, streamName, startTr.id) )
                tr = startTr
        if endTransaction is not None:
            endTrHist = self.TryHist(depot=depot, trNum=endTransaction)
            if endTrHist is None:
                return None

            endTr = endTrHist.transactions[0]
            if endTr.id < tr.id:
                self.config.logger.info( "The first transaction (#{0}) for stream {1} is later than the conversion end transaction (#{2}).".format(tr.id, streamName, startTr.id) )
                tr = None

        return tr

    def GetLastCommitHash(self, branchName=None):
        cmd = []
        for i in range(0, AccuRev2Git.commandFailureRetryCount):
            cmd = [u'git', u'log', u'-1', u'--format=format:%H']
            if branchName is not None:
                cmd.append(branchName)
            commitHash = self.gitRepo.raw_cmd(cmd)
            if commitHash is not None:
                commitHash = commitHash.strip()
                if len(commitHash) == 0:
                    commitHash = None
                else:
                    break

        if commitHash is None:
            self.config.logger.error("Failed to retrieve last git commit hash. Command `{0}` failed.".format(' '.join(cmd)))

        return commitHash

    def GetStateForCommit(self, commitHash, branchName=None):
        stateObj = None

        # Try and search the branch namespace.
        if branchName is None:
            branchName = AccuRev2Git.gitNotesRef_AccurevHistXml

        stateJson = None
        for i in range(0, AccuRev2Git.commandFailureRetryCount):
            stateJson = self.gitRepo.notes.show(obj=commitHash, ref=branchName)
            if stateJson is not None:
                break

        if stateJson is not None:
            stateJson = stateJson.strip()
            stateObj = json.loads(stateJson)
        else:
            self.config.logger.error("Failed to load the last transaction for commit {0} from {1} notes.".format(commitHash, branchName))
            self.config.logger.error("  i.e git notes --ref={0} show {1}    - returned nothing.".format(branchName, commitHash))

        return stateObj

    def GetHistForCommit(self, commitHash, branchName=None, stateObj=None):
        #self.config.logger.dbg("GetHistForCommit(commitHash={0}, branchName={1}, stateObj={2}".format(commitHash, branchName, stateObj))
        if stateObj is None:
            stateObj = self.GetStateForCommit(commitHash=commitHash, branchName=branchName)
        if stateObj is not None:
            trNum = stateObj["transaction_number"]
            depot = stateObj["depot"]
            if trNum is not None and depot is not None:
                hist = accurev.hist(depot=depot, timeSpec=trNum, useCache=self.config.accurev.UseCommandCache())
                return hist
        return None

    def CreateCleanGitBranch(self, branchName):
        # Create the git branch.
        self.config.logger.info( "Creating {0}".format(branchName) )
        self.gitRepo.checkout(branchName=branchName, isOrphan=True)

        # Clear the index as it may contain the [start-point] info...
        self.gitRepo.rm(fileList=['.'], force=True, recursive=True)
        self.ClearGitRepo()

    def Commit(self, depot, stream, transaction, branchName=None, isFirstCommit=False):
        self.PreserveEmptyDirs()

        # Add all of the files to the index
        self.gitRepo.add(force=True, all=True, gitOpts=[u'-c', u'core.autocrlf=false'])

        # Make the first commit
        messageFilePath = None
        with tempfile.NamedTemporaryFile(mode='w+', prefix='ac2git_commit_', delete=False) as messageFile:
            messageFilePath = messageFile.name
            if transaction.comment is None or len(transaction.comment) == 0:
                messageFile.write(' ') # White-space is always stripped from commit messages. See the git commit --cleanup option for details.
            else:
                # In git the # at the start of the line indicate that this line is a comment inside the message and will not be added.
                # So we will just add a space to the start of all the lines starting with a # in order to preserve them.
                messageFile.write(transaction.comment)
        
        if messageFilePath is None:
            self.config.logger.error("Failed to create temporary file for commit message for transaction {0}, stream {1}, branch {2}".format(transaction.id, stream, branchName))
            return None

        committer = self.GetGitUserFromAccuRevUser(transaction.user)
        committerDate, committerTimezone = self.GetGitDatetime(accurevUsername=transaction.user, accurevDatetime=transaction.time)
        if not isFirstCommit:
            lastCommitHash = self.GetLastCommitHash()
            if lastCommitHash is None:
                self.config.logger.info("No last commit hash available. Non-fatal error, continuing.")
        else:
            lastCommitHash = None
        commitHash = None

        # Since the accurev.obj namespace is populated from the XML output of accurev commands all times are given in UTC.
        # For now just force the time to be UTC centric but preferrably we would have this set-up to either use the local timezone
        # or allow each user to be given a timezone for geographically distributed teams...
        # The PyTz library should be considered for the timezone conversions. Do not roll your own...
        if self.gitRepo.commit(messageFile=messageFilePath, committer=committer, committer_date=committerDate, committer_tz=committerTimezone, author=committer, date=committerDate, tz=committerTimezone, allow_empty_message=True, gitOpts=[u'-c', u'core.autocrlf=false']):
            commitHash = self.GetLastCommitHash()
            if commitHash is not None:
                if lastCommitHash != commitHash:
                    self.config.logger.dbg( "Committed {0}".format(commitHash) )
                    xmlNoteWritten = False
                    for i in range(0, AccuRev2Git.commandFailureRetryCount):
                        ref = branchName
                        if ref is None:
                            ref = AccuRev2Git.gitNotesRef_AccurevHistXml
                            self.config.logger.error("Commit to an unspecified branch. Using default `git notes` ref for the script [{0}] at current time.".format(ref))
                        stateNoteWritten = ( self.AddScriptStateNote(depotName=depot, stream=stream, transaction=transaction, commitHash=commitHash, ref=ref, committer=committer, committerDate=committerDate, committerTimezone=committerTimezone) is not None )
                        if stateNoteWritten:
                            break
                    if not stateNoteWritten:
                        # The XML output in the notes is how we track our conversion progress. It is not acceptable for it to fail.
                        # Undo the commit and print an error.
                        branchName = 'HEAD'
                        self.config.logger.error("Couldn't record last transaction state. Undoing the last commit {0} with `git reset --hard {1}^`".format(commitHash, branchName))
                        self.gitRepo.raw_cmd([u'git', u'reset', u'--hard', u'{0}^'.format(branchName)])

                        return None
                else:
                    self.config.logger.error("Commit command returned True when nothing was committed...? Last commit hash {0} didn't change after the commit command executed.".format(lastCommitHash))
                    return None
            else:
                self.config.logger.error("Failed to commit! No last hash available.")
                return None
        elif "nothing to commit" in self.gitRepo.lastStdout:
            self.config.logger.dbg( "nothing to commit after populating transaction {0}...?".format(transaction.id) )
        else:
            self.config.logger.error( "Failed to commit transaction {0}".format(transaction.id) )
            self.config.logger.error( "\n{0}\n{1}\n".format(self.gitRepo.lastStdout, self.gitRepo.lastStderr) )
        os.remove(messageFilePath)

        return commitHash

    def TryDiff(self, streamName, firstTrNumber, secondTrNumber):
        for i in range(0, AccuRev2Git.commandFailureRetryCount):
            diff = accurev.diff(all=True, informationOnly=True, verSpec1=streamName, verSpec2=streamName, transactionRange="{0}-{1}".format(firstTrNumber, secondTrNumber), useCache=self.config.accurev.UseCommandCache())
            if diff is not None:
                break
        if diff is None:
            self.config.logger.error( "accurev diff failed! stream: {0} time-spec: {1}-{2}".format(streamName, firstTrNumber, secondTrNumber) )
        return diff
    
    def FindNextChangeTransaction(self, streamName, startTrNumber, endTrNumber, deepHist=None):
        # Iterate over transactions in order using accurev diff -a -i -v streamName -V streamName -t <lastProcessed>-<current iterator>
        if self.config.method == "diff":
            nextTr = startTrNumber + 1
            diff = self.TryDiff(streamName=streamName, firstTrNumber=startTrNumber, secondTrNumber=nextTr)
            if diff is None:
                return (None, None)
    
            # Note: This is likely to be a hot path. However, it cannot be optimized since a revert of a transaction would not show up in the diff even though the
            #       state of the stream was changed during that period in time. Hence to be correct we must iterate over the transactions one by one unless we have
            #       explicit knowlege of all the transactions which could affect us via some sort of deep history option...
            while nextTr <= endTrNumber and len(diff.elements) == 0:
                nextTr += 1
                diff = self.TryDiff(streamName=streamName, firstTrNumber=startTrNumber, secondTrNumber=nextTr)
                if diff is None:
                    return (None, None)
        
            self.config.logger.dbg("FindNextChangeTransaction diff: {0}".format(nextTr))
            return (nextTr, diff)
        elif self.config.method == "deep-hist":
            if deepHist is None:
                raise Exception("Script error! deepHist argument cannot be none when running a deep-hist method.")
            # Find the next transaction
            for tr in deepHist:
                if tr.id > startTrNumber:
                    diff = self.TryDiff(streamName=streamName, firstTrNumber=startTrNumber, secondTrNumber=tr.id)
                    if diff is None:
                        return (None, None)
                    elif len(diff.elements) > 0:
                        self.config.logger.dbg("FindNextChangeTransaction deep-hist: {0}".format(tr.id))
                        return (tr.id, diff)
                    else:
                        self.config.logger.dbg("FindNextChangeTransaction deep-hist skipping: {0}, diff was empty...".format(tr.id))

            diff = self.TryDiff(streamName=streamName, firstTrNumber=startTrNumber, secondTrNumber=endTrNumber)
            return (endTrNumber + 1, diff) # The end transaction number is inclusive. We need to return the one after it.
        elif self.config.method == "pop":
            self.config.logger.dbg("FindNextChangeTransaction pop: {0}".format(startTrNumber + 1))
            return (startTrNumber + 1, None)
        else:
            self.config.logger.error("Method is unrecognized, allowed values are 'pop', 'diff' and 'deep-hist'")
            raise Exception("Invalid configuration, method unrecognized!")

    def DeleteDiffItemsFromRepo(self, diff):
        # Delete all of the files which are even mentioned in the diff so that we can do a quick populate (wouth the overwrite option)
        deletedPathList = []
        for element in diff.elements:
            for change in element.changes:
                for stream in [ change.stream1, change.stream2 ]:
                    if stream is not None and stream.name is not None:
                        name = stream.name.replace('\\', '/').lstrip('/')
                        path = os.path.join(self.gitRepo.path, name)
                        if os.path.lexists(path): # Ensure that broken links are also deleted!
                            if not self.DeletePath(path):
                                self.config.logger.error("Failed to delete '{0}'.".format(path))
                                raise Exception("Failed to delete '{0}'".format(path))
                            else:
                                deletedPathList.append(path)

        return deletedPathList

    def TryHist(self, depot, trNum):
        for i in range(0, AccuRev2Git.commandFailureRetryCount):
            endTrHist = accurev.hist(depot=depot, timeSpec="{0}.1".format(trNum), useCache=self.config.accurev.UseCommandCache())
            if endTrHist is not None:
                break
        return endTrHist

    def TryPop(self, streamName, transaction, overwrite=False):
        for i in range(0, AccuRev2Git.commandFailureRetryCount):
            popResult = accurev.pop(verSpec=streamName, location=self.gitRepo.path, isRecursive=True, isOverride=overwrite, timeSpec=transaction.id, elementList='.')
            if popResult:
                break
            else:
                self.config.logger.error("accurev pop failed:")
                for message in popResult.messages:
                    if message.error is not None and message.error:
                        self.config.logger.error("  {0}".format(message.text))
                    else:
                        self.config.logger.info("  {0}".format(message.text))
        
        return popResult

    def ProcessStream(self, depot, stream, branchName, startTransaction, endTransaction):
        self.config.logger.info( "Processing {0} -> {1} : {2} - {3}".format(stream.name, branchName, startTransaction, endTransaction) )

        # Find the matching git branch
        branch = None
        for b in self.gitBranchList:
            if branchName == b.name:
                branch = b
                break

        status = None
        if branch is not None:
            # Get the last processed transaction
            self.ClearGitRepo()
            self.gitRepo.checkout(branchName=branchName)
            status = self.gitRepo.status()

        tr = None
        commitHash = None
        if status is None or status.initial_commit:
            # We are tracking a new stream:
            tr = self.GetFirstTransaction(depot=depot, streamName=stream.name, startTransaction=startTransaction, endTransaction=endTransaction)
            if tr is not None:
                if branch is None:
                    self.CreateCleanGitBranch(branchName=branchName)
                try:
                    destStream = self.GetDestinationStreamName(history=hist, depot=None)
                except:
                    destStream = None
                self.config.logger.dbg( "{0} pop (init): {1} {2}{3}".format(stream.name, tr.Type, tr.id, " to {0}".format(destStream) if destStream is not None else "") )
                popResult = self.TryPop(streamName=stream.name, transaction=tr, overwrite=True)
                if not popResult:
                    return (None, None)
                
                stream = accurev.show.streams(depot=depot, stream=stream.streamNumber, timeSpec=tr.id).streams[0]
                commitHash = self.Commit(depot=depot, stream=stream, transaction=tr, branchName=branchName, isFirstCommit=True)
                if not commitHash:
                    self.config.logger.dbg( "{0} first commit has failed. Is it an empty commit? Continuing...".format(stream.name) )
                else:
                    self.config.logger.info( "stream {0}: tr. #{1} {2} into {3} -> commit {4} on {5}".format(stream.name, tr.id, tr.Type, destStream if destStream is not None else 'unknown', commitHash[:8], branchName) )
            else:
                self.config.logger.info( "Failed to get the first transaction for {0} from accurev. Won't process any further.".format(stream.name) )
                return (None, None)
        else:
            # Get the last processed transaction
            commitHash = self.GetLastCommitHash(branchName=branchName)
            hist = self.GetHistForCommit(commitHash=commitHash, branchName=branchName)

            # This code should probably be controlled with some flag in the configuration/command line...
            if hist is None:
                self.config.logger.error("Repo in invalid state. Attempting to auto-recover.")
                resetCmd = ['git', 'reset', '--hard', '{0}^'.format(branchName)]
                self.config.logger.error("Deleting last commit from this branch using, {0}".format(' '.join(resetCmd)))
                try:
                    subprocess.check_call(resetCmd)
                except subprocess.CalledProcessError:
                    self.config.logger.error("Failed to reset branch. Aborting!")
                    return (None, None)

                commitHash = self.GetLastCommitHash(branchName=branchName)
                hist = self.GetHistForCommit(commitHash=commitHash, branchName=branchName)

                if hist is None:
                    self.config.logger.error("Repo in invalid state. Please reset this branch to a previous commit with valid notes.")
                    self.config.logger.error("  e.g. git reset --hard {0}~1".format(branchName))
                    return (None, None)

            tr = hist.transactions[0]
            stream = accurev.show.streams(depot=depot, stream=stream.streamNumber, timeSpec=tr.id).streams[0]
            self.config.logger.dbg("{0}: last processed transaction was #{1}".format(stream.name, tr.id))

        endTrHist = self.TryHist(depot=depot, trNum=endTransaction)
        if endTrHist is None:
            self.config.logger.dbg("accurev hist -p {0} -t {1}.1 failed.".format(depot, endTransaction))
            return (None, None)
        endTr = endTrHist.transactions[0]
        self.config.logger.info("{0}: processing transaction range #{1} - #{2}".format(stream.name, tr.id, endTr.id))

        deepHist = None
        if self.config.method == "deep-hist":
            ignoreTimelocks=True # The code for the timelocks is not tested fully yet. Once tested setting this to false should make the resulting set of transactions smaller
                                 # at the cost of slightly larger number of upfront accurev commands called.
            self.config.logger.dbg("accurev.ext.deep_hist(depot={0}, stream={1}, timeSpec='{2}-{3}', ignoreTimelocks={4})".format(depot, stream.name, tr.id, endTr.id, ignoreTimelocks))
            deepHist = accurev.ext.deep_hist(depot=depot, stream=stream.name, timeSpec="{0}-{1}".format(tr.id, endTr.id), ignoreTimelocks=ignoreTimelocks)
            self.config.logger.info("Deep-hist returned {count} transactions to process.".format(count=len(deepHist)))
            if deepHist is None:
                raise Exception("accurev.ext.deep_hist() failed to return a result!")
        while True:
            nextTr, diff = self.FindNextChangeTransaction(streamName=stream.name, startTrNumber=tr.id, endTrNumber=endTr.id, deepHist=deepHist)
            if nextTr is None:
                self.config.logger.dbg( "FindNextChangeTransaction(streamName='{0}', startTrNumber={1}, endTrNumber={2}, deepHist={3}) failed!".format(stream.name, tr.id, endTr.id, deepHist) )
                return (None, None)

            self.config.logger.dbg( "{0}: next transaction {1} (end tr. {2})".format(stream.name, nextTr, endTr.id) )
            if nextTr <= endTr.id:
                # Right now nextTr is an integer representation of our next transaction.
                # Delete all of the files which are even mentioned in the diff so that we can do a quick populate (wouth the overwrite option)
                popOverwrite = (self.config.method == "pop")
                if self.config.method == "pop":
                    self.ClearGitRepo()
                else:
                    if diff is None:
                        return (None, None)
                    
                    try:
                        deletedPathList = self.DeleteDiffItemsFromRepo(diff=diff)
                    except:
                        popOverwrite = True
                        self.config.logger.info("Error trying to delete changed elements. Fatal, aborting!")
                        # This might be ok only in the case when the files/directories were changed but not in the case when there
                        # was a deletion that occurred. Abort and be safe!
                        # TODO: This must be solved somehow since this could hinder this script from continuing at all!
                        return (None, None)

                    # Remove all the empty directories (this includes directories which contain an empty .gitignore file since that's what we is done to preserve them)
                    try:
                        self.DeleteEmptyDirs()
                    except:
                        popOverwrite = True
                        self.config.logger.info("Error trying to delete empty directories. Fatal, aborting!")
                        # This might be ok only in the case when the files/directories were changed but not in the case when there
                        # was a deletion that occurred. Abort and be safe!
                        # TODO: This must be solved somehow since this could hinder this script from continuing at all!
                        return (None, None)

                # The accurev hist command here must be used with the depot option since the transaction that has affected us may not
                # be a promotion into the stream we are looking at but into one of its parent streams. Hence we must query the history
                # of the depot and not the stream itself.
                hist = self.TryHist(depot=depot, trNum=nextTr)
                if hist is None:
                    self.config.logger.dbg("accurev hist -p {0} -t {1}.1 failed.".format(depot, endTransaction))
                    return (None, None)
                tr = hist.transactions[0]
                stream = accurev.show.streams(depot=depot, stream=stream.streamNumber, timeSpec=tr.id).streams[0]

                # Populate
                #destStream = self.GetDestinationStreamName(history=hist, depot=depot) # Slower: This performes an extra accurev.show.streams() command for correct stream names.
                destStream = self.GetDestinationStreamName(history=hist, depot=None) # Quicker: This does not perform an extra accurev.show.streams() command for correct stream names.
                self.config.logger.dbg( "{0} pop: {1} {2}{3}".format(stream.name, tr.Type, tr.id, " to {0}".format(destStream) if destStream is not None else "") )

                popResult = self.TryPop(streamName=stream.name, transaction=tr, overwrite=popOverwrite)
                if not popResult:
                    return (None, None)

                # Commit
                commitHash = self.Commit(depot=depot, stream=stream, transaction=tr, branchName=branchName, isFirstCommit=False)
                if commitHash is None:
                    if"nothing to commit" in self.gitRepo.lastStdout:
                        if diff is not None:
                            self.config.logger.dbg( "diff info ({0} elements):".format(len(diff.elements)) )
                            for element in diff.elements:
                                for change in element.changes:
                                    self.config.logger.dbg( "  what changed: {0}".format(change.what) )
                                    self.config.logger.dbg( "  original: {0}".format(change.stream1) )
                                    self.config.logger.dbg( "  new:      {0}".format(change.stream2) )
                        self.config.logger.dbg( "deleted {0} files:".format(len(deletedPathList)) )
                        for p in deletedPathList:
                            self.config.logger.dbg( "  {0}".format(p) )
                        self.config.logger.dbg( "populated {0} files:".format(len(popResult.elements)) )
                        for e in popResult.elements:
                            self.config.logger.dbg( "  {0}".format(e.location) )
                        self.config.logger.info("stream {0}: tr. #{1} is a no-op. Potential but unlikely error. Continuing.".format(stream.name, tr.id))
                    else:
                        break # Early return from processing this stream. Restarting should clean everything up.
                else:
                    self.config.logger.info( "stream {0}: tr. #{1} {2} into {3} -> commit {4} on {5}".format(stream.name, tr.id, tr.Type, destStream if destStream is not None else 'unknown', commitHash[:8], branchName) )
            else:
                self.config.logger.info( "Reached end transaction #{0} for {1} -> {2}".format(endTr.id, stream.name, branchName) )
                break

        return (tr, commitHash)

    def ProcessStreams(self):
        if self.config.accurev.commandCacheFilename is not None:
            accurev.ext.enable_command_cache(self.config.accurev.commandCacheFilename)
        
        for stream in self.config.accurev.streamMap:
            branch = self.config.accurev.streamMap[stream]
            depot  = self.config.accurev.depot
            streamInfo = None
            try:
                streamInfo = accurev.show.streams(depot=depot, stream=stream).streams[0]
            except IndexError:
                self.config.logger.error( "Failed to get stream information. `accurev show streams -p {0} -s {1}` returned no streams".format(depot, stream) )
                return
            except AttributeError:
                self.config.logger.error( "Failed to get stream information. `accurev show streams -p {0} -s {1}` returned None".format(depot, stream) )
                return

            if depot is None or len(depot) == 0:
                depot = streamInfo.depotName
            tr, commitHash = self.ProcessStream(depot=depot, stream=streamInfo, branchName=branch, startTransaction=self.config.accurev.startTransaction, endTransaction=self.config.accurev.endTransaction)
            if tr is None or commitHash is None:
                self.config.logger.error( "Error while processing stream {0}, branch {1}".format(stream, branch) )
        
        if self.config.accurev.commandCacheFilename is not None:
            accurev.ext.disable_command_cache()

    def InitGitRepo(self, gitRepoPath):
        gitRootDir, gitRepoDir = os.path.split(gitRepoPath)
        if os.path.isdir(gitRootDir):
            if git.isRepo(gitRepoPath):
                # Found an existing repo, just use that.
                self.config.logger.info( "Using existing git repository." )
                return True
        
            self.config.logger.info( "Creating new git repository" )
            
            # Create an empty first commit so that we can create branches as we please.
            if git.init(path=gitRepoPath) is not None:
                self.config.logger.info( "Created a new git repository." )
            else:
                self.config.logger.error( "Failed to create a new git repository." )
                sys.exit(1)
                
            return True
        else:
            self.config.logger.error("{0} not found.\n".format(gitRootDir))
            
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
                    stream = accurev.show.streams(depot=depot, stream=streamNumber, timeSpec=transaction.id).streams[0] # could be expensive
                    if stream is not None and stream.name is not None:
                        return stream.name
                except:
                    pass
            return streamName
        return None

    def GetStreamNameFromBranch(self, branchName):
        if branchName is not None:
            for stream in self.config.accurev.streamMap:
                if branchName == self.config.accurev.streamMap[stream]:
                    return stream
        return None

    # Arranges the stream1 and stream2 into a tuple of (parent, child) according to accurev information
    def GetParentChild(self, stream1, stream2, timeSpec=u'now', onlyDirectChild=False):
        parent = None
        child = None
        if stream1 is not None and stream2 is not None:
            #print ("self.GetParentChild(stream1={0}, stream2={1}, timeSpec={2}".format(str(stream1), str(stream2), str(timeSpec)))
            stream1Children = accurev.show.streams(depot=self.config.accurev.depot, stream=stream1, timeSpec=timeSpec, listChildren=True)
            stream2Children = accurev.show.streams(depot=self.config.accurev.depot, stream=stream2, timeSpec=timeSpec, listChildren=True)

            found = False
            for stream in stream1Children.streams:
                if stream.name == stream2:
                    if not onlyDirectChild or stream.basis == stream1:
                        parent = stream1
                        child = stream2
                    found = True
                    break
            if not found:
                for stream in stream2Children.streams:
                    if stream.name == stream1:
                        if not onlyDirectChild or stream.basis == stream2:
                            parent = stream2
                            child = stream1
                        break
        return (parent, child)

    def GetStreamName(self, state=None, commitHash=None):
        if state is None:
            return None
        stream = state.get('stream')
        if stream is None:
            self.config.logger.error("Could not get stream name from state {0}. Trying to reverse map from the containing branch name.".format(state))
            if commitHash is not None:
                branches = self.gitRepo.branch_list(containsCommit=commitHash) # This should only ever return one branch since we are processing things in order...
                if branches is not None and len(branches) == 1:
                    branch = branches[0]
                    stream = self.GetStreamNameFromBranch(branchName=branch.name)
                    if stream is None:
                        self.config.logger.error("Could not get stream name for branch {0}.".format(branch.name))
        
        return stream

    def StitchBranches(self):
        self.config.logger.dbg("Getting branch revision map from git_stitch.py")
        branchRevMap = git_stitch.GetBranchRevisionMap(self.config.git.repoPath)
        
        self.config.logger.info("Stitching git branches")
        commitRewriteMap = OrderedDict()
        if branchRevMap is not None:
            commitStateMap = {}
            # Build a dictionary that will act as our "squashMap". Both the key and value are a commit hash.
            # The commit referenced by the key will be replaced by the commit referenced by the value in this map.
            aliasMap = {}
            for tree_hash in branchRevMap:
                for commit in branchRevMap[tree_hash]:
                    if not commit or re.match("^[0-9A-Fa-f]+$", commit[u'hash']) is None:
                        raise Exception("Commit {commit} is not a valid hash!".format(commit=commit))
                    aliasMap[commit[u'hash']] = commit[u'hash'] # Initially each commit maps to itself.

            for tree_hash in branchRevMap:
                if len(branchRevMap[tree_hash]) > 1:
                    # We should make some decisions about how to merge these commits which reference the same tree
                    # and what their ideal parents are. Once we decide we will write it to file in a nice bash friendly
                    # format and use the git filter-branch --parent-filter ... to fix it all up!
                    inOrder = sorted(branchRevMap[tree_hash], key=lambda x: int(x[u'committer'][u'time']))
                    #print(u'tree: {0}'.format(tree_hash))
                    
                    for i in range(0, len(inOrder) - 1):
                        first = inOrder[i]
                        second = inOrder[i + 1]
                        
                        firstTime = int(first[u'committer'][u'time'])
                        secondTime = int(second[u'committer'][u'time'])
    
                        wereSwapped = False
                        if firstTime == secondTime:
                            # Normally both commits would have originated from the same transaction. However, if not, let's try and order them by transaciton number first.
                            firstState = self.GetStateForCommit(commitHash=first[u'hash'], branchName=first[u'branch'].name)
                            secondState = self.GetStateForCommit(commitHash=second[u'hash'], branchName=second[u'branch'].name)

                            commitStateMap[first[u'hash']] = firstState
                            commitStateMap[second[u'hash']] = secondState

                            firstTrId = firstState["transaction_number"]
                            secondTrId = secondState["transaction_number"]

                            if firstTrId < secondTrId:
                                # This should really never be true given that AccuRev is centralized and synchronous and that firstTime == secondTime above...
                                pass # Already in the correct order
                            elif firstTrId > secondTrId:
                                # This should really never be true given that AccuRev is centralized and synchronous and that firstTime == secondTime above...
                                # Swap them
                                wereSwapped = True
                                first, second = second, first
                                firstState, secondState = secondState, firstState
                            else:
                                # The same transaction affected both commits (the id's are unique in accurev)...
                                # Must mean that they are substreams of eachother or sibling substreams of a third stream. Let's see which it is.

                                # Get the information for the first stream
                                firstStream = self.GetStreamName(state=firstState, commitHash=first[u'hash'])
                                if firstStream is None:
                                    self.config.logger.error("Branch stitching error: incorrect state. Could not get stream name for branch {0}.".format(firstBranch))
                                    raise Exception("Branch stitching failed!")

                                # Get the information for the second stream
                                secondStream = self.GetStreamName(state=secondState, commitHash=second[u'hash'])
                                if secondStream is None:
                                    self.config.logger.error("Branch stitching error: incorrect state. Could not get stream name for branch {0}.".format(secondBranch))
                                    raise Exception("Branch stitching failed!")

                                # Find which one is the parent of the other. They must be inline since they were affected by the same transaction (since the times match)
                                parentStream, childStream = self.GetParentChild(stream1=firstStream, stream2=secondStream, timeSpec=firstTrId, onlyDirectChild=False)
                                if parentStream is not None and childStream is not None:
                                    if firstStream == childStream:
                                        aliasMap[first[u'hash']] = second[u'hash']
                                        self.config.logger.info(u'  squashing: {0} ({1}/{2}) as equiv. to {3} ({4}/{5}). tree {6}.'.format(first[u'hash'][:8], firstStream, firstTrId, second[u'hash'][:8], secondStream, secondTrId, tree_hash[:8]))
                                    elif secondStream == childStream:
                                        aliasMap[second[u'hash']] = first[u'hash']
                                        self.config.logger.info(u'  squashing: {3} ({4}/{5}) as equiv. to {0} ({1}/{2}). tree {6}.'.format(first[u'hash'][:8], firstStream, firstTrId, second[u'hash'][:8], secondStream, secondTrId, tree_hash[:8]))
                                    else:
                                        Exception("Invariant violation! Either (None, None), (firstStream, secondStream) or (secondStream, firstStream) should be possible")
                                else:
                                    self.config.logger.info(u'  unrelated: {0} ({1}/{2}) is equiv. to {3} ({4}/{5}). tree {6}.'.format(first[u'hash'][:8], firstStream, firstTrId, second[u'hash'][:8], secondStream, secondTrId, tree_hash[:8]))
                                    
                        elif firstTime < secondTime:
                            # Already in the correct order...
                            pass
                        else:
                            raise Exception(u'Error: wrong sort order!')

                        if first is not None and second is not None:
                            if second[u'hash'] not in commitRewriteMap:
                                # Mark the commit for rewriting.
                                commitRewriteMap[second[u'hash']] = OrderedDict() # We need a set (meaning no duplicates) but we also need them to be in order so lets use an OrderedDict().
                                # Add the existing parrents
                                if u'parents' in second:
                                    for parent in second[u'parents']:
                                        commitRewriteMap[second[u'hash']][parent] = True
                            # Add the new parent
                            commitRewriteMap[second[u'hash']][first[u'hash']] = True
                            self.config.logger.info(u'  merge:     {0} as parent of {1}. tree {2}. parents {3}'.format(first[u'hash'][:8], second[u'hash'][:8], tree_hash[:8], [x[:8] for x in commitRewriteMap[second[u'hash']].keys()] ))

            # Reduce the aliasMap to only the items that are actually aliased and remove indirect links to the non-aliased commit (aliases of aliases).
            reducedAliasMap = {}
            for alias in aliasMap:
                if alias != aliasMap[alias]:
                    finalAlias = aliasMap[alias]
                    while finalAlias != aliasMap[finalAlias]:
                        if finalAlias not in aliasMap:
                            raise Exception("Invariant error! The aliasMap contains a value '{0}' but no key for it!".format(finalAlias))
                        if finalAlias == alias:
                            raise Exception("Invariant error! Circular reference in aliasMap for key '{0}'!".format(alias))
                        finalAlias = aliasMap[finalAlias]
                    reducedAliasMap[alias] = finalAlias

            # Write the reduced alias map to file.
            aliasFilePath = os.path.join(self.cwd, 'commit_alias_list.txt')
            self.config.logger.info("Writing the commit alias mapping to '{0}'.".format(aliasFilePath))
            with codecs.open(aliasFilePath, 'w', 'ascii') as f:
                for alias in reducedAliasMap:
                    original = reducedAliasMap[alias]

                    aliasState = None
                    if alias in commitStateMap:
                        aliasState = commitStateMap[alias]
                    originalState = None
                    if original in commitStateMap:
                        originalState = commitStateMap[original]
                    f.write('original: {original} -> alias: {alias}, original state: {original_state} -> alias state: {alias_state}\n'.format(original=original, original_state=originalState, alias=alias, alias_state=aliasState))

            self.config.logger.info("Remapping aliased commits.")
            # Remap the commitRewriteMap keys w.r.t. the aliases in the aliasMap
            discardedRewriteCommits = []
            for commitHash in commitRewriteMap:
                # Find the non-aliased commit
                if commitHash in reducedAliasMap:
                    if commitHash == reducedAliasMap[commitHash]:
                        raise Exception("Invariant error! The reducedAliasMap must not contain non-aliased commits!")

                    # Aliased commit.
                    discardedRewriteCommits.append(commitHash) # mark for deletion from map.
                    
                    h = reducedAliasMap[commitHash]
                    if h not in commitRewriteMap:
                        commitRewriteMap[h] = commitRewriteMap[commitHash]
                    else:
                        for parent in commitRewriteMap[commitHash]:
                            commitRewriteMap[h][parent] = True
                else:
                    Exception("Invariant falacy! aliasMap should contain every commit that we have processed.")

            # Delete aliased keys
            for commitHash in discardedRewriteCommits:
                del commitRewriteMap[commitHash]
            
            
            self.config.logger.info("Remapping aliased parent commits.")
            # Remap the commitRewriteMap values (parents) w.r.t. the aliases in the aliasMap
            for commitHash in commitRewriteMap:
                discardedParentCommits = []
                for parent in commitRewriteMap[commitHash]:
                    if parent in reducedAliasMap:
                        if parent == reducedAliasMap[parent]:
                            raise Exception("Invariant error! The reducedAliasMap must not contain non-aliased commits!")
                            
                        # Aliased parent commit.
                        discardedParentCommits.append(parent)

                        # Remap the parent
                        p = reducedAliasMap[parent]
                        commitRewriteMap[commitHash][p] = True # Add the non-aliased parent
                    else:
                        Exception("Invariant falacy! aliasMap should contain every commit that we have processed.")

                # Delete the aliased parents
                for parent in discardedParentCommits:
                    del commitRewriteMap[commitHash][parent]

            # Write parent filter shell script
            parentFilterPath = os.path.join(self.cwd, 'parent_filter.sh')
            self.config.logger.info("Writing parent filter '{0}'.".format(parentFilterPath))
            with codecs.open(parentFilterPath, 'w', 'ascii') as f:
                # http://www.tutorialspoint.com/unix/case-esac-statement.htm
                f.write('#!/bin/sh\n\n')
                f.write('case "$GIT_COMMIT" in\n')
                for commitHash in commitRewriteMap:
                    parentString = ''
                    for parent in commitRewriteMap[commitHash]:
                        parentString += '"{parent}" '.format(parent=parent)
                    f.write('    "{commit_hash}") echo "res="echo"; for x in {parent_str}; do res=\\"\\$res -p \\$(map "\\$x")\\"; done; \\$res"\n'.format(commit_hash=commitHash, parent_str=parentString))
                    f.write('    ;;\n')
                f.write('    *) echo "cat < /dev/stdin"\n') # If we don't have the commit mapping then just print out whatever we are given on stdin...
                f.write('    ;;\n')
                f.write('esac\n\n')

            # Write the commit filter shell script
            commitFilterPath = os.path.join(self.cwd, 'commit_filter.sh')
            self.config.logger.info("Writing commit filter '{0}'.".format(commitFilterPath))
            with codecs.open(commitFilterPath, 'w', 'ascii') as f:
                # http://www.tutorialspoint.com/unix/case-esac-statement.htm
                f.write('#!/bin/sh\n\n')
                f.write('case "$GIT_COMMIT" in\n')
                for commitHash in aliasMap:
                    if commitHash != aliasMap[commitHash]:
                        # Skip this commit
                        f.write('    "{0}") echo skip_commit \\$@\n'.format(commitHash))
                        f.write('    ;;\n')
                f.write('    *) echo git_commit_non_empty_tree \\$@;\n') # If we don't want to skip this commit then just commit it...
                f.write('    ;;\n')
                f.write('esac\n\n')

            stitchScriptPath = os.path.join(self.cwd, 'stitch_branches.sh')
            self.config.logger.info("Writing branch stitching script '{0}'.".format(stitchScriptPath))
            with codecs.open(stitchScriptPath, 'w', 'ascii') as f:
                # http://www.tutorialspoint.com/unix/case-esac-statement.htm
                f.write('#!/bin/sh\n\n')
                f.write('chmod +x {0}\n'.format(parentFilterPath))
                f.write('chmod +x {0}\n'.format(commitFilterPath))
                f.write('cd {0}\n'.format(self.config.git.repoPath))

                rewriteHeads = ""
                branchList = self.gitRepo.branch_list()
                for branch in branchList:
                    rewriteHeads += " {0}".format(branch.name)
                f.write("git filter-branch --parent-filter 'eval $({parent_filter})' --commit-filter 'eval $({commit_filter})' -- {rewrite_heads}\n".format(parent_filter=parentFilterPath, commit_filter=commitFilterPath, rewrite_heads=rewriteHeads))
                f.write('cd -\n')

            self.config.logger.info("Branch stitching script generated: {0}".format(stitchScriptPath))
            self.config.logger.info("To apply execute the following command:")
            self.config.logger.info("  chmod +x {0}".format(stitchScriptPath))

    # Start
    #   Begins a new AccuRev to Git conversion process discarding the old repository (if any).
    def Start(self, isRestart=False):
        global maxTransactions

        if not os.path.exists(self.config.git.repoPath):
            self.config.logger.error( "git repository directory '{0}' doesn't exist.".format(self.config.git.repoPath) )
            self.config.logger.error( "Please create the directory and re-run the script.".format(self.config.git.repoPath) )
            return 1
        
        if isRestart:
            self.config.logger.info( "Restarting the conversion operation." )
            self.config.logger.info( "Deleting old git repository." )
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
            self.gitBranchList = self.gitRepo.branch_list()
            if not isRestart:
                #self.gitRepo.reset(isHard=True)
                self.gitRepo.clean(force=True)
            
            acInfo = accurev.info()
            isLoggedIn = False
            if self.config.accurev.username is None:
                # When a username isn't specified we will use any logged in user for the conversion.
                isLoggedIn = accurev.ext.is_loggedin(infoObj=acInfo)
            else:
                # When a username is specified that specific user must be logged in.
                isLoggedIn = (acInfo.principal == self.config.accurev.username)
            
            doLogout = False
            if not isLoggedIn:
                # Login the requested user
                if accurev.ext.is_loggedin(infoObj=acInfo):
                    # Different username, logout the other user first.
                    logoutSuccess = accurev.logout()
                    self.config.logger.info("Accurev logout for '{0}' {1}".format(acInfo.principal, 'succeeded' if logoutSuccess else 'failed'))
    
                loginResult = accurev.login(self.config.accurev.username, self.config.accurev.password)
                if loginResult:
                    self.config.logger.info("Accurev login for '{0}' succeeded.".format(self.config.accurev.username))
                else:
                    self.config.logger.error("AccuRev login for '{0}' failed.\n".format(self.config.accurev.username))
                    self.config.logger.error("AccuRev message:\n{0}".format(loginResult.errorMessage))
                    return 1
                
                doLogout = True
            else:
                self.config.logger.info("Accurev user '{0}', already logged in.".format(acInfo.principal))
            
            # If this script is being run on a replica then ensure that it is up-to-date before processing the streams.
            accurev.replica.sync()

            if self.config.git.finalize is not None and self.config.git.finalize:
                self.StitchBranches()
            else:
                self.gitRepo.raw_cmd([u'git', u'config', u'--local', u'gc.auto', u'0'])
                self.ProcessStreams()
                self.gitRepo.raw_cmd([u'git', u'config', u'--local', u'--unset-all', u'gc.auto'])
              
            if doLogout:
                if accurev.logout():
                    self.config.logger.info( "Accurev logout successful." )
                else:
                    self.config.logger.error("Accurev logout failed.\n")
                    return 1
        else:
            self.config.logger.error( "Could not create git repository." )

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
        <!-- The stream-list is optional. If not given all streams are processed -->
        <!-- The branch-name attribute is also optional for each stream element. If provided it specifies the git branch name to which the stream will be mapped. -->
        <stream-list>
            <stream branch-name="some_branch">some_stream</stream>
            <stream>some_other_stream</stream>
        </stream-list>
    </accurev>
    <git repo-path="/put/the/git/repo/here" finalize="false" /> <!-- The system path where you want the git repo to be populated. Note: this folder should already exist. 
                                                                     The finalize attribute switches this script from converting accurev transactions to independent orphaned
                                                                     git branches to the "branch stitching" mode which should be activated only once the conversion is completed.
                                                                     Make sure to have a backup of your repo just in case. Once finalize is set to true this script will rewrite
                                                                     the git history in an attempt to recreate merge points.
                                                                -->
    <method>deep-hist</method> <!-- The method specifies what approach is taken to perform the conversion. Allowed values are 'deep-hist', 'diff' and 'pop'.
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
                               -->
    <logfile>accurev2git.log</logfile>
    <!-- The user maps are used to convert users from AccuRev into git. Please spend the time to fill them in properly. -->
    <usermaps>
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
            config.logger.error("No accurev username provided for auto-configuration.")
        return 1
    else:
        info = accurev.info()
        if info.principal != config.accurev.username:
            if config.accurev.password is None:
                config.logger.error("No accurev password provided for auto-configuration. You can either provide one on the command line, in the config file or just login to accurev before running the script.")
                return 1
            if not accurev.login(config.accurev.username, config.accurev.password):
                config.logger.error("accurev login for '{0}' failed.".format(config.accurev.username))
                return 1
        elif config.accurev.password is None:
            config.accurev.password = ''

    if config.accurev.depot is None:
        depots = accurev.show.depots()
        if depots is not None and depots.depots is not None and len(depots.depots) > 0:
            config.accurev.depot = depots.depots[0].name
            config.logger.info("No depot specified. Selecting first depot available: {0}.".format(config.accurev.depot))
        else:
            config.logger.error("Failed to find an accurev depot. You can specify one on the command line to resolve the error.")
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
        <stream-list>""".format(accurev_username=config.accurev.username, accurev_password=config.accurev.password, accurev_depot=config.accurev.depot, start_transaction=1, end_transaction="now"))

        if preserveConfig:
            for stream in config.accurev.streamMap:
                file.write("""
            <stream branch-name="{branch_name}">{stream_name}</stream>""".format(stream_name=stream, branch_name=config.accurev.streamMap[stream]))

        streams = accurev.show.streams(depot=config.accurev.depot)
        if streams is not None and streams.streams is not None:
            for stream in streams.streams:
                if not (preserveConfig and stream in config.accurev.streamMap):
                    file.write("""
            <stream branch-name="accurev/{stream_name}">{stream_name}</stream>""".format(stream_name=stream.name))
                    # TODO: Add depot and start/end transaction overrides for each stream...

        file.write("""
        </stream-list>
    </accurev>
    <git repo-path="{git_repo_path}" finalize="false" /> <!-- The system path where you want the git repo to be populated. Note: this folder should already exist.
                                                              The finalize attribute switches this script from converting accurev transactions to independent orphaned
                                                              git branches to the "branch stitching" mode which should be activated only once the conversion is completed.
                                                              Make sure to have a backup of your repo just in case. Once finalize is set to true this script will rewrite
                                                              the git history in an attempt to recreate merge points.
                                                         -->
    <method>{method}</method>
    <logfile>{log_filename}<logfile>
    <!-- The user maps are used to convert users from AccuRev into git. Please spend the time to fill them in properly. -->""".format(git_repo_path=config.git.repoPath, method=config.method, log_filename=config.logFilename))
        file.write("""
    <usermaps>
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

def TryGetAccurevUserlist(username, password):
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
            config.logger.info("Unmapped accurev users:")
            for user in missingUsers:
                config.logger.info("    {0}".format(user))

def SetConfigFromArgs(config, args):
    if args.accurevUsername is not None:
        config.accurev.username = args.accurevUsername
    if args.accurevPassword is not None:
        config.accurev.password = args.accurevPassword
    if args.accurevDepot is not None:
        config.accurev.depot    = args.accurevDepot
    if args.gitRepoPath is not None:
        config.git.repoPath     = args.gitRepoPath
    if args.finalize is not None:
        config.git.finalize     = args.finalize
    if args.conversionMethod is not None:
        config.method = args.conversionMethod
    if args.logFile is not None:
        config.logFilename      = args.logFile

def ValidateConfig(config):
    # Validate the program args and configuration up to this point.
    isValid = True
    if config.accurev.depot is None:
        config.logger.error("No AccuRev depot specified.\n")
        isValid = False
    if config.git.repoPath is None:
        config.logger.error("No Git repository specified.\n")
        isValid = False

    return isValid

def LoadConfigOrDefaults(configFilename):
    config = Config.fromfile(configFilename)

    if config is None:
        config = Config(accurev=Config.AccuRev(None), git=Config.Git(None), usermaps=[], logFilename=None)
        
    return config

def PrintConfigSummary(config):
    if config is not None:
        config.logger.info('Config info:')
        config.logger.info('  now: {0}'.format(datetime.now()))
        config.logger.info('  git')
        config.logger.info('    repo path: {0}'.format(config.git.repoPath))
        config.logger.info('    finalize:  {0}'.format(config.git.finalize))
        config.logger.info('  accurev:')
        config.logger.info('    depot: {0}'.format(config.accurev.depot))
        if config.accurev.streamMap is not None:
            config.logger.info('    stream list:')
            for stream in config.accurev.streamMap:
                config.logger.info('      - {0} -> {1}'.format(stream, config.accurev.streamMap[stream]))
        else:
            config.logger.info('    stream list: all included')
        config.logger.info('    start tran.: #{0}'.format(config.accurev.startTransaction))
        config.logger.info('    end tran.:   #{0}'.format(config.accurev.endTransaction))
        config.logger.info('    username: {0}'.format(config.accurev.username))
        config.logger.info('    command cache: {0}'.format(config.accurev.commandCacheFilename))
        config.logger.info('  method: {0}'.format(config.method))
        config.logger.info('  usermaps: {0}'.format(len(config.usermaps)))
        config.logger.info('  log file: {0}'.format(config.logFilename))
        config.logger.info('  verbose:  {0}'.format(config.logger.isDbgEnabled))
    
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
    parser.add_argument('-t', '--accurev-depot', dest='accurevDepot',        metavar='<accurev-depot>',     help="The AccuRev depot in which the streams that are being converted are located. This script currently assumes only one depot is being converted at a time.")
    parser.add_argument('-g', '--git-repo-path', dest='gitRepoPath',         metavar='<git-repo-path>',     help="The system path to an existing folder where the git repository will be created.")
    parser.add_argument('-f', '--finalize',      dest='finalize', action='store_const', const=True,         help="Finalize the git repository by creating branch merge points. This flag will trigger this scripts 'branch stitching' mode and should only be used once the conversion has been completed. It won't work as expected if the repo continues to be processed after this step. The script will attempt to collapse commits which are a result of a promotion into a parent stream where the diff between the parent and the child is empty. It will also try to link promotions correctly into a merge commit from the child into the parent.")
    parser.add_argument('-M', '--method', dest='conversionMethod', choices=['pop', 'diff', 'deep-hist'], metavar='<conversion-method>', help="Specifies the method which is used to perform the conversion. Can be either 'pop', 'diff' or 'deep-hist'. 'pop' specifies that every transaction is populated in full. 'diff' specifies that only the differences are populated but transactions are iterated one at a time. 'deep-hist' specifies that only the differences are populated and that only transactions that could have affected this stream are iterated.")
    parser.add_argument('-r', '--restart',    dest='restart', action='store_const', const=True, help="Discard any existing conversion and start over.")
    parser.add_argument('-v', '--verbose',    dest='debug',   action='store_const', const=True, help="Print the script debug information. Makes the script more verbose.")
    parser.add_argument('-L', '--log-file',   dest='logFile', metavar='<log-filename>',         help="Sets the filename to which all console output will be logged (console output is still printed).")
    parser.add_argument('-q', '--no-log-file', dest='disableLogFile',  action='store_const', const=True, help="Do not log info to the log file. Alternatively achieved by not specifying a log file filename in the configuration file.")
    parser.add_argument('-l', '--reset-log-file', dest='resetLogFile', action='store_const', const=True, help="Instead of appending new log info to the file truncate it instead and start over.")
    parser.add_argument('--example-config', nargs='?', dest='exampleConfigFilename', const=defaultExampleConfigFilename, default=None, metavar='<example-config-filename>', help="Generates an example configuration file and exits. If the filename isn't specified a default filename '{0}' is used. Commandline arguments, if given, override all options in the configuration file.".format(defaultExampleConfigFilename, configFilename))
    parser.add_argument('-m', '--check-missing-users', dest='checkMissingUsers', action='store_const', const=True, help="It will print a list of usernames that are in accurev but were not found in the usermap.")
    parser.add_argument('--auto-config', nargs='?', dest='autoConfigFilename', const=configFilename, default=None, metavar='<config-filename>', help="Auto-generate the configuration file from known AccuRev information. It is required that an accurev username and password are provided either in an existing config file or via the -u and -p options. If there is an existing config file it is backed up and only the accurev username and password will be copied to the new configuration file. If you wish to preserve the config but add more information to it then it is recommended that you use the --fixup-config option instead.")
    parser.add_argument('--fixup-config', nargs='?', dest='fixupConfigFilename', const=configFilename, default=None, metavar='<config-filename>', help="Fixup the configuration file by adding updated AccuRev information. It is the same as the --auto-config option but the existing configuration file options are preserved. Other command line arguments that are provided will override the existing configuration file options for the new configuration file.")
    
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
    
    config.logger.isDbgEnabled = ( args.debug == True )

    state = AccuRev2Git(config)
    
    if config.logFilename is not None and not args.disableLogFile:
        mode = 'a'
        if args.resetLogFile:
            mode = 'w'
        with codecs.open(config.logFilename, mode, 'utf-8') as f:
            f.write(u'{0}\n'.format(u" ".join(sys.argv)))
            state.config.logger.logFile = f
            state.config.logger.logFileDbgEnabled = ( args.debug == True )
    
            PrintConfigSummary(state.config)
            if args.checkMissingUsers:
                PrintMissingUsers(state.config)
            state.config.logger.info("Restart:" if args.restart else "Start:")
            state.config.logger.referenceTime = datetime.now()
            rv = state.Start(isRestart=args.restart)
    else:
        PrintConfigSummary(state.config)
        if args.checkMissingUsers:
            PrintMissingUsers(state.config)
        state.config.logger.info("Restart:" if args.restart else "Start:")
        state.config.logger.referenceTime = datetime.now()
        rv = state.Start(isRestart=args.restart)

    return rv
        
# ################################################################################################ #
# Script Start                                                                                     #
# ################################################################################################ #
if __name__ == "__main__":
    AccuRev2GitMain(sys.argv)
