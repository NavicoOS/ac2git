#!/usr/bin/python2

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
import pytz

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
            if self.referenceTime is not None:
                outMessage = "{0: >6.2f}s: ".format(time.clock() - self.referenceTime)
            else:
                outMessage = ""
            
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
                
                return cls(depot, username, password, startTransaction, endTransaction, streamMap)
            else:
                return None
            
        def __init__(self, depot = None, username = None, password = None, startTransaction = None, endTransaction = None, streamMap = None):
            self.depot    = depot
            self.username = username
            self.password = password
            self.startTransaction = startTransaction
            self.endTransaction   = endTransaction
            self.streamMap = streamMap
    
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
            
    class Git(object):
        @classmethod
        def fromxmlelement(cls, xmlElement):
            if xmlElement is not None and xmlElement.tag == 'git':
                repoPath = xmlElement.attrib.get('repo-path')
                
                return cls(repoPath)
            else:
                return None
            
        def __init__(self, repoPath):
            self.repoPath = repoPath

        def __repr__(self):
            str = "Config.Git(repoPath=" + repr(self.repoPath)
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
            
            logFilename = None
            logFileElem = xmlRoot.find('logfile')
            if logFileElem is not None:
                logFilename = logFileElem.text

            usermaps = []
            userMapsElem = xmlRoot.find('usermaps')
            if userMapsElem is not None:
                for userMapElem in userMapsElem.findall('map-user'):
                    usermaps.append(Config.UserMap.fromxmlelement(userMapElem))
            
            return cls(accurev=accurev, git=git, usermaps=usermaps, logFilename=logFilename)
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

    def __init__(self, accurev = None, git = None, usermaps = None, logFilename = None):
        self.accurev     = accurev
        self.git         = git
        self.usermaps    = usermaps
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
    
    def __init__(self, config):
        self.config = config
        self.cwd = None
        self.gitRepo = None
        self.gitBranchList = None
 
    def DeletePath(self, path):
        if os.path.exists(path):
            if os.path.isdir(path):
                for root, dirs, files in os.walk(path, topdown=False):
                    for name in files:
                        p = os.path.join(root, name)
                        os.remove(p)
                    for name in dirs:
                        p = os.path.join(root, name)
                        os.rmdir(p)
            elif os.path.isfile(path):
                os.remove(path)
   
    def ClearGitRepo(self):
        # Delete everything except the .git folder from the destination (git repo)
        for root, dirs, files in os.walk(self.gitRepo.path, topdown=False):
            for name in files:
                path = os.path.join(root, name)
                if git.GetGitDirPrefix(path) is None:
                    os.remove(path)
            for name in dirs:
                path = os.path.join(root, name)
                if git.GetGitDirPrefix(path) is None:
                    os.rmdir(path)

    def PreserveEmptyDirs(self):
        for root, dirs, files in os.walk(self.gitRepo.path, topdown=True):
            for name in dirs:
                path = os.path.join(root, name).replace('\\','/')
                # Preserve empty directories that are not under the .git/ directory.
                if git.GetGitDirPrefix(path) is None and len(os.listdir(path)) == 0:
                    filename = os.path.join(path, '.gitignore')
                    with codecs.open(filename, 'w', 'utf-8') as file:
                        #file.write('# accurev2git.py preserve empty dirs\n')
                        pass
                    if not os.path.exists(filename):
                        self.config.logger.error("Failed to preserve directory. Couldn't create {0}.".format(filename))

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

    def AddAccurevHistNote(self, commitHash, ref, depot, transaction, isXml=False):
        # Write the commit notes consisting of the accurev hist xml output for the given transaction.
        # Note: It is important to use the depot instead of the stream option for the accurev hist command since if the transaction
        #       did not occur on that stream we will get the closest transaction that was a promote into the specified stream instead of an error!
        arHistXml = accurev.raw.hist(depot=depot, timeSpec="{0}.1".format(transaction.id), isXmlOutput=isXml)
        notesFilePath = os.path.join(self.cwd, 'notes_message')
        with codecs.open(notesFilePath, 'w', "utf-8") as notesFile:
            if arHistXml is None or len(arHistXml) == 0:
                self.config.logger.error('accurev hist returned an empty xml for transaction {0} (commit {1})'.format(transaction.id, commitHash))
                return False
            else:
                notesFile.write(arHistXml.decode("utf-8"))

        rv = self.gitRepo.notes.add(messageFile=notesFilePath, obj=commitHash, ref=ref, force=True)
        
        if rv is not None:
            os.remove(notesFilePath)
            self.config.logger.dbg( "Added accurev hist{0} note for {1}.".format(' xml' if isXml else '', commitHash) )
        else:
            self.config.logger.error( "Failed to add accurev hist{0} note for {1}".format(' xml' if isXml else '', commitHash) )
            self.config.logger.error(self.gitRepo.lastStderr)

        return rv

    def GetFirstTransaction(self, depot, streamName, startTransaction=None, endTransaction=None):
        # Get the stream creation transaction (mkstream). Note: The first stream in the depot doesn't have an mkstream transaction.
        mkstream = accurev.hist(stream=streamName, transactionKind="mkstream", timeSpec="now")
        tr = None
        if len(mkstream.transactions) == 0:
            self.config.logger.info( "The root stream has no mkstream transaction. Starting at transaction 1." )
            # the assumption is that the depot name matches the root stream name (for which there is no mkstream transaction)
            firstTr = accurev.hist(depot=depot, timeSpec="1")
            if len(firstTr.transactions) == 0:
                raise Exception("Error: assumption that the root stream has the same name as the depot doesn't hold. Aborting...")
            tr = firstTr.transactions[0]
        else:
            tr = mkstream.transactions[0]
            if len(mkstream.transactions) != 1:
                self.config.logger.error( "There seem to be multiple mkstream transactions for this stream... Using {0}".format(tr.id) )

        if startTransaction is not None:
            startTrHist = accurev.hist(depot=depot, timeSpec="{0}.1".format(startTransaction))
            startTr = startTrHist.transactions[0]
            if tr.id < startTr.id:
                self.config.logger.info( "The first transaction (#{0}) for strem {1} is earlier than the conversion start transaction (#{2}).".format(tr.id, streamName, startTr.id) )
                tr = startTr.transactions[0]
        if endTransaction is not None:
            endTrHist = accurev.hist(depot=depot, timeSpec="{0}.1".format(endTransaction))
            endTr = endTrHist.transactions[0]
            if endTr.id < tr.id:
                self.config.logger.info( "The first transaction (#{0}) for strem {1} is later than the conversion end transaction (#{2}).".format(tr.id, streamName, startTr.id) )
                tr = None

        return tr

    def GetLastCommitHash(self, branchName=None):
        for i in xrange(0, 3):
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
            self.config.logger.error("Failed to retrieve last git commit hash. Command `{0}` failed.".format(' '.join(cmd)))

        return commitHash

    def GetHistForCommit(self, commitHash):
        hist = None
        for i in xrange(0, 3):
            lastHistXml = self.gitRepo.notes.show(obj=commitHash, ref=AccuRev2Git.gitNotesRef_AccurevHistXml)
            if lastHistXml is not None:
                break

        if lastHistXml is not None:
            lastHistXml = lastHistXml.strip().encode('utf-8')
            hist = accurev.obj.History.fromxmlstring(lastHistXml)
        else:
            self.config.logger.error("Failed to load the last transaction for commit {0} from {1} notes.".format(commitHash, AccuRev2Git.gitNotesRef_AccurevHistXml))
            self.config.logger.error("  i.e git notes --ref={0} show {1}    - returned nothing.".format(AccuRev2Git.gitNotesRef_AccurevHistXml, commitHash))

        return hist

    def CreateCleanGitBranch(self, branchName):
        # Create the git branch.
        self.config.logger.info( "Creating {0}".format(branchName) )
        self.gitRepo.checkout(branchName=branchName, isOrphan=True)

        # Clear the index as it may contain the [start-point] info...
        self.gitRepo.rm(fileList=['.'], force=True, recursive=True)
        self.ClearGitRepo()

    def Commit(self, depot, transaction, isFirstCommit=False):
        self.PreserveEmptyDirs()

        # Add all of the files to the index
        self.gitRepo.add(force=True, all=True, gitOpts=[u'-c', u'core.autocrlf=false'])

        # Make the first commit
        messageFilePath = os.path.join(self.cwd, 'commit_message')
        with codecs.open(messageFilePath, 'w', "utf-8") as messageFile:
            if transaction.comment is None or len(transaction.comment) == 0:
                messageFile.write(' ') # White-space is always stripped from commit messages. See the git commit --cleanup option for details.
            else:
                # In git the # at the start of the line indicate that this line is a comment inside the message and will not be added.
                # So we will just add a space to the start of all the lines starting with a # in order to preserve them.
                messageFile.write(transaction.comment)
        
        committer = self.GetGitUserFromAccuRevUser(transaction.user)
        committerDate, committerTimezone = self.GetGitDatetime(accurevUsername=transaction.user, accurevDatetime=transaction.time)
        if not isFirstCommit:
            lastCommitHash = self.GetLastCommitHash()
        else:
            lastCommitHash = None
        commitHash = None

        # Since the accurev.obj namespace is populated from the XML output of accurev commands all times are given in UTC.
        # For now just force the time to be UTC centric but preferrably we would have this set-up to either use the local timezone
        # or allow each user to be given a timezone for geographically distributed teams...
        # The PyTz library should be considered for the timezone conversions. Do not roll your own...
        if self.gitRepo.commit(messageFile=messageFilePath, committer=committer, committer_date=committerDate, committer_tz=committerTimezone, author=committer, date=committerDate, tz=committerTimezone, allow_empty_message=True, gitOpts=[u'-c', u'core.autocrlf=false']):
            commitHash = self.GetLastCommitHash()
            if lastCommitHash != commitHash:
                self.config.logger.dbg( "Committed {0}".format(commitHash) )
                xmlNoteWritten = False
                for i in xrange(0, 3):
                    xmlNoteWritten = ( self.AddAccurevHistNote(commitHash=commitHash, ref=AccuRev2Git.gitNotesRef_AccurevHistXml, depot=depot, transaction=transaction, isXml=True) is not None )
                    if xmlNoteWritten:
                        break
                if not xmlNoteWritten:
                    # The XML output in the notes is how we track our conversion progress. It is not acceptable for it to fail.
                    # Undo the commit and print an error.
                    branchName = 'HEAD'
                    self.config.logger.error("Couldn't record last transaction state. Undoing the last commit {0} with `git reset --soft {1}^`".format(commitHash, branchName))
                    self.gitRepo.raw_cmd([u'git', u'reset', u'--soft', u'{0}^'.format(branchName)])

                    return None
                self.AddAccurevHistNote(commitHash=commitHash, ref=AccuRev2Git.gitNotesRef_AccurevHist, depot=depot, transaction=transaction, isXml=False)
            else:
                self.config.logger.error("Commit command returned True when nothing was committed...? Last commit hash {0} didn't change after the commit command executed.".format(lastCommitHash))
                return None
        elif "nothing to commit" in self.gitRepo.lastStdout:
            self.config.logger.error( "nothing to commit after populating transaction {0}...?".format(transaction.id) )
        else:
            self.config.logger.error( "Failed to commit transaction {0}".format(transaction.id) )
            self.config.logger.error( "\n{0}\n{1}\n".format(self.gitRepo.lastStdout, self.gitRepo.lastStderr) )
        os.remove(messageFilePath)

        return commitHash

    def TryDiff(self, streamName, firstTrNumber, secondTrNumber):
        for i in xrange(0, 3):
            diff = accurev.diff(all=True, informationOnly=True, verSpec1=streamName, verSpec2=streamName, transactionRange="{0}-{1}".format(firstTrNumber, secondTrNumber))
            if diff is not None:
                break
        if diff is None:
            self.config.logger.error( "accurev diff failed! stream: {0} time-spec: {1}-{2}".format(streamName, startTrNumber, endTrNumber) )
        return diff
    
    def FindNextChangeTransaction(self, streamName, startTrNumber, endTrNumber):
        # Iterate over transactions in order using accurev diff -a -i -v streamName -V streamName -t <lastProcessed>-<current iterator>
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
        
        return (nextTr, diff)

    def DeleteDiffItemsFromRepo(self, diff):
        # Delete all of the files which are even mentioned in the diff so that we can do a quick populate (wouth the overwrite option)
        deletedPathList = []
        for element in diff.elements:
            for change in element.changes:
                for stream in [ change.stream1, change.stream2 ]:
                    if stream is not None and stream.name is not None:
                        name = stream.name.replace('\\', '/').lstrip('/')
                        path = os.path.join(self.gitRepo.path, name)
                        self.DeletePath(path)
                        deletedPathList.append(path)

        return deletedPathList 

    def TryPop(self, streamName, transaction):
        for i in xrange(0, 3):
            popResult = accurev.pop(verSpec=streamName, location=self.gitRepo.path, isRecursive=True, timeSpec=transaction.id, elementList='.')
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

    def ProcessStream(self, depot, streamName, branchName, startTransaction, endTransaction):
        self.config.logger.info( "Processing {0}".format(streamName) )

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
            tr = self.GetFirstTransaction(depot=depot, streamName=streamName, startTransaction=startTransaction, endTransaction=endTransaction)
            if tr is not None:
                if branch is None:
                    self.CreateCleanGitBranch(branchName=branchName)
                try:
                    destStream = tr.versions[0].virtualNamedVersion.stream
                except:
                    destStream = None
                self.config.logger.dbg( "{0} pop (init): {1} {2}{3}".format(streamName, tr.Type, tr.id, " to {0}".format(destStream) if destStream is not None else "") )
                popResult = self.TryPop(streamName=streamName, transaction=tr)
                if not popResult:
                    return (None, None)
                
                commitHash = self.Commit(depot=depot, transaction=tr, isFirstCommit=True)
                if not commitHash:
                    self.config.logger.dbg( "{0} first commit has failed. Is it an empty commit? Continuing...".format(streamName) )
                else:
                    self.config.logger.info( "stream {0}: tr. #{1} {2} into {3} -> commit {4} on {5}".format(streamName, tr.id, tr.Type, destStream if destStream is not None else 'unknown', commitHash[:8], branchName) )
            else:
                return (None, None)
        else:
            # Get the last processed transaction
            commitHash = self.GetLastCommitHash(branchName=branchName)
            hist = self.GetHistForCommit(commitHash=commitHash)

            if hist is None:
                self.config.logger.error("Repo in invalid state. Please reset this branch to a previous commit with valid notes.")
                self.config.logger.error("  e.g. git reset --hard {0}~1".format(branchName))
                return (None, None)

            tr = hist.transactions[0]
            self.config.logger.dbg("{0}: last processed transaction was #{1}".format(streamName, tr.id))

        endTrHist = accurev.hist(depot=depot, timeSpec="{0}.1".format(endTransaction))
        endTr = endTrHist.transactions[0]
        self.config.logger.info("{0}: processing transaction range #{1} - #{2}".format(streamName, tr.id, endTr.id))

        while True:
            nextTr, diff = self.FindNextChangeTransaction(streamName=streamName, startTrNumber=tr.id, endTrNumber=endTr.id)
            if nextTr is None or diff is None:
                self.config.logger.dbg( "FindNextChangeTransaction(streamName='{0}', startTrNumber={1}, endTrNumber={2}) failed!".format(streamName, startTrNumber, endTrNumber) )
                return (None, None)

            self.config.logger.dbg( "{0}: next transaction {1}".format(streamName, nextTr) )
            if nextTr <= endTr.id:
                # Right now nextTr is an integer representation of our next transaction.
                # Delete all of the files which are even mentioned in the diff so that we can do a quick populate (wouth the overwrite option)
                deletedPathList = self.DeleteDiffItemsFromRepo(diff=diff)

                # The accurev hist command here must be used with the depot option since the transaction that has affected us may not
                # be a promotion into the stream we are looking at but into one of its parent streams. Hence we must query the history
                # of the depot and not the stream itself.
                hist = accurev.hist(depot=depot, timeSpec="{0}.1".format(nextTr))
                tr = hist.transactions[0]

                # Populate
                destStream = self.GetDestinationStreamName(history=hist)
                self.config.logger.dbg( "{0} pop: {1} {2}{3}".format(streamName, tr.Type, tr.id, " to {0}".format(destStream) if destStream is not None else "") )
                popResult = self.TryPop(streamName=streamName, transaction=tr)
                if not popResult:
                    return (None, None)

                # Commit
                commitHash = self.Commit(depot=depot, transaction=tr)
                if commitHash is None:
                    if"nothing to commit" in self.gitRepo.lastStdout:
                        self.config.logger.error( "diff info ({0} elements):".format(len(diff.elements)) )
                        for element in diff.elements:
                            for change in element.changes:
                                self.config.logger.error( "  what changed: {0}".format(change.what) )
                                self.config.logger.error( "  original: {0}".format(change.stream1) )
                                self.config.logger.error( "  new:      {0}".format(change.stream2) )
                        self.config.logger.error( "deleted {0} files:".format(len(deletedPathList)) )
                        for p in deletedPathList:
                            self.config.logger.error( "  {0}".format(p) )
                        self.config.logger.info("Non-fatal error. Continuing.")
                    else:
                        break # Early return from processing this stream. Restarting should clean everything up.
                else:
                    self.config.logger.info( "stream {0}: tr. #{1} {2} into {3} -> commit {4} on {5}".format(streamName, tr.id, tr.Type, destStream if destStream is not None else 'unknown', commitHash[:8], branchName) )
            else:
                self.config.logger.info( "Reached end transaction #{0} for {1} -> {2}".format(endTr.id, streamName, branchName) )
                break

        return (tr, commitHash)

    def ProcessStreams(self):
        for stream in self.config.accurev.streamMap:
            branch = self.config.accurev.streamMap[stream]
            depot  = self.config.accurev.depot
            tr, commitHash = self.ProcessStream(depot=depot, streamName=stream, branchName=branch, startTransaction=self.config.accurev.startTransaction, endTransaction=self.config.accurev.endTransaction)
            if tr is None or commitHash is None:
                self.config.logger.error( "Error while processing stream {0}, branch {1}".format(streamName, branch) )

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

    def GetDestinationStreamName(self, history=None, transaction=None):
        destStream = None
        if history is not None:
            if len(history.streams) == 1:
                return history.streams[0].name
            else:
                try:
                    history.transactions[0].versions[0].virtualNamedVersion.stream
                except:
                    pass
        elif transaction is not None:
            try:
                transaction.versions[0].virtualNamedVersion.stream
            except:
                pass
        return destStream

    def GetStreamNameFromBranch(self, branchName):
        if branchName is not None:
            for stream in self.config.accurev.streamMap:
                if branchName == self.config.accurev.streamMap[stream]:
                    return stream
        return None

    # Arranges the stream1 and stream2 into a tuple of (parent, child) according to accurev information
    def GetParentChild(self, stream1, stream2, timeSpec=u'now'):
        parent = None
        child = None
        if stream1 is not None and stream2 is not None:
            #print ("self.GetParentChild(stream1={0}, stream2={1}, timeSpec={2}".format(str(stream1), str(stream2), str(timeSpec)))
            stream1Children = accurev.show.streams(depot=self.config.accurev.depot, stream=stream1, timeSpec=timeSpec, listChildren=True)
            stream2Children = accurev.show.streams(depot=self.config.accurev.depot, stream=stream2, timeSpec=timeSpec, listChildren=True)

            found = False
            for stream in stream1Children.streams:
                if stream.name == stream2:
                    parent = stream1
                    child = stream2
                    found = True
                    break
            if not found:
                for stream in stream2Children.streams:
                    if stream.name == stream1:
                        parent = stream2
                        child = stream1
                        break
        return (parent, child)

    def StitchBranches(self):
        branchRevMap = git_stitch.GetBranchRevisionMap(self.config.git.repoPath)
        
        self.config.logger.info("Stitching git branches")

        commitRewriteMap = OrderedDict()
        if branchRevMap is not None:
            for tree_hash in branchRevMap:
                if len(branchRevMap[tree_hash]) > 1:
                    # We should make some decisions about how to merge these commits which reference the same tree
                    # and what their ideal parents are. Once we decide we will write it to file in a nice bash friendly
                    # format and use the git filter-branch --parent-filter ... to fix it all up!
                    inOrder = sorted(branchRevMap[tree_hash], key=lambda x: int(x[u'committer'][u'time']))
                    #print(u'tree: {0}'.format(tree_hash))
                    
                    for i in xrange(0, len(inOrder) - 1):
                        first = inOrder[i]
                        second = inOrder[i + 1]
                        
                        firstTime = int(first[u'committer'][u'time'])
                        secondTime = int(second[u'committer'][u'time'])
    
                        wereSwapped = False
                        if firstTime == secondTime:
                            # Normally both commits would have originated from the same transaction. However, if not, let's try and order them by transaciton number first.
                            firstHist = self.GetHistForCommit(commitHash=first[u'hash'])
                            secondHist = self.GetHistForCommit(commitHash=second[u'hash'])

                            if firstHist.transactions[0].id < secondHist.transactions[0].id:
                                # This should really never be true given that AccuRev is centralized and synchronous and that firstTime == secondTime above...
                                pass # Already in the correct order
                            elif firstHist.transactions[0].id > secondHist.transactions[0].id:
                                # This should really never be true given that AccuRev is centralized and synchronous and that firstTime == secondTime above...
                                # Swap them
                                wereSwapped = True
                                first, second = second, first
                                firstHist, secondHist = secondHist, firstHist
                            else:
                                # The same transaction affected both commits (the id's are unique in accurev)...
                                # Must mean that they are substreams of eachother or sibling substreams of a third stream. Let's see which it is.

                                # Get the information for the first stream
                                firstStream = None
                                firstBranches = self.gitRepo.branch_list(containsCommit=first[u'hash']) # This should only ever return one branch since we are processing things in order...
                                if firstBranches is not None and len(firstBranches) == 1:
                                    firstBranch = firstBranches[0]
                                    firstStream = self.GetStreamNameFromBranch(branchName=firstBranch.name)
                                else:
                                    # ERROR: We cannot determine what branch this commit came from and we don't include any information about which stream we were processing against the commit.
                                    #        This means that we are processing items below a merge point and we shouldn't do that...
                                    # SKIP!
                                    self.config.logger.error("Branch stitching error: incorrect state. Commit {0} can be reached from multiple branches.".format(first[u'hash'][:8]))
                                    continue

                                # Get the information for the second stream
                                secondStream = None
                                secondBranches = self.gitRepo.branch_list(containsCommit=second[u'hash'])
                                if secondBranches is not None and len(secondBranches) == 1:
                                    secondBranch = secondBranches[0]
                                    secondStream = self.GetStreamNameFromBranch(branchName=secondBranch.name)
                                else:
                                    # ERROR: We cannot determine what branch this commit came from and we don't include any information about which stream we were processing against the commit.
                                    #        This means that we are processing items below a merge point and we shouldn't do that...
                                    # SKIP!
                                    self.config.logger.error("Branch stitching error: incorrect state. Commit {0} can be reached from multiple branches.".format(second[u'hash'][:8]))
                                    continue

                                # Find which one is the parent of the other. They must be inline since they were affected by the same transaction (since the times match)
                                parentStream, childStream = self.GetParentChild(stream1=firstStream, stream2=secondStream, timeSpec=firstHist.transactions[0].id)
                                if parentStream is None and childStream is None:
                                    # The two streams are unrelated and are probably substreams of a third stream. Hence we should not merge them!
                                    self.config.logger.info(u'  unrelated: {0} ({1}/{2}) is equiv. to {3} ({4}/{5}). tree {6}.'.format(first[u'hash'][:8], firstStream, firstHist.transactions[0].id, second[u'hash'][:8], secondStream, secondHist.transactions[0].id, tree_hash[:8]))
                                    first, second = None, None
                                    # This thrid stream should be listed as the destination stream for the transaction in our firstHist and secondHist so
                                    # we can at least do a sanity check...
                                    # TODO: do the sanity check!!!
                                    pass
                                elif parentStream == firstStream:
                                    pass # They are already in the correct order.
                                elif parentStream == secondStream:
                                    # Swap them
                                    wereSwapped = True
                                    first, second = second, first
                                    firstHist, secondHist = secondHist, firstHist
                                    firstStream, secondStream = secondStream, firstStream

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
                            self.config.logger.info(u'  merge:     {0} as parent of {1}. tree {2}. parents {3}'.format(first[u'hash'][:8], second[u'hash'][:8], tree_hash[:8], [x[:8] for x in commitRewriteMap[second[u'hash']].iterkeys()] ))

            # Write shell script
            parentFilterPath = os.path.join(self.cwd, 'parent_filter.sh')
            with codecs.open(parentFilterPath, 'w', 'ascii') as f:
                # http://www.tutorialspoint.com/unix/case-esac-statement.htm
                f.write('#!/bin/sh\n\n')
                f.write('case "$GIT_COMMIT" in\n')
                for commitHash in commitRewriteMap:
                    parentString = ''
                    for parent in commitRewriteMap[commitHash]:
                        parentString += '-p {0}'.format(parent)
                    f.write('    "{0}") echo "{1}"\n'.format(commitHash, parentString))
                    f.write('    ;;\n')
                f.write('    *) cat < /dev/stdin\n') # If we don't have the commit mapping then just print out whatever we are given on stdin...
                f.write('    ;;\n')
                f.write('esac\n\n')

            self.config.logger.info("Branch stitching script generated: {0}".format(parentFilterPath))
            self.config.logger.info("To apply execute the following commands:")
            self.config.logger.info("  chmod +x {0}".format(parentFilterPath))
            self.config.logger.info("  cd {0}".format(self.config.git.repoPath))
            self.config.logger.info("  git filter-branch --parent-filter {0} --prune-empty".format(parentFilterPath))
            self.config.logger.info("  rm {0}".format(parentFilterPath))
            self.config.logger.info("  cd -")

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
            isLoggedIn = (acInfo.principal == self.config.accurev.username)
    
            # Login the requested user
            if not isLoggedIn:
                if accurev.ext.is_loggedin(infoObj=acInfo):
                    # Different username, logout the other user first.
                    logoutSuccess = accurev.logout()
                    self.config.logger.info("Accurev logout for '{0}' {1}".format(acInfo.principal, 'succeeded' if logoutSuccess else 'failed'))
    
                if accurev.login(self.config.accurev.username, self.config.accurev.password):
                    self.config.logger.info("Accurev login for '{0}' succeeded.".format(self.config.accurev.username))
                else:
                    self.config.logger.error("AccuRev login for '{0}' failed.\n", self.config.accurev.username)
                    return 1
            else:
                self.config.logger.info("Accurev user '{0}', already logged in.".format(acInfo.principal))
            
            # If this script is being run on a replica then ensure that it is up-to-date before processing the streams.
            accurev.replica.sync()

            self.ProcessStreams()
            #self.StitchBranches()
              
            if not isLoggedIn:
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
            username:             The username that will be used to log into AccuRev and retrieve and populate the history
            password:             The password for the given username. Note that you can pass this in as an argument which is safer and preferred!
            depot:                The depot in which the stream/s we are converting are located
            start-transaction:    The conversion will start at this transaction. If interrupted the next time it starts it will continue from where it stopped.
            end-transaction:      Stop at this transaction. This can be the keword "now" if you want it to convert the repo up to the latest transaction.
    -->
    <accurev 
        username="joe_bloggs" 
        password="joanna" 
        depot="Trunk" 
        start-transaction="1" 
        end-transaction="now" >
        <!-- The stream-list is optional. If not given all streams are processed -->
        <!-- The branch-name attribute is also optional for each stream element. If provided it specifies the git branch name to which the stream will be mapped. -->
        <stream-list>
            <stream branch-name="some_branch">some_stream</stream>
            <stream>some_other_stream</stream>
        </stream-list>
    </accurev>
    <git repo-path="/put/the/git/repo/here" /> <!-- The system path where you want the git repo to be populated. Note: this folder should already exist. -->
    <logfile>accurev2git.log<logfile>
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
    -->
    <accurev 
        username="{accurev_username}" 
        password="{accurev_password}" 
        depot="{accurev_depot}" 
        start-transaction="{start_transaction}" 
        end-transaction="{end_transaction}" >
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
            <stream branch-name="accurev/{stream_name}" depot="{stream_depot}">{stream_name}</stream>""".format(stream_name=stream.name, stream_depot=stream.depotName))

        file.write("""
        </stream-list>
    </accurev>
    <git repo-path="{git_repo_path}" /> <!-- The system path where you want the git repo to be populated. Note: this folder should already exist. -->
    <logfile>{log_filename}<logfile>
    <!-- The user maps are used to convert users from AccuRev into git. Please spend the time to fill them in properly. -->""".format(git_repo_path=config.git.repoPath, log_filename=config.logFilename))
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
    if args.logFile is not None:
        config.logFilename      = args.logFile

def ValidateConfig(config):
    # Validate the program args and configuration up to this point.
    isValid = True
    if config.accurev.username is None:
        config.logger.error("No AccuRev username specified.\n")
        isValid = False
    if config.accurev.password is None:
        config.logger.error("No AccuRev password specified.\n")
        isValid = False
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
    parser.add_argument('-u', '--accurev-username',  dest='accurevUsername', metavar='<accurev-username>', help="The username which will be used to retrieve and populate the history from AccuRev.")
    parser.add_argument('-p', '--accurev-password',  dest='accurevPassword', metavar='<accurev-password>', help="The password for the provided accurev username.")
    parser.add_argument('-t', '--accurev-depot', dest='accurevDepot',        metavar='<accurev-depot>',    help="The AccuRev depot in which the streams that are being converted are located. This script currently assumes only one depot is being converted at a time.")
    parser.add_argument('-g', '--git-repo-path', dest='gitRepoPath',         metavar='<git-repo-path>',    help="The system path to an existing folder where the git repository will be created.")
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
        earlyReturnCode = DumpExampleConfigFile(exampleConfigFilename)
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
            state.config.logger.referenceTime = time.clock()
            rv = state.Start(isRestart=args.restart)
    else:
        PrintConfigSummary(state.config)
        if args.checkMissingUsers:
            PrintMissingUsers(state.config)
        state.config.logger.info("Restart:" if args.restart else "Start:")
        state.config.logger.referenceTime = time.clock()
        rv = state.Start(isRestart=args.restart)

    return rv
        
# ################################################################################################ #
# Script Start                                                                                     #
# ################################################################################################ #
if __name__ == "__main__":
    AccuRev2GitMain(sys.argv)
