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
from datetime import datetime
import time
import re
import types
import copy
import codecs

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
            
        def __init__(self, depot, username = None, password = None, startTransaction = None, endTransaction = None, streamMap = None):
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
                
                accurevElement = xmlElement.find('accurev')
                if accurevElement is not None:
                    accurevUsername = accurevElement.attrib.get('username')
                gitElement = xmlElement.find('git')
                if gitElement is not None:
                    gitName  = gitElement.attrib.get('name')
                    gitEmail = gitElement.attrib.get('email')
                
                return cls(accurevUsername, gitName, gitEmail)
            else:
                return None
            
        def __init__(self, accurevUsername, gitName, gitEmail):
            self.accurevUsername = accurevUsername
            self.gitName         = gitName
            self.gitEmail        = gitEmail
    
        def __repr__(self):
            str = "Config.UserMap(accurevUsername=" + repr(self.accurevUsername)
            str += ", gitName="                     + repr(self.gitName)
            str += ", gitEmail="                    + repr(self.gitEmail)
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

    def GetLastCommitHash(self):
        for i in xrange(0, 3):
            cmd = [u'git', u'log', u'-1', u'--format=format:%H']
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
        lastHistXml = self.gitRepo.notes.show(obj=commitHash, ref=AccuRev2Git.gitNotesRef_AccurevHistXml)
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

    def Commit(self, depot, transaction):
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
        committerDate = transaction.time
        lastCommitHash = self.GetLastCommitHash()
        commitHash = None
        if self.gitRepo.commit(messageFile=messageFilePath, committer=committer, committer_date=committerDate, author=committer, date=committerDate, allow_empty_message=True, gitOpts=[u'-c', u'core.autocrlf=false']):
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
    
    def FindNextChangeTransaction(self, streamName, startTrNumber, endTrNumber):
        # Iterate over transactions in order using accurev diff -a -i -v streamName -V streamName -t <lastProcessed>-<current iterator>
        nextTr = startTrNumber + 1
        diff = accurev.diff(all=True, informationOnly=True, verSpec1=streamName, verSpec2=streamName, transactionRange="{0}-{1}".format(startTrNumber, nextTr))

        # Note: This is likely to be a hot path. However, it cannot be optimized since a revert of a transaction would not show up in the diff even though the
        #       state of the stream was changed during that period in time. Hence to be correct we must iterate over the transactions one by one unless we have
        #       explicit knowlege of all the transactions which could affect us via some sort of deep history option...
        while nextTr <= endTrNumber and len(diff.elements) == 0:
            nextTr += 1
            diff = accurev.diff(all=True, informationOnly=True, verSpec1=streamName, verSpec2=streamName, transactionRange="{0}-{1}".format(startTrNumber, nextTr))
        
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

    def ProcessStream(self, depot, streamName, branchName, startTransaction, endTransaction):
        self.config.logger.info( "Processing {0}".format(streamName) )

        # Find the matching git branch
        branch = None
        for b in self.gitBranchList:
            if branchName == b.name:
                branch = b
                break
        tr = None
        commitHash = None
        if branch is None:
            # We are tracking a new stream:
            tr = self.GetFirstTransaction(depot=depot, streamName=streamName, startTransaction=startTransaction, endTransaction=endTransaction)
            if tr is not None:
                self.CreateCleanGitBranch(branchName=branchName)
                try:
                    destStream = tr.versions[0].virtualNamedVersion.stream
                except:
                    destStream = None
                self.config.logger.dbg( "{0} pop (init): {1} {2}{3}".format(streamName, tr.Type, tr.id, " to {0}".format(destStream) if destStream is not None else "") )
                accurev.pop(verSpec=streamName, location=self.gitRepo.path, isRecursive=True, timeSpec=tr.id, elementList='.')
                commitHash = self.Commit(depot=depot, transaction=tr)
                if not commitHash:
                    self.config.logger.dbg( "{0} first commit has failed. Is it an empty commit? Continuing...".format(streamName) )
                else:
                    self.config.logger.info( "stream {0}: tr. #{1} {2} into {3} -> commit {4} on {5}".format(streamName, tr.id, tr.Type, destStream if destStream is not None else 'unknown', commitHash[:8], branchName) )
            else:
                return
        else:
            # Get the last processed transaction
            self.ClearGitRepo()
            self.gitRepo.checkout(branchName=branchName)
            commitHash = self.GetLastCommitHash()
            hist = self.GetHistForCommit(commitHash=commitHash)
            if hist is None:
                self.config.logger.error("Repo in invalid state. Please reset this branch to a previous commit with valid notes.")
                self.config.logger.error("  e.g. git reset --soft {0}~1".format(branchName))
                return
            tr = hist.transactions[0]
            self.config.logger.dbg("{0}: last processed transaction was #{1}".format(streamName, tr.id))

        endTrHist = accurev.hist(depot=depot, timeSpec="{0}.1".format(endTransaction))
        endTr = endTrHist.transactions[0]
        self.config.logger.info("{0}: processing transaction range #{1} - #{2}".format(streamName, tr.id, endTr.id))

        while True:
            nextTr, diff = self.FindNextChangeTransaction(streamName=streamName, startTrNumber=tr.id, endTrNumber=endTr.id)

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
                accurev.pop(verSpec=streamName, location=self.gitRepo.path, isRecursive=True, timeSpec=tr.id, elementList='.')

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
            self.ProcessStream(depot=depot, streamName=stream, branchName=branch, startTransaction=self.config.accurev.startTransaction, endTransaction=self.config.accurev.endTransaction)

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
        
        if isRestart:
            self.config.logger.info( "Restarting the conversion operation." )
            self.config.logger.info( "Deleting old git repository." )
            git.delete(self.config.git.repoPath)
            
        # From here on we will operate from the git repository.
        self.cwd = os.getcwd()
        os.chdir(self.config.git.repoPath)
        
        if self.InitGitRepo(self.config.git.repoPath):
            self.gitRepo = git.open(self.config.git.repoPath)
            self.gitBranchList = self.gitRepo.branch_list()
            if not isRestart:
                #self.gitRepo.reset(isHard=True)
                self.gitRepo.clean(force=True)
            
            if accurev.login(self.config.accurev.username, self.config.accurev.password):
                self.config.logger.info( "Accurev login successful" )
                
                self.ProcessStreams()
                self.StitchBranches()
              
                # Restore the working directory.
                os.chdir(self.cwd)
                
                if accurev.logout():
                    self.config.logger.info( "Accurev logout successful" )
                    return 0
                else:
                    self.config.logger.error("Accurev logout failed\n")
                    return 1
            else:
                self.config.logger.error("AccuRev login failed.\n")
                return 1
        else:
            self.config.logger.error( "Could not create git repository." )
            
# ################################################################################################ #
# Script Functions                                                                                 #
# ################################################################################################ #
def DumpExampleConfigFile(outputFilename):
    exampleContents = """<accurev2git>
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
        end-transaction="500" >
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
        <map-user><accurev username="joe_bloggs" /><git name="Joe Bloggs" email="joe@bloggs.com" /></map-user>
    </usermaps>
</accurev2git>
"""
    with codecs.open(outputFilename, 'w') as file:
        file.write(exampleContents)

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
    defaultExampleConfigFilename = '{0}.example'.format(configFilename)
    
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
    parser.add_argument('-e', '--dump-example-config', nargs='?', dest='exampleConfigFilename', const='no-filename', default=None, metavar='<example-config-filename>', help="Generates an example configuration file and exits. If the filename isn't specified a default filename '{0}' is used. Commandline arguments, if given, override all options in the configuration file.".format(defaultExampleConfigFilename, configFilename))
    
    args = parser.parse_args()
    
    # Dump example config if specified
    if args.exampleConfigFilename is not None:
        if args.exampleConfigFilename == 'no-filename':
            exampleConfigFilename = defaultExampleConfigFilename
        else:
            exampleConfigFilename = args.exampleConfigFilename
        
        DumpExampleConfigFile(exampleConfigFilename)
        return 0
    
    # Load the config file
    config = Config.fromfile(filename=args.configFilename)
    if config is None:
        sys.stderr.write("Config file '{0}' not found.\n".format(args.configFilename))
        return 1
    elif config.git is not None:
        if not os.path.isabs(config.git.repoPath):
            config.git.repoPath = os.path.abspath(config.git.repoPath)

    # Set the overrides for in the configuration from the arguments
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
            state.config.logger.info("Restart:" if args.restart else "Start:")
            state.config.logger.referenceTime = time.clock()
            rv = state.Start(isRestart=args.restart)
    else:
        PrintConfigSummary(state.config)
        state.config.logger.info("Restart:" if args.restart else "Start:")
        state.config.logger.referenceTime = time.clock()
        rv = state.Start(isRestart=args.restart)

    return rv
        
# ################################################################################################ #
# Script Start                                                                                     #
# ################################################################################################ #
if __name__ == "__main__":
    AccuRev2GitMain(sys.argv)
