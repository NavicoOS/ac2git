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
import shutil
import subprocess
import xml.etree.ElementTree as ElementTree
import datetime
import time
import re
import types

import accurev
import git

# ################################################################################################ #
# Script Globals.                                                                                  #
# ################################################################################################ #
state = None
maxCmdItems = 25

# ################################################################################################ #
# Script Core. Git helper functions                                                                #
# ################################################################################################ #
branchList = None
def CheckoutBranch(gitRepo, branchName):
    global branchList
    
    if branchList is None:
        branchList = gitRepo.branch_list()
    
    if branchList is None or len(branchList) == 0:
        # If we are here, we are in a state of Initial Commit and there are no branches to speak of yet.
        status = gitRepo.status()
        if status is not None and status.branch != branchName:
            gitRepo.checkout(branchName=branchName, isNewBranch=True)
        branch = git.GitBranchListItem(name=branchName, shortHash=None, remote=None, shortComment="Initial Commit", isCurrent=True)
        branchList = [ branch ]
        
        return branch
    
    isNewBranch = True
    for branch in branchList:
        if branch.name == branchName:
            if branch.isCurrent:
               return branch 
            else:
                isNewBranch = False
                break
    gitRepo.checkout(branchName=branchName, isNewBranch=isNewBranch)
    branchList = gitRepo.branch_list()
    
    for branch in branchList:
        if branch.isCurrent:
            return branch
    
    raise Exception("CheckoutBranch: Unexpected termination, there must be at least one current git branch!")

# LocalElementPath converts a depot relative accurev path into either an absolute or a relative path
# i.e. removes the \.\ from the front and sensibly converts the path...
def LocalElementPath(accurevDepotElementPath, gitRepoPath=None, isAbsolute=False):
    # All accurev depot paths start with \.\ or /./ so strip that before joining.
    relpath = accurevDepotElementPath[3:]
    if isAbsolute:
        if gitRepoPath is not None:
            return os.path.join(gitRepoPath, relpath)
        else:
            return os.path.abspath(relpath)
    return relpath

# Gets the stream name/id from the virtual version of one of the elements.
def GetTransactionDestStream(transaction):
    if transaction is not None:
        if transaction.versions is not None and len(transaction.versions) > 0:
            return transaction.versions[0].virtualNamedVersion.stream
    return None

# Gets all the elements (as path strings) that are participating in a transaction
def GetTransactionElements(transaction, skipDirs=True):
    if transaction is not None:
        if transaction.versions is not None:
            elemList = []
            for version in transaction.versions:
                # Populate it only if it is not a directory. (note: symlinks can be classed as directories even though they are not)
                if not skipDirs or not version.dir: # or version.elemType == "elink" or version.elemType == "slink"
                    elemList.append(version.path)
            #    else:
            #        state.config.logger.dbg( "GetTransactionElements: skipping dir [{0}] {1}".format(version.dir, version.path) )
            #
            #state.config.logger.dbg( "GetTransactionElements:  {0}".format(elemList) )
            
            return elemList
        else:
            state.config.logger.dbg( "GetTransactionElements: transaction.versions is None" )
    else:
        state.config.logger.dbg( "GetTransactionElements: transaction is None" )

    return None

def DiffChangeWhatPriority(changeWhat):
    if changeWhat == "defuncted":
        return 4
    elif changeWhat == "moved":
        return 3
    elif changeWhat == "version":
        return 2
    elif changeWhat == "created":
        # warning: this element doesn't have a change.stream1!
        return 1
    elif changeWhat == "eid":
        # Special case...? Seen with symlinks...
        return 0
    else:
        return 0
    
def SplitElementsByStatusViaDiff(stream, transactionId, elemList):
    lastTransactionId = state.GetLastAccuRevTransaction()
    
    diffResult = accurev.diff(all=True, informationOnly=True, verSpec1=stream, verSpec2=stream, transactionRange="{0}-{1}".format(lastTransactionId, transactionId))
    
    # All items in the elem list are depot relative path names. Here we convert them to absolute paths
    if len(elemList) > 0:
        depotRelativePrefix = elemList[0][:2]
        
        if diffResult is not None:
            memberList = []
            defunctList = []
            for diffElem in diffResult.elements:
                mostRecentChange = None
                # Pick the most resent or the most pressing change!
                for change in diffElem.changes:
                    if change is not None and change.stream2 is not None and change.stream2.version is not None: # Special check change.stream2.version is for eid changes... Skip please!
                        if mostRecentChange is None or mostRecentChange.stream2.version.version < change.stream2.version.version:
                            mostRecentChange = change
                        elif mostRecentChange.stream2.version.version == change.stream2.version.version:
                            if DiffChangeWhatPriority(mostRecentChange.what) < DiffChangeWhatPriority(change.what):
                                mostRecentChange = change
                    
                stream2Name = "{0}{1}".format(depotRelativePrefix, mostRecentChange.stream2.name)
                stream1Name = None
                if mostRecentChange.stream1 is not None:
                    stream1Name = "{0}{1}".format(depotRelativePrefix, mostRecentChange.stream1.name)
                
                if stream2Name in elemList:
                    if mostRecentChange.what == "version":
                        memberList.append(stream2Name)
                    elif mostRecentChange.what == "defuncted":
                        defunctList.append(stream2Name)
                    elif mostRecentChange.what == "created":
                        # warning: this element doesn't have a mostRecentChange.stream1!
                        memberList.append(stream2Name)
                    elif mostRecentChange.what == "moved":
                        defunctList.append(stream1Name)
                        memberList.append(stream2Name)
                    else:
                        raise Exception("SplitElementsByStatusViaDiff: Unexpected [{0}] value for Change element's What attribute found!")
            
            return memberList, defunctList
        raise Exception("SplitElementsByStatusViaDiff: could not diff stream {0} transaction range {1}-{2}".format(stream, lastTransactionId, transactionId))
    return [], []
    
def SplitElementsByStatusViaStat(stream, transactionId, elemList):
    if len(elemList) > maxCmdItems:
        tmpFilename = '.stat_list'
        tmpFile = open(name=tmpFilename, mode='w')
        for elem in elemList:
            tmpFile.write('{0}\n'.format(elem))
        tmpFile.close()
        info = accurev.stat(stream=stream, timeSpec=transactionId, listFile=tmpFilename)
        os.remove(tmpFilename)
    elif len(elemList) > 0:
        info = accurev.stat(stream=stream, timeSpec=transactionId, elementList=elemList)
    else:
        return ([], [])

    if info is not None:
        defunctList = []
        nonDefunctList = []
        
        for elem in elemList:
            # Try and find the element in the defunct element list
            isDefunct = False
            for infoElem in info.elements:
                if infoElem.location == elem:
                    isDefunct = ("(defunct)" in infoElem.statusList)
                    break
            # Add it to the correct sublist
            if isDefunct:
                defunctList.append(elem)
            else:
                nonDefunctList.append(elem)
        
        return nonDefunctList, defunctList
    else:
        #state.config.logger.dbg( "SplitElementsByStatusViaStat: info is None" )
        #return elemList, []
        raise Exception("Error in SplitElementsByStatusViaStat: accurev.stat(stream={0}, timeSpec={1}, elementList={2}) returned None".format(stream, transactionId, elemList))

def PopAccurevTransaction(stream, transactionId, elemList, dstPath='.', skipDirs=True):
    state.config.logger.dbg( "pop #{0}: {1} files to {2}".format(transactionId, len(elemList), dstPath) )
    
    # If the element list is large, use the list file feature of AccuRev
    if len(elemList) > maxCmdItems:
        tmpFilename = '.pop_list'
        tmpFile = open(name=tmpFilename, mode='w')
        for elem in elemList:
            tmpFile.write('{0}\n'.format(elem))
        tmpFile.close()
        rv = accurev.pop(isOverride=True, verSpec=stream, location=dstPath, timeSpec=transactionId, listFile=tmpFilename)
        os.remove(tmpFilename)
        return rv
    # Otherwise just pass the element list directly
    else:
        return accurev.pop(isOverride=True, verSpec=stream, location=dstPath, timeSpec=transactionId, elementList=elemList)
    
# CatAccurevFile is used if you only want to get the file contents of the accurev file. The only
# difference between cat and co/pop is that cat doesn't set the executable bits on Linux.
def CatAccurevFile(elementId=None, depotName=None, verSpec=None, element=None, gitPath=None, isBinary=False):
    # TODO: Change back to using accurev pop -v <stream-name> -t <transaction-number> -L <git-repo-path> <element-list>
    filePath = LocalElementPath(accurevDepotElementPath=element, gitRepoPath=gitPath)
    fileDir  = os.path.dirname(filePath)
    if fileDir is not None and len(fileDir) > 0 and not os.path.exists(fileDir):
        os.makedirs(fileDir)
    success = accurev.cat(elementId=elementId, depotName=depotName, verSpec=str(verSpec), outputFilename=filePath)
    if success is None:
        return False
    return True

def GitCommit(gitRepo, branch, author, date, message, addList, rmList):
    if branch is not None:
        CheckoutBranch(gitRepo, branch)
    
    maxItems = maxCmdItems
    
    # Add the items in groups of maxItems so that we don't exceed the max number of command line args.
    while True:
        tmpList = addList[:min(maxItems, len(addList))]
        addList = addList[min(maxItems, len(addList)):]
        if len(tmpList) > 0:
            gitRepo.add(tmpList, force=True)
        else:
            break
    
    # Remove the items in groups of maxItems so that we don't exceed the max number of command line args.
    while True:
        tmpList = rmList[:min(maxItems, len(rmList))]
        rmList  = rmList[min(maxItems, len(rmList)):]
        if len(tmpList) > 0:
            gitRepo.rm(tmpList)
        else:
            break
    
    return gitRepo.commit(message=message, author=author, date=date)

# GetGitUserDetails returns the git information from the config mapping for the given accurev user.
def GetGitUserDetails(accurevUsername):
    if accurevUsername is not None:
        for usermap in state.config.usermaps:
            if usermap.accurevUsername == accurevUsername:
                return (usermap.gitName, usermap.gitEmail)
                
    return None, None

# AccurevUser2GitAuthor converts an accurev username into a git username by using the provided mapping and
# it also handles usernames which aren't in the mapping by creating one for them.
def AccurevUser2GitAuthor(accurevUser):
    name, email = GetGitUserDetails(accurevUser)
    if name is None:
        name = accurevUser
    if email is None:
        email = "{0}@no-email.com".format(accurevUser)
    return "{0} <{1}>".format(name, email)

# AccurevComment2GitMessage tags the commit message from AccuRev with information regarding the transaction
# from which the given commit has been generated.
def AccurevComment2GitMessage(accurevTransaction):
    transTag = "[AccuRev Transaction #{0}]".format(accurevTransaction.id)
    comment = "[no original comment]"
    if accurevTransaction.comment is not None:
        comment = accurevTransaction.comment
    return "{0}\n\n{1}".format(comment, transTag)

# ################################################################################################ #
# Script Core. AccuRev transaction to Git conversion handlers.                                     #
# ################################################################################################ #
def OnAdd(transaction):
    # Due to the fact that the accurev pop operation cannot retrieve defunct files when we use it
    # outside of a workspace and that workspaces cannot reparent themselves under workspaces
    # we must restrict ourselves to only processing stream operations, promotions.
    state.config.logger.dbg( "Ignored transaction #{0}: add".format(transaction.id) )

def OnChstream(transaction):
    # We determine which branch something needs to go to on the basis of the real/virtual version
    # there is no need to track all the stream changes.
    state.config.logger.dbg( "Ignored transaction #{0}: chstream".format(transaction.id) )
    
def OnCo(transaction):
    # The co (checkout) transaction can be safely ignored.
    state.config.logger.dbg( "Ignored transaction #{0}: co".format(transaction.id) )

def OnKeep(transaction):
    # Due to the fact that the accurev pop operation cannot retrieve defunct files when we use it
    # outside of a workspace and that workspaces cannot reparent themselves under workspaces
    # we must restrict ourselves to only processing stream operations, promotions.
    state.config.logger.dbg( "Ignored transaction #{0}: keep".format(transaction.id) )

def OnPromote(transaction):
    global state
    state.config.logger.dbg( "Transaction #{0} (promote)".format(transaction.id) )
    
    if len(transaction.versions) > 0:
        stream = GetTransactionDestStream(transaction)
        elemList = GetTransactionElements(transaction, skipDirs=True)
        
        if stream is not None and state.config.accurev.streamList is not None:
            if stream not in state.config.accurev.streamList:
                state.config.logger.dbg( "Skipped transaction #{0}. Stream {1} is not in the stream list.".format(transaction.id, stream) )
                return False
        
        if stream is not None and elemList is not None and len(elemList) > 0:
            state.config.logger.dbg( "Branch:", stream )
            
            # Generate the lists of files to add and remove
            memberList, defunctList = SplitElementsByStatusViaDiff(stream, transaction.id, elemList)
            
            addList = []
            if memberList is not None and len(memberList) > 0:
                if PopAccurevTransaction(stream, transaction.id, memberList, state.config.git.repoPath):
                    for elem in memberList:
                        addList.append(LocalElementPath(accurevDepotElementPath=elem, gitRepoPath=state.config.git.repoPath, isAbsolute=False))
                else:
                    state.config.logger.error( "Failed to populate transaction #{0}.".format(transaction.id) )
                    sys.exit(1)
            else:
                state.config.logger.dbg( "Did not pop transaction #{0}, stream {1}, elements {2}".format(transaction.id, stream, memberList) )
                    
            rmList = []
            if len(defunctList) > 0:
                for elem in defunctList:
                    rmList.append(LocalElementPath(accurevDepotElementPath=elem, gitRepoPath=state.config.git.repoPath, isAbsolute=False))
            
            dbgFileMsg = "+{0}/-{1}".format(str(len(memberList)), str(len(defunctList)))
            
            # If we have something to commit then:
            if len(addList) > 0 or len(rmList) > 0:
                # Generate the commit message
                commitMessage = AccurevComment2GitMessage(transaction)

                # Generate the git Author
                gitAuthor = AccurevUser2GitAuthor(transaction.user)
                
                # Do the commit
                status = GitCommit(gitRepo=state.gitRepo, branch=stream, author=gitAuthor, date=transaction.time, message=commitMessage, addList=addList, rmList=rmList)

                if status:
                    state.config.logger.info( "Committed transaction #{0}. {1} files".format(transaction.id, dbgFileMsg) )
                elif state.gitRepo.lastError is not None:
                    if "nothing to commit, working directory clean" in state.gitRepo.lastError.output:
                        state.config.logger.info( "Skipped transaction #{0}. {1} files. Nothing to commit.".format(transaction.id, dbgFileMsg) )
                        status = True
                else:
                    state.config.logger.error( "Failed to commit transaction #{0}. {1} files.\n{2}".format(transaction.id, dbgFileMsg, state.gitRepo.lastError.output) )
                    sys.exit(1)
                return status
            else:
                state.config.logger.dbg( "Did not commit empty transaction #{0}.".format(transaction.id) )
        elif elemList is None or len(elemList) <= 0:
            state.config.logger.dbg( "Transaction #{0}. No non-directory elements promoted.".format(transaction.id) )
        else:
            state.config.logger.dbg( "Transaction #{0}. Could not determine stream.".format(transaction.id) )
    else:
        state.config.logger.dbg( "Transaction #{0} has no elements.".format(transaction.id) )
        
    return False

def OnMove(transaction):
    state.config.logger.dbg( "Transaction #{0} (move) [ignored]".format(transaction.id) )
    # stream = GetTransactionDestStream(transaction)
    # addList = []
    # for move in transaction.moves:
    #     if os.path.exists(move.dest):
    #         state.config.logger.error( "Transaction #{0}. Moves {1} to an existing file/dir {2}".format(transaction.id, move.source, move.dest) )
    #         sys.exit(1)
    #     else:
    #         try:
    #             os.rename(move.source, move.dest)
    #         except OSError:
    #             state.config.logger.error( "Transaction #{0}. Failed to move {1} to {2}".format(transaction.id, move.source, move.dest) )
    #             sys.exit(1)
    #         
    #         addList.append(move.dest)
    # 
    # # Generate the commit message
    # commitMessage = AccurevComment2GitMessage(transaction)
    # 
    # # Generate the git Author
    # gitAuthor = AccurevUser2GitAuthor(transaction.user)
    # 
    # # Do the commit
    # status = GitCommit(gitRepo=state.gitRepo, branch=stream, author=gitAuthor, date=transaction.time, message=commitMessage, addList=addList, rmList=[])

def OnMkstream(transaction):
    # The mkstream command doesn't contain enough information to create a branch in git.
    # Silently ignore.
    state.config.logger.dbg( "Ignored transaction #{0}: mkstream".format(transaction.id) )

def OnPurge(transaction):
    state.config.logger.dbg( "Ignored transaction #{0}: purge".format(transaction.id) )
    # If done on a stream, must be translated to a checkout of the original element from the basis.

def OnDefunct(transaction):
    state.config.logger.dbg( "OnDefunct:", transaction.id )

def OnUndefunct(transaction):
    state.config.logger.dbg( "OnUndefunct:", transaction.id )

def OnDefcomp(transaction):
    # The defcomp command is not visible to the user; it is used in the implementation of the 
    # include/exclude facility CLI commands incl, excl, incldo, and clear.
    # Source: http://www.accurev.com/download/docs/5.5.0_books/AccuRev_WebHelp/AccuRev_Admin/wwhelp/wwhimpl/common/html/wwhelp.htm#context=admin&file=pre_op_trigs.html
    state.config.logger.dbg( "Ignored transaction #{0}: defcomp".format(transaction.id) )

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
        
        def _FormatMessage(self, messages):
            if self.referenceTime:
                outMessage = "{0: >6.2f}s:".format(time.clock() - self.referenceTime)
            else:
                outMessage = None
            
            outMessage = " ".join([str(x) for x in messages])
            
            return outMessage
        
        def info(self, *message):
            if self.isInfoEnabled:
                print self._FormatMessage(message)

        def dbg(self, *message):
            if self.isDbgEnabled:
                print self._FormatMessage(message)
        
        def error(self, *message):
            if self.isErrEnabled:
                sys.stderr.write(self._FormatMessage(message))
                sys.stderr.write("\n")
        
    class AccuRev(object):
        @classmethod
        def fromxmlelement(cls, xmlElement):
            if xmlElement is not None and xmlElement.tag == 'accurev':
                depot    = xmlElement.attrib.get('depot')
                username = xmlElement.attrib.get('username')
                password = xmlElement.attrib.get('password')
                startTransaction = xmlElement.attrib.get('start-transaction')
                endTransaction   = xmlElement.attrib.get('end-transaction')
                
                streamList = None
                streamListElement = xmlElement.find('stream-list')
                if streamListElement is not None:
                    streamList = []
                    streamElementList = streamListElement.findall('stream')
                    for streamElement in streamElementList:
                        streamList.append(streamElement.text)
                
                return cls(depot, username, password, startTransaction, endTransaction, streamList)
            else:
                return None
            
        def __init__(self, depot, username = None, password = None, startTransaction = None, endTransaction = None, streamList = None):
            self.depot    = depot
            self.username = username
            self.password = password
            self.startTransaction = startTransaction
            self.endTransaction   = endTransaction
            self.streamList = streamList
    
        def __repr__(self):
            str = "Config.AccuRev(depot=" + repr(self.depot)
            str += ", username="          + repr(self.username)
            str += ", password="          + repr(self.password)
            str += ", startTransaction="  + repr(self.startTransaction)
            str += ", endTransaction="    + repr(self.endTransaction)
            if streamList is not None:
                str += ", streamList="    + repr(self.streamList)
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
        return root + '.config'

    @classmethod
    def fromxmlstring(cls, xmlString):
        # Load the XML
        xmlRoot = ElementTree.fromstring(xmlString)
        
        if xmlRoot is not None and xmlRoot.tag == "accurev2git":
            accurev = Config.AccuRev.fromxmlelement(xmlRoot.find('accurev'))
            git     = Config.Git.fromxmlelement(xmlRoot.find('git'))
            
            usermaps = []
            userMapsElem = xmlRoot.find('usermaps')
            if userMapsElem is not None:
                for userMapElem in userMapsElem.findall('map-user'):
                    usermaps.append(Config.UserMap.fromxmlelement(userMapElem))
            
            return cls(accurev, git, usermaps)
        else:
            # Invalid XML for an accurev2git configuration file.
            return None

    def __init__(self, accurev = None, git = None, usermaps = None):
        self.accurev  = accurev
        self.git      = git
        self.usermaps = usermaps
        self.logger   = Config.Logger()
        
    def __repr__(self):
        str = "Config(accurev=" + repr(self.accurev)
        str += ", git="         + repr(self.git)
        str += ", usermaps="    + repr(self.usermaps)
        str += ")"
        
        return str

class AccuRev2Git(object):
    def __init__(self, config):
        self.config = config
        self.transactionHandlers = { "add": OnAdd, "chstream": OnChstream, "co": OnCo, "defunct": OnDefunct, "keep": OnKeep, "promote": OnPromote, "move": OnMove, "mkstream": OnMkstream, "purge": OnPurge, "undefunct": OnUndefunct, "defcomp": OnDefcomp }
        self.cwd = None
        self.gitRepo = None
        self.accurevStreams = None
    
    def GetStreamNameFromId(self, streamId):
        if self.accurevStreams is None:
            self.accurevStreams = accurev.show.streams(depot=self.config.accurev.depot)
            
        for stream in self.accurevStreams:
            if str(streamId) == str(stream.streamNumber):
                return stream.name
        
        return None
    
    def GetLastAccuRevTransaction(self):
        # TODO: Fix me! We don't have a way of retrieving the last accurev transaction that we
        #               processed yet. This will most likely involve parsing the git history in some
        #               way.
        status = subprocess.check_output(['git', 'status'])
        if status.find('Initial commit') >= 0:
            # This is an initial commit so we need to start from the beginning.
            return self.config.accurev.startTransaction
        else:
            #lastbranch = subprocess.check_output('git config accurev.lastbranch').strip()
            #subprocess.check_output('git checkout {0}'.format(lastbranch))
            lastCommit = subprocess.check_output(['git', 'log', '-1']).strip()
            reMsgTemplate = re.compile('AccuRev Transaction #([0-9]+)')
            reMatchObj = reMsgTemplate.search(lastCommit)
            if reMatchObj:
                transactionId = int(reMatchObj.group(1))
                return transactionId + 1 # continue from the next transaction
                
        return None
        
    # ProcessAccuRevTransaction
    #   Processes an individual AccuRev transaction by calling its corresponding handler.
    def ProcessAccuRevTransaction(self, transaction):
        handler = self.transactionHandlers.get(transaction.Type)
        if handler is not None:
            handler(transaction)
        else:
            self.config.logger.error("Error: No handler for [\"{0}\"] transactions\n".format(transaction.Type))
        
    # ProcessAccuRevTransactionRange
    #   Iterates over accurev transactions between the startTransaction and endTransaction processing
    #  each of them in turn. If maxTransactions is given as a positive integer it will process at 
    #  most maxTransactions.
    def ProcessAccuRevTransactionRange(self, startTransaction="1", endTransaction="now", maxTransactions=None):
        timeSpec = "{0}-{1}".format(startTransaction, endTransaction)
        if maxTransactions is not None and maxTransactions > 0:
            timeSpec = "{0}.{1}".format(timeSpec, maxTransactions)
        
        self.config.logger.info( "Querying history" )
        arHist = accurev.hist(depot=self.config.accurev.depot, timeSpec=timeSpec, allElementsFlag=True)
        
        for transaction in arHist.transactions:
            self.ProcessAccuRevTransaction(transaction)
        
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
    
    # Start
    #   Begins a new AccuRev to Git conversion process discarding the old repository (if any).
    def Start(self, isRestart=False):
        if isRestart:
            self.config.logger.info( "Restarting the conversion operation." )
            self.config.logger.info( "Deleting old git repository." )
            git.delete(self.config.git.repoPath)
            
        # From here on we will operate from the git repository.
        self.cwd = os.getcwd()
        os.chdir(self.config.git.repoPath)
        
        if self.InitGitRepo(self.config.git.repoPath):
            self.gitRepo = git.open(self.config.git.repoPath)
            if not isRestart:
                #self.gitRepo.reset(isHard=True)
                self.gitRepo.clean(force=True)
            
            continueFromTransaction = self.GetLastAccuRevTransaction()
            if continueFromTransaction is None:
                state.config.logger.error( "Could not determine at which transaction to continue." )
                state.config.logger.error( "Error in parsing the git repo history. Conversion aborted!" )
                return 1
            elif self.config.accurev.startTransaction == continueFromTransaction:
                state.config.logger.info( "Starting from transaction: #{0}".format(continueFromTransaction) )
            else:
                state.config.logger.info( "Continuing from transaction: #{0}".format(continueFromTransaction) )
            
            if accurev.login(self.config.accurev.username, self.config.accurev.password):
                state.config.logger.info( "Accurev login successful" )
                
                self.ProcessAccuRevTransactionRange(startTransaction=continueFromTransaction, endTransaction=self.config.accurev.endTransaction)
                
                os.chdir(self.cwd)
                
                if accurev.logout():
                    state.config.logger.info( "Accurev logout successful" )
                    return 0
                else:
                    state.config.logger.error("Accurev logout failed\n")
                    return 1
            else:
                state.config.logger.error("AccuRev login failed.\n")
                return 1
        else:
            state.config.logger.error( "Could not create git repository." )
            
    def Restart(self):
        return self.Start(isRestart=True)
        
# ################################################################################################ #
# Script Functions                                                                                 #
# ################################################################################################ #
def DumpExampleConfigFile(outputFilename):
    exampleContents = """
<accurev2git>
    <accurev 
        username="joe_bloggs"
        password="joanna"
        depot="Trunk"
        start-transaction="1"
        end-transaction="500" >
        <!-- The stream-list is optional. If not given all streams are processed -->
        <stream-list>
            <stream>NOS</stream>
        </stream-list>
    </accurev>
    <git repo-path="/put/the/git/repo/here" />
    <usermaps>
        <map-user><accurev username="joe_bloggs" /><git name="Joe Bloggs" email="joe@bloggs.com" /></map-user>
    </usermaps>
</accurev2git>
"""
    file = open(outputFilename, 'w')
    file.write(exampleContents)
    file.close()

def ValidateConfig(config):
    # Validate the program args and configuration up to this point.
    isValid = True
    if config.accurev.username is None:
        state.config.logger.error("No AccuRev username specified.\n")
        isValid = False
    if config.accurev.password is None:
        state.config.logger.error("No AccuRev password specified.\n")
        isValid = False
    if config.accurev.depot is None:
        state.config.logger.error("No AccuRev depot specified.\n")
        isValid = False
    if config.git.repoPath is None:
        state.config.logger.error("No Git repository specified.\n")
        isValid = False
    
    return isValid

def LoadConfigOrDefaults(scriptName):
    # Try and load the config file
    doesConfigExist = True
    
    configFilename = Config.FilenameFromScriptName(scriptName)
    configXml = None
    try:
        configFile = open(configFilename)
        configXml = configFile.read()
        configFile.close()
    except:
        doesConfigExist = False
        
    config = None
    if configXml is not None:
        config = Config.fromxmlstring(configXml)

    if config is None:
        config = Config(Config.AccuRev(), Config.Git(), [])
        
    return config

def PrintConfigSummary(config):
    if config is not None:
        config.logger.info('Config info:')
        config.logger.info('  git')
        config.logger.info('    repo path:{0}'.format(config.git.repoPath))
        config.logger.info('  accurev:')
        config.logger.info('    depot: {0}'.format(config.accurev.depot))
        if config.accurev.streamList is not None:
            config.logger.info('    stream list:')
            for stream in config.accurev.streamList:
                config.logger.info('      - {0}'.format(stream))
        else:
            config.logger.info('    stream list: all included')
        config.logger.info('    start tran.: #{0}'.format(config.accurev.startTransaction))
        config.logger.info('    end tran.:   #{0}'.format(config.accurev.endTransaction))
        config.logger.info('    username: {0}'.format(config.accurev.username))
        config.logger.info('  usermaps: {0}'.format(len(config.usermaps)))
    
# ################################################################################################ #
# Script Main                                                                                      #
# ################################################################################################ #
def AccuRev2GitMain(argv):
    global state
    
    config = LoadConfigOrDefaults(argv[0])
    
    # Set-up and parse the command line arguments. Examples from https://docs.python.org/dev/library/argparse.html
    parser = argparse.ArgumentParser(description="Conversion tool for migrating AccuRev repositories into Git")
    parser.add_argument('--accurev-username', nargs='?', dest='accurevUsername', default=config.accurev.username)
    parser.add_argument('--accurev-password', nargs='?', dest='accurevPassword', default=config.accurev.password)
    parser.add_argument('--accurev-depot',    nargs='?', dest='accurevDepot',    default=config.accurev.depot)
    parser.add_argument('--git-repo-path',    nargs='?', dest='gitRepoPath',     default=config.git.repoPath)
    parser.add_argument('--restart',    nargs='?', dest='restart', const="true")
    parser.add_argument('--debug',    nargs='?', dest='debug', const="true")
    parser.add_argument('--dump-example-config', nargs='?', dest='exampleConfigFilename', const='no-filename', default=None)
    
    args = parser.parse_args()
    
    # Dump example config if specified
    if args.exampleConfigFilename is not None:
        if args.exampleConfigFilename == 'no-filename':
            exampleConfigFilename = Config.FilenameFromScriptName(scriptName) + '.example'
        else:
            exampleConfigFilename = args.exampleConfigFilename
        
        DumpExampleConfigFile(exampleConfigFilename)
    
    # Set the overrides for in the configuration from the arguments
    config.accurev.username = args.accurevUsername
    config.accurev.password = args.accurevPassword
    config.accurev.depot    = args.accurevDepot
    config.git.repoPath     = args.gitRepoPath
    
    if not ValidateConfig(config):
        return 1

    PrintConfigSummary(config)
    
    state = AccuRev2Git(config)
    
    state.config.logger.isDbgEnabled = ( args.debug == "true" )
        
    if args.restart == "true":
        return state.Restart()
    else:
        return state.Start()
        
# ################################################################################################ #
# Script Start                                                                                     #
# ################################################################################################ #
if __name__ == "__main__":
    AccuRev2GitMain(sys.argv)
