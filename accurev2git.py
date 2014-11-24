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

import accurev

# GitPython is used to interact with git. Its project page is: https://gitorious.org/git-python
# The repo it was cloned from: git://gitorious.org/git-python/mainline.git at commit 5eb7fd3f0dd99dc6c49da6fd7e78a392c4ef1b33
# You might also need: https://pypi.python.org/pypi/setuptools#installation-instructions
from git import *

# ################################################################################################ #
# Script Globals.                                                                                  #
# ################################################################################################ #
state = None

# ################################################################################################ #
# Script Core. Git helper functions                                                                #
# ################################################################################################ #
def CheckoutBranch(branchName):
    global state
    
    branch = None
    
    try:
        branch = getattr(state.gitRepo.heads, branchName)
        #print "branch:", branchName, "exists"
    except:
        branch = state.gitRepo.create_head(branchName)
        #print "branch:", branchName, "created"
        
    return branch

def GetGitElementPath(accurevDepotElementPath, gitRepoPath=None, isAbsolute=False):
    # All accurev depot paths start with \.\ or /./ so strip that before joining.
    relpath = accurevDepotElementPath[3:]
    if isAbsolute:
        if gitRepoPath is not None:
            return os.path.join(gitRepoPath, relpath)
        else:
            return os.path.abspath(relpath)
    return relpath

def GetAccurevFile(elementId=None, depotName=None, verSpec=None, element=None, gitPath=None, isBinary=False):
    filePath = GetGitElementPath(accurevDepotElementPath=element, gitRepoPath=gitPath)
    fileDir  = os.path.dirname(filePath)
    if fileDir is not None and len(fileDir) > 0 and not os.path.exists(fileDir):
        os.makedirs(fileDir)
    success = accurev.cat(elementId=elementId, depotName=depotName, verSpec=str(verSpec), outputFilename=filePath)
    if success is None:
        return False
    return True
                
# ################################################################################################ #
# Script Core. AccuRev transaction to Git conversion handlers.                                     #
# ################################################################################################ #
def OnAdd(transaction):
    # Due to the fact that the accurev pop operation cannot retrieve defunct files when we use it
    # outside of a workspace and that workspaces cannot reparent themselves under workspaces
    # we must restrict ourselves to only processing stream operations, promotions.
    print "Ignored transaction #{0}: add".format(transaction.id)

def OnChstream(transaction):
    # We determine which branch something needs to go to on the basis of the real/virtual version
    # there is no need to track all the stream changes.
    print "Ignored transaction #{0}: chstream".format(transaction.id)
    
def OnCo(transaction):
    # The co (checkout) transaction can be safely ignored.
    print "Ignored transaction #{0}: co".format(transaction.id)

def OnKeep(transaction):
    # Due to the fact that the accurev pop operation cannot retrieve defunct files when we use it
    # outside of a workspace and that workspaces cannot reparent themselves under workspaces
    # we must restrict ourselves to only processing stream operations, promotions.
    print "Ignored transaction #{0}: keep".format(transaction.id)

def OnPromote(transaction):
    global state
    print "OnPromote: #{0}".format(transaction.id)
    if len(transaction.versions) > 0:
        print "Branch:", transaction.versions[0].virtualNamedVersion.stream
        
        CheckoutBranch(transaction.versions[0].virtualNamedVersion.stream)
        addCount = 0
        for version in transaction.versions:
            # Populate it only if it is not a directory.
            if version.dir != "yes":
                print "Version:", version.virtualNamedVersion
                print "gitRepoPath:", state.gitRepoPath
                print "VersionPath:", version.path
                print "ElementPath:", GetGitElementPath(version.path, state.gitRepoPath)
            
                if GetAccurevFile(elementId=version.eid, depotName=state.config.accurev.depot, verSpec=str(version.virtual), element=version.path, gitPath=state.gitRepoPath):
                    state.gitRepo.index.add([GetGitElementPath(version.path)])
                    addCount += 1
                else:
                    print "Cat failed for {0}".format(version.path)
                    
            else:
                print "Skip:", version
        
        if addCount > 0:
            state.gitRepo.index.commit(message=transaction.comment)
            sys.exit(0)
        else:
            print "Did not commit"

def OnMove(transaction):
    print "OnMove:", transaction

def OnMkstream(transaction):
    # The mkstream command doesn't contain enough information to create a branch in git.
    # Silently ignore.
    print "Ignored transaction #{0}: mkstream".format(transaction.id)

def OnPurge(transaction):
    print "Ignored transaction #{0}: purge".format(transaction.id)
    # If done on a stream, must be translated to a checkout of the original element from the basis.

def OnDefunct(transaction):
    print "OnDefunct:", transaction

def OnUndefunct(transaction):
    print "OnUndefunct:", transaction

def OnDefcomp(transaction):
    # The defcomp command is not visible to the user; it is used in the implementation of the 
    # include/exclude facility CLI commands incl, excl, incldo, and clear.
    # Source: http://www.accurev.com/download/docs/5.5.0_books/AccuRev_WebHelp/AccuRev_Admin/wwhelp/wwhimpl/common/html/wwhelp.htm#context=admin&file=pre_op_trigs.html
    print "Ignored transaction #{0}: defcomp".format(transaction.id)

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
                
                return cls(depot, username, password, startTransaction, endTransaction)
            else:
                return None
            
        def __init__(self, depot, username = None, password = None, startTransaction = None, endTransaction = None):
            self.depot    = depot
            self.username = username
            self.password = password
            self.startTransaction = startTransaction
            self.endTransaction   = endTransaction
    
        def __repr__(self):
            str = "Config.AccuRev(depot=" + repr(self.depot)
            str += ", username="          + repr(self.username)
            str += ", password="          + repr(self.password)
            str += ", startTransaction="  + repr(self.startTransaction)
            str += ", endTransaction="    + repr(self.endTransaction)
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
                accurevUsername = xmlElement.attrib.get('accurev-username')
                gitName         = xmlElement.attrib.get('git-name')
                gitEmail        = xmlElement.attrib.get('git-email')
                
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
        
        # gitRepo - is a git.Repo object. See https://pythonhosted.org/GitPython/0.3.1/tutorial.html#initialize-a-repo-object
        #   It is guaranteed NOT to be None when the Handlers execute (or at least it is an error if this
        #   object is not a valid/initialised/existing git repo when the handlers are called)
        self.gitRepo = None
        self.gitRepoPath = None
        self.accuRevVersionCachePath = None
        
    def GetLastAccuRevTransaction(self):
        # TODO: Fix me! We don't have a way of retrieving the last accurev transaction that we
        #               processed yet. This will most likely involve parsing the git history in some
        #               way.
        return self.config.accurev.startTransaction
        
    # ProcessAccuRevTransaction
    #   Processes an individual AccuRev transaction by calling its corresponding handler.
    def ProcessAccuRevTransaction(self, transaction):
        handler = self.transactionHandlers.get(transaction.Type)
        if handler is not None:
            handler(transaction)
        else:
            sys.stderr.write("Error: No handler for [\"{0}\"] transactions\n".format(transaction.Type))
        
    # ProcessAccuRevTransactionRange
    #   Iterates over accurev transactions between the startTransaction and endTransaction processing
    #  each of them in turn. If maxTransactions is given as a positive integer it will process at 
    #  most maxTransactions.
    def ProcessAccuRevTransactionRange(self, startTransaction="1", endTransaction="now", maxTransactions=None):
        timeSpec = "{0}-{1}".format(startTransaction, endTransaction)
        if maxTransactions is not None and maxTransactions > 0:
            timeSpec = "{0}.{1}".format(timeSpec, maxTransactions)
        
        print "Querying history"
        arHist = accurev.hist(depot=self.config.accurev.depot, timeSpec=timeSpec, allElementsFlag=True)
        
        for transaction in arHist.transactions:
            self.ProcessAccuRevTransaction(transaction)
        
    def NewGitRepo(self, gitRepoPath):
        gitRootDir, gitRepoDir = os.path.split(gitRepoPath)
        if os.path.isdir(gitRootDir):
            if os.path.isdir(os.path.join(gitRepoPath, ".git")):
                sys.stderr.write("Found an existing git repository.\n")
                sys.stderr.write("Are you sure you want to overwrite? (yes/no)\n> ")
                input = raw_input()
                while input not in [ "yes", "no" ]:
                    sys.stderr.write("Please enter yes or no\n> ")
                    input = raw_input()

                if input == "no":
                    return None
                
                print "Removing existing git repository"
                shutil.rmtree(os.path.join(gitRepoPath, ".git"))
        
            print "Creating new git repository"
            repo = Repo.init(gitRepoPath, bare=False)
        
            # Create an empty first commit so that we can create branches as we please.
            self.cwd = os.getcwd()
            os.chdir(gitRepoPath)
            command = subprocess.Popen([ 'git', 'commit', '--allow-empty', '-m', 'initial commit' ], stdout=subprocess.PIPE)
            command.wait()
            if command.returncode != 0:
                print "Error creating initial commit!"
                sys.exit(1)
            
            return repo
        else:
            sys.stderr.write("{0} not found.\n".format(gitRootDir))
            
        return None
    
    def GetExistingGitRepo(self, gitRepoPath):
        gitRootDir, gitRepoDir = os.path.split(gitRepoPath)
        if os.path.isdir(gitRootDir):
            if os.path.isdir(os.path.join(gitRepoPath, ".git")):
                self.cwd = os.getcwd()
                os.chdir(gitRepoPath)
                print "Opening git repository"
                return Repo(gitRepoPath)

        sys.stderr.write("Failed to find git repository at: {0}\n".format(gitRepoPath))
        return None
        
    # Start
    #   Begins a new AccuRev to Git conversion process discarding the old repository (if any).
    def Start(self):
        global gitRepo
        global gitRepoPath
        
        print "Starting a new conversion operation"
        
        self.gitRepo = self.NewGitRepo(self.config.git.repoPath)
        self.gitRepoPath = self.config.git.repoPath
        self.accuRevVersionCachePath = os.path.join(self.cwd, ".accuRevVersionCache")
        
        if accurev.login(self.config.accurev.username, self.config.accurev.password):
            print "Login successful"
            
            self.ProcessAccuRevTransactionRange(startTransaction=self.config.accurev.startTransaction, endTransaction=self.config.accurev.endTransaction)
            
            os.chdir(self.cwd)
            
            if accurev.logout():
                print "Logout successful"
                return 0
            else:
                sys.stderr.write("Logout failed\n")
                return 1
        else:
            sys.stderr.write("AccuRev login failed.\n")
            return 1
    
    def Resume(self):
        global gitRepo
        global gitRepoPath
        
        # For now the resume feature is not supported and causes us to start from scratch again.
        print "Resuming last conversion operation"
        
        self.gitRepo = self.GetExistingGitRepo(self.config.git.repoPath)
        self.gitRepoPath = self.config.git.repoPath
        self.accuRevVersionCachePath = os.path.join(self.cwd, ".accuRevVersionCache")
        
        # TODO: Start by verifying where we have stopped last time and restoring state.
        
        # TODO: Do exactly what Start does but start in the middle...
        if accurev.login(self.config.accurev.username, self.config.accurev.password):
            print "Login successful"
            
            # TODO: Figure out at which transaction we have stopped and use it as the startTransaction.
            self.ProcessAccuRevTransactionRange(startTransaction=self.GetLastAccuRevTransaction(), endTransaction=self.config.accurev.endTransaction)
            
            os.chdir(self.cwd)
            
            if accurev.logout():
                print "Logout successful"
                return 0
            else:
                sys.stderr.write("Logout failed\n")
                return 1
        else:
            sys.stderr.write("AccuRev login failed.\n")
            return 1
        
# ################################################################################################ #
# Script Functions                                                                                 #
# ################################################################################################ #
def DumpExampleConfigFile(outputFilename):
    exampleContents = """
<accurev2git>
    <accurev username="joe_bloggs" password="joanna" depot="Trunk" />
    <git repo-path="/put/the/git/repo/here" />
    <usermaps>
        <map-user accurev-username="joe_bloggs" git-name="Joe Bloggs" git-email="joe@bloggs.com" />
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
        sys.stderr.write("No AccuRev username specified.\n")
        isValid = False
    if config.accurev.password is None:
        sys.stderr.write("No AccuRev password specified.\n")
        isValid = False
    if config.accurev.depot is None:
        sys.stderr.write("No AccuRev depot specified.\n")
        isValid = False
    if config.git.repoPath is None:
        sys.stderr.write("No Git repository specified.\n")
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
    parser.add_argument('--resume',    nargs='?', dest='resume', const="true")
    parser.add_argument('--dump-example-config', nargs='?', dest='exampleConfigFilename', const='no-filename', default=None)
    
    args = parser.parse_args()
    
    # Dump example config if specified
    if args.exampleConfigFilename is not None:
        if args.exampleConfigFilename == 'no-filename':
            exampleConfigFilename = configFilename + '.example'
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

    state = AccuRev2Git(config)
    
    if args.resume == "true":
        return state.Resume()
    else:
        return state.Start()
        
# ################################################################################################ #
# Script Start                                                                                     #
# ################################################################################################ #
if __name__ == "__main__":
    AccuRev2GitMain(sys.argv)
