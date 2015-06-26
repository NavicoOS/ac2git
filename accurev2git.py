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
import datetime
import time
import re
import types
import copy

from collections import OrderedDict

import accurev
import git

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
    def __init__(self, config):
        self.config = config
        self.cwd = None
        self.gitRepo = None
        self.gitBranchList = None
    
    def ClearGitRepo(self):
        # Delete everything except the .git folder from the destination (git repo)
        for root, dirs, files in os.walk(self.gitRepo.path, topdown=False):
            for name in files:
                path = os.path.join(root, name).replace('\\', '/')
                if path[-1:] != '/' and os.path.isdir(path):
                    path += ('/')
                if '/.git/' not in path and path[:len('.git/')] != '.git/':
                    os.remove(path)
            for name in dirs:
                path = os.path.join(root, name).replace('\\', '/')
                if path[-1:] != '/' and os.path.isdir(path):
                    path += ('/')
                if '/.git/' not in path and path[:len('.git/')] != '.git/':
                    os.rmdir(path)

    def PreserveEmptyDirs(self):
        for root, dirs, files in os.walk(self.gitRepo.path, topdown=True):
            for name in dirs:
                path = os.path.join(root, name).replace('\\','/')
                if len(os.listdir(path)) == 0:
                    filename = os.path.join(path, '.gitignore')
                    with open(filename, 'w') as file:
                        file.write('# accurev2git.py preserve empty dirs\n')

    def GetGitUserFromAccuRevUser(self, accurevUsername):
        if accurevUsername is not None:
            for usermap in self.config.usermaps:
                if usermap.accurevUsername == accurevUsername:
                    return "{0} <{1}>".format(usermap.gitName, usermap.gitEmail)
        state.config.logger.error("Cannot find git details for accurev username {0}".format(accurevUsername))
        return accurevUsername

    def NewStreamInitialCommit(self, streamName, branchName):
        # Get the stream creation transaction (mkstream). Note: The first stream in the depot doesn't have an mkstream transaction.
        mkstream = accurev.hist(stream=streamName, transactionKind="mkstream", timeSpec="now")
        if len(mkstream.transactions) == 0:
            self.config.logger.info( "The root stream has no mkstream transaction. Starting at transaction 1." )
            trId = 1
            trComment = ''
        else:
            tr = mkstream.transactions[0]
            trId = tr.id
            trComment = tr.comment
            if len(mkstream.transactions) != 1:
                self.config.logger.warning( "Error: There seem to be multiple mkstream transactions for this stream... Using {0}".format(trId) )
        

        # Create the git branch.
        self.config.logger.info( "Creating {0}".format(branchName) )
        self.gitRepo.checkout(branchName=branchName, isOrphan=True)

        # Clear the index as it may contain the [start-point] info...
        self.gitRepo.rm(fileList=['.'], force=True, recursive=True)
        self.ClearGitRepo()

        # Populate the stream's state at creation
        self.config.logger.info( "Populating mkstream {0} for {1}".format(trId, streamName) )
        accurev.pop(verSpec=streamName, location=self.gitRepo.path, isRecursive=True, timeSpec=trId, elementList='.')
        self.PreserveEmptyDirs()

        # Add all of the files to the index
        self.gitRepo.add(force=True, all=True)

        # Make the first commit
        if self.gitRepo.commit(message=trComment, committer=self.GetGitUserFromAccuRevUser(tr.user), committer_date=tr.time, allow_empty_message=True):
            self.config.logger.info( "Committed" )
        else:
            self.config.logger.error( "Failed to commit" )

        # Write the commit notes consisting of the accurev hist xml output for the transaction

    def ProcessStream(self, streamName, branchName):
        self.config.logger.info( "Processing {0}".format(streamName) )
        # Find the matching git branch
        branch = None
        for b in self.gitBranchList:
            if branchName == branch.name:
                branch = b
                break
        if branch is None:
            # We are tracking a new stream:
            #   * Create the git branch
            #   * Get the stream create (mkstream) transaction number. This is our first transaction.
            self.NewStreamInitialCommit(streamName, branchName)
        else:
            # We have a git branch that matches and by assumption an existing stream.
            #   * 
            pass

    def ProcessStreams(self):
        for stream in self.config.accurev.streamMap:
            branch = self.config.accurev.streamMap[stream]
            self.ProcessStream(stream, branch)

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
            
    def Restart(self):
        return self.Start(isRestart=True)
        
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
    <!-- The user maps are used to convert users from AccuRev into git. Please spend the time to fill them in properly. -->
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
        config = Config(Config.AccuRev(None), Config.Git(None), [])
        
    return config

def PrintConfigSummary(config):
    if config is not None:
        config.logger.info('Config info:')
        config.logger.info('  git')
        config.logger.info('    repo path:{0}'.format(config.git.repoPath))
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
    
# ################################################################################################ #
# Script Main                                                                                      #
# ################################################################################################ #
def AccuRev2GitMain(argv):
    global state
    
    config = LoadConfigOrDefaults(argv[0])
    configFilename = Config.FilenameFromScriptName(argv[0])
    defaultExampleConfigFilename = '{0}.example'.format(configFilename)
    
    # Set-up and parse the command line arguments. Examples from https://docs.python.org/dev/library/argparse.html
    parser = argparse.ArgumentParser(description="Conversion tool for migrating AccuRev repositories into Git")
    parser.add_argument('-u', '--accurev-username', nargs='?', dest='accurevUsername', default=config.accurev.username, help="The username which will be used to retrieve and populate the history from AccuRev. (overrides the username in the '{0}')".format(configFilename))
    parser.add_argument('-p', '--accurev-password', nargs='?', dest='accurevPassword', default=config.accurev.password, help="The password for the username provided with the --accurev-username option or specified in the '{0}' file. (overrides the password in the '{0}')".format(configFilename))
    parser.add_argument('-t', '--accurev-depot',    nargs='?', dest='accurevDepot',    default=config.accurev.depot, help="The AccuRev depot in which the stream/s that is/are being converted is/are located.")
    parser.add_argument('-g', '--git-repo-path',    nargs='?', dest='gitRepoPath',     default=config.git.repoPath, help="The system path to an existing folder where the git repository will be created.")
    parser.add_argument('-r', '--restart',    dest='restart', action='store_const', const=True, help="Discard any existing conversion and start over.")
    parser.add_argument('-v', '--verbose',    dest='debug',   action='store_const', const=True, help="Print the script debug information. Makes the script more verbose.")
    parser.add_argument('-e', '--dump-example-config', nargs='?', dest='exampleConfigFilename', const='no-filename', default=None, help="Generates an example configuration file and exits. If the filename isn't specified a default filename '{0}' is used. The script automatically loads the configuration file named '{1}' when it is run. Commandline arguments, if given, override all options in the configuration file.".format(defaultExampleConfigFilename, configFilename))
    
    args = parser.parse_args()
    
    # Dump example config if specified
    if args.exampleConfigFilename is not None:
        if args.exampleConfigFilename == 'no-filename':
            exampleConfigFilename = defaultExampleConfigFilename
        else:
            exampleConfigFilename = args.exampleConfigFilename
        
        DumpExampleConfigFile(exampleConfigFilename)
        sys.exit(0)
    
    # Set the overrides for in the configuration from the arguments
    config.accurev.username = args.accurevUsername
    config.accurev.password = args.accurevPassword
    config.accurev.depot    = args.accurevDepot
    config.git.repoPath     = args.gitRepoPath
    
    if not ValidateConfig(config):
        return 1

    PrintConfigSummary(config)
    
    state = AccuRev2Git(config)
    
    state.config.logger.isDbgEnabled = ( args.debug == True )
        
    if args.restart == True:
        return state.Restart()
    else:
        return state.Start()
        
# ################################################################################################ #
# Script Start                                                                                     #
# ################################################################################################ #
if __name__ == "__main__":
    AccuRev2GitMain(sys.argv)
