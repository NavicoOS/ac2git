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
import xml.etree.ElementTree as ElementTree

import accurev

# GitPython is used to interact with git. Its project page is: https://gitorious.org/git-python
# The repo it was cloned from: git://gitorious.org/git-python/mainline.git at commit 5eb7fd3f0dd99dc6c49da6fd7e78a392c4ef1b33
# You might also need: https://pypi.python.org/pypi/setuptools#installation-instructions
from git import *

# ################################################################################################ #
# Script Core. AccuRev transaction to Git conversion handlers.                                     #
# ################################################################################################ #

def OnAdd(transaction):
    print "OnAdd:", transaction

def OnChstream(transaction):
    print "OnChstream:", transaction
    
def OnCo(transaction):
    print "OnCo:", transaction

def OnDefunct(transaction):
    print "OnDefunct:", transaction

def OnKeep(transaction):
    print "OnKeep:", transaction

def OnMove(transaction):
    print "OnMove:", transaction

def OnMkstream(transaction):
    print "OnMkstream:", transaction

def OnPurge(transaction):
    print "OnPurge:", transaction

def OnDefunct(transaction):
    print "OnDefunct:", transaction

def OnUndefunct(transaction):
    print "OnDefunct:", transaction
    
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
        self.transactionHandlers = { "add": OnAdd, "chstream": OnChstream, "co": OnCo, "defunct": OnDefunct, "keep": OnKeep, "move": OnMove, "mkstream": OnMkstream, "purge": OnPurge, "undefunct": OnUndefunct }
        
    def ProcessAccuRevTransaction(self, transaction):
        handler = self.transactionHandlers.get(transaction.type)
        if handler is not None:
            handler(transaction)
        
    def Start(self):
        if accurev.Login(self.config.accurev.username, self.config.accurev.password):
            print "Login successful"
            
            print "Querying history"
            arHist = accurev.History(depot=self.config.accurev.depot, timeSpec="{0}-{1}.1000".format(self.config.accurev.startTransaction, self.config.accurev.endTransaction))
            
            for transaction in arHist.transactions:
                self.ProcessAccuRevTransaction(transaction)
            
            if accurev.Logout():
                print "Logout successful"
                return 0
            else:
                print "Logout failed"
                return 1
        else:
            print "AccuRev login failed."
            return 1
    
    def Resume(self):
        # For now the resume feature is not supported and causes us to start from scratch again.
        return self.Start()
        
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
        print "No AccuRev username specified."
        isValid = False
    elif config.accurev.password is None:
        print "No AccuRev password specified."
        isValid = False
    elif config.accurev.depot is None:
        print "No AccuRev depot specified."
        isValid = False
    elif config.git.repoPath is None:
        print "No Git repository specified."
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

    accurev2git = AccuRev2Git(config)
    
    if args.resume == "true":
        return accurev2git.Resume()
    else:
        return accurev2git.Start()
        
# ################################################################################################ #
# Script Start                                                                                     #
# ################################################################################################ #
if __name__ == "__main__":
    AccuRev2GitMain(sys.argv)
