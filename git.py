#!/usr/bin/python2

# ################################################################################################ #
# Git utility script                                                                               #
# Author: Lazar Sumar                                                                              #
# Date:   03/12/2014                                                                               #
#                                                                                                  #
# This script is a library that is intended to expose a Python API for the git commands and        #
# command result data structures.                                                                  #
# ################################################################################################ #

import sys
import os
import subprocess
import xml.etree.ElementTree as ElementTree
import datetime
import re
import types

gitCmd = u'git'

# Borrowed from https://github.com/pypa/pip/issues/1137#issuecomment-23613766
# for subprocess.check_output(cmd) calls...
def to_utf8(string):
    if isinstance(string, unicode):
        return string.encode('utf-8')
    else:
        return string

class GitStatus(object):
    # Regular expressions used in fromgitoutput classmethod for parsing the different git lines.
    branchRe        = re.compile(r'^On branch (\w)$')
    blankRe         = re.compile(r'^\s*$')
    commentRe       = re.compile(r'^\s+\(.*\)$')
    # The fileRe - Has a clause at the end for possible submodule modifications where git prints 
    #                (untracked content, modified content)
    #              suffixed messages. This suffix is currently ignored.
    fileRe          = re.compile(r'^\s+(new file|modified|deleted):\s+(\S+)\s*(\(.+\))?$')
    untrackedFileRe = re.compile(r'^\s+(\S+)\s*$')
        
    def __init__(self, branch=None, staged=[], changed=[], untracked=[]):
        self.branch    = branch    # Name of the branch.
        self.staged    = staged    # A list of (filename, file_status) tuples
        self.changed   = changed   # A list of (filename, file_status) tuples
        self.untracked = untracked # A list of (filename,) tuples

    def __repr__(self):
        str  = u'On branch {0}\n'.format(self.branch)
        if self.staged is not None and len(self.staged) > 0:
            str += u'Changes to be committed:\n\n'
            for file, status in self.staged:
                str += u' {0}: {1}\n'.format(status, file)
            str += u'\n'
        if self.changed is not None and len(self.changed) > 0:
            str += u'Changes not staged for commit:\n\n'
            for file, status in self.changed:
                str += u' {0}: {1}\n'.format(status, file)
            str += u'\n'
        if self.untracked is not None and len(self.untracked) > 0:
            str += u'Untracked files:\n\n'
            for file in self.untracked:
                str += u' {0}\n'.format(file[0])
            str += u'\n'
        return str
    
    @classmethod
    def fromgitoutput(cls, gitOutput):
        lines = gitOutput.split(u'\n')
        # git status output example
        # On branch <branch name>
        # Changes to be committed:
        #   (use "git reset HEAD <file>..." to unstage)
        #  
        #  new file:   file1.ext
        #  modified:   file2.ext
        #  deleted:    file3.ext
        #  
        # Changes not staged for commit:
        #   (use git add <file>..." to update what will be committed)
        #   (use "git checkout -- <file>..." to discard changes in working directory)
        #  
        #  modified:    file2.ext
        #  deleted:     file4.ext
        #  
        # Untracked files:
        #   (use "git add <file>..." to include in what will be committed)
        #  
        #  file5.ext
        #  file6.ext
        
        # Parse the branch
        branchName    = None
        branchSpec    = lines.pop(0)
        branchReMatch = GitStatus.branchRe.match(branchSpec)
        if branchReMatch:
            branchName = branchReMatch.group(1)
        
        stagedFiles = []
        changedFiles = []
        untrackedFiles = []
        
        lastHeading = lines.pop(0)
        while len(lines) > 0:
            if lastHeading == u'Changes to be committed:':
                # Find the first blank line
                nextLine = lines.pop(0)
                while not GitStatus.blankRe.match(nextLine) and len(lines) > 0:
                    nextLine = lines.pop(0)
                # Parse files until blank line
                nextLine = lines.pop(0)
                while not GitStatus.blankRe.match(nextLine) and len(lines) > 0:
                    fileMatch = GitStatus.fileRe.match(nextLine)
                    if not fileMatch:
                        raise Exception(u'Line [{0}] did not match [{1}]'.format(nextLine, GitStatus.fileRe.pattern))
                    fileStatus = fileMatch.group(1)
                    fileName   = fileMatch.group(2)
                    stagedFiles.append((fileName, fileStatus))
                    
                    nextLine = lines.pop(0)
            elif lastHeading == u'Changes not staged for commit:':
                # Find the first blank line
                nextLine = lines.pop(0)
                while not GitStatus.blankRe.match(nextLine) and len(lines) > 0:
                    nextLine = lines.pop(0)
                # Parse files until blank line
                nextLine = lines.pop(0)
                while not GitStatus.blankRe.match(nextLine) and len(lines) > 0:
                    fileMatch = GitStatus.fileRe.match(nextLine)
                    if not fileMatch:
                        raise Exception(u'Line [{0}] did not match [{1}]'.format(nextLine, GitStatus.fileRe.pattern))
                    fileStatus = fileMatch.group(1)
                    fileName   = fileMatch.group(2)
                    changedFiles.append((fileName, fileStatus))
                    
                    nextLine = lines.pop(0)
            elif lastHeading == u'Untracked files:':
                # Find the first blank line
                nextLine = lines.pop(0)
                while not GitStatus.blankRe.match(nextLine) and len(lines) > 0:
                    nextLine = lines.pop(0)
                # Parse files until blank line
                nextLine = lines.pop(0)
                while not GitStatus.blankRe.match(nextLine) and len(lines) > 0:
                    fileMatch = GitStatus.untrackedFileRe.match(nextLine)
                    if not fileMatch:
                        raise Exception(u'Line [{0}] did not match [{1}]'.format(nextLine, GitStatus.untrackedFileRe.pattern))
                    fileName   = fileMatch.group(1)
                    untrackedFiles.append((fileName,))
                    
                    nextLine = lines.pop(0)
            
            if len(lines) > 0:
                lastHeading = lines.pop(0)
        
        # stagedFiles and changedFiles are lists of tuples containing two items: (filename, file_status)
        # untracked is also a list of tuples containing two items but the second items is always empty: (filename,)
        return cls(branch=branchName, staged=stagedFiles, changed=changedFiles, untracked=untrackedFiles)

# GitBranchListItem is an object serialization of a single branch output when the git branch -vv
# command is run.
class GitBranchListItem(object):
    branchVVRe = re.compile(r'^(?P<iscurrent>\*)?\s+(?P<name>\S+)\s+(?P<hash>\S+)\s+(?:(?P<remote>\[\S+\])\s+)?(?P<comment>.*)$')
    def __init__(self, name, shortHash, remote, shortComment, isCurrent):
        self.name = name
        self.shortHash = shortHash
        self.remote = remote
        self.shortComment = shortComment
        self.isCurrent = isCurrent
    
    def __repr__(self):
        if self.isCurrent:
            str = u'*'
        else:
            str = u' '
        str += u' {0} {1}'.format(self.name, self.shortHash)
        if self.remote is not None:
            str += u' {0}'.format(self.remote)
        str += u' {0}'.format(self.shortComment)
        
        return str
        
    def __eq__(self, other):
        if type(other) == GitBranchListItem:
            return (self.name == other.name and self.shortHash == other.shortHash)
        raise Exception(u"Can't compare {0} with {1}".format(type(self), type(other)))
        
    @classmethod
    def fromgitbranchoutput(cls, outputLine):
        branchVVMatch = GitBranchListItem.branchVVRe.match(outputLine)
        if branchVVMatch is not None:
            name = branchVVMatch.group(u'name')
            shortHash = branchVVMatch.group(u'hash')
            comment = branchVVMatch.group(u'comment')
            remote =  branchVVMatch.group(u'remote')
            isCurrent = branchVVMatch.group(u'iscurrent')
            isCurrent = (isCurrent is not None)
            
            return cls(name=name, shortHash=shortHash, remote=remote, shortComment=comment, isCurrent=isCurrent)
        return None
    
class repo(object):
    def __init__(self, path):
        self.path = path
        self._cwdQueue = []
        self._lastCommand = None
        self.notes = repo.notes(self)
        
    def _pushd(self, newPath):
        self._cwdQueue.insert(0, os.getcwd())
        os.chdir(newPath)
    
    def _popd(self):
        os.chdir(self._cwdQueue.pop(0))
    
    def _docmd(self, cmd, env=None):
        process = subprocess.Popen(args=(to_utf8(c) for c in cmd), cwd=self.path, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        output = ''
        error  = ''
        process.poll()
        while process.returncode is None:
            stdoutdata, stderrdata = process.communicate()
            output += stdoutdata
            error  += stderrdata
            process.poll()
        
        self._lastCommand = process
 
        if process.returncode == 0:
            return output
        else:
            return None

    def raw_cmd(self, cmd):
        return self._docmd(cmd)
        
    def checkout(self, branchName=None, isNewBranch=False, isOrphan=False):
        cmd = [ gitCmd, u'checkout' ]
        
        if isNewBranch:
            cmd.append(u'-b')
        elif isOrphan:
            cmd.append(u'--orphan')
        
        if branchName is not None:
            cmd.append(branchName)
        
        return self._docmd(cmd)

    def branch(self):
        pass
    
    def rm(self, fileList = [], recursive=False, force=False, cached=False):
        if len(fileList) > 0:
            cmd = [ gitCmd, u'rm' ]

            if recursive:
                cmd.append(u'-r')
            if force:
                cmd.append(u'-f')
            if cached:
                cmd.append(u'--cached')

            cmd.append(u'--')
            cmd.extend(fileList)
            
            output = self._docmd(cmd)
            
            return (output is not None)
        else:
            raise Exception(u'Error, tried to add empty file list')
    
    def add(self, fileList = [], force=False, update=False, all=False):
        cmd = [ gitCmd, u'add' ]
        
        if force:
            cmd.append(u'-f')
        if update:
            cmd.append(u'-u')
        if all:
            cmd.append(u'--all')
        
        if fileList is not None and len(fileList) > 0:
            cmd.append(u'--')
            if isinstance(fileList, list):
                cmd.extend(fileList)
            else:
                cmd.append(unicode(fileList))
        
        output = self._docmd(cmd)
        
        return (output is not None)
    
    def commit(self, message=None, messageFile=None, author=None, date=None, committer=None, committer_date=None, allow_empty=False, allow_empty_message=False):
        cmd = [ gitCmd, u'commit' ]
        
        if allow_empty:
            cmd.append(u'--allow-empty')
        if allow_empty_message:
            cmd.append(u'--allow-empty-message')

        if author is not None:
            cmd.append(u'--author="{0}"'.format(author))
        
        if date is not None:
            if isinstance(date, datetime.datetime):
                date = date.isoformat()
            cmd.append(u'--date="{0}"'.format(date))
        
        if message is not None and len(message) > 0:
            cmd.extend([ u'-m', unicode(message) ])
        elif messageFile is not None:
            cmd.extend([ u'-F', unicode(messageFile) ])
        elif not allow_empty_message:
            raise Exception(u'Error, tried to commit with empty message')
        
        newEnv = os.environ.copy()
        
        # Set the new commiter information
        if committer is not None:
            m = re.search(r'(.*?)<(.*?)>', committer)
            if m is not None:
                committerName = m.group(0).strip()
                committerEmail = m.group(1).strip()
                newEnv[u'GIT_COMMITTER_NAME'] = committerName
                newEnv[u'GIT_COMMITTER_EMAIL'] = committerEmail
        
        if committer_date is not None:
            if committer_date is not None:
                if isinstance(committer_date, datetime.datetime):
                    committer_date = committer_date.isoformat()
            newEnv[u'GIT_COMMITTER_DATE'] = u'{0}'.format(committer_date)
        
        # Execute the command
        output = self._docmd(cmd, env=newEnv)
        
        return (output is not None)
    
    def branch_list(self):
        cmd = [ gitCmd, u'branch', u'-vv' ]
            
        output = self._docmd(cmd)
        
        if output is not None:
            branchList = []
            outputLines = output.split(u'\n')
            for line in outputLines:
                if len(line.strip()) > 0:
                    branchList.append(GitBranchListItem.fromgitbranchoutput(line))
            return branchList
        return None

    def status(self):
        cmd = [ gitCmd, u'status' ]
            
        output = self._docmd(cmd)
        if output is not None:
            return GitStatus.fromgitoutput(output)
        return None

    def reset(self, branch=None, isHard=False, isSoft=False):
        cmd = [ gitCmd, u'reset' ]
        
        if isHard:
            cmd.append(u'--hard')
        if isSoft:
            cmd.append(u'--soft')
        
        if branch is not None:
            cmd.append(branch)
        
        return self._docmd(cmd)
    
    def clean(self, force=False):
        cmd = [ gitCmd, u'clean' ]
    
        if force:
            cmd.append(u'-f')
        
        return self._docmd(cmd)

    class notes(object):
        def __init__(self, repo):
            self.repo = repo
        
        def _docmd(self, cmd, ref=None):
            cmd = [ gitCmd, u'notes' ]

            if ref is not None:
                cmd.extend([ u'--ref', ref ])

            cmd.extend(cmd)

            self.repo._docmd(cmd)

        def add(self, obj, ref=None, force=False, allowEmpty=False, messageFile=None, message=None, reuseMessage=None, reeditMessage=None):
            cmd = [ u'add' ]

            if force:
                cmd.append(u'-f')
            if allowEmpty:
                cmd.append(u'--allow-empty')
            
            if messageFile is not None:
                cmd.extend([ u'-F', messageFile ])
            elif message is not None:
                cmd.extend([ u'-m', message ])
            elif reuseMessage is not None:
                cmd.extend([ u'-C', reuseMessage ])
            elif reeditMessage is not None:
                cmd.extend([ u'-c', reeditMessage ])

            cmd.append(obj)

            self._docmd(cmd=cmd, ref=ref)

        def show(self, obj, ref=None):
            cmd = self.notesCmd + u'show' + obj
            
            return self._docmd(cmd=cmd, ref=ref)
        
def isRepo(path=None):
    if path is not None and os.path.isdir(path):
        if os.path.isdir(os.path.join(path, u'.git')):
            return True
    return False

# GetGitDirPrefix finds the .git/ directory in the given path and returns the path upto the .git/.
# If the path does not contain a .git/ directory then None is returned.
# e.g. Calling GetGitDirPrefix('/home/developer/.git/src') would return '/home/developer/.git'.
#      It is guaranteed that the returned path will not be terminated with a slash.
gitDirRegex = re.compile(r'((^|.*[\\/]).git)([\\/]|$)')
def GetGitDirPrefix(path):
    # This regex will work even for paths which mix \ and /.
    global gitDirRegex
    gitDirMatch = gitDirRegex.match(path)
    if gitDirMatch is not None:
        return gitDirMatch.group(1)
    return None


def init(isBare=False, path=None):
    try:
        cmd = [ gitCmd, u'init' ]
        if isBare:
            cmd.append(u'--bare')
        if path is not None:
            cmd.append(str(path))
        
        output = subprocess.check_output(cmd)
    except:
        return None
    return repo(path)

def open(path):
    if isRepo(path):
        return repo(path=path)
    return None

def delete(path=None):
    if path is None:
        path = os.getcwd()
    if isRepo(path=path):
        for root, dirs, files in os.walk(path, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        return True
    return False

