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
from math import floor

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
    fileRe          = re.compile(r'^\s+(new file|modified|deleted):\s+(.+)\s*(\(.+\))?$')
    untrackedFileRe = re.compile(r'^\s+(\S+)\s*$')
        
    def __init__(self, branch=None, staged=[], changed=[], untracked=[], initial_commit=None):
        self.branch    = branch    # Name of the branch.
        self.staged    = staged    # A list of (filename, file_status) tuples
        self.changed   = changed   # A list of (filename, file_status) tuples
        self.untracked = untracked # A list of (filename,) tuples
        self.initial_commit = initial_commit # A boolean value indicating if this is an initial commit.

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
        # git status output example 1
        # ===========================
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
        # ---------------------------
        
        # git status output example 2 (not yet fully handled. TODO: nothing to commit message)
        # ===========================
        # On branch master
        # 
        # Initial commit
        # 
        # nothing to commit (create/copy files and use "git add" to track)
        # ---------------------------

        # git status output example 3 (not yet fully handled. TODO: Remote branch and nothing to commit message)
        # ===========================
        # On branch master
        # Your branch is up-to-date with 'origin/master'.
        # Untracked files:
        #   (use "git add <file>..." to include in what will be committed)
        # 
        # 	ac2git.config.xml
        # 
        # nothing added to commit but untracked files present (use "git add" to track)
        # ---------------------------

        # Parse the branch
        branchName    = None
        branchSpec    = lines.pop(0)
        branchReMatch = GitStatus.branchRe.match(branchSpec)
        if branchReMatch:
            branchName = branchReMatch.group(1)
        
        isInitialCommit = False
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
            elif lastHeading == u'Initial commit':
                isInitialCommit = True

            if len(lines) > 0:
                lastHeading = lines.pop(0)
        
        # stagedFiles and changedFiles are lists of tuples containing two items: (filename, file_status)
        # untracked is also a list of tuples containing two items but the second items is always empty: (filename,)
        return cls(branch=branchName, staged=stagedFiles, changed=changedFiles, untracked=untrackedFiles, initial_commit=isInitialCommit)

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
    
def getDatetimeString(date, timezone=None):
    dateStr = None
    if date is not None:
        if isinstance(date, datetime.datetime):
            date = date.isoformat()
            if timezone is None:
                tzoffset = date.utcoffset()
                if tzoffset is not None:
                    tzseconds = tzoffset.total_seconds()
                    tzmin   = int(floor(abs(tzseconds) / 60))
                    tzhours = int(floor(tzmin / 60))
                    tzmin   %= 60

                    timezone = int((tzhours * 100) + tzmin)
                    if tzseconds < 0:
                        timezone = -timezone

        dateStr = u'{0}'.format(date)
        if timezone is not None:
            if isinstance(timezone, float):
                timezone = int(timezone)

            if isinstance(timezone, int):
                dateStr = u'{0} {1:+05}'.format(dateStr, timezone)
            else:
                dateStr = u'{0} {1}'.format(dateStr, timezone)
    
    return dateStr

class repo(object):
    def __init__(self, path):
        self.path = path
        self.notes = repo.notes(self)
        # Debug
        self.lastStderr = None
        self.lastStdout = None
        self.lastReturnCode = None
        # Private
        self._cwdQueue = []
        self._lastCommand = None
        
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
        self.lastStderr = error.decode('utf-8')
        self.lastStdout = output.decode('utf-8')
        self.lastReturnCode = process.returncode

        if process.returncode == 0:
            return output.decode('utf-8')
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
    
    def add(self, fileList = [], force=False, update=False, all=False, gitOpts=[]):
        cmd = [ gitCmd ]
        
        if gitOpts is not None and len(gitOpts) > 0:
            cmd.extend(gitOpts)

        cmd.append(u'add')
        
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
    
    def commit(self, message=None, messageFile=None, author=None, date=None, tz=None, committer=None, committer_date=None, committer_tz=None, allow_empty=False, allow_empty_message=False, gitOpts=[]):
        # git commit example output
        # =========================
        # git commit -m "Parameterizing hardcoded values."
        # [master 0a0d053] Parameterizing hardcoded values.
        #  1 file changed, 9 insertions(+), 7 deletions(-)
        #--------------------------
        cmd = [ gitCmd ]
        
        if gitOpts is not None and len(gitOpts) > 0:
            cmd.extend(gitOpts)

        cmd.append(u'commit')
        
        if allow_empty:
            cmd.append(u'--allow-empty')
        if allow_empty_message:
            cmd.append(u'--allow-empty-message')

        if author is not None:
            cmd.append(u'--author="{0}"'.format(author))
        
        if date is not None:
            dateStr = getDatetimeString(date, tz)
            if dateStr is not None:
                cmd.append(u'--date="{0}"'.format(dateStr))
        
        if message is not None and len(message) > 0:
            cmd.extend([ u'-m', unicode(message) ])
        elif messageFile is not None:
            cmd.extend([ u'-F', unicode(messageFile) ])
        elif not allow_empty_message:
            raise Exception(u'Error, tried to commit with empty message')
        
        newEnv = os.environ.copy()
        
        # Set the new committer information
        if committer is not None:
            m = re.search(r'(.*?)<(.*?)>', committer)
            if m is not None:
                committerName = m.group(1).strip()
                committerEmail = m.group(2).strip()
                newEnv['GIT_COMMITTER_NAME'] = str(committerName)
                newEnv['GIT_COMMITTER_EMAIL'] = str(committerEmail)
        
        if committer_date is not None:
            committer_date_str = getDatetimeString(committer_date, committer_tz)
            if committer_date_str is not None:
                newEnv['GIT_COMMITTER_DATE'] = str('{0}'.format(committer_date_str))
        
        # Execute the command
        output = self._docmd(cmd, env=newEnv)
        
        return (output is not None)
    
    def branch_list(self, containsCommit=None, mergedCommit=None, noMergedCommit=None):
        cmd = [ gitCmd, u'branch', u'-vv' ]

        if containsCommit is not None:
            cmd.extend([ u'--contains', containsCommit ])
        elif mergedCommit is not None:
            cmd.extend([ u'--merged', mergedCommit ])
        elif noMergedCommit is not None:
            cmd.extend([ u'--no-merged', noMergedCommit ])
            
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
        
        def _docmd(self, cmd, ref=None, env=None):
            fullCmd = [ gitCmd, u'notes' ]

            if ref is not None:
                fullCmd.extend([ u'--ref', ref ])

            fullCmd.extend(cmd)
            
            return self.repo._docmd(cmd=fullCmd, env=env)

        def add(self, obj, ref=None, force=False, allowEmpty=False, messageFile=None, message=None, reuseMessage=None, reeditMessage=None, committer=None, committerDate=None, committerTimezone=None, author=None, authorDate=None, authorTimezone=None):
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
        
            newEnv = os.environ.copy()

            # Set the new committer information
            if committer is not None:
                m = re.search(r'(.*?)<(.*?)>', committer)
                if m is not None:
                    committerName = m.group(1).strip()
                    committerEmail = m.group(2).strip()
                    newEnv['GIT_COMMITTER_NAME'] = str(committerName)
                    newEnv['GIT_COMMITTER_EMAIL'] = str(committerEmail)
            
            if committerDate is not None:
                committer_date_str = getDatetimeString(committerDate, committerTimezone)
                if committer_date_str is not None:
                    newEnv['GIT_COMMITTER_DATE'] = str('{0}'.format(committer_date_str))
        
            # Set the new author information
            if author is not None:
                m = re.search(r'(.*?)<(.*?)>', author)
                if m is not None:
                    authorName = m.group(1).strip()
                    authorEmail = m.group(2).strip()
                    newEnv['GIT_AUTHOR_NAME'] = str(authorName)
                    newEnv['GIT_AUTHOR_EMAIL'] = str(authorEmail)
            
            if authorDate is not None:
                author_date_str = getDatetimeString(authorDate, authorTimezone)
                if author_date_str is not None:
                    newEnv['GIT_AUTHOR_DATE'] = str('{0}'.format(author_date_str))

            return self._docmd(cmd=cmd, ref=ref, env=newEnv)

        def show(self, obj, ref=None):
            cmd = [ u'show', obj ]
            
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

