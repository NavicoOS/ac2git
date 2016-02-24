#!/usr/bin/python3

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

class GitStatus(object):
    # Regular expressions used in fromgitoutput classmethod for parsing the different git lines.
    branchRe        = re.compile(pattern=r'^(HEAD detached at |HEAD detached from |On branch )(\S+)$')
    blankRe         = re.compile(pattern=r'^\s*$')
    commentRe       = re.compile(pattern=r'^\s+\(.*\)$')
    # The fileRe - Has a clause at the end for possible submodule modifications where git prints 
    #                (untracked content, modified content)
    #              suffixed messages. This suffix is currently ignored.
    fileRe          = re.compile(pattern=r'^\s+(new file|modified|deleted|renamed):\s+(.+)\s*(\(.+\))?$')
    untrackedFileRe = re.compile(pattern=r'^\s+(.+?)\s*?$')
        
    def __init__(self, branch=None, staged=[], changed=[], untracked=[], initial_commit=None, detached_head=None):
        self.branch    = branch    # Name of the branch.
        self.staged    = staged    # A list of (filename, file_status) tuples
        self.changed   = changed   # A list of (filename, file_status) tuples
        self.untracked = untracked # A list of (filename,) tuples
        self.initial_commit = initial_commit # A boolean value indicating if this is an initial commit.
        self.detached_head = detached_head   # A boolean value indicating if we are in a detached HEAD state.

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
        #     ac2git.config.xml
        # 
        # nothing added to commit but untracked files present (use "git add" to track)
        # ---------------------------

        # git status output example 4
        # ===========================
        # HEAD detached at refs/notes/accurev/Foreman_Frazier_Development
        # nothing to commit, working directory clean
        # ---------------------------

        # git status output example 5
        # ===========================
        # HEAD detached from refs/notes/accurev/Foreman_Frazier_Development
        # nothing to commit, working directory clean
        # ---------------------------

        # git status output example 6 (not yet handled.)
        # ===========================
        # HEAD detached at 2b13a24
        # nothing to commit, working directory clean
        # ---------------------------

        # Parse the branch
        branchName     = None
        isDetachedHead = None
        branchSpec     = lines.pop(0)
        branchReMatch  = GitStatus.branchRe.match(branchSpec)
        if branchReMatch:
            isDetachedHead = (branchReMatch.group(1) in [ "HEAD detached at ", "HEAD detached from " ])
            branchName = branchReMatch.group(2)
        else:
            raise Exception(u'Line [{0}] did not match [{1}]'.format(branchSpec, GitStatus.branchRe.pattern))
            
        
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
        return cls(branch=branchName, staged=stagedFiles, changed=changedFiles, untracked=untrackedFiles, initial_commit=isInitialCommit, detached_head=isDetachedHead)

# GitBranchListItem is an object serialization of a single branch output when the git branch -vv
# command is run.
class GitBranchListItem(object):
    branchVVRe = re.compile(pattern=r'^(?P<iscurrent>\*)?\s+(?P<name>.+?)\s+(?P<hash>[A-Fa-f0-9]+)\s+(?:(?P<remote>\[\S+\])\s+)?(?P<comment>.*)$')
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

class GitRemoteListItem(object):
    remoteVVRe = re.compile(pattern='^(?P<name>\S+)\s+(?P<url>\S+)\s+(?P<action>\S+)', flags=re.MULTILINE)
    def __init__(self, name, url, pushUrl=None):
        self.name    = name
        self.url     = url
        self.pushUrl = pushUrl

    def __repr__(self):
        str = '{name}\t{url} (fetch)\n{name}\t{pushUrl} (push)'.format(name=self.name, url=self.url, pushUrl=self.url if self.pushUrl is None else self.pushUrl)
        return str

    @classmethod
    def fromgitremoteoutput(cls, output):
        remotes = {}
        for remoteVVMatch in GitRemoteListItem.remoteVVRe.finditer(output):
            if remoteVVMatch is not None:
                name = remoteVVMatch.group(u'name')
                action = remoteVVMatch.group(u'action')
                url = None
                pushUrl = None
                if action == "(fetch)":
                    url  = remoteVVMatch.group(u'url')
                elif action == "(push)":
                    pushUrl = remoteVVMatch.group(u'url')
                else:
                    raise Exception("Unrecognized suffix {suffix} for remote string {s}!".format(suffix=action, s=remoteVVMatch.group(0)))
                
                if name not in remotes:
                    remotes[name] = cls(name=name, url=url, pushUrl=pushUrl)
                else:
                    if url is not None:
                        remotes[name].url = url
                    if pushUrl is not None:
                        remotes[name].pushUrl = pushUrl

        return remotes.values()

class GitCommit(object):
    # Regular expressions used in fromgitoutput classmethod for parsing the different git lines.
    infoRe = re.compile(pattern=r'^\[(?P<branch>\S+|detached HEAD)\s(?P<root>\(root-commit\)\s)?(?P<shortHash>[A-Fa-f0-9]+)\]\s(?P<title>.*)$')

    # Git commit output examples:
    #
    # Example 1:
    # > git commit -m "Cleaning up the log output for transactions method."
    # [master 66d6c95] Cleaning up the log output for transactions method.
    #  1 file changed, 3 insertions(+), 1 deletion(-)
    #
    # Example 2:
    # > git commit -m "Fixing the cherry-pick commit message for transactions method."
    # [master a536a2c] Fixing the cherry-pick commit message for transactions method.
    #  1 file changed, 12 insertions(+), 9 deletions(-)
    #
    # Example 3:
    # > git commit -m "Adding parsing of git commit output."
    # [master b712533] Adding parsing of git commit output.
    #  2 files changed, 53 insertions(+), 13 deletions(-)
    #
    # Example 4:
    # > git commit -m "Notes remapped by 'remap_notes.py'"
    # [detached HEAD deef69f] Notes remapped by 'remap_notes.py'

    def __init__(self, branch=None, shortHash=None, title=None, isRoot=False):
        self.branch    = branch    # Name of the branch on which the commit was made.
        self.shortHash = shortHash # The string representing the short commit hash (a 7 digit hex number).
        self.title     = title     # The first line of the commit message.
        self.isRoot    = isRoot

    def __repr__(self):
        str = '[{br} {short_hash}] {title}'.format(br=self.branch, short_hash=self.shortHash, title=self.title)
        return str
    
    @classmethod
    def fromgitoutput(cls, gitOutput):
        if gitOutput is not None:
            lines = gitOutput.split(u'\n')
            if len(lines) > 0:
                infoMatch = GitCommit.infoRe.match(lines[0])
                if infoMatch is not None:
                    branch = infoMatch.group("branch")
                    shortHash = infoMatch.group("shortHash")
                    title = infoMatch.group("title")
                    isRoot = infoMatch.group("root") is not None
                    return cls(branch=branch, shortHash=shortHash, title=title, isRoot=isRoot)
                else:
                    raise Exception("Failed to match git commit output! re: '{re}'\noutput:\n{output}".format(re=GitCommit.infoRe.pattern, output=lines[0]))
            else:
                raise Exception("Git commit returned no lines!")
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
        self._lastCommand = None
    
    def _docmd(self, cmd, env=None):
        process = subprocess.Popen(args=cmd, cwd=self.path, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

        output = ''
        error  = ''
        process.poll()
        while process.returncode is None:
            stdoutdata, stderrdata = process.communicate()
            output += stdoutdata
            error  += stderrdata
            process.poll()

        if len(output) + len(error) == 0:
            try:
                stdoutdata, stderrdata = process.communicate()
                output += stdoutdata
                error  += stderrdata
            except:
                pass
        
        self._lastCommand = process
        self.lastStderr = error
        self.lastStdout = output
        self.lastReturnCode = process.returncode

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
        raise Exception("Not yet implemented!")
    
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
    
    def add(self, fileList = [], force=False, update=False, all=False, git_opts=[]):
        cmd = [ gitCmd ]
        
        if git_opts is not None and len(git_opts) > 0:
            cmd.extend(git_opts)

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
                cmd.append(fileList)
        
        output = self._docmd(cmd)
        
        return (output is not None)
    
    def write_tree(self, missingOk=False, prefix=None, git_opts=[]):
        cmd = [ gitCmd ]
        
        if git_opts is not None and len(git_opts) > 0:
            cmd.extend(git_opts)

        cmd.append(u'write-tree')

        if missingOk:
            cmd.append(u'--missing-ok')
        if prefix is not None:
            cmd.append(u'--prefix={prefix}'.format(prefix=prefix))

        # Execute the command
        output = self._docmd(cmd)

        return output

    def commit_tree(self, tree=None, parents=[], message=None, message_file=None, author_name=None, author_email=None, author_date=None, author_tz=None, committer_name=None, committer_email=None, committer_date=None, committer_tz=None, gpg_key=None, no_gpg_sign=False, git_opts=[], allow_empty=False):
        # git commit example output
        # =========================
        # git commit -m "Parameterizing hardcoded values."
        # [master 0a0d053] Parameterizing hardcoded values.
        #  1 file changed, 9 insertions(+), 7 deletions(-)
        #--------------------------
        cmd = [ gitCmd ]
        
        if git_opts is not None and len(git_opts) > 0:
            cmd.extend(git_opts)

        cmd.append(u'commit-tree')

        if parents is not None:
            for parent in parents:
                cmd.extend([ u'-p', parent ])

        if no_gpg_sign:
            cmd.append(u'--no-gpg-sign')
        elif gpg_key is not None:
            cmd.append(u'-S{key}'.format(key=gpgKey))

        if message is not None and len(message) > 0:
            cmd.extend([ u'-m', message ])
        elif message_file is not None:
            cmd.extend([ u'-F', message_file ])
        elif not allow_empty_message:
            raise Exception(u'Error, tried to commit with empty message')

        if tree is None:
            if allow_empty:
                tree = '4b825dc642cb6eb9a060e54bf8d69288fbee4904' # Git's empty tree. See http://stackoverflow.com/questions/9765453/gits-semi-secret-empty-tree
            else:
                raise Exception("git.commit_tree() - Cannot commit empty without the allow_empty being set to true.")

        cmd.append(tree)
        
        newEnv = os.environ.copy()
        
        # Set the author information
        if author_name is not None:
            newEnv['GIT_AUTHOR_NAME'] = author_name
        if author_email is not None:
            newEnv['GIT_AUTHOR_EMAIL'] = author_email
        
        if author_date is not None:
            author_date_str = getDatetimeString(author_date, author_tz)
            if author_date_str is not None:
                newEnv['GIT_AUTHOR_DATE'] = str('{0}'.format(author_date_str))
        
        # Set the committer information
        if committer_name is not None:
            newEnv['GIT_COMMITTER_NAME'] = committer_name
        if committer_email is not None:
            newEnv['GIT_COMMITTER_EMAIL'] = committer_email
        
        if committer_date is not None:
            committer_date_str = getDatetimeString(committer_date, committer_tz)
            if committer_date_str is not None:
                newEnv['GIT_COMMITTER_DATE'] = str('{0}'.format(committer_date_str))
        
        # Execute the command
        output = self._docmd(cmd, env=newEnv)

        return output

    def commit(self, message=None, message_file=None, author_name=None, author_email=None, author_date=None, author_tz=None, committer_name=None, committer_email=None, committer_date=None, committer_tz=None, allow_empty=False, allow_empty_message=False, cleanup=None, git_opts=[]):
        # git commit example output
        # =========================
        # git commit -m "Parameterizing hardcoded values."
        # [master 0a0d053] Parameterizing hardcoded values.
        #  1 file changed, 9 insertions(+), 7 deletions(-)
        #--------------------------
        cmd = [ gitCmd ]
        
        if git_opts is not None and len(git_opts) > 0:
            cmd.extend(git_opts)

        cmd.append(u'commit')
        
        if allow_empty:
            cmd.append(u'--allow-empty')
        if allow_empty_message:
            cmd.append(u'--allow-empty-message')

        if message is not None and len(message) > 0:
            cmd.extend([ u'-m', message ])
        elif message_file is not None:
            cmd.extend([ u'-F', message_file ])
        elif not allow_empty_message:
            raise Exception(u'Error, tried to commit with empty message')
        
        if cleanup is not None:
            # This option determines how the supplied commit message should be cleaned up before committing. The <mode> can be strip, whitespace, verbatim, scissors or default.
            #   * strip      - Strip leading and trailing empty lines, trailing whitespace, commentary and collapse consecutive empty lines.
            #   * whitespace - Same as strip except #commentary is not removed.
            #   * verbatim   - Do not change the message at all.
            #   * scissors   - Same as whitespace, except that everything from (and including) the line "# ------------------------ >8 ------------------------" is truncated if the message is to be edited. "#" can be customized with core.commentChar.
            #   * default    - Same as strip if the message is to be edited. Otherwise whitespace.
            # See: https://www.kernel.org/pub/software/scm/git/docs/git-commit.html
            allowedValues = [ 'strip', 'whitespace', 'verbatim', 'scissors', 'default' ]
            if cleanup not in allowedValues:
                raise Exception("git.commit() unrecognized value for parameter cleanup. Got '{val}' but expected one of '{allowed}'.".format(val=cleanup, allowed="', '".join(allowedValues)))
            cmd.append(u'--cleanup={cleanup}'.format(cleanup=cleanup))

        newEnv = os.environ.copy()
        
        # Set the author information
        if author_name is not None:
            newEnv['GIT_AUTHOR_NAME'] = author_name
        if author_email is not None:
            newEnv['GIT_AUTHOR_EMAIL'] = author_email
        
        if author_date is not None:
            author_date_str = getDatetimeString(author_date, author_tz)
            if author_date_str is not None:
                newEnv['GIT_AUTHOR_DATE'] = str('{0}'.format(author_date_str))
        
        # Set the committer information
        if committer_name is not None:
            newEnv['GIT_COMMITTER_NAME'] = committer_name
        if committer_email is not None:
            newEnv['GIT_COMMITTER_EMAIL'] = committer_email
        
        if committer_date is not None:
            committer_date_str = getDatetimeString(committer_date, committer_tz)
            if committer_date_str is not None:
                newEnv['GIT_COMMITTER_DATE'] = str('{0}'.format(committer_date_str))
        
        # Execute the command
        output = self._docmd(cmd, env=newEnv)

        return GitCommit.fromgitoutput(output)
    
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

    def remote_list(self):
        cmd = [ gitCmd, u'remote', u'-vv' ]
        output = self._docmd(cmd)
        return GitRemoteListItem.fromgitremoteoutput(output)

    def remote_add(self, name, url, branch=None, master=None, fetch=False, importTags=None):
        cmd = [ gitCmd, u'remote', u'add' ]
        
        if branch is not None:
            cmd.extend([ u'-t', branch ])
        if master is not None:
            cmd.extend([ u'-m', master ])
        if fetch:
            cmd.append(u'-f')
        if importTags is not None:
            if importTags == True:
                cmd.append(u'--tags')
            elif importTags == False:
                cmd.append(u'--no-tags')
            else:
                raise Exception("Invalid value for import tags! Expected True or False but got {v}".format(v=str(importTags)))

        cmd.extend([ name, url ])

        return self._docmd(cmd)

    def remote_set_url(self, name, url, oldUrlRegex=None, isPushUrl=False, add=False, delete=False):
        cmd = [ gitCmd, u'remote', u'add', u'set-url' ]

        if add and delete:
            raise Exception("Can't add and delete the url {u} for remote {r}".format(u=url, r=name))
        elif add:
            cmd.append(u'--add')
        elif delete:
            cmd.append(u'--delete')
        elif oldUrlRegex is not None:
            raise Exception("Can't specify an oldUrlRegex when using isAdd or isDelete!")

        if isPushUrl:
            cmd.append(u'--push')
        
        cmd.extend([ name, url ])
        return self._docmd(cmd)

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
    
    def clean(self, directories=False, force=False, forceSubmodules=False, dryRun=False, quiet=False, includeIgnored=False, onlyIgnored=False):
        cmd = [ gitCmd, u'clean' ]
    
        if directories:
            cmd.append(u'-d')

        if forceSubmodules:
            cmd.append(u'-f')
            cmd.append(u'-f')
        elif force:
            cmd.append(u'-f')
            
        if dryRun:
            cmd.append(u'-n')
        if quiet:
            cmd.append(u'-q')
        if includeIgnored:
            cmd.append(u'-x')
        if onlyIgnored:
            cmd.append(u'-X')
        
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

        def add(self, obj, ref=None, force=False, allowEmpty=False, messageFile=None, message=None, reuseMessage=None, reeditMessage=None, committerName=None, committerEmail=None, committerDate=None, committerTimezone=None, authorName=None, authorEmail=None, authorDate=None, authorTimezone=None):
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

            # Set the author information
            if authorName is not None:
                newEnv['GIT_AUTHOR_NAME'] = authorName
            if authorEmail is not None:
                newEnv['GIT_AUTHOR_EMAIL'] = authorEmail
            
            if authorDate is not None:
                author_date_str = getDatetimeString(authorDate, authorTimezone)
                if author_date_str is not None:
                    newEnv['GIT_AUTHOR_DATE'] = str('{0}'.format(author_date_str))
            
            # Set the committer information
            if committerName is not None:
                newEnv['GIT_COMMITTER_NAME'] = committerName
            if committerEmail is not None:
                newEnv['GIT_COMMITTER_EMAIL'] = committerEmail
            
            if committerDate is not None:
                committer_date_str = getDatetimeString(committerDate, committerTimezone)
                if committer_date_str is not None:
                    newEnv['GIT_COMMITTER_DATE'] = str('{0}'.format(committer_date_str))

            return self._docmd(cmd=cmd, ref=ref, env=newEnv)

        def show(self, obj, ref=None):
            cmd = [ u'show', obj ]
            
            return self._docmd(cmd=cmd, ref=ref)
        
        def diff(self, refs=[], files=[], stat=False):
            cmd = [u'git', u'diff' ]
            if stat:
                cmd.append(u'--stat')
            cmd.extend(refs)
            cmd.append(u'--')
            cmd.extend(files)
            return self._docmd(cmd=cmd)
        
        def merge_base(self, commits=[], all=False, octopus=False, is_ancestor=False, independent=False, fork_point=False, ref=None):
            cmd = [u'git', u'merge-base']
            if all:
                cmd.append(u'--all')
            elif octopus:
                cmd.append(u'--octopus')
            elif is_ancestor:
                cmd.append(u'--is-ancestor')
                if len(commits) != 2:
                    raise Exception("git merge-base --is-ancestor <commit> <commit>, only accepts two commits!")
            elif independent:
                cmd.append(u'--independent')
            elif fork_point:
                if ref is None:
                    raise Exception("Must provide ref to git merge-base when specifying fork_point=True!")
                elif len(commits) > 1:
                    raise Exception("Only one, optional, commit can be provided to git merge-base when fork_point=True is specified!")
                cmd.extend(['--fork-point', ref])
            cmd.extend(commits)
            
            output = self._docmd(cmd=cmd)
            if is_ancestor:
                if self.lastReturnCode == 0:
                    return True
                elif self.lastReturnCode == 1:
                    return False
                else:
                    return None
            return output
        
        def rev_parse(self, args=[], verify=False):
            cmd = [u'git', u'rev-parse']
            if verify:
                cmd.append(u'--verify')
            cmd.extend(args)
            return self._docmd(cmd=cmd)
        
def isRepo(path=None):
    try:
        cmd = [ gitCmd, u'-C', path, u'rev-parse', u'--is-inside-work-tree' ]
        output = subprocess.check_output(cmd)
        return True
    except:
        return False

# GetGitDirPrefix finds the .git/ directory in the given path and returns the path upto the .git/.
# If the path does not contain a .git/ directory then None is returned.
# e.g. Calling GetGitDirPrefix('/home/developer/.git/src') would return '/home/developer/.git'.
#      It is guaranteed that the returned path will not be terminated with a slash.
gitDirRegex = re.compile(pattern=r'((^|.*[\\/]).git)([\\/]|$)')
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
        try:
            cmd = [ gitCmd, u'-C', path, u'rev-parse', u'--show-toplevel' ]
            output = subprocess.check_output(cmd)
            output = output.strip()
            return repo(output)
        except:
            pass
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

