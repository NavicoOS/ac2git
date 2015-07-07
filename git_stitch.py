#!/usr/bin/python2

# git repository stitching tool
# Author: Lazar Sumar
# Date:   18/03/2015

import sys
import os
import os.path
import subprocess
import re

# Python magic that gets the directory of our script
# and adds its subdirectory accurev2git/python/ to
# the search path for importing the git.py module.
scriptPath, scriptFilename = os.path.split(sys.argv[0])
relPath = os.path.join(u'accurev2git', u'python')
relPath = os.path.join(scriptPath, relPath)
absPath = os.path.join(os.getcwd(), relPath)
sys.path.append( absPath )
import git

def BuildDatabase():
    pass

# CatFileCommit function
# This function executes the `git cat-file -p <commit-hash>` command and
# parses its result and converts it to a dictionary with the following format:
# {
#   'hash': <commit-hash>,
#   'object': {
#     'type': 'blob'|'tree'|'commit'|'tag', # Should be tree most of the time.
#     'hash': <hash>
#   },
#   'parents': [ # optional, may not be present
#     <hash>,
#     ...
#    ],
#    'author': {
#      'name': <author-name>
#      'email': <author-email>
#      'time': <author-time>
#      'timezone': <author-timezone>
#    },
#    'committer': {
#      'name': <committer-name>
#      'email': <committer-email>
#      'time': <committer-time>
#      'timezone': <committer-timezone>
#    },
#    'comment': <comment>
def CatFileCommit(commit_hash):
    refRe             = re.compile(r'(?P<type>tree|blob) (?P<hash>[a-fA-F0-9]+)')
    parentRe          = re.compile(r'parent (?P<hash>[a-fA-F0-9]+)')
    authorCommitterRe = re.compile(r'(?P<who>author|committer) (?P<name>\w.*) <(?P<email>.*)> (?P<time>[0-9]+) (?P<timezone>[\+-]?[0-9]+)')

    git_cmd = u'git cat-file -p {0}'.format(commit_hash)
    cmd = git_cmd.split()
    try:
        cat_file_output = subprocess.check_output(git.to_utf8(c) for c in cmd)
    except subprocess.CalledProcessError as e:
        print(u'Failed to execute command: {0}'.format(git_cmd))
        raise e

    commit_info = {}
    commit_info[u'hash'] = commit_hash
    cat_file_lines = cat_file_output.split(git.to_utf8(u'\n'))

    # The first line is the object to which the commit points, parse it.
    nextIndex = 0
    line = cat_file_lines[nextIndex]
    m = refRe.match(line)
    obj = {}
    if m:
        obj[u'type'] = m.group(u'type')
        obj[u'hash'] = m.group(u'hash')
    commit_info[u'object'] = obj

    # The next few lines are the parent/s. Consume them all.
    nextIndex += 1
    parents = []
    for i in xrange(nextIndex, len(cat_file_lines)):
        m = parentRe.match(cat_file_lines[i])
        if m:
            parents.append(m.group(u'hash'))
        else:
            nextIndex = i
            break
    
    if len(parents) > 0:
        commit_info[u'parents'] = parents
    
    # The next two lines are the author followed by the committer.
    line = cat_file_lines[nextIndex]
    m = authorCommitterRe.match(line)
    if m:
        author = {}
        author[u'name'] = m.group(u'name')
        author[u'email'] = m.group(u'email')
        author[u'time'] = m.group(u'time')
        author[u'timezone'] = m.group(u'timezone')
        commit_info[u'author'] = author
    
    nextIndex += 1
    line = cat_file_lines[nextIndex]
    m = authorCommitterRe.match(line)
    if m:
        committer = {}
        committer[u'name'] = m.group(u'name')
        committer[u'email'] = m.group(u'email')
        committer[u'time'] = m.group(u'time')
        committer[u'timezone'] = m.group(u'timezone')
        commit_info[u'committer'] = committer

    nextIndex += 1
    if len(cat_file_lines[nextIndex]) == 0:
        nextIndex += 1
    comment = git.to_utf8(u'\n').join(cat_file_lines[nextIndex:])
    commit_info[u'comment'] = comment

    return commit_info

def GetBranchRevisionMap(gitRepoPath):
    # 1. Compare all of the different branch's commit timestamps.
    # 2. Select the earliest commit and iterate over commits (from all branches).
    # 3. Store each processed commit hash against the hash of the "tree" object to which it
    #    points. You can get the tree hashes via the `git cat-file -p <commit-hash>` command.
    # 4. When processing any new commit, look up to see if the tree object has already been
    #    cataloged in step 3.
    #      YES -> Figure out if it makes sense for these branches to be stitched together.
    #             It is recommended that you consider the committer, the commit time, the author
    #             and the author time of each commit.
    #      NO  -> Continue.

    if git.isRepo(gitRepoPath):
        repo = git.open(gitRepoPath)
        branchList = repo.branch_list()
        branchRevMap = {}

        # For each branch in the branch list get the commit history which doesn't share
        # ancestry with any other branch.
        # See 'git rev-list'
        # Example command:
        #   git rev-list --reverse my-branch ^other-branch ^another-branch
        for current_branch in branchList:
            # Get the commits that are only on this branch.
            revListArgs = [ current_branch.name ]
            for other_branch in branchList:
                if current_branch != other_branch:
                    revListArgs.append('^{0}'.format(other_branch.name))

            git_cmd = u'git rev-list --reverse'
            for arg in revListArgs:
                git_cmd = u'{0} {1}'.format(git_cmd, arg)
            
            cmd = git_cmd.split()
            try:
              revlist = subprocess.check_output(git.to_utf8(c) for c in cmd)
            except subprocess.CalledProcessError as e:
              print(u'Failed to execute command: {0}'.format(git_cmd))
              raise e

            #print(u'Executed: {0}'.format(git_cmd))
            revlist = revlist.split()

            # For each of the commits returned by the git rev-list command get the tree hash
            # to which they point and store it in a map against the tree hash.
            # For each commit map it to its tree by using the git cat-file command.
            for rev in revlist:
                commit_info = CatFileCommit(rev)
                commit_info[u'branch'] = current_branch
                tree_hash = commit_info[u'object'][u'hash']
                
                #print(u'commit: {0}, tree: {1}, branch: {2}'.format(rev, tree_hash, current_branch.name))
                
                if not branchRevMap.has_key(tree_hash):
                    branchRevMap[tree_hash] = []
                branchRevMap[tree_hash].append(commit_info)

        return branchRevMap
    return None

def Main(argv):
    branchRevMap = GetBranchRevisionMap(u'.')
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

                    if firstTime == secondTime:
                        print(u'  squash {0} as equiv. to {1}. tree {2}.'.format(first[u'hash'][:8], second[u'hash'][:8], tree_hash[:8]))
                    elif firstTime < secondTime:
                        try:
                            parents = second[u'parents']
                            if parents is None:
                                raise Exception()
                        except:
                            parents = []

                        parents.append(first[u'hash'])
                        print(u'  merge  {0} as parent of {1}. tree {2}. parents {3}'.format(first[u'hash'][:8], second[u'hash'][:8], tree_hash[:8], [x[:8] for x in parents] ))
                    else:
                        raise Exception(u'Error: wrong sort order!')

        return True
    return False

if __name__ == "__main__":
    if not Main(sys.argv):
        sys.exit(1)


