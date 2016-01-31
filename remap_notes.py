#!/usr/bin/python3

# ################################################################################################ #
# Git note rewriting utility script                                                                #
# Author: Lazar Sumar                                                                              #
# Date:   29/01/2016                                                                               #
#                                                                                                  #
# This script is intended to be used after a branch stitching operation has been performed by      #
# ac2git on its converted repository. It uses a commit map to re-target all the git notes which    #
# store the state for each tracked stream. This script should make it possible to continue using   #
# a stitched repository for tracking purposes.                                                     #
# ################################################################################################ #

import sys
import os
import posixpath
import codecs
import json
import argparse

import git

def moveNote(repo, oldHash, newHash):
    oldDir, oldFile = oldHash[:2], oldHash[2:]
    newDir, newFile = newHash[:2], newHash[2:]
    oldPath = posixpath.join(oldDir, oldFile)
    newPath = posixpath.join(newDir, newFile)
    if os.path.exists(os.path.join(oldDir, oldFile)):
        if not os.path.exists(os.path.join(newDir, newFile)):
            if not os.path.exists(newDir):
                os.mkdir(newDir)
            if repo.raw_cmd(['git', 'mv', oldPath, newPath]) is None:
                raise Exception("Failed to move {old} to {new}. Err: {err}".format(old=oldPath, new=newPath, err=repo.lastStderr))
            else:
                return True
        else:
            jsonList = None
            with codecs.open(os.path.join(oldDir, oldFile)) as f:
                contents = f.read()
                jsonObj = json.loads(contents)
                if not isinstance(jsonObj, list):
                    jsonList = [ jsonObj ]
                else:
                    jsonList = jsonObj
            with codecs.open(os.path.join(newDir, newFile)) as f:
                contents = f.read()
                jsonObj = json.loads(contents)
                if not isinstance(jsonObj, list):
                    jsonList.append(jsonObj)
                else:
                    jsonList.extend(jsonObj)
            with codecs.open(os.path.join(newDir, newFile), 'w') as f:
                jsonStr = json.dumps(jsonList)
                f.write(jsonStr)
            
            if repo.raw_cmd(['git', 'add', newPath]) is None:
                raise Exception("Failed to add {new}. Err: {err}".format(new=newPath, err=repo.lastStderr))
            elif repo.raw_cmd(['git', 'rm', oldPath]) is None:
                raise Exception("Failed to remove {old}. Err: {err}".format(old=oldPath, err=repo.lastStderr))
            else:
                return True
    else:
        #print("No note found for commit {hash}".format(hash=oldHash))
        pass
    
    return False

def remapNotesCommand(args):
    argparser = argparse.ArgumentParser(description='Rewrites git notes for the ac2git script on the basis of a commit map that was output by ac2git from its finalize step. Not intended to be used manually!')
    argparser.add_argument('-r', '--git-repo', required=True, dest='repoPath', metavar='<git-repo-path>', help='The path to the git repository in which the notes should be rewritten.')
    argparser.add_argument('-c', '--commit-map', required=True, dest='commitMap', metavar='<map-file>', help='The path to a comma separated map file from old to new commits.')
    argparser.add_argument('notesRef', nargs='*', metavar='<notes-ref>', help='The path to the git repository in which the notes should be rewritten.')

    # Parse the arguments and execute
    args = argparser.parse_args()

    repo = git.open(args.repoPath)
    if repo is None:
        raise Exception("Failed to open git repository: {path}".format(path=args.repoPath))
    
    print("Git repo {path}, opened.".format(path=args.repoPath))

    print("Loading commit map {path}".format(path=args.commitMap))

    commitMap = {}
    with codecs.open(args.commitMap) as f:
        lines = f.read().split('\n')
        lineNumber = 0
        for line in lines:
            lineNumber += 1
            if len(line) > 0:
                pair = line.split(',')
                old, new = pair[0].strip(), pair[1].strip()
                if old != new:
                    if old in commitMap:
                        print("Warning! Overwriting mapping {old} -> {new1}, with {new2}".format(old=old, new1=commitMap[old], new2=new))
                    commitMap[old]=new
                else:
                    #print("Warning, commit {hash} maps onto itself on line {line}. Skipping.".format(hash=old, line=lineNumber))
                    pass

    print("Loaded commit map. Remapping {size} items.".format(size=len(commitMap)))

    notesRefs = args.notesRef
    if len(notesRefs) == 0:
        notesRefs.append(repo.raw_cmd([ 'git', 'notes', 'get-ref' ]).strip())

    print("Processing note refs:")
    for ref in notesRefs:
        print("  - {ref}".format(ref=ref))

    status = repo.status()
    print("On branch {b}.".format(b=status.branch))
    print("Resetting {b}.".format(b=status.branch))
    repo.reset(isHard=True)
    print("Cleaning {b}.".format(b=status.branch))
    repo.clean(directories=True, force=True, forceSubmodules=True, quiet=True)

    # We will work on the files in the repo directory.
    os.chdir(repo.path)
    for notesRef in notesRefs:
        if not notesRef.startswith("refs"):
            notesRef = "refs/notes/{ref}".format(ref=notesRef)
        print("Checking out {b}.".format(b=notesRef))
        if repo.checkout(branchName=notesRef) is None:
            raise Exception("Failed to checkout {ref}. Error: {err}".format(ref=notesRef, err=repo.lastStderr))

        # The contents of the repository should now be a list of directories whose names are 2 digit hexadecimal numbers.
        # They are the first two digits of the commit hashes with the remaining digits of each hash forming the filename
        # within each of the directories. See git objects: https://git-scm.com/book/en/v2/Git-Internals-Git-Objects
        # Same directory structure.

        # The path + filename is the hash of the commit while the contents of each file is the actual note for the commit.
        # Hence all we need to do to remap an old commit to a new one is to move the file to a different path + filename location.
        doCommit = False
        for oldHash in commitMap:
            newHash = commitMap[oldHash]
            doCommit = moveNote(repo=repo, oldHash=oldHash, newHash=newHash) or doCommit

        # Here we just need to make the commit.
        if doCommit:
            commit = repo.commit(message="Notes remapped by 'remap_notes.py'")
            if commit is None:
                raise Exception("Failed to commit remapped notes! Err: {err}".format(err=repo.lastStderr))

            print("Commited {hash}".format(hash=commit.shortHash))
            headCommitHash = repo.raw_cmd(['git', 'log', '--format=%H', '-1'])
            if headCommitHash is None:
                raise Exception("Failed to get last commit hash! Err: {err}".format(err=repo.lastStderr))
            else:
                headCommitHash = headCommitHash.strip()

            if commit.shortHash is not None and not headCommitHash.startswith(commit.shortHash):
                if len(commit.shortHash) > len(headCommitHash):
                    headCommitHash = commit.shortHash # The git update-ref command correctly expands short hashes.
                    print("Warning! Failed to get last commit hash from log. Using short hash {sh} from commit command.".format(sh=commit.shortHash))
                else:
                    raise Exception("Invariant error: the last commited hash {sh} doesn't match our detached head hash {h}".format(sh=commit.shortHash, h=headCommitHash))

            if repo.raw_cmd(['git', 'update-ref', notesRef, headCommitHash]) is None:
                raise Exception("Failed to update ref {ref} to {hash}! Err: {err}".format(ref=notesRef, hash=headCommitHash, err=repo.lastStderr))
            print("Updated {ref} to {hash}".format(ref=notesRef, hash=headCommitHash))
        else:
            print("Nothing to update for {ref}".format(ref=notesRef))

    print("Checkout branch {b}.".format(b=status.branch))
    if repo.checkout(branchName=status.branch) is None:
        raise Exception("Failed to restore repo to the original state. Couldn't checkout {b}.".format(b=status.branch))

    print("Done.".format(b=status.branch))
    return 0

if __name__ == "__main__":
    rv = remapNotesCommand(sys.argv)
    if rv != 0:
        sys.exit(rv)

