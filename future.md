Future work
===========

Here I want to note down ideas for possible future work.

The round trip
--------------

It should be possible to take a git branch which is branched from a tracked branch (one converted from an accurev stream) and convert its commits into a series of promotes into an accurev stream of your choice. Here is a sample procedure to do so, but it requires access to an empty accurev workspace and an empty accurev stream:

 1. Find the branch point.
  1. Get the branch name and by association the AccuRev stream name from which we have branched.
  2. Get the transaction number from the commit from which we branched.
 2. Set up the AccuRev workspace and stream.
  1. Reparent the AccuRev workspace under the stream to which we want to promote the branch.
  2. Timelock the AccuRev stream to which we are promoting to the transaction number we retrieved in step 1.2.
 3. For each commit, in order, do the following:
  1. Generate a patch file for the commit with `git diff ... > patch-file.patch`
  2. Apply the patch file to the workspace using `git apply patch-file.patch`
  3. Carefully interpret file additions, renames, deletions and modifications and convert to accurev add, move, defunct and keep commands respectively. See https://www.reddit.com/r/git/comments/1oi0tw/is_there_any_way_have_git_diff_show_that_a_file/ for details, but the summary is to use the `git diff -M --summary A B` command...
  4. Promote the changes into the stream.
 4. Remove the timelock or just quit. Done.

Implement the following merge strategies
----------------------------------------

 - Aggressive -- Only look at the src/dst info in the accurev hist output and do the merge regardless of the state of the streams/branches.
 - Pedantic   -- Process the transactions in order and generate some sort of unique hash for the contents of the transaction which we will use later to detect "out of order" promotes and treat them as merges also.
 - Normal     -- Process the transactions in order and generate merge points only when the `git diff` command between the two branches returns no changes. It is important to use `git diff` in order to leverage `git attributes` to allow the user to set up files to be ignored in this process. See http://stackoverflow.com/questions/10415100/want-to-exclude-file-from-git-diff.
 - Fractal    -- Process the streams in order (ordered by their stream id from lowest to highest) and branch them from their basis at their `mkstream` transaction or their last `chstream` transaction that changed the basis.
 - Orphanage  -- Process the streams in the order specified in the config, making each branch an orpaned branch in git. See https://www.kernel.org/pub/software/scm/git/docs/git-branch.html.
 - Skip       -- Do not process the merges.

Using git remotes
-----------------

Add configuration options to allow us to have more flexible use of remotes. A remote could be used to fetch a subset of the needed streams before we process them ourselves or ...

