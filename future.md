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

The continuous merge
--------------------

It should be possible to infer merge-points on the fly so long as the entire parent hierarchy of streams from accurev is available. This should make the converted repo more useful but it also prevents you from excluding large streams if they are in your parent hierarchy since they are required to propagate merges in full. For complex repositories this may be more of a hinderance.

Merges with missing streams
---------------------------

Currenty a merge point is only considered for direct parents. This is a simple boolean switch in the code right now but can be expanded to figure out what the _next available_ parent is instead. Which would allow streams to be left out and yet have some useful merge history. Ofcourse this would not work for all cases but would be a best effort approach.
