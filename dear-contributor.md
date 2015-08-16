## Dear contributor ##

Welcome! Thanks for joining our crusade. I know the code isn't great so let me try and clarify some of its structure.

You will find 4 main files in this repository:
  - `ac2git.py` - the main script that contains the pop, diff and deep-hist algorithms.
  - `accurev.py` - my python wrapper and extensions for accurev commands.
  - `git.py` - my git wrapper because I couldn't figure out how to use an existing one.
  - `git_stitch.py` - the core of the _branch stitching_ functionality..

## accurev.py ##

### Description ###

Let's start with the accurev CLI wrapper. This file has 3 classes which I use primarily as namespaces, and they are:
 - `obj` - Which prefixes all of the python _objects_ which represent results of the _unprefixed_ commands.
 - `raw` - The namespace which exists solely to map python functions and their arguments to accurev commands and command line arguments. The return value of all of these functions is the raw text that was returned by the accurev command.
 - `ext` - This namespace contains extensions to the accurev commandline, like `deep_hist()`, which help make getting information out of accurev later simpler.

There are also functions in the _global namespace_ which return objects from the `obj` _namespace_.

### Examples ###

For example let us look at the `accurev pop` command. To invoke it you can do the following:

    import accurev
	
	result = accurev.pop(verSpec="MyStream", location=".", timeSpec="now", isRecursive=True, elementList="*")
	
	if result:
		print("Successfully populated latest from MyStream")
	else:
		print("Error! Failed to populate MyStream!")

The result is an `accurev.obj.Pop` object which contains two lists: `result.messages` and `result.elements`. Each element of the `result.messages` list is an `accurev.obj.Pop.Message` and each element of the `result.elements` list is an `accurev.obj.Pop.Element`. You can read their attributes directly and they are _loaded_ from the XML output of an accurev command.

### Global vs raw ###

Effectively the `pop()` function from earlier is a convenience function that you could have decomposed into its constituents like so:

    xmlResult = accurev.raw.pop(verSpec="MyStream", location=".", timeSpec="now", isRecursive=True, elementList="*", isXmlOutput=True)
	result = accurev.obj.Pop.fromxmlstring(xmlResult)
	
	if result:
		print("Successfully populated latest from MyStream")
	else:
		print("Error! Failed to populate MyStream!")

In the end the only purpose of the fully specified `accurev.obj` namespace, as I call it, is to explicitly state what possible members we might be getting from the accurev XML output.

_Note: Accurev doesn't always produce the full output for each object listed and some components of the object may be uninitialized and left as_ `None`_. This is dependent on the accurev command that you've run and whether or not it failed to execute._

### git.py ###

This script followes a similar format as the `accurev.py` script but is a lot simpler and its size is still managable. Both are henious boiler-plate that could have been avoided but were handy in detecting errors in the early code.

### ac2git.py ###

This script is the main script that actually performs the conversion. It contains the implementations for all of the algorithms mentioned in the `README.md`.

This script contains 2 primary classes,`Config` and `AccuRev2Git`; an `AccuRev2GitMain()` function and a few helper functions.

`AccuRev2Git` is a monstrosity that grew over time and I haven't split up yet. If you're looking at the internals of my script I would recommend having a look at the following functions:
 1. `AccuRev2GitMain()` - This is the entry point to the script and should be your first point of call. It sets up the command line options, loads the configuration into an `ac2git.Config` file, creates an instance of `AccuRev2Git` and starts the conversion.
 2. `AccuRev2Git.Start()` - This is the function that starts the conversion. It ensures a couple of things like that you have logged into accurev and then calls into `AccuRev2Git.ProcessStreams()`.
 3. `AccuRev2Git.ProcessStreams()` - Is a very simple function that iterates over the streams specified in the configuration.
 4. `AccuRev2Git.ProcessStream()` - This is the **workhorse**. It is where all the logic for converting a stream is located and the rest of the functions in the `AccuRev2Git` class are merely helpers for this function.

As a bonus there is one more function that may interest you and that is the monstrosity `AccuRev2Git.StitchBranches()`. This function does all of the work required to turn many orphaned branches into a repo with branch merges. It is still a work in progress and needs to be split up.

### git_stitch.py ###

This script is mainly a support script for the `AccuRev2Git.StitchBranches()` function, mentioned earlier. It runs a `git rev-list` command on each branch and a `git cat-file -p` command for each commit hash returned by `git rev-list`.
The commits are then stored in a dictionary whose keys are _tree hashes_ and values _lists of commits (that point to those trees)_.

This way we discover which commits have the exact same file contents from the top-level all the way down and mark them as _candidates_ for merge commits given certain criteria. That is the idea but the truth lies in `AccuRev2Git.StitchBranches()`.
