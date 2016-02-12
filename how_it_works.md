### How it works ###

The conversion of an Accurev depot to a Git repository is split into two stages. The first stage retrieves the information from Accurev that is needed to construct a Git repository while the second stage processes the retrieved information and constructs Git branches for your streams from it.

This method was developed in colaboration with [Robert Smithson](https://github.com/fatfreddie).

#### First stage: retrieval ####

The first stage is controlled by the `--method` argument to the script or the `<method>` tag in the script config file. Depending on the method flag, further described in the _"Converting the contents of a single stream"_ section below, this stage can be quick or slow. For all cases this stage only stores the transactions which affect the streams that were specified in the config file, or all streams if no streams were specified.

The downloaded information is stored in Git, and is stored in the refs found in the `.git/refs/ac2git/` folder of the repository. I will refer to these refs as being in the _"ac2git namespace"_ and will ommit the `.git/` part of their path in further text.

For each transaction that affects the stream that we are processing we retrieve 4 pieces of information from Accurev:
 - The output of the `accurev hist -p <depot> -t <transaction_number> -fex` command, stored in the `hist.xml` file.
 - The output of the `accurev show streams -t <transaction_number> -fixg` command, stored in the `streams.xml` file.
 - The output of the `accurev diff -a -i -v <stream_name> -V <stream_name> -t <transaction_number - 1>-<transaction_number>` command, stored in the `diff.xml` file. This file is missing for `mkstream` transactions.
 - The result of the `accurev pop -R -v <stream_name> -L <git_repo_dir> -t <transaction_number>` command.

_Note: The TaskId field in each accurev command's XML output is set to zero in order to optimize the use of git's hashing of blobs (i.e. the same command output will always hash to the same blob if we replace this one field, saving some minimal space and processing on the git side and making diffs more useful between different commits)._

The first 3 items, `hist.xml`, `streams.xml` and `diff.xml` are committed together, for each transaction, on the `refs/ac2git/<depot_name>/streams/stream_<stream_number>_info` ref with the commit message that has the following format `transaction <transaction_number>`. This is meta-data needed to make decisions about the stream w.r.t. other streams later on and is used to produce merges when processing git branches in the second stage.

The 4th item is the actual state of the stream at this transaction and will be the contents of the Git commit for this transaction while the message will come from the `hist.xml` mentioned earlier. The contents is committed on a separate ref, `refs/ac2git/<depot_name>/streams/stream_<stream_number>_data`, with the commit message also formatted as `transaction <transaction_number>`.

You can inspect these 'hidden branches' with `git log refs/ac2git/<depot_name>/streams/stream_<stream_number>_data` to see the processed transaction numbers for a particular stream.

To view the output of any individual file you can use `git show <commit_hash>:<filename>` so viewing what the last transaction's `hist.xml` looks like you can run the following command `git show refs/ac2git/<depot_name>/streams/stream_<stream_number>_info:hist.xml` where `<depot_name>` and `<stream_number>` are placeholders that you'll need to replace with information from your Accurev depot. So if your depot is called `MyDepot` and you are interested in the last processed transaction on the root stream you can use the following command `git show refs/ac2git/MyDepot/streams/stream_1_info:hist.xml`.

Since the contents of the two refs are so different it would be inefficient to do a `git checkout` of one and then the other for each transaction so the script first processes all the transactions for the `ref/ac2git/<depot_name>/streams/stream_<stream_number>_info` and then checks out the `ref/ac2git/<depot_name>/streams/stream_<stream_number>_data` ref and processes the same transactions it just added to the `..._info` ref. Even if there are no changes the `..._data` ref is always committed to (`--allow-empty` is set for the `git commit`) so that each transaction on the `..._info` ref has a corresponding transaction on the `..._data` ref.

#### Second stage: processing ####

_Work in progress..._

### Converting the contents of a single stream ###

There are three methods available for converting an accurev stream into a single, orphaned, git branch. The resulting git branch's commits represent transactions that have changed to _contents_ of the stream at various points in time. Each method described is an optimization of the previous and will run quicker but may not be possible to use on an older version of accurev.

The method can be specified in the config file and is documented in the example config with the `<method>` tag (see `python ac2git.py --help` for the `--example-config` option), or specified on the command line by passing the `--method` option. See `python ac2git.py --help` for details.

All methods begin by finding the `mkstream` transaction for each stream and populating it into a fresh branch. All methods create a hidden orphaned git branch (in the `.git/refs/ac2git/` folder) for each indivitual stream.

#### Pop method (slow) ####

The first method is the one Ryan LaNeve implemented, which I call the _pop method_, which works like this:
 - Find the `mkstream` transaction and populate it.
 - Populate it in full and commit into git as an orphaned branch.
 - Start loop:
  + Increment the transaction number by 1
  + Delete the contents of the git repository.
  + Populate the transaction and commit it into git.
  + Repeat loop until done.

This method is really slow since you're always getting the whole history from accurev at each point in time. It does have one optimization that only works for the root stream (which has no parents by definition), in that getting the history for just that stream is sufficient to give you a smaller and still accurate list of transactions to process.

_Ryan only processed the root stream in his script._

#### Diff method ####

The second and third method were devised by [Robert Smithson](https://github.com/fatfreddie) and are a lot faster than the _pop method_ but rely on some features that came in the AccuRev 6.1 client.

I refer to the second method as the _diff method_ and it is a simple optimisation over the _pop method_. It works as follows:
 - Find the `mkstream` transaction and populate it.
 - Populate it in full and commit into git as an orphaned branch.
 - Start loop:
  + Increment the transaction number by 1
  + _Do an_ `accurev diff -a -i -v <stream> -V <stream>` _between this transaction and the last transaction that we populated._
  + _Delete only the files that_ `accurev diff` _reported as changed from the git repository._
  + Populate the transaction and commit it into git. _(The populate here is done with the recursive option but without the overwrite option. Meaning that only the changed items are downloaded over the network.)_.
  + Repeat loop until done.

_Note: There isn't any way to optimize the increments! Incrementing the transaction by more than 1 can mean that we miss a revert operation which could have been performed on a stream. It is important that we increment by *only* 1._

#### Deep-hist method ####

The third method is a little more complicated and requires an understanding of the `accurev hist` command and its caveats.

The `accurev hist` command when used to get the history for the stream only returns the transactions that occured in that stream.
However, a promotion into the parent stream could affect this stream and these transactions are *not* included in the ouput of the `accurev hist` command.

The _deep-hist method_ relies on creating a custom command for accurev that would return the set of all the transactions which could have possibly affected our stream.

This command is implemented in the `accurev.py` script. Here's a sample invocation:

### Adding or removing branches in a converted repository ###

Adding or removing new streams for conversion in an already converted repository has potential to break things unless you're using the _orphanage_ strategy. In the case that history is rewritten I recommend looking into the `git replace` command in order to _graft_ the new commits that you probably made in git onto the new commits that came from the new conversion.

```
import accurev
deepHistory = accurev.ext.deep_hist(depot="MyDepot", stream="MyStream", timeSpec="50-100")
print(deepHistory)
```

You can also use it directly by invocing the `accurev.py` script as follows:

```
python accurev.py deep-hist -p MyDepot -s MyStream -t 50-100
```

_Note: This command currently doesn't understand accurev time locks. This means that some transactions may be shown that do not have any affect on your stream because of a time lock._

Effectively this command does the heavy lifting for us so that the _diff method_ doesn't have to search through transactions one by one. Which finally brings us to how the _deep-hist method_ works:
 - Find the `mkstream` transaction and populate it.
 - Populate it in full and commit into git as an orphaned branch.
 - _Run the deep-hist function and get a list of transactions that affect this stream._
 - _Iterate over the transactions that deep-hist returned:_
  + Do an `accurev diff -a -i -v <stream> -V <stream>` between this transaction and the last transaction that we populated.
  + Delete only the files that `accurev diff` reported as changed from the git repository.
  + Populate the transaction and commit it into git. _(The populate here is done with the recursive option but without the overwrite option. Meaning that only the changed items are downloaded over the network.)_.
  + Repeat loop until done.


