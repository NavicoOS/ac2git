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

The first 3 items, `hist.xml`, `streams.xml` and `diff.xml` are committed together, for each transaction, on the `refs/ac2git/depots/<depot_number>/streams/<stream_number>/info` ref with the commit message that has the following format `transaction <transaction_number>`. This is meta-data needed to make decisions about the stream w.r.t. other streams later on and is used to produce merges when processing git branches in the second stage.

The 4th item is the actual state of the stream at this transaction and will be the contents of the Git commit for this transaction while the message will come from the `hist.xml` mentioned earlier. The contents is committed on a separate ref, `refs/ac2git/depots/<depot_number>/streams/<stream_number>/data`, with the commit message also formatted as `transaction <transaction_number>`.

You can inspect these 'hidden branches' with `git log refs/ac2git/depots/<depot_number>/streams/<stream_number>/data` to see the processed transaction numbers for a particular stream.

To view the output of any individual file you can use `git show <commit_hash>:<filename>` so viewing what the last transaction's `hist.xml` looks like you can run the following command `git show refs/ac2git/depots/<depot_number>/streams/<stream_number>/info:hist.xml` where `<depot_number>` and `<stream_number>` are placeholders that you'll need to replace with information from your Accurev depot. So if your depot is called `MyDepot` whose number is 7 and you are interested in the last processed transaction on the root stream you can use the following command `git show refs/ac2git/depots/7/streams/1/info:hist.xml`.

Since the contents of the two refs are so different it would be inefficient to do a `git checkout` of one and then the other for each transaction so the script first processes all the transactions for the `ref/ac2git/depots/<depot_number>/streams/<stream_number>/info` and then checks out the `ref/ac2git/depots/<depot_number>/streams/<stream_number>/data` ref and processes the same transactions it just added to the `..._info` ref. Even if there are no changes the `..._data` ref is always committed to (`--allow-empty` is set for the `git commit`) so that each transaction on the `..._info` ref has a corresponding transaction on the `..._data` ref.

Once we have completed processing both refs successfully for a single stream we record the transaction up to which we have processed it in the high water mark ref `refs/ac2git/depots/<depot_number>/streams/<stream_number>/hwm`, which is used by the next stage of the process.

#### Second stage: processing ####

The second stage begins by converting stream names into their corresponding unique IDs. This process is done without contacting accurev and is currently reasonably expensive so once completed its results are cached in the ref `refs/ac2git/cache/depots/<depot_number>/stream_names`. It would be better to maintain this as the streams are downloaded instead of doing it here but for now that's the way it works.

After this step the streams that are being converted and all the current branches are stored in the `refs/ac2git/state/<depot_number>/last` ref. This ref will from now on maintain the list of streams that we are processing (which will never again change) and the last valid commit hash for each of their branches in git. This is because the script can be interrupted at any time and since a transaction can now cause multiple commits the script can be left in an invalid state. So after each transaction is processed the heads of all the tracked branches are stored in this ref. Each time the script begins it will restore the heads to their last known state.

However, this information alone is not suficient for us to be able to determine where each branch was at a specific point in time which is required in order to deal with timelocks and additions of streams after a conversion has already started. In order to track where a branch has been `git reflog` sounds like the perfect choice but it appears that since we use `git update-ref` that this command circumvents the reflog and so we don't get the history. Additionally, if a branch is deleted its reflog entries are also deleted which is inconvenient for situations where a stream was being processed, then removed and then re-added again. For this purpose we store the location of each stream's branch on a hidden ref `refs/ac2git/state/depots/<depot_number>/streams/<stream_number>/commit_history` which starts as an orphaned branch (its first commit has no parents) and each subsequent commit has exactly 2 parents. The first parent is the previous commit on the hidden ref and the second parent is the commit at which the stream's branch was at the transaction which is indicated in the commit message. Each commit points to the empty tree hash and so has no contents and is cheap to store. A commit on this hidden ref is made every time the stream's branch is updated (i.e. committed to or fast-forwarded).

Since each stream only contains transactions that have affected it we need to take all of them and sort them so that we can process them in order. A map of transactions to lists of affected streams is created along with all the hashes to the relevant data in git for each stream.

Then we iterate over all of the streams and look for the lowest high water mark (which is kept in the `refs/ac2git/depots/<depot_number>/streams/<stream_number>/hwm` ref) which is the highest processed transaction for that stream. The lowest high water mark becomes our end transaction.

At this stage everything is ready and we begin to iterate over each transaction in turn in order to generate the merged hisotry.

The transactions are split into two groups, the first group being the `mkstream` & `chstream` transactions and all other transactions (`promote`, `keep`, `defunct`, etc) making up the second group.

The `mkstream` transaction is processed in the same way as a `chstream` transaction except that since there is no potential previous state of the stream we can just create it instead of having to figure out what to do with the history or any currently pending transactions.

A `chstream` transaction is processed by first checking if the basis stream has changed...

_Work in progress. TODO: finish describing chstreams and be wary of issue #57..._

For all other transactions we then check if they occurred on a stream or in a workspace. For workspaces we simply make a cherry-pick and continue processing but for streams we have to do some more work w.r.t. its children streams which may have also been affected by this transaction (usually a `promote` but can be a `keep`, `purge` or a `defunct` in case of a revert).

For depots that have lived through one or more Accurev upgrades you will find that as you go deeper into the history less and less information is available to you via accurev commands. For example, the `accurev hist -fex` XML output for newer transactions includes the `fromStreamName` and `fromStreamNumber` attributes for the transaction but if you go back far enough these attributes dissappear. Hence, newer history will always look better than the older one because Accurev is providing us with more information about what has actually happened. _Note: it may be possible to infer from which stream a promote occurred in a number of different ways but I won't go into that right now_.

If we know the source stream and the destination stream and both are being converted by the script (they are both specified in the stream list in the configuration file) then we can record that a merge occurred. This merge can take two forms depending on how you've configured the `source-stream-fast-forward="false"` option in the config file or the `--source-stream-fast-forward=false` command line argument.

For a promote (transaction 83) from the stream `Development` to the stream `Test` this is what it would look like for each of the two options:

```
--source-stream-fast-forward=false

* (Test) Transaction 83 by Test Lead
|\
| * (Development) Transaction 78 by Joanna Blobs
* | Transaction 67 by Joe Bloggs
| | 
```

So if subsequently two more promotes were made into each stream the git conversion would look like this:

```
--source-stream-fast-forward=false

*   (Test) Transaction 85 by Joe Bloggs
| * (Development) Transaction 84 by Joanna Blobs
* | Transaction 83 by Test Lead
|\|
| * Transaction 78 by Joanna Blobs
* | Transaction 67 by Joe Bloggs
| | 
```

Where if we were to change the `--source-stream-fast-forward` flag to true we would get this for the same transactions:

```
--source-stream-fast-forward=true

* (Test) (Development) Transaction 83 by Test Lead
|\
| * Transaction 78 by Joanna Blobs
* | Transaction 67 by Joe Bloggs
| | 
```

And again, with an additional promote into each stream we would get this graph in Git.

```
--source-stream-fast-forward=true

*   (Test) Transaction 85 by Joe Bloggs
| * (Development) Transaction 84 by Joanna Blobs
|/
* Transaction 83 by Test Lead
|\
| * Transaction 78 by Joanna Blobs
* | Transaction 67 by Joe Bloggs
| | 
```

Notice that the graph looks a little like a letter K hence the single letter option for this same flag is `-K`.

The branch `Development` has been moved to the merge commit when the `--source-stream-fast-forward` option was set to `true`. This is not normally desired so the default and recommended setting is `false`.

But if we either don't know the source stream or if we are not tracking it the commit is turned itno a cherry-pick onto the destination stream _(which must be tracked otherwise why are we processing it?)_.

Once this is done we will then figure out which of our child streams (that we are converting/tracking) are affected and recursively process them ([depth-first in-order](https://en.wikipedia.org/wiki/Tree_traversal#In-order)).

If the child stream is empty, which is determined by doing a `git diff` between the commit we just made and the commit that we are about to make on the child stream being empty, then we will try and do a fast-forward of the child stream to this commit provided that it is a direct ancestor of us (meaning it has already been merged). If the fast-forward failse (because the child stream's git branch is not an ancestor of the basis streams git branch), depending on the setting of the `empty-child-stream-action` in the config file or `--empty-child-stream-action` command line argument the script will do one of the following two things:

For `--empty-child-stream-action=merge`:

The script will record a merge from the parent/basis stream's git branch into the child stream's git branch like this:

```
| * (Child stream) Transaction 97 by Maria Teresa
|/|
* | (Basis stream) Transaction 97 by Maria Teresa
| |
```

For `--empty-child-stream-action=cherry-pick`:

The script will record a cherry-pick from the basis stream's git branch into the child strea'ms git branch like this:

```
| * (Child stream) Transaction 97 by Maria Teresa
| |
* | (Basis stream) Transaction 97 by Maria Teresa
| |
```

However, if the child stream is deteremened to be non-empty (the `git diff` was non-empty) then a cherry-pick is performed on the child stream like in the example above but without the attempted fast-forward.

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


