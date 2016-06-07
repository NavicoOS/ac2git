### Overview ###

ac2git is a tool to convert an Accurev depot into a git repo. All specified Accurev streams will be the target of the conversion, and an attempt is made to map the Accurev stream model to a Git branching model. There are fundemental differences between the two that can make the converted repo history look strange at times but we've done our best to maintain correctness over beauty.

### Getting started ###
- Install python 3.4

- Make sure the paths to the `accurev` and `git` executables are correct for your machine, and that git default configuration has been set.

- Clone the **ac2git** repo.

- Run `python ac2git.py --help` to see all the options. _It is recommended that you at least take a look._

- Run `python ac2git.py --example-config` to get an example configuration. _It is recommended that you at least take a look._

- Follow the steps outlined in the **How to use** section.

_Note: It is recommented that you run the conversion on a *Linux* machine if your Accurev depot contains symbolic links. Additionally the converted repo is going to have more correct file permissions if it is run on a Linux machine._

### How to use ###

#### Converting a Depot's Streams ####
- Make an example config file:

 ```
 python ac2git.py --example-config
 ```

- Modify the generated file, whose filename defaults to `ac2git.config.example.xml`, (there are plenty of notes in the file and the script help menu to do this)

- Rename the `ac2git.config.example.xml` file as `ac2git.config.xml`

- Modify the configuration file and add the following information:

 - Accurev username & password
 
 - The name of the Depot. You may only convert a single depot at a time and it is recommended that one Depot is mapped to one git repository.

 - The start & end transactions which correspond to what you would enter in the `accurev hist` command as the `<time-spec>` (so a number, the keyword `highest` or the keyword `now`).

 - The list of streams you wish to convert (must exist in the Depot).

 - The path to the git repository that the script is to create. The folder must exist and should preferably be empty, although it is ok for it to be an existing git repository.

 - A user mapping from Accurev usernames to git. _Hint: Run `accurev show -fi users` to see a list of all the users which you might need to add._

 - Choose the preferred method to use for converting the streams. If the streams are sparse (the transactions that have changed the stream contents are far apart) I recommend the `deep-hist` method, otherwise the `diff` method may be more optimal (read the _"Converting the contents of a single stream"_ section for more details). If in doubt, use `deep-hist`.

- Run the script

 ```
 python ac2git.py
 ```

- If you encounter any trouble. Run the script with the `--help` flag for more options.

 ```
 python ac2git.py --help
 ```

### The result ###

What this script will spit out is a git repository with independent orphaned branches representing your streams. Meaning, that each stream is converted separately on a branch that has no merge points with any other branch.
This is by design as it was a simpler model to begin with.

Each git branch accurately depicts the stream from which it was created w.r.t. **time**. This means that at each point in time the git branch represents the state of your stream. Not only are the transactions for this stream commited to git but so are any transactions that occurred in the parent stream which automatically flowed down to us.
When combined with my statement from the previous paragraph, this implies that you will see a number of commits on different branches with the same time, author and commit message, most often because they represent the same _promote_ transaction.

Ideally, if you have promoted all of your changes to the parent stream this should be identified as a merge commit and recorded as such. Though it would now be possible to extend this script to do so, it is not on my radar for now as it would be a reasonably large undertaking.
However, there is hope because I've implemented an experimental feature, described below, that does just that but it operates as a post processing step. It is still a little buggy and requires iteration but it proves the concept. Patches are welcomed!

### Files that break history ###

If you have a legacy repository it is possible that you may have some files that break history. One typical example is a version file that has different coppies accross different streams and is never promoted. This will affect the ability of the algorithm to infer merge points between branches. Hence, it would be great if we could ignore these files when determining merge points.

This is possible to do but it is not a part of the script's functionality. It is a function of git that you can specify your own diff driver (see [gitattributes](https://www.kernel.org/pub/software/scm/git/docs/gitattributes.html)) for particular files. This [answer](http://stackoverflow.com/a/10421385/1667513) to the StackOverflow question titled [_Want to exclude file from "git diff"_](http://stackoverflow.com/questions/10415100/want-to-exclude-file-from-git-diff) suggests the same. It might also pay to take a look at this [answer](http://stackoverflow.com/a/1017676/1667513) on a similarly titled question from StackOverflow ([_Excluding files from git-diff_](http://stackoverflow.com/questions/1016798/excluding-files-from-git-diff)).

Ignoring the whole file should be easy but ignoring only a small part of it will require you to write a script that does it. Bash or Python, it will be custom in each situation so I can't really cater for it in this script which is why this note is here.

_Note: I recommend using the `.git/info/attributes` file and not making a `.gitattributes` file in the main repo since it may be deleted by the script or overwritten if it was ever promoted in Accurev._

Example for Linux (from [this stackoverflow answer](http://stackoverflow.com/a/10421385)):

Add the following to your `.git/config` file:
```
[diff "nodiff"]
    command = /bin/true
```

Add something like the following to the `.git/info/attributes` file of the conversion repository:
```
folder/bad_file.c diff=nodiff
```

On Windows you might need to find where the command `true` lives but it should be included with Git.

### Tested with ###

#### master branch ####

- `Accurev 6.1.1 (2014/05/05)`, `git version 2.5.5` and `Python 3.4.3` on a Fedora 21 host.

- `Accurev 5.4.0 (2012/01/21)`, `git version 2.5.0.windows.1` and `Python 3.5.1` on a windows 8.1 Pro host.

##### Known compatibility issues #####

- Fails with `Accurev 4.7.2 (05/08/2009)` due to:
 + malformed XML for `mkstream` transactions. The `<transaction ... /></transaction>` type construct is invalid and python refuses to parse it. It would require rewriting the `accurev.py` script to handle it.
 + The `accurev pop` command does not have the `-t` option. (It is not possible to get past versions of the source code without a workspace)
 + The `accurev diff -a -i -v -V` command does not have the `-t` option. (Inferring incremental changes becomes impossible)

_Note: It may be possible to convert an `Accurev 4.7` depot by creating a single workspace that starts at transaction 1 and updating the workspace to every transaction up to `highest`, commiting into git if there are any differences. This approach would be easier to implement in Ryan's original script. See [issue 11](https://github.com/parsley72/accurev2git/issues/11) on [parsley72's accurev2git repo](https://github.com/parsley72/accurev2git)._

- Fails with `python 3.1` due to using prefixed `u'string literals'`, minimum python that has them is `python 3.3`. Changing `u'some string'` to `'some string'` would fix the issue.

- Fails with `git 1.7` due to missing `-C` flag. Not sure when this flag was added to git.

#### Version 0.2 and earlier were tested with ####
- `Accurev 6.1.1 (2014/05/05)`, `git version 2.1.0` and `Python 2.7.8` on a Fedora 21 host.

- `Accurev 6.1.1 (2014/05/05)`, `git version 1.9.0.msysgit.0` and `Python 2.7.6` on a Window 7 host.

- `Accurev 6.0`, `git 1.9.5` on a Windows 8.1 host. By [Gary](https://github.com/bigminer) in [this comment](https://github.com/orao/ac2git/issues/13#issuecomment-136392393) from issue #13.

- `Accurev 5.4.0 (2012/01/21)`, `git version 2.5.0.windows.1` and `Python 2.7.11` on a windows 8.1 Pro host.

----

### Credits ###

This tool was inspired by the work done by [Ryan LaNeve](https://github.com/rlaneve) in his https://github.com/rlaneve/accurev2git repository and the desire to improve it. Since this script is sufficiently different I have placed it in a separate repository here. I must also thank [Tom Isaacson](https://github.com/parsley72) for his contribusion to the discussions about the tool and how it could be improved. It was his work that prompted me to start on this implementation. You can find his fork of the original repo here https://github.com/parsley72/accurev2git.

The algorithm used here was colaboratibely devised by [Robert Smithson](https://github.com/fatfreddie), whose stated goal is to rid the multiverse of Accurev since ridding just our verse is not good enough, and myself.

My work is in the implementation and the merging part of the algorithm all of which I humbly offer to anyone who doesn't want to remain stuck with Accurev.

----

### Dear contributors ###

I am not a python developer which should be evident to anyone who's seen the code. A lot of it was written late at night and was meant to be just a brain dump, to be cleaned up at a later date, but it remained. Please don't be dissuaded from contributing and helping me improve it because it will get us all closer to ditching Accurev! I will do my best to add some notes about my method and how the code works in the sections that follow so please read them.

I strongly recommend reading the `how_it_works.md` for a word explanation of what the algorithm is meant to do and the `hacking_guide.md` for more information on the file structure and interesting functions.

For now it works as I need it to and that's enough.

---
---

Copyright (c) 2015 Lazar Sumar

Permission is hereby granted, free of charge, to any person
obtaining a copy of this software and associated documentation
files (the "Software"), to deal in the Software without restriction,
including without limitation the rights to use, copy, modify, merge,
publish, distribute, sublicense, and/or sell copies of the Software,
and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

