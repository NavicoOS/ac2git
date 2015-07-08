### Overview ###

AccuRev2Git is a tool to convert an AccuRev depot into a git repo. A specified AccuRev stream will be the target of the conversion, and all promotes to that stream will be turned into commits within the new git repository.

### Getting Started ###
- Install python 2.7

- Make sure the paths to the `accurev` and `git` executables are correct for your machine, and that git default configuration has been set.

### How to use ###

#### Converting a Depot's Streams ####
- Make an example config file:

 ```
 python accurev2git.py --dump-example-config
 ```

- Modify the generated file, whose filename defaults to `accurev2git.config.example.xml`, (there are plenty of notes in the file and the script help menu to do this)

- Rename the `accurev2git.config.example.xml` file as `accurev2git.config.xml`

- Modify the configuration file and add the following information:

 - AccuRev username & password
 
 - The name of the Depot. You may only convert a single depot at a time and it is recommended that one Depot is mapped to one git repository.

 - The start & end transactions which correspond to what you would enter in the `accurev hist` command as the `<time-spec>` (so a number, the keyword `highest` or the keyword `now`).

 - The list of streams you wish to convert (must exist in the Depot).

 - The path to the git repository that the script is to create. The folder must exist and should preferably be empty, although it is ok for it to be an existing git repository.

 - A user mapping from AccuRev usernames to git. _Hint: Run `accurev show users` to see a list of all the users which you might need to add._

- Run the script

 ```
 python accurev2git.py
 ```

- If you encounter any trouble. Run the script with the `--help` flag for more options.

 ```
 python accurev2git.py --help
 ```

### How it works ###

For each stream listed the script finds its `mkstream` transaction and populates it into a fresh branch.

Then it iteratively runs `accurev diff -a -i -v <stream> -V <stream>` commands for each subsequent transaction number.

When it finds that something has changed it deletes it, re-populates from accurev and commits this.


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
