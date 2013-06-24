Datamake
========

[![Travis CI Status](https://api.travis-ci.org/tims/datamake.png)](https://travis-ci.org/tims/datamake)

This is an experiment, feedback welcome.

A simple tool for managing parameterized job flows with data dependencies. This is not a scheduler.

Each task specifies an artifact with a URI and a command to be executed on the shell.

Tasks can specify tasks they depend on. A task can depend on and be depended on by multiple tasks.

Flows are run by specifying the end task that you wish to run: all upstream tasks are run first. Tasks in the flow file, but not upstream of the target task will not be run.

When a flow runs, for each task in the flow, if its artifact does not exist then its command will be run.
If its artifact does exist, then its command will not be run - and any upstream tasks solely dependent on this task will also not run.

Downstream jobs pass parameters to upstream jobs.

Command line parameters can be eval'd to multiple values and will run the whole flow for each value. Eg: an aggregation script can be passed dates for the last 7 days and only the missing days will have anything to do.

The downstream jobs are the ones scheduled by cron / citrine / something else and pull on the tasks upstream and run them as necessary.
So they pull rather than push.

Flow file format
------------------

Example 1:

    {
      "version": "1.0",
      "description": "This is a contrived example showing a diamond of dependencies.",
      "tasks":
      [
        {
          "id": "download",
          "command": "curl -i https://api.github.com/users/${username} > /tmp/datamake-diamond-example-${username}.json",
          "cleanup": true,
          "artifact": "/tmp/datamake-diamond-example-${username}.json"
        },
        {
          "id": "grep-email",
          "command": "grep email /tmp/datamake-diamond-example-${username}.json",
          "dependencies": ["download"]
        },
        {
          "id": "grep-name",
          "command": "grep name /tmp/datamake-diamond-example-${username}.json",
          "dependencies": ["download"]
        },
        {
          "id": "user-details",
          "dependencies": ["grep-email", "grep-name"]
        }
      ]
    }

run with:

    datamake user-details examples/diamond.json --param username=tims

Example 2:

    {
      "version": "1.0",
      "description": "This is a contrived example showing eval params and a helpful date util function.",
      "tasks":
      [
        {
          "id": "download",
          "command": "touch /tmp/datamake-date-example-${date}.json",
        }
      ]
    }

run with

    datamake main examples/date.json --eval-param date='days_range(-2,0)'


Help
----

    datamake --help

Install
-------

(Not in pypi)

    pip install git+http://github.com/tims/datamake.git


There's some irritating dependencies. Like oursql, which it used for hacky mysql artifacts.
One day artifact types should be pluggable, because this sucks.




