Datamake
========

[![Travis CI Status](https://api.travis-ci.org/tims/datamake.png)](https://travis-ci.org/tims/datamake)

This is an experiment, feedback welcome.

A simple tool for managing parametrized job flows with data dependencies. This is not a scheduler.

Each task specifies an artifact with a URI and a command to be executed on the shell.

Tasks can specify tasks they depend on. A task can be depend on and be depended on by multiple tasks.

Flows are run by specifying the end task that you wish to run, all upstream tasks are run first. Tasks in the flow file, but not upstream of the target task will not be run.

When a flow runs, for each task in the flow if it's artifact does not exist it's command is run.

Downstream jobs pass parameters to upstream jobs.

Command line parameters can be eval'd to multiple values and will run the whole flow for each value. Eg: an aggregation script can be passed dates for the last 7 days and only the missing days will have anything to do.

The downstream jobs are the ones scheduled by cron / citrine / something else and pull on the tasks upstream and run them as neccessary.
So they pull rather than push.

Flow file format
------------------

eg:

    {
      "version": "1.0",
      "description": "This is a contrived example.",
      "tasks":
      [
        {
          "id": "download",
          "command": "curl -i https://api.github.com/users/${username} > /tmp/datamake-example-${username}.json",
          "cleanup": true,
          "artifact": "/tmp/datamake-example-${username}.json"
        },
        {
          "id": "grep-email",
          "command": "grep email /tmp/datamake-example-${username}.json",
          "dependencies": ["download"]
        },
        {
          "id": "grep-name",
          "command": "grep name /tmp/datamake-example-${username}.json",
          "dependencies": ["download"]
        },
        {
          "id": "user-details",
          "dependencies": ["grep-email", "grep-name"]
        }
      ]
    }

Run flow
--------

  datamakenew flow.json user-details --param username=tims

Help
----

  datamakenew --help
