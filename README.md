Datamake
========

[![Travis CI Status](https://api.travis-ci.org/tims/datamake.png)](https://travis-ci.org/tims/datamake)

This is an experiment, feedback welcome.

A simple tool for managing parametrized job flows with data dependencies.

Each job must usually specifies an artifact with a URI and a command to be executed on the shell.

When a job "builds": it checks it's artifact does not exist, builds it's dependencies then runs it's command.

This is not a scheduler. 

My current thinking is that the downstream jobs are the ones scheduled by cron / citrine / something else and they run any upstream jobs. So they pull downstream rather than push. Citrine's simple dependencies are push only. The downstream jobs can pass parameters to the upstream jobs. Eg: a weekly aggregation script can depend on the last 7 days of upstream jobs.

Currently a job can depend on multiple jobs, but multiple jobs can't depend on the same job. The next step is to separate job flow representation from execution so it can choose the next job to run with complete knowledge of the flow.

