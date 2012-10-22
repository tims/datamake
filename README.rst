========
datamake
========

This is an experiment, feedback welcome.

A simple tool for managing parametrized job flows with data dependencies.

Each job must specify an artifact with a URI and a command.

When a job "builds": it checks it's artifact does not exist, builds it's dependencies then runs it's command.

This is not a scheduler. 

My current thinking is that the downstream jobs are the ones scheduled by cron / citrine / something else and they run any upstream jobs. So they pull downstream rather than push. Citrine's simple dependencies are push only. I think you can have both, but you need a tool that knows about all .job files so it can find the downstream jobs.

To be actually useful, I'd need to add locking of jobs. So two downstream jobs can't trigger the same upstream jobs at the same time. Considering jobs might be running on more than one machine, maybe zookeeper would be a good idea.

