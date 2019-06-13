# Bert
A microframework for simple ETL solutions

## Begin with

```
$ virtualenv -p $(which python3) env
$ source env/bin/activate
$ pip install bert-etl
$ pip install librosa # for demo project
$ docker run -p 6379:6379 -d redis # bert-etl runs on redis to share data across CPUs
$ bert-runner.py -n demo
$ PYTHONPATH='.' bert-runner.py -m demo -j sync_sounds -f
```

## Fund Bounty Target Upgrades

Bert provides a boiler plate framework that'll allow one to write concurrent ETL code using Pythons' `microprocessing` module. One function starts the process, piping data into a Redis backend that'll then be consumed by the next function. The queues are respectfully named for the scope of the function: Work(start) and Done(end) queue. Please consider contributing to Bert Bounty Targets to improve this documentation

https://www.patreon.com/jbcurtin

## More to come
Documentation is being worked on

