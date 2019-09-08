[![Documentation Status](https://readthedocs.org/projects/bert-etl/badge/?version=latest)](https://bert-etl.readthedocs.io/en/latest/?badge=latest)

# Bert
A microframework for simple ETL solutions

## Begin with

Lets begin with an example of loading data from a file-server and than loading it into numpy arrays

```
$ virtualenv -p $(which python3) env
$ source env/bin/activate
$ pip install bert-etl
$ pip install librosa # for demo project
$ docker run -p 6379:6379 -d redis # bert-etl runs on redis to share data across CPUs
$ bert-runner.py -n demo
$ PYTHONPATH='.' bert-runner.py -m demo -j sync_sounds -f
```

## Release Notes

### 0.3.0

* Added Error Management. When an error occurs, bert-runner will log the error and re-run the job. If the same error happens often enough, the job will be aborted

### 0.2.1

* Added Release Notes

### 0.2.0

* Added Redis Service auto run. Using docker, redis will be pulled and started in the background
* Added Redis Service channels, sometimes you'll want to run to etl-jobs on the same machine

## Fund Bounty Target Upgrades

Bert provides a boiler plate framework that'll allow one to write concurrent ETL code using Pythons' `microprocessing` module. One function starts the process, piping data into a Redis backend that'll then be consumed by the next function. The queues are respectfully named for the scope of the function: Work(start) and Done(end) queue. Please consider contributing to Bert Bounty Targets to improve this documentation

https://www.patreon.com/jbcurtin


## Roadmap

* Create configuration file, `bert-etl.yaml`
* Support conda venv
* Support pyenv venv
* Support dynamodb flush
* Support multipule invocations per AWS account
* Support undeploy AWS Lambda
* Support Bottle functions in AWS Lambda


## Tutorial Roadmap

* Introduce Bert API
* Explain `bert.binding`
* Explain `comm_binder`
* Explain `work_queue`
* Explain `done_queue`
* Explain `ologger`
* Explain `DEBUG` and how turning it off allows for x-concurrent processes
* Show an example on how to load timeseries data, calcualte the mean, and display the final output of the mean
* Expand the example to show how to scale the application implicitly
* Show how to run locally using Redis
* Show how to run locally without Redis, using Dynamodb instead
* Show how to run remotly using AWSLambda and Dynamodb 
* Talk about dynamodb and eventual consistency

