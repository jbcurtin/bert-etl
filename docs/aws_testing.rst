##################
AWS Lambda Testing
##################

With the AWS Lambda deployment target, you can test the functions through the AWS Lambda Console or locally with `bert-runner.py`. The environment var `DEBUG` is central to identifying the correct execution context of testing and non-testing. Only with `DEBUG` set to true, will you be able to test the lambda without invoking other lambdas. If `DEBUG` is set to false, you may pass an event into the function and the result pushed into `done_queue` will be streamed to the next lambda in the pipeline.


For illustrative purposes, lets test a funciton. Go ahead and generate a new job-template.

.. code-block:: bash

    $ conda activate bert-etl
    $ pip install bert-etl 
    $ git clone https://github.com/jbcurtin/bert-etl-tutorial bert_tutorial
    # Test to make sure you have all the dependencies installed
    $ bert-deploy.py -m bert_tutorial -s aws-lambda
    $ bert-deploy.py -m bert_tutorial -i bert_tutorial/test-event.json


`bert-deploy.py` evaluates bert_tutorial, builds the lambdas, and installs packages relative to `bert-etl.yaml` only. Zips up each function from `jobs.py` into isolated environments with the appropriate dependecies. Uploads each zip file to AWS Lambda, and then connects the Lambdas through DynamoDB Streaming Service. `bert-deploy.py -m bert_tutorial -i bert_tutorial/test-event.json` then invokes the function with the contents of `test-event.json`. You could also paste the contents of `test-event.json` into the AWS Lambda TestConfig console.

When the functions/lambdas are ready. Update `environment` in `bert-etl.yaml` to include `DEBUG: false` and turn on DynamoDB Streaming Service in the functions/lambdas.

