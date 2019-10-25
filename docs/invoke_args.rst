######################
AWS Lambda Invoke Args
######################

After successfully deploying `jobs.py` file to `AWS Lambda <https://console.aws.amazon.com/lambda/home?region=us-east-1#/functions>`_, you'll be able to invoke your arguments through the `bert-etl` commandline interface using the `-i` option. Here is an example,

.. code-block:: bash

    $ conda create -n bert-etl-invoke-args
    $ conda activate bert-etl-invoke-args
    $ pip install bert-etl -U
    $ git clone https://github.com/jbcurtin/bert-etl-testing bert-etl-invoke-args
    $ cd bert-etl-invoke-args


Inside the directory `bert-etl-invokes-args`, open `bert-etl.yaml` and insert the following lines in `init_job_queue`

.. code-block:: yaml

    init_job_queue:
        invoke_args:
            - invokes_args.invocation.test_one
            - '{"json_literal": {"test": 2}}'
            - yaml_literal:
                test: 3
            - ./invoke_args/invocation.json
            - ./invoke_args/invocation.yaml


With `invoke_args` defined in `bert-etl.yaml`, we're ready to test our function


.. code-block:: bash

    bert-deploy.py -m bert_testing_jobs -i



`bert-deploy.py` evaluates bert_tutorial, builds the lambdas, and installs packages relative to `bert-etl.yaml` only. Zips up each function from `jobs.py` into isolated environments with the appropriate dependecies. Uploads each zip file to AWS Lambda, and then connects the Lambdas through DynamoDB Streaming Service. `bert-deploy.py -m bert_tutorial -i bert_tutorial/test-event.json` then invokes the function with the contents of `test-event.json`. You could also paste the contents of `test-event.json` into the AWS Lambda TestConfig console.

When the functions/lambdas are ready. Update `environment` in `bert-etl.yaml` to include `DEBUG: false` and turn on DynamoDB Streaming Service in the functions/lambdas.


