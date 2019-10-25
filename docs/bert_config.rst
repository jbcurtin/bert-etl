#######################
Bert Configuration File
#######################


bert-etl.yaml
+++++++++++++

With every function in the `jobs.py` file treated as a seperate lambda. Configuring each function, or all functions at once is possbile through the `bert-etl.yaml` file located relative to the module containing `jobs.py`. Lets start with an example,

.. code-block:: yaml

    deployment:
        s3_bucket: my-lambda-function-source

    every_lambda:
        batch_size: 150
        memory_size: 512
        timeout: 900
        environment:
            DEBUG: 'false'
            HOME: /tmp

    bert_tess_fullframe_main_0:
        timeout: 600
        invoke_args:
            - bucket: ffi-lc-cache
              key: mullally_input_list_001.txt
              use_cache: "true"


In the above example, a few different things are happening. `deployment` is telling `bert-etl` which bucket to upload the lambda source to. `every_lambda` is telling `bert-etl` how to configure each lambda when deployed. Finally, 'bert_tess_fullframe_main_0` is telling `bert-etl` that when invoked through the commandline, invoke with one set of arguments.

First, lets drill into how `bert-etl` makes decisions on how to apply the configuration to all lambdas. The first two entries, `deployment` and `every_lambda` are reserved for the `bert-etl.yaml` file. They perform special duties when deploying the codes to AWS Lambda or running the codes locally using `bert-runner.py`.

`deployment` tells `bert-etl` where things go in the cloud. Currently only `s3_bucket` is configured, but it could also contain `region` and an Amazon Resource Name(arn) for an ACM cert.

`every_lambda` idiomaticly means every option defined is applied as default to every lambda in the `jobs.py` file. Overwriting each lambda is possible through defining new entries such as `bert_tess_fullframe_main_0`. `bert_tess_fullframe_main_0` maps directly to a function defined inside `jobs.py`.

At deployment time, AWS lambda function `bert_tess_fullframe_main_0` will have `timeout` set to `600`, while `memory_size` will remain at `512`.


bert-etl.yaml attributes
------------------------

With a firm understanding on how to use `bert-etl.yaml`, lets start talking about some of the options available to the configuration file. Here is a full list with example values.

.. code-block: yaml

    deployment:
        s3_bucket: my-lambda-function-source

    every_lambda:
        runtime: python3.7
        concurrency_limit:      # AWS Lambda Reserved Concurrency Limit
        batch_size: 150         # Dynamodb Streaming BatchSize
        batch_delay: 3          # Dynamodb Streaming BatchSize Proc Delay
        memory_size: 512        # AWS Lambda Memory Limit
        timeout: 900            # AWS Lambda Timeout
        environment:            # AWS Lambda Environment Variables
            DEBUG: 'false'

        requirements:           # Pip requirements.txt pass wit -U
          - numpy==1.17.3

        identity_encoders:      # Used to encode python objects to str
          - 'bert.encoders.numpy.NumpyIdentityEncoder'
          - 'bert.encoders.base.IdentityEncoder'

        queue_encoders:         # Used to encode python objects to str
          - 'bert.encoders.numpy.encode_aws_object'
          - 'bert.encoders.base.encode_aws_object'

        queue_decoders:         # Used to encode python objects to str
          - 'bert.encoders.numpy.decode_aws_object'
          - 'bert.encoders.base.decode_aws_object'
    
    bert_tess_fullframe_main_0:
        invoke_args:
          - bucket: ffi-lc-cache
            key: mullally_input_list_001.txt
            use_cache: "true"


deployment
==========

`deployment` tells `bert-etl` how to and where to deploy the AWS Lambda functions.


=================== =============================================================================== =============================
VAR Name            Description                                                                     Example
------------------- ------------------------------------------------------------------------------- -----------------------------
s3_bucket           Which bucket to deploy to? bert-etl-lambda-source-bucket                        bert-etl-lambda-source-code
=================== =============================================================================== =============================


.. code-block:: yaml

    deployment:
        s3_bucket: bert-etl-lambda-source-code



every_lambda
============

`every_lambda` block in `bert-etl.yaml` will apply settings to every lambda function deployed to AWS Lambda as well as every function ran locally using `bert-runner.py`


=================== =============================================================================== =============================
VAR Name            Description                                                                     Example
------------------- ------------------------------------------------------------------------------- -----------------------------
batch_size          How many dynamodb records to process for each lambda?                           150 items
batch_size_delay    How many seconds to wait for dynamodb records before invocation of next lambda? 3 seconds
runtime             Which python runtime shall the lambda use?                                      python 3.7
environment         Which ENVVars would you like to pass to the lambda function?                    DEBUG = false
timeout             How long is the function allowed to run?                                        15 minutes, or 900 seconds
concurrency_limit   How many reserved concurrent lamdba executions would you like to allocate?      100
memory_size         How much memory/cpu shall the lambda utilize?                                   512
=================== =============================================================================== =============================


.. code-block:: yaml

    every_lambda:
        batch_size: 150
        batch_size_delay: 3
        runtime: python3.7
        environment:
            DEBUG: false

        timeout: 900
        concurrency_limit: 100
        memory_size: 512


init_job_queue
==============

`bert-etl` ships with the possibility to specify settings for each function created. Tagging the function with a supported var requires redefination of <function name> in `bert-etl.yaml`. The makeup of the settings is exactly the same as `every_lambda` block. Using the `bert-etl-testing` module, anything defined in `init_job_queue` will override anything defined in `every_lambda`


=================== =============================================================================== =============================
VAR Name            Description                                                                     Example
------------------- ------------------------------------------------------------------------------- -----------------------------
concurrency_limit   How many reserved concurrent lamdba executions would you like to allocate?      100
memory_size         How much memory/cpu shall the lambda utilize?                                   512
=================== =============================================================================== =============================


.. code-block:: yaml

    every_lambda:
        concurrency_limit: 50
        memory_size: 256

    init_job_queue:
        concurrency_limit: 100
        memory_size: 512


