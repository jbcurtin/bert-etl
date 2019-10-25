##################
AWS Lambda Testing
##################

`bert-etl` supports AWS Lambda Schedule expressions. Inside `bert-etl.yaml`, add the `events.schedule_expression` with `rate(1 minute)` or `cron(10 15 * * ? *)` to scheduling periodic invocation the `bert-etl` pipeline init function. Here is an example,

.. code-block:: bash

    $ conda create -n bert-etl-scheduling python=3.7 pip
    $ conda activate bert-etl-scheduling
    $ pip install bert-etl -U
    $ git clone git@github.com:jbcurtin/bert-etl-testing.git bert-etl-scheduling
    $ cd bert-etl-scheduling


Inside the directory `bert-etl-scheduling`, open `bert-etl.yaml` in your editor and insert the following lines in `init_job_queue`


.. code-block:: yaml

    init_job_queue:
        events:
            schedule_expression: rate(1 minute)
            
       
With the `schedule_expression` created in `bert-etl.yaml`, we're ready to deploy    

.. code-block:: bash

    bert-deploy.py -m bert_testing_jobs

