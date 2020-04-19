#########################
Subscribing to SNS Topics
#########################

`bert-etl` supports `AWS Lambda SNS Topic Subscriptions <https://docs.aws.amazon.com/en_pv/sns/latest/dg/sns-lambda-as-subscriber.html>`_. Inside `bert-etl.yaml`, add the `events.sns_topic_arn` with an sns topic you've created. Here is an example,

.. code-block:: bash

    $ conda create -n bert-etl-sns-topic python=3.7 pip
    $ conda activate bert-etl-sns-topic
    $ pip install bert-etl -U
    $ git clone git@github.com:jbcurtin/bert-etl-testing.git bert-etl-sns-topic
    $ cd bert-etl-sns-topic


Inside the directory `bert-etl-scheduling`, open `bert-etl.yaml` in your editor and insert the following lines in `init_job_queue`


.. code-block:: yaml

    init_job_queue:
        events:
            sns_topic_arn: arn:aws:sns:us-east-1:<account-number>:billingTest
            
       
With the `sns_topic_arn` created in `bert-etl.yaml`, we're ready to deploy    

.. code-block:: bash

    bert-deploy.py -m bert_test_sns

