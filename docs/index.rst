########
bert-etl
########

A microframework for simple ETL solutions

Engineer First API Design
-------------------------

Concurrent Processing in any language can be hazardous. Like many programming and scripting languages out there,
Python has its own Concurrent Processing Models. Engineers learn to understand these Concurrent Processing Models in
order to process data in parallel. `bert-etl` attempts to abstract away concurrency as much as possible. Asking the
Engineer writing code to follow these simple concepts instead of having to write boiler-plate logic for handling
Pythons' Concurrent Processing Models


Pure Functions
##############

In compute programming, a pure function is a function that has the following properties

* Its return value is the same for the same arguments
* Its evaluation has no side effects


The simplist form of a Pure Function could be written as,

.. code-block:: python

    def add(*args: 'args') -> int:
        return sum(args)

Stream Processing
#################

Using stream processing, calculations and variations can be abstracted away into functions that
alter data. `bert-etl` treats every function as its own isolated environment. Python, Logic, and Data is all
considered unique in the context of a Function Execution. This allows for random-interval data calculations, while
maintaining sequential order of Function Executions

.. code-block:: python

    from bert import binding, utils, constants

    @binding.follow('noop')
    def create_data():
        work_queue, done_queue, ologger = utils.comm_binders(create_data)

        for idx in range(0, 100):
            done_queue.put({
                'idx': idx
            })

    @binding.follow(create_data, pipeline_type=constants.PipelineType.CONCURRENT)
    def calculate_data():
        import math

        work_queue, done_queue, ologger = utils.comm_binders(calculate_data)

        for details in work_queue:
            details['calculated-result'] = math.pow(details['idx'], 2)
            done_queue.put(details)

    @binding.follow(calculate_data, pipeline_type=constants.PipelineType.CONCURRENT)
    def show_variation():
        work_queue, done_queue, ologger = utils.comm_binders(show_variation)

        for details in work_queue:
            for key, value in details.keys():
                ologger.info(f'Key[{key}], Value[{value}], Alteration[{value % 5}]')


Easily Debug without Logging
############################

`bert-etl` encourages the use of your favorite debugger. Today, `pdb`, and `ipdb` are known to work within
Function Executions

.. code-block:: python

    from bert improt binding, utils, constants

    @binding.follow('noop')
    def create_data():
        work_queue, done_queue, ologger = utils.comm_binders(create_data)

        for idx in range(0, 10):
            done_queue.put({
                'idx': idx
            })

    @binding.follow(create_data)
    def print_data() -> None:
        work_queue, done_queue, ologger = utils.comm_binders(print_data)
        for details in work_queue:
            import pdb; pdb.set_trace()
            ologger.info(f'Idx: {details["idx"]}')


Encouraging this kind of API provides for very powerful debugging experiences. When ready to test code in a concurrent
manor, set an Environment Variable `DEBUG=False` and invoke `bert-runner.py`


bert-runner.py
##############

`bert-runner.py` provides invocation that'll run Bert ETL Jobs in sequence, one-function at a time or
concurrently on local hardware using Python `multiprocessing` module. Data is shared between Function Executions with
Redis_

.. _Redis: https://redis.io/

Lets search for Exoplanets with `bert-runner.py` using local hardware

.. code-block:: bash

    $ docker run -p 6379:6379 -d redis
    $ bert-example.py --project-name tess-exoplanet-search
    $ cd /tmp/tess-exoplanet-search
    $ DEBUG=False bert-runner.py -m tess_exoplanet_search -f

Using `bert-example.py` to view a list of available example projects

.. code-block:: bash

    $ bert-example.py --list-example-projects


.. toctree::
    :maxdepth: 2

    commands/index.rst
    features/index.rst

