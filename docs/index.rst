########
bert-etl
########

A microframework for simple ETL solutions

Engineer Oriented API Design
----------------------------

Concurrent Processing in any language can be hazardous. Like many other programming and scripting languages out there,
Python has its own limitations the Engineer must overcome in order to process data in parallel. `bert-etl` attempts to
abstract away concurrency as much as possible. Instead assuming the Engineer writing code knows that the function
they're writing follows a few simple concepts


Pure Functions
##############

In compute programming, a pure function is a function that has the following properties

* Its return value is the same for the same arguments
* Its evaluation has no side effects


The simplist Pure Function to be written could be,

.. code-block:: python

    def add(*args: 'args') -> int:
        return sum(args)

Stream Processing
#################

Using stream processing, calculations, alterations, mutations, and variations can be abstracted away into functions that
alter the `state` of the data. `bert-etl` treats every function as its own namespace. Python, Logic, and Data is all
considered unique in the context of a Function Execution. This allows for random-interval data calculations, while
maintaining sequential order of Function Execution

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
    def variation():
        work_queue, done_queue, ologger = utils.comm_binders(variation)

        for details in work_queue:
            for key, value in details.keys():
                ologger.info(f'Key[{key}], Value[{value}], Alteration[{value % 5}]')


Easily Debug without Logging
############################

`bert-etl` encourages the use of using your favorite debugger. Today, `pdb`, and `ipdb` are know to be able to 
work within Function Execution context

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


Encouraging this API provides for a very powerful debugging experiance. When ready to test code in a concurrent manor,
set an Environment Variable `DEBUG=False` and invoke `bert-runner.py`


bert-runner.py
##############

`bert-runner.py` provides an execution context that'll run `bert-etl` jobs in sequence, one-function at a time or
concurrently on local hardware using Python multiprocessing module. Data is shared between Function Executions through
Redis_

.. _Redis: https://redis.io/

Lets search for Exoplanets using `bert-runner.py` and with out local hardware after using `bert-example.py` to clone
the example from Github

.. code-block:: bash

    $ docker run -p 6379:6379 -d redis
    $ bert-example.py --project-name tess-exoplanet-search
    $ cd /tmp/tess-exoplanet-search
    $ DEBUG=False bert-runner.py -m tess_exoplanet_search -f

You can view a list of example projects using `bert-example.py`

.. code-block:: bash

    $ bert-example.py --list-example-projects


.. toctree::
    :maxdepth: 2

    commands/index.rst
    features/index.rst

