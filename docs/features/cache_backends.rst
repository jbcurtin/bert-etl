Backend Cache
#############


Backend caching provides an API that allows you to process data in real time, without having to step through all the
functions again each time you want to debug an issue with a job.


.. code-block:: python

    @binding.follow('noop', cache_backend=backends.RedisCacheBackend)
    def first_job():
        ...

    @binding.follow(first_job):
    def second_job():
        ...


.. code-block:: bash

    $ bert-runner.py --enable-job-cache --stop-after-job first_job
    $ bert-runner.py --enable-job-cache --start-before-job second_job

.. toctree::
    :maxdepth: 2

