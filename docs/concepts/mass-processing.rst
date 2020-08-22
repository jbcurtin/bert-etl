Mass Processing
###############

Sometimes its nessicary to process large lists of files, but writing code in the pipeline can be a time sink waiting
for data to feed back into the queue and exit. Luckly the cache_backend is available to populate data to help speed up
development. Here are a few strategies to help


Offline Data
------------

Scraping the same data two or more times from the same online resource is frowned upon. Install a `cache_backend` in
the `@binding.follow` decorator to stay in good standing with the online resource you're sourcing and to speed up your
local development

.. code-block:: python

    @binding.follow('noop', cache_backend=backends.RedisCacheBackend)
    def download_contents():
        ...


.. code-block:: bash

    $ bert-runner.py -m myModule -f


Restarting in the middle of the pipeline
----------------------------------------

`bert-etl` ships with the ability to fill in a `job.work_queue` without having to execute the entire pipeline. Setup
a `cache_backend.RedisCacheBackend` on the previous `job`. Fill the cache; after `bert-runner.py` exists, restart
`bert-runner.py` at the `job` you'd like to work on

.. code-block:: bash

    $ bert-runner.py -m myModule -f
    $ bert-runner.py -m myModule -j transform_downloaded_content


Stoping execution at a specific job
-----------------------------------

`bert-etl` also provides the ability to stop in the middle of the pipeline

.. code-block:: bash

    $ bert-runner.py -m myModule -f -s transform_downloaded_content

