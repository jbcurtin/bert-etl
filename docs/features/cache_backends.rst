Cache Backends
##############

Cache Backends provide a space to store inputs and outputs of functions. This is useful in development when you want
to skip processing of earlier functions in the call chain. Currently there is only support for a REDIS Backend

Cache backends can be added to the `@binding.follow` decorator

Cache Backends take the full output of the function in an clean execution state. Store it in cache to be used nexttime
the function is called. The function is invoked reguardless of the cache, the work_queue will be empty if the cache
is full

.. code-block:: python

    from bert import binding, constants, backends

    @binding.follow(earlier_function, cache_backend=backends.RedisCacheBackend)
    def later_function():
        ...

        

.. toctree::
    :maxdepth: 2

    invoke_args
    queue_encoders__and__queue_decoders
    bert_config
    schedule_expressions
    sns_topics
    assume_role


