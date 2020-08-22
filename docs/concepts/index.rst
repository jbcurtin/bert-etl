Distributed System & Computer Programming Concepts
##################################################

Pure Function
-------------

In compute programming, a pure function is a function that has the following properties

* Its return value is the same for the same arguments
* Its evaluation has no side effects


The simplist form of a Pure Function could be written as,

.. code-block:: python

    def add(*args: 'args') -> int:
        return sum(args)

Citation
********

`Pure Function`_

.. _Pure Function: https://en.wikipedia.org/wiki/Pure_function

Stream Processing
-----------------

Stream processing is especially suitable for applications that exhibit three application characteristics:

* Compute Intensity, the number of arithmetic operations per I/O or global memory reference. In many signal processing applications today it is well over 50:1 and increasing with algorithmic complexity
* Data Parallelism exists in a kernel if the same function is applied to all records of an input stream and a number of records can be processed simultaneously without waiting for results from previous records
* Data Locality is a specific type of temporal locality common in signal and media processing applications where data is produced once, read once or twice later in the application, and never read again. Intermediate streams passed between kernels as well as intermediate data within kernel functions can capture this locality directly using the stream processing programming model

Citation
********

`Stream Processing`_

.. _Stream Processing: https://en.wikipedia.org/wiki/Stream_processing#Applications

Further Reading
***************

| `Kafka Architecture`_
| `Amazon Dynamodb Streams`_

.. _Kafka Architecture: https://kafka.apache.org/24/documentation/streams/architecture
.. _Amazon Dynamodb Streams: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html

Sequential Consistency
----------------------

the result of any execution is the same as if the operations of all the processors were executed in some sequential
order, and the operations of each individual processor appear in this sequence in the order specified by its program[0]

Citation
********

| How to Make a Multiprocessor Computer That Correctly Executes Multiprocess Programs
|   IEEE Trans. Comput. C-28,9 (Sept. 1979), 690-691
|   Leslie Lamport
| `Sequential Consistency`_

.. _Sequential Consistency: https://en.wikipedia.org/wiki/Sequential_consistency

.. toctree::
    :maxdepth: 0

    mass-processing

