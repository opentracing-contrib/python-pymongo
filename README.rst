#################
PyMongo OpenTracing
#################

This package enables tracing `Mongo`_ commands in a `PyMongo`_ ``MongoClient`` via `The OpenTracing Project`_. 
Once a production system contends with real concurrency or splits into many services, crucial (and
formerly easy) tasks become difficult: user-facing latency optimization, root-cause analysis of backend
errors, communication about distinct pieces of a now-distributed system, etc. Distributed tracing
follows a request on its journey from inception to completion from mobile/browser all the way to the
microservices. 

As core services and libraries adopt OpenTracing, the application builder is no longer burdened with
the task of adding basic tracing instrumentation to their own code. In this way, developers can build
their applications with the tools they prefer and benefit from built-in tracing instrumentation.
OpenTracing implementations exist for major distributed tracing systems and can be bound or swapped
with a one-line configuration change.

If you want to learn more about the underlying Python API, visit the Python `source code`_.

.. _Mongo: https://www.mongodb.com/
.. _PyMongo: http://api.mongodb.com/python/current/
.. _The OpenTracing Project: http://opentracing.io/
.. _source code: https://github.com/signalfx/python-pymongo/

Installation
============

Run the following command:

.. code-block:: 

    $ git clone https://github.com/signalfx/python-pymongo.git && pip install ./python-pymongo

Usage
=====

This PyMongo monitor allows the tracing of database queries and commands using the OpenTracing API.
All that it requires is for a ``CommandTracing`` listener instance to be initialized using an instance
of an OpenTracing tracer and registered via the ``pymongo.monitoring`` module.

Initialize
----------

``CommandTracing`` takes the ``Tracer`` instance that is supported by OpenTracing and an optional
dictionary of desired tags for each created span. To create a ``CommandTracing`` object, you can
either pass in a tracer object directly or default to the ``opentracing.tracer`` global tracer that's
set elsewhere in your application:

.. code-block:: python

    from pymongo_opentracing import CommandTracing
    from pymongo import monitoring

    opentracing_tracer = ## some OpenTracing tracer implementation
    # All future clients will have CommandTracing registered for their events
    monitoring.register(CommandTracing(opentracing_tracer,
                                       span_tags={'MyCustomTag': 'HelpfulVal'}))

or

.. code-block:: python

    from pymongo_opentracing import CommandTracing
    import opentracing
    import pymongo

    opentracing.tracer = ## some OpenTracing tracer implementation
    # Only this client will have CommandTracing trace their events
    client = pymongo.MongoClient(event_listeners=[CommandTracing()])

Further Information
===================

If you're interested in learning more about the OpenTracing standard, please visit
`opentracing.io`_ or `join the mailing list`_. If you would like to implement OpenTracing
in your project and need help, feel free to send us a note at `community@opentracing.io`_.

.. _opentracing.io: http://opentracing.io/
.. _join the mailing list: http://opentracing.us13.list-manage.com/subscribe?u=180afe03860541dae59e84153&id=19117aa6cd
.. _community@opentracing.io: community@opentracing.io
