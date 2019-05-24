# Copyright (C) 2018 SignalFx, Inc. All rights reserved.
from bson import json_util as json
from opentracing.ext import tags
import pymongo.monitoring
from six import text_type
import opentracing


class CommandTracing(pymongo.monitoring.CommandListener):

    _scopes = {}

    def __init__(self, tracer=None, span_tags=None):
        try:
            global_tracer = opentracing.global_tracer()
        except AttributeError:
            global_tracer = opentracing.tracer

        self._tracer = tracer or global_tracer
        self._span_tags = span_tags or {}

    def started(self, event):
        scope = self._tracer.start_active_span(event.command_name)
        self._scopes[event.request_id] = scope
        span = scope.span

        span.set_tag(tags.DATABASE_TYPE, 'mongodb')
        span.set_tag(tags.COMPONENT, 'PyMongo')
        span.set_tag(tags.DATABASE_INSTANCE, event.database_name)
        for tag, value in self._span_tags.items():
            span.set_tag(tag, value)

        if not event.command:
            return

        command_name, collection = next(iter(event.command.items()))
        span.set_tag('command.name', command_name)
        namespace = text_type('{}.{}').format(event.database_name, collection)
        span.set_tag('namespace', namespace)
        span.set_tag('command', json.dumps(event.command)[:512])

    def succeeded(self, event):
        scope = self._scopes.pop(event.request_id, None)
        if scope is None:
            return
        span = scope.span
        span.set_tag('event.reply', json.dumps(event.reply)[:512])
        span.set_tag('reported_duration', event.duration_micros)
        scope.close()

    def failed(self, event):
        scope = self._scopes.pop(event.request_id, None)
        if scope is None:
            return
        span = scope.span
        span.set_tag(tags.ERROR, True)
        span.set_tag('event.failure', json.dumps(event.failure))
        span.set_tag('reported_duration', event.duration_micros)
        scope.close()
