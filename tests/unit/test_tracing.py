# Copyright (C) 2018 SignalFx, Inc. All rights reserved.
from opentracing.mocktracer import MockTracer
from opentracing.ext import tags as ext_tags
import opentracing
import pytest
import mock
import json

from pymongo_opentracing.tracing import CommandTracing


class MockEvent(object):

    database_name = 'someDatabase'
    command_name = operation_name = 'someOperation'
    command = dict(someOperation='someCollection')
    request_id = 'request_id'
    reply = dict(someReply='ok')
    failure = dict(someFailure='not_ok')
    duration_micros = 123


class TestCommandTracing(object):

    def test_sources_tracer(self):
        tracer = MockTracer()
        tracing = CommandTracing(tracer)
        assert tracing._tracer == tracer

    def test_sources_global_tracer_by_default(self):
        tracing = CommandTracing()
        assert tracing._tracer is opentracing.tracer

    def test_sources_global_tracer_helper_by_default(self):
        if not hasattr(opentracing, 'global_tracer'):
            pytest.skip()

        with mock.patch('opentracing.global_tracer') as gt:
            gt.return_value = True
            tracing = CommandTracing()
            assert tracing._tracer is True
            assert gt.called

    def test_sources_span_tags(self):
        assert CommandTracing()._span_tags == {}
        desired_tags = dict(one=123)
        assert CommandTracing(span_tags=desired_tags)._span_tags is desired_tags

    def test_started(self):
        tracer = MockTracer()
        tracing = CommandTracing(tracer, span_tags=dict(one=123))
        event = MockEvent()
        tracing.started(event)
        scope = tracing._scopes.get('request_id')
        span = scope.span
        assert span.operation_name == 'someOperation'
        tags = span.tags
        assert tags['one'] == 123
        assert tags['command.name'] == 'someOperation'
        assert tags['command'] == json.dumps(event.command)
        assert tags['namespace'] == 'someDatabase.someCollection'
        assert tags[ext_tags.COMPONENT] == 'PyMongo'

    def test_succeeded_no_existing_scope(self):
        tracer = MockTracer()
        tracing = CommandTracing(tracer, span_tags=dict(one=123))
        tracing.succeeded(MockEvent())

    def test_succeeded_existing_span(self):
        tracer = MockTracer()
        tracing = CommandTracing(tracer, span_tags=dict(one=123))
        event = MockEvent()
        tracing.started(event)
        scope = tracing._scopes.get('request_id')
        tracing.succeeded(event)
        assert tracing._scopes.get('request_id') is None

        tags = scope.span.tags
        assert tags['event.reply'] == json.dumps(event.reply)
        assert tags['reported_duration'] == event.duration_micros

    def test_failed_no_existing_scope(self):
        tracer = MockTracer()
        tracing = CommandTracing(tracer, span_tags=dict(one=123))
        tracing.failed(MockEvent())

    def test_failed_existing_span(self):
        tracer = MockTracer()
        tracing = CommandTracing(tracer, span_tags=dict(one=123))
        event = MockEvent()
        tracing.started(event)
        scope = tracing._scopes.get('request_id')
        tracing.failed(event)
        assert tracing._scopes.get('request_id') is None

        tags = scope.span.tags
        assert tags['event.failure'] == json.dumps(event.failure)
        assert tags['reported_duration'] == event.duration_micros
