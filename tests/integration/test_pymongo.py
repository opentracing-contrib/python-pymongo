# Copyright (C) 2018 SignalFx, Inc. All rights reserved.
from random import randint

from opentracing.mocktracer import MockTracer
from pymongo.errors import OperationFailure
from six import text_type, unichr
from opentracing.ext import tags
from pymongo import WriteConcern
from textwrap import dedent as d
from pymongo import MongoClient
from bson import json_util
from bson.code import Code
from bson.son import SON
import docker
import pytest
import json

from pymongo_opentracing import CommandTracing


@pytest.fixture(scope='session')
def mongo_container():
    client = docker.from_env()
    mongo = client.containers.run('mongo:latest', ports={'27017/tcp': 27017}, detach=True)
    try:
        yield mongo
    finally:
        mongo.remove(force=True, v=True)


class TestCommandTracing(object):

    _min = int('0x2700', 16)
    _max = int('0x27bf', 16)

    def random_string(self):
        """Returns a valid unicode field name"""
        rands = []
        while len(rands) < 10:
            rand = randint(self._min, self._max)
            if rand not in (0, 36, 46):
                rands.append(rand)
        return text_type('').join(unichr(i) for i in rands)

    def namespace(self, db_name, collection_name):
        return text_type('{}.{}').format(db_name, collection_name)

    @pytest.fixture
    def command_tracing(self, mongo_container):
        tracer = MockTracer()
        client = MongoClient(event_listeners=[CommandTracing(tracer, span_tags=dict(custom='tag'))])
        return tracer, client

    @pytest.fixture
    def tracer(self, command_tracing):
        return command_tracing[0]

    @pytest.fixture
    def client(self, command_tracing):
        return command_tracing[1]

    def test_successful_insert_many(self, tracer, client):
        db_name = self.random_string()
        collection_name = self.random_string()
        collection = client[db_name][collection_name]
        docs = [{self.random_string(): self.random_string() for _ in range(5)} for __ in range(5)]
        collection.insert_many(docs)
        spans = tracer.finished_spans()
        assert len(spans) == 1
        span = spans[0]
        assert span.operation_name == 'insert'
        assert span.tags['namespace'] == self.namespace(db_name, collection_name)
        assert span.tags['custom'] == 'tag'
        assert span.tags['command.name'] == 'insert'
        assert span.tags[tags.COMPONENT] == 'PyMongo'
        assert span.tags['reported_duration']
        assert span.tags['event.reply']
        assert tags.ERROR not in span.tags
        assert 'event.failure' not in span.tags

    def test_unsuccessful_insert_many(self, tracer, client):
        db_name = self.random_string()
        collection_name = self.random_string()

        # requiring replication on standalone will cause failure
        write_concern = WriteConcern(w=3)
        collection = client[db_name].get_collection(collection_name, write_concern=write_concern)

        docs = [{self.random_string(): self.random_string() for _ in range(5)} for __ in range(5)]
        with pytest.raises(OperationFailure):
            collection.insert_many(docs)

        spans = tracer.finished_spans()
        assert len(spans) == 1
        span = spans[0]
        assert span.operation_name == 'insert'
        assert span.tags['namespace'] == self.namespace(db_name, collection_name)
        assert span.tags['custom'] == 'tag'
        assert span.tags['command.name'] == 'insert'
        assert span.tags[tags.COMPONENT] == 'PyMongo'
        assert span.tags['reported_duration']
        assert span.tags[tags.ERROR] is True
        expected_failure = dict(code=2, codeName='BadValue', ok=0.0,
                                errmsg="cannot use 'w' > 1 when a host is not replicated")
        assert json.loads(span.tags['event.failure']) == expected_failure
        assert 'event.reply' not in span.tags

    def test_successful_find_one(self, tracer, client):
        db_name = self.random_string()
        collection_name = self.random_string()
        collection = client[db_name][collection_name]
        docs = [{self.random_string(): self.random_string() for _ in range(5)} for __ in range(5)]
        collection.insert_many(docs)
        tracer.reset()

        list(collection.find(docs[3]))
        spans = tracer.finished_spans()
        assert len(spans) == 1
        span = spans[0]
        assert span.operation_name == 'find'
        assert span.tags['namespace'] == self.namespace(db_name, collection_name)
        assert span.tags['custom'] == 'tag'
        assert span.tags['command.name'] == 'find'
        assert span.tags[tags.COMPONENT] == 'PyMongo'
        assert span.tags['reported_duration']
        assert span.tags['event.reply']
        assert tags.ERROR not in span.tags
        assert 'event.failure' not in span.tags

    def test_successful_find_many(self, tracer, client):
        db_name = self.random_string()
        collection_name = self.random_string()
        collection = client[db_name][collection_name]
        docs = [{self.random_string(): i} for i in range(500)]
        collection.insert_many(docs)
        tracer.reset()

        list(collection.find(batch_size=2))
        spans = tracer.finished_spans()
        assert len(spans) == 251
        span = spans[0]
        assert span.operation_name == 'find'
        assert span.tags['namespace'] == self.namespace(db_name, collection_name)
        assert span.tags['custom'] == 'tag'
        assert span.tags['command.name'] == 'find'
        assert span.tags[tags.COMPONENT] == 'PyMongo'
        assert span.tags['reported_duration']
        assert tags.ERROR not in span.tags
        assert 'event.failure' not in span.tags

        cursor = json_util.loads(span.tags['event.reply'])['cursor']
        assert cursor['firstBatch'] == docs[:2]

        cursor_id = cursor['id']

        for i, span in enumerate(spans[1:], 1):
            assert span.operation_name == 'getMore'
            assert span.tags['namespace'] == self.namespace(db_name, cursor_id)
            assert span.tags['custom'] == 'tag'
            assert span.tags['command.name'] == 'getMore'
            assert span.tags[tags.COMPONENT] == 'PyMongo'
            assert span.tags['reported_duration']
            cursor = json_util.loads(span.tags['event.reply'])['cursor']
            batch = docs[i * 2:i * 2 + 2] if i * 2 < len(docs) else []
            assert cursor['nextBatch'] == batch

    def test_successful_aggregation(self, tracer, client):
        db_name = self.random_string()
        collection_name = self.random_string()
        collection = client[db_name][collection_name]
        docs = [dict(items=[1, 2, 3]), dict(items=[2, 3]), dict(items=[3])]
        collection.insert_many(docs)
        tracer.reset()
        pipeline = [{"$unwind": "$items"},
                    {"$group": {"_id": "$items", "count": {"$sum": 1}}},
                    {"$sort": SON([("count", -1), ("_id", -1)])}]
        collection.aggregate(pipeline)
        spans = tracer.finished_spans()
        span = spans[0]
        assert span.operation_name == 'aggregate'
        assert span.tags['namespace'] == self.namespace(db_name, collection_name)
        assert span.tags['custom'] == 'tag'
        assert span.tags['command.name'] == 'aggregate'
        assert span.tags[tags.COMPONENT] == 'PyMongo'
        assert span.tags['reported_duration']
        cursor = json_util.loads(span.tags['event.reply'])['cursor']
        assert cursor['firstBatch'] == [dict(count=3, _id=3),
                                        dict(count=2, _id=2),
                                        dict(count=1, _id=1)]

    def test_successful_map_reduce(self, tracer, client):
        db_name = self.random_string()
        collection_name = self.random_string()
        collection = client[db_name][collection_name]
        docs = [dict(items=[1, 2, 3]), dict(items=[2, 3]), dict(items=[3])]
        collection.insert_many(docs)
        tracer.reset()
        mapper = Code(d('''
            function() {
                this.items.forEach(function(i) {
                    emit(i, 1);
                });
            }
        '''))
        reducer = Code(d('''
            function (key, values) {
                var total = 0;
                for (var i = 0; i < values.length; i++) {
                    total += values[i];
                }
                return total;
           }
        '''))
        results = collection.map_reduce(mapper, reducer, 'results')
        list(results.find(batch_size=2))
        spans = tracer.finished_spans()
        assert len(spans) == 3
        mr_span = spans[0]
        assert mr_span.operation_name == 'mapreduce'
        assert mr_span.tags['namespace'] == self.namespace(db_name, collection_name)
        assert mr_span.tags['custom'] == 'tag'
        assert mr_span.tags['command.name'] == 'mapreduce'
        assert mr_span.tags[tags.COMPONENT] == 'PyMongo'
        assert mr_span.tags['reported_duration']
        assert tags.ERROR not in mr_span.tags
        assert 'event.failure' not in mr_span.tags

        counts = json_util.loads(mr_span.tags['event.reply'])['counts']
        assert counts == dict(input=3, reduce=2, emit=6, output=3)

        f_span = spans[1]
        assert f_span.operation_name == 'find'
        assert f_span.tags['namespace'] == self.namespace(db_name, 'results')
        assert f_span.tags['custom'] == 'tag'
        assert f_span.tags['command.name'] == 'find'
        assert f_span.tags[tags.COMPONENT] == 'PyMongo'
        assert f_span.tags['reported_duration']
        cursor = json_util.loads(f_span.tags['event.reply'])['cursor']
        assert cursor['firstBatch'] == [dict(_id=1.0, value=1.0), dict(_id=2.0, value=2.0)]

        cursor_id = cursor['id']
        n_span = spans[2]
        assert n_span.operation_name == 'getMore'
        assert n_span.tags['namespace'] == self.namespace(db_name, cursor_id)
        assert n_span.tags['custom'] == 'tag'
        assert n_span.tags['command.name'] == 'getMore'
        assert n_span.tags[tags.COMPONENT] == 'PyMongo'
        assert n_span.tags['reported_duration']
        cursor = json_util.loads(n_span.tags['event.reply'])['cursor']
        assert cursor['nextBatch'] == [dict(_id=3.0, value=3.0)]

    def test_unsuccessful_map_reduce(self, tracer, client):
        db_name = self.random_string()
        collection_name = self.random_string()
        collection = client[db_name][collection_name]
        docs = [dict(items=[1, 2, 3]), dict(items=[2, 3]), dict(items=[3])]
        collection.insert_many(docs)
        tracer.reset()

        mapper = Code(d('''
            function() {
                this.items.forEach(function(i) {
                    throw new Error('Bomb!');
                });
            }
        '''))
        reducer = Code('function (key, values) { }')
        with pytest.raises(OperationFailure):
            collection.map_reduce(mapper, reducer, 'results')

        spans = tracer.finished_spans()
        assert len(spans) == 1
        span = spans[0]
        assert span.operation_name == 'mapreduce'
        assert span.tags['namespace'] == self.namespace(db_name, collection_name)
        assert span.tags['custom'] == 'tag'
        assert span.tags['command.name'] == 'mapreduce'
        assert span.tags[tags.COMPONENT] == 'PyMongo'
        assert span.tags['reported_duration']
        assert span.tags[tags.ERROR] is True

        failure = json.loads(span.tags['event.failure'])
        assert failure['code'] == 139
        assert failure['codeName'] == 'JSInterpreterFailure'
        assert failure['ok'] == 0.0
        assert 'Error: Bomb!' in failure['errmsg']
