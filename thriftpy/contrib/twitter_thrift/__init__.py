"""
Implementation of twitter thrift for thriftpy


"""

from __future__ import absolute_import

import warnings
from random import Random
from pkg_resources import resource_stream
from ...contrib.twitter_thrift.context import broadcast
from ...protocol import TBinaryProtocolFactory
from ...transport import TSocket, TSSLSocket, TFramedTransportFactory
from ...thrift import TClient, TApplicationException, TMessageType, TType
from ...parser import load_fp

random = Random()

# From:
# https://github.com/twitter/finagle/blob/master/finagle-thrift/src/main/scala/com/twitter/finagle/thrift/ThriftTracing.scala
trace_method = "__can__finagle__trace__v3__"
trace_thrift_stream = resource_stream('thriftpy', 'thriftpy/contrib/twitter_thrift/tracing.thrift')
trace_thrift = load_fp(trace_thrift_stream, 'trace_thrift')

__all__ = ["TwitterTClient"]


def make_twitter_thrift_client(client_id, service, host="localhost", port=9090, unix_socket=None,
                               proto_factory=TBinaryProtocolFactory(),
                               trans_factory=TFramedTransportFactory(),
                               timeout=None,
                               cafile=None, ssl_context=None, certfile=None, keyfile=None):
    if unix_socket:
        socket = TSocket(unix_socket=unix_socket)
        if certfile:
            warnings.warn("SSL only works with host:port, not unix_socket.")
    elif host and port:
        if cafile or ssl_context:
            socket = TSSLSocket(host, port, socket_timeout=timeout,
                                cafile=cafile,
                                certfile=certfile, keyfile=keyfile,
                                ssl_context=ssl_context)
        else:
            socket = TSocket(host, port, socket_timeout=timeout)
    else:
        raise ValueError("Either host/port or unix_socket must be provided.")

    transport = trans_factory.get_transport(socket)
    protocol = proto_factory.get_protocol(transport)
    transport.open()
    return TwitterTClient(client_id, service, protocol)


def marshal_broadcast_contexts(contexts):
    return [trace_thrift.RequestContext(c.key.encode('utf-8'), c.value.encode('utf-8'))
            for c in contexts]


class TwitterTClient(TClient):
    """
    https://github.com/twitter/finagle/blob/develop/finagle-core/src/main/scala/com/twitter/finagle/tracing/Id.scala

    """
    def __init__(self, client_id, *args, **kwargs):
        super(TwitterTClient, self).__init__(*args, **kwargs)
        self.use_upgraded_thrift = self._negotiation()
        self.client_id = client_id

    def _negotiation(self):
        self._oprot.write_message_begin(trace_method, TMessageType.CALL,
                                        self._seqid)

        args = trace_thrift.ConnectionOptions()
        args.write(self._oprot)

        self._oprot.write_message_end()
        self._oprot.trans.flush()

        api, msg_type, seqid = self._iprot.read_message_begin()

        if msg_type == TMessageType.EXCEPTION:
            x = TApplicationException()
            x.read(self._iprot)
            self._iprot.read_message_end()
            return False
        else:
            result = trace_thrift.UpgradeReply()
            result.read(self._iprot)
            self._iprot.read_message_end()
            return True

    def _recv(self, _api):
        if self.use_upgraded_thrift:
            response_header = trace_thrift.ResponseHeader()
            response_header.read(self._iprot)

        return super(TwitterTClient, self)._recv(_api)

    def _send(self, _api, **kwargs):
        """
        :param _api:
        :param kwargs:
        :return:
        """
        if self.use_upgraded_thrift:
            request_header = trace_thrift.RequestHeader()

            span_id = int(random.getrandbits(32))
            request_header.trace_id = span_id
            request_header.parent_span_id = span_id
            request_header.span_id = span_id

            request_header.sampled = False
            request_header.flags = 0  # Flags is a bit set of enabled features/flags for the request
            request_header.contexts = [trace_thrift.RequestContext(
                'com.twitter.finagle.thrift.ClientIdContext', self.client_id)]
            request_header.contexts += marshal_broadcast_contexts(broadcast.contexts)
            request_header.delegations = []

            request_header.write(self._oprot)

        super(TwitterTClient, self)._send(_api, **kwargs)
