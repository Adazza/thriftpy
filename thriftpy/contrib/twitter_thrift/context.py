"""
Collection of finagle compliant context tools.
"""
import contextlib
import threading

broadcast = threading.local()
broadcast.contexts = []


class Context(object):
    def __init__(self, key, value):
        self.key = key
        self.value = value


@contextlib.contextmanager
def broadcast_context(key, value):
    """
    When used with `with` introduces a broadcast context for
    the scope of the thrift rpc call.
    :param key:
    :param value:
    :return:
    """
    c = Context(key, value)
    broadcast.contexts.append(c)
    yield
    broadcast.contexts.pop()


@contextlib.contextmanager
def authorize(token):
    broadcast.contexts.append(Context("com.adazza.common.finagle.AuthToken", token))
    yield
    broadcast.contexts.pop()
