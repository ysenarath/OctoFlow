from octoflow.utils import hashutils


@hashutils.hashable("dill", version="0.0.1")
def some_callable():
    return "hello world"


print(hashutils.hash(some_callable))


@hashutils.hashable("dill", version="0.0.2")
def some_callable():
    return "hello world"


print(hashutils.hash(some_callable))


@hashutils.hashable("src", version="0.0.1")
def some_callable():
    return "hello world"


print(hashutils.hash(some_callable))


@hashutils.hashable("src", version="0.0.1")
def some_callable():
    return "hello world"


print(hashutils.hash(some_callable))
