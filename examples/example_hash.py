from octoflow.utils import hash


@hash.hashable("dill", version="0.0.1")
def some_callable():
    return "hello world"


print(hash.hash(some_callable))


@hash.hashable("dill", version="0.0.2")
def some_callable():
    return "hello world"


print(hash.hash(some_callable))


@hash.hashable("src", version="0.0.1")
def some_callable():
    return "hello world"


print(hash.hash(some_callable))


@hash.hashable("src", version="0.0.1")
def some_callable():
    return "hello world"


print(hash.hash(some_callable))
