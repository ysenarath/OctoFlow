"""
Fast cryptographic hash of Python objects, with a special case for fast
hashing of numpy arrays.
"""

# Author: Gael Varoquaux <gael dot varoquaux at normalesup dot org>
# Copyright (c) 2009 Gael Varoquaux
# License: BSD Style, 3 clauses.

import decimal
import io
import logging
import pickle  # noqa: S403
import struct
import sys
import types
from typing import (
    Any,
    Callable,
    Generic,
    Literal,
    Type,
    TypeVar,
    overload,
    runtime_checkable,
)

import xxhash
from typing_extensions import ParamSpec, Protocol

logger = logging.getLogger(__name__)

Pickler = pickle._Pickler

T = TypeVar("T")
P = ParamSpec("P")


@runtime_checkable
class Hashable(Protocol):
    """A class that can be hashed."""

    def _hash_repr_(self):  # noqa: PLW3201
        """Get the parameters that define the hash."""
        raise NotImplementedError


class _ConsistentSet:
    """Class used to ensure the hash of Sets is preserved
    whatever the order of its items.
    """

    def __init__(self, set_sequence):
        # Forces order of elements in set to ensure consistent hash.
        try:
            # Trying first to order the set assuming the type of elements is
            # consistent and orderable.
            # This fails on python 3 when elements are unorderable
            # but we keep it in a try as it's faster.
            self._sequence = sorted(set_sequence)
        except (TypeError, decimal.InvalidOperation):
            # If elements are unorderable, sorting them using their hash.
            # This is slower but works in any case.
            self._sequence = sorted(hash(e) for e in set_sequence)


class _MyHash:
    """Class used to hash objects that won't normally pickle"""

    def __init__(self, *args):
        self.args = args


class Hasher(Pickler):
    """A subclass of pickler, to do cryptographic hashing, rather than
    pickling.
    """

    def __init__(self):
        self.stream = io.BytesIO()
        # By default we want a pickle protocol that only changes with
        # the major python version and not the minor one
        protocol = 3
        Pickler.__init__(self, self.stream, protocol=protocol)
        # Initialise the hash obj
        self._hash = xxhash.xxh64()

    @overload
    def hash(
        self, obj: Hashable, return_digest: Literal[True] = ...
    ) -> str: ...

    def hash(self, obj, return_digest=True):
        try:
            self.dump(obj)
        except pickle.PicklingError as e:
            e.args += (f"PicklingError while hashing {obj!r}: {e!r}",)
            raise
        dumps = self.stream.getvalue()
        self._hash.update(dumps)
        if return_digest:
            return self._hash.hexdigest()

    def save(self, obj):
        if isinstance(obj, Hashable):
            obj = obj._hash_repr_()
        if isinstance(obj, (types.MethodType, type({}.pop))):
            # the Pickler cannot pickle instance methods; here we decompose
            # them into components that make them uniquely identifiable
            if hasattr(obj, "__func__"):
                func_name = obj.__func__.__name__
            else:
                func_name = obj.__name__
            inst = obj.__self__
            if type(inst) is type(pickle):
                obj = _MyHash(func_name, inst.__name__)
            elif inst is None:
                # type(None) or type(module) do not pickle
                obj = _MyHash(func_name, inst)
            else:
                cls = obj.__self__.__class__
                obj = _MyHash(func_name, inst, cls)
        Pickler.save(self, obj)

    def memoize(self, obj):
        # We want hashing to be sensitive to value instead of reference.
        # For example we want ['aa', 'aa'] and ['aa', 'aaZ'[:2]]
        # to hash to the same value and that's why we disable memoization
        # for strings
        if isinstance(obj, (bytes, str)):
            return
        Pickler.memoize(self, obj)

    # The dispatch table of the pickler is not accessible in Python
    # 3, as these lines are only bugware for IPython, we skip them.
    def save_global(self, obj, name=None, pack=struct.pack):
        # We have to override this method in order to deal with objects
        # defined interactively in IPython that are not injected in
        # __main__
        kwargs = {"name": name, "pack": pack}
        del kwargs["pack"]
        try:
            Pickler.save_global(self, obj, **kwargs)
        except pickle.PicklingError:
            Pickler.save_global(self, obj, **kwargs)
            module = getattr(obj, "__module__", None)
            if module == "__main__":
                my_name = name
                if my_name is None:
                    my_name = obj.__name__
                mod = sys.modules[module]
                if not hasattr(mod, my_name):
                    # IPython doesn't inject the variables define
                    # interactively in __main__
                    setattr(mod, my_name, obj)

    dispatch = Pickler.dispatch.copy()
    # builtin
    dispatch[type(len)] = save_global
    # type
    dispatch[type(object)] = save_global
    # classobj
    dispatch[type(Pickler)] = save_global
    # function
    dispatch[type(pickle.dump)] = save_global

    def _batch_setitems(self, items):
        # forces order of keys in dict to ensure consistent hash.
        try:
            # Trying first to compare dict assuming the type of keys is
            # consistent and orderable.
            # This fails on python 3 when keys are unorderable
            # but we keep it in a try as it's faster.
            Pickler._batch_setitems(self, iter(sorted(items)))
        except TypeError:
            # If keys are unorderable, sorting them using their hash. This is
            # slower but works in any case.
            Pickler._batch_setitems(
                self, iter(sorted((hash(k), v) for k, v in items))
            )

    def save_set(self, set_items):
        # forces order of items in Set to ensure consistent hash
        Pickler.save(self, _ConsistentSet(set_items))

    dispatch[type(set())] = save_set


class NumpyHasher(Hasher):
    """Special case the hasher for when numpy is loaded."""

    def __init__(self, coerce_mmap=False):
        """
        Parameters
        ----------
        hash_name: string
            The hash algorithm to be used
        coerce_mmap: boolean
            Make no difference between np.memmap and np.ndarray
            objects.
        """
        self.coerce_mmap = coerce_mmap
        Hasher.__init__(self)
        # delayed import of numpy, to avoid tight coupling
        import numpy as np  # noqa: PLC0415

        self.np = np
        if hasattr(np, "getbuffer"):
            self._getbuffer = np.getbuffer
        else:
            self._getbuffer = memoryview

    def save(self, obj):
        """Subclass the save method, to hash ndarray subclass, rather
        than pickling them. Off course, this is a total abuse of
        the Pickler class.
        """
        if isinstance(obj, self.np.ndarray) and not obj.dtype.hasobject:
            # Compute a hash of the object
            # The update function of the hash requires a c_contiguous buffer.
            if obj.shape == ():
                # 0d arrays need to be flattened because viewing them as bytes
                # raises a ValueError exception.
                obj_c_contiguous = obj.flatten()
            elif obj.flags.c_contiguous:
                obj_c_contiguous = obj
            elif obj.flags.f_contiguous:
                obj_c_contiguous = obj.T
            else:
                # Cater for non-single-segment arrays: this creates a
                # copy, and thus alleviates this issue.
                # XXX: There might be a more efficient way of doing this
                obj_c_contiguous = obj.flatten()

            # memoryview is not supported for some dtypes, e.g. datetime64, see
            # https://github.com/numpy/numpy/issues/4983. The
            # workaround is to view the array as bytes before
            # taking the memoryview.
            self._hash.update(
                self._getbuffer(obj_c_contiguous.view(self.np.uint8))
            )

            # We store the class, to be able to distinguish between
            # Objects with the same binary content, but different
            # classes.
            if self.coerce_mmap and isinstance(obj, self.np.memmap):
                # We don't make the difference between memmap and
                # normal ndarrays, to be able to reload previously
                # computed results with memmap.
                klass = self.np.ndarray
            else:
                klass = obj.__class__
            # We also return the dtype and the shape, to distinguish
            # different views on the same data with different dtypes.

            # The object will be pickled by the pickler hashed at the end.
            obj = (klass, ("HASHED", obj.dtype, obj.shape, obj.strides))
        elif isinstance(obj, self.np.dtype):
            # numpy.dtype consistent hashing is tricky to get right. This comes
            # from the fact that atomic np.dtype objects are interned:
            # ``np.dtype('f4') is np.dtype('f4')``. The situation is
            # complicated by the fact that this interning does not resist a
            # simple pickle.load/dump roundtrip:
            # ``pickle.loads(pickle.dumps(np.dtype('f4'))) is not
            # np.dtype('f4') Because pickle relies on memoization during
            # pickling, it is easy to
            # produce different hashes for seemingly identical objects, such as
            # ``[np.dtype('f4'), np.dtype('f4')]``
            # and ``[np.dtype('f4'), pickle.loads(pickle.dumps('f4'))]``.
            # To prevent memoization from interfering with hashing, we isolate
            # the serialization (and thus the pickle memoization) of each dtype
            # using each time a different ``pickle.dumps`` call unrelated to
            # the current Hasher instance.
            self._hash.update(b"_HASHED_DTYPE")
            self._hash.update(pickle.dumps(obj))
            return
        Hasher.save(self, obj)


def hash(*obj, coerce_mmap=False):
    """Quick calculation of a hash to identify uniquely Python objects
    containing numpy arrays.

    Parameters
    ----------
    obj: list of objects
        The objects to hash
    coerce_mmap: boolean
        Make no difference between np.memmap and np.ndarray
    """
    if "numpy" in sys.modules:
        hasher = NumpyHasher(coerce_mmap=coerce_mmap)
    else:
        hasher = Hasher()
    objr = ", ".join([
        o.__class__.__name__ if hasattr(o, "__class__") else "?" for o in obj
    ])
    logger.debug("Generating hash of objects of type(s): %s", objr)
    obj = _MyHash("==HashGroup==", *obj)
    return hasher.hash(obj)


class Wrapped(Generic[T]):
    def __init__(self, cls: Callable[P, T]):
        self.cls = cls

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        obj = self.cls(*args, **kwargs)

        def hash_repr():
            return _MyHash("==ObjectInitHash==", self.cls, args, kwargs)

        obj._hash_repr_ = hash_repr
        return obj

    def __getattr__(self, name: str) -> Any:
        return getattr(self.cls, name)


def init_based_hash(cls: Type[T]) -> Wrapped[T]:
    return Wrapped(cls)


def hashable(func: Callable[P, T]) -> Callable[P, T]:
    def hash_repr() -> Any:
        return _MyHash("==FunctionHash==", func)

    func._hash_repr_ = hash_repr

    return func
