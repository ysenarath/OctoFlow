"""
This is a pure Python implementation of the rsync algorithm.

This code is based on https://code.activestate.com/recipes/577518-rsync-algorithm/.

Licensed under the MIT License.

[TM96] Andrew Tridgell and Paul Mackerras. The rsync algorithm.
Technical Report TR-CS-96-05, Canberra 0200 ACT, Australia, 1996.
http://samba.anu.edu.au/rsync/.

Example Use Case:
    On the system containing the file that needs to be patched:
    >>> unpatched = open("unpatched.file", "rb")
    >>> hashes = blockchecksums(unpatched)

    On the remote system after having received `hashes`:
    >>> patchedfile = open("patched.file", "rb")
    >>> delta = rsyncdelta(patchedfile, hashes)

    System with the unpatched file after receiving `delta`:
    >>> unpatched.seek(0)
    >>> save_to = open("locally-patched.file", "wb")
    >>> patchstream(unpatched, save_to, delta)
"""

import collections
import hashlib
from typing import IO, Deque, List, Tuple, Union

__all__ = [
    "rollingchecksum",
    "weakchecksum",
    "patchstream",
    "rsyncdelta",
    "blockchecksums",
]


def rsyncdelta(
    datastream: IO,
    remotesignatures: Tuple[List[int], List[str]],
    blocksize: int = 4096,
):
    """
    Generates a binary patch when supplied with the weak and strong
    hashes from an unpatched target and a readable stream for the
    up-to-date data. The blocksize must be the same as the value
    used to generate remotesignatures.
    """
    remote_weak, remote_strong = remotesignatures
    match = True
    matchblock = -1
    deltaqueue: Deque[int, Deque[int]] = collections.deque()
    while True:
        if match and datastream is not None:
            # Whenever there is a match or the loop is running for the first
            # time, populate the window using weakchecksum instead of rolling
            # through every single byte which takes at least twice as long.
            window: Deque[int] = collections.deque(
                bytes(datastream.read(blocksize))
            )
            checksum, a, b = weakchecksum(window)
        try:
            # If there are two identical weak checksums in a file, and the
            # matching strong hash does not occur at the first match, it will
            # be missed and the data sent over. May fix eventually, but this
            # problem arises very rarely.
            matchblock = remote_weak.index(checksum, matchblock + 1)
            stronghash = hashlib.sha256(bytes(window)).hexdigest()
            matchblock = remote_strong.index(stronghash, matchblock)
            # The weakchecksum matched, so the stronghash should match too
            match = True
            deltaqueue.append(matchblock)
            # Get the next byte and affix to the window
            if datastream.closed:
                break
            continue
        except ValueError:
            # The weakchecksum did not match
            match = False
            try:
                if datastream:
                    # Get the next byte and affix to the window
                    newbyte = ord(datastream.read(1))
                    window.append(newbyte)
            except TypeError:
                # No more data from the file; the window will slowly shrink.
                # newbyte needs to be zero from here on to keep the checksum
                # correct.
                newbyte = 0
                tailsize = datastream.tell() % blocksize
                datastream = None
            if datastream is None and len(window) <= tailsize:
                # The likelihood that any blocks will match after this is
                # nearly nil so call it quits.
                deltaqueue.append(window)
                break
            # Yank off the extra byte and calculate the new window checksum
            oldbyte = window.popleft()
            checksum, a, b = rollingchecksum(oldbyte, newbyte, a, b, blocksize)
            # Add the old byte the file delta. This is data that was not found
            # inside of a matching block so it needs to be sent to the target.
            try:
                deltaqueue[-1].append(oldbyte)
            except (AttributeError, IndexError):
                deltaqueue.append([oldbyte])
    # Return a delta that starts with the blocksize and converts all iterables
    # to bytes.
    deltastructure = [blocksize]
    for element in deltaqueue:
        if isinstance(element, int):
            deltastructure.append(element)
        elif element:
            deltastructure.append(bytes(element))
    return deltastructure


def blockchecksums(
    instream: IO,
    blocksize: int = 4096,
) -> Tuple[List[int], List[str]]:
    """
    Returns a list of weak and strong hashes for each block of the
    defined size for the given data stream.
    """
    weakhashes: List[int] = []
    stronghashes: List[str] = []
    read = instream.read(blocksize)
    while read:
        weakhashes.append(weakchecksum(bytes(read))[0])
        stronghashes.append(hashlib.sha256(read).hexdigest())
        read = instream.read(blocksize)
    return weakhashes, stronghashes


def patchstream(instream: IO, outstream: IO, delta) -> None:
    """
    Patches instream using the supplied delta and write the resultantant
    data to outstream.
    """
    blocksize = delta[0]
    for element in delta[1:]:
        if isinstance(element, int) and blocksize:
            instream.seek(element * blocksize)
            element = instream.read(blocksize)
        outstream.write(element)


def rollingchecksum(
    removed: int,
    new: int,
    a: int,
    b: int,
    blocksize: int = 4096,
) -> Tuple[int, int, int]:
    """
    Generates a new weak checksum when supplied with the internal state
    of the checksum calculation for the previous window, the removed
    byte, and the added byte.
    """
    a -= removed - new
    b -= removed * blocksize - a
    return (b << 16) | a, a, b


def weakchecksum(data: Union[bytes, Deque[int]]) -> Tuple[int, int, int]:
    """Generates a weak checksum from an iterable set of bytes."""
    a = b = 0
    dl = len(data)
    for i in range(dl):
        a += data[i]
        b += (dl - i) * data[i]
    return (b << 16) | a, a, b


if __name__ == "__main__":
    pass
    with open("README.md", "rb") as fp:
        hashes = blockchecksums(fp)
        delta = rsyncdelta(fp, hashes)
