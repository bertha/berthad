BerthaD
=======

BerthaD is a simple, fast, no-nonsense TCP server to store blobs of data
by its SHA256 hash with just three operations:

1. __PUT__ – Stores the data on disk and return its hash.
2.  __LIST__ – Return the hashes of all blobs stored on this server.
3.  __GET__ – Given the hash of a blob it returns the content of the blob.

Features
--------
* __GET__s are fast.  They are implemented using Linux' `splice` and FreeBSD's
  `sendfile` syscall.
* Small codebase.
* No authentication.  No SSL.  If you don't need them, they are only
  an overhead.

Clients
-------
### Python
```python
>>> from bertha import BerthaClient
>>> c = BerthaClient('localhost', 1234)
>>> list(c.list())
[]
>>> c.put_str("Example blob")
'a0869d836f643fae5d740ad4407c97e174d03169fd788fce690341d03f8d8f44'
>>> list(c.list())
['a0869d836f643fae5d740ad4407c97e174d03169fd788fce690341d03f8d8f44']
>>> c.get('a0869d836f643fae5d740ad4407c97e174d03169fd788fce690341d03f8d8f44').read()
'Example blob'
```
See [py-bertha].

Building
--------

```
$ cd release
$ ./build
```

Running
-------
    Usage: berthad-vfs <bound host> <port> <data dir> <tmp dir>

* `bound host` is address or name of the address to be bound.  For instance:
  `localhost` or `0.0.0.0`.
* `port` is the port on which to bind.  Clients default to 819.
* `data dir` is the directory which will contain the blobs.  It must exist.
* `tmp dir` is the directory which will contain the blobs while they are
   streamed to disk during a __PUT__.  The directory must be on the same
   mounting point as `data dir`.

Protocol
--------
The protocol is described in [PROTOCOL.md].

[py-bertha]: http://github.com/bertha/py-bertha
[PROTOCOL.md]: https://github.com/bertha/berthad/blob/master/PROTOCOL.md
