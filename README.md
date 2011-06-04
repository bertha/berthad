BerthaD
=======

BerthaD is a simple, fast, no-nonsense TCP server to store blobs of data
by its SHA256 hash with just three operations:

1. __PUT__ – Stores the data on disk and return its hash.
2.  __LIST__ – Return the hashes of all blobs stored on this server.
3.  __GET__ – Given the hash of a blob it returns the content of the blob.

Features
--------
* __GET__s are fast.  They are implemented using Linux' `splice` syscall.
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
>>> list(c.list())
['a0869d836f643fae5d740ad4407c97e174d03169fd788fce690341d03f8d8f44']
>>> c.get('a0869d836f643fae5d740ad4407c97e174d03169fd788fce690341d03f8d8f44').read()
'Example blob'
```
See [py-bertha].

[py-bertha]: http://github.com/bwesterb/py-bertha
