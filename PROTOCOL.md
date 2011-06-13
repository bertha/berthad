The Bertha protocol
===================

The Bertha server stores blobs of data.
Blobs of data are accessed by their SHA-256 hashes: 
the __key__ of a blob of data is its SHA-256 hash.

Commands
--------

After the TCP connection is established, the client sends a
single octet.  This is the __command byte__ and is one of
       
- __0__ for __LIST__, lists the keys of the blobs
- __1__ for __PUT__, adds a blob
- __2__ for __GET__, retrieves a blob by its key
- __3__ for __QUIT__, quits the server
- __4__ for __SPUT__, adds a blob and give a hint of its size
- __5__ for __SGET__, retrieves a blob and its size by its key
- __6__ for __SIZE__, retreives the size of a blob by its key

### LIST
Used to list all the keys.

1.  The server sends the keys of all blobs in a unspecified order.
    The keys are send as 32 byte binary words.
2.  The server closes the connection.

### PUT
Used to add a new blob to the server.

1.  The client sends the blob to the server.
2.  The client shuts its socket down for writing.
3.  The server sends the key of the added blob.
4.  The server closes the connection.

### GET
Used to retrieve a blob by its key

1.  The client sends the key to server.
2.  The server sends the blob if it exists.  Otherwise the server closes
    the connection.
3.  The server closes the connection.

### QUIT
Used to quit the server.

1.  The server closes the connection and shuts itself down.

### SPUT
Similar to __PUT__.  The client additionally sends the probable size of the
blob.  This allows the server to pre-allocate room for the blob.

1.  The client sends the probable size of the blob to the server as a
    little endian 64 bit unsigned integer.
2.  The client sends the blob to the server.
3.  The client shuts its socket down for writing.
4.  The server sends the key of the added blob.
5.  The server closes the connection.

### SGET
Similar to __GET__.  The server additionally sends the size of the blob.

1.  The client sends the key to server.
2.  The server sends the size of the blob as a little endian 64 bit unsigned
    integer, if the blob exists.  Otherwise the server closes the
    connection.
3.  The server sends the blob.
4.  The server closes the connection.

### SIZE
Used to retrieve the size of a blob by its key. Also useful to check whether
a blob exists without having to execute LIST.

1.  The client sends the key to server.
2.  The server sends the size of the blob, if it exists.
3.  The server closes the connection.
