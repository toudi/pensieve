## Pensieve

This is a project I came up with while serializing data into time-based database. While there are dedicated solutions for dealing with this, I wanted to create an abstraction layer on top of that so that I could change the backend.

There are couple of key concepts:

### Dimensions

Defines a field which you can filter by. In vast majority of examples I found on the internet, a database would
record stock value and would use the 3 letter company code for the dimension (which is also referred to as ticker symbol)

### Backends

The high-level API is always the same: you initialize the storage and you tell it to persist some points. It is up to the backend to convert pydantic's structure into the underlying structure. In the filesystem backend I wrote pydantic's structs are serialized via python's `struct` module into strams of bytes. however for aws timestream backend, the `boto` library is used to reach the database via API.

In order to see the implementations of the backends, refer to `backends` directory
