## Binary I/O

A binary input or output file can be opened by using the `fopen` function with
a "b" mode to get an input or output port. The functions `read-byte` and
`write-byte` will read integer-valued bytes from such streams.

The function `write-data` can be used to write a packet to a file or output
stream. `(write-data packet stream-or-filename)` writes the bytes in a packet
directly to the output stream.

[DTypes](concepts.html#dtypes) can be written to binary output ports with the
function `write-dtype` and read with the function `read-dtype`.

An object's DTYPE representation can be written to a file with `write-dtype-
to-file`; a DTYPE representation for an object can be added to the end of a
file with the function `add-dtype-to-file`. These can be used together with
`read-dtype-from-file` to accumulate a set of objects in a file.

DTypes can also be written to packets with the function `write-dtype-to-
packet` and read from packets with the function `read-dtype-from-packet`. For
example,

    
    
          #|kno>|# (write-dtype-to-packet "foo")
          [#PACKET 8 0x0600000003666f6f]
          #|kno>|# (write-dtype-to-packet "föb")
          [#PACKET 9 0x400206006600f60062]
          #|kno>|# (write-dtype-to-packet 88)
          [#PACKET 5 0x0300000058]
        
    

Direct binary I/O is possible with four functions:

read-byte

    Reads a single byte from the stream as an integer between 0 and 255

Operating System Functions | FDScript provides a variety of functions for
interacting with the host operating system. These can be useful in the
construction of system utilities and in connecting systems of description to
the systems they are describing.  
---|---  
  
FDScript also provides a number of functions for accessing operating system
functions. These are useful for tracking resources, converting non-Kno
data into Kno data, and other operations.

