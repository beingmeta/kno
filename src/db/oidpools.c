/* Layout of new DBPOOL files
   [256 bytes of header]
   [offset table]
   ...data items...

   Header consists of

   0x00 XXXX     Magic number
   0x04 XXXX     Pool information bits/flags
   0x08 XXXX     Base OID of pool (8 bytes
        XXXX
   0x10 XXXX     Capacity of pool
   0x14 XXXX     Load of pool
   0x18 XXXX     file offset of the schemas record (8 bytes)
        XXXX
   0x20 XXXX     file offset of the pool label (8 bytes)
        XXXX
   0x28 XXXX     file offset of pool metadata (8 bytes)
        XXXX
   0x30 XXXX     pool creation time_t (8 bytes)
        XXXX
   0x38 XXXX     pool repack time_t (8 bytes)
        XXXX
   0x40 XXXX     pool modification time_t (8 bytes)
        XXXX
   0x48 XXXX     repack generation (8 bytes)
        XXXX

   The flags are divided as follows:
     MASK        INTERPRETATION
     0x0003      Structure of offsets table:
                    0= 4 bytes of position followed by 4 bytes of length (32B form)
                    1= 5 bytes of position followed by 3 bytes of length (40B form)
                    2= 8 bytes of position followed by 4 bytes of length (64B form)
                    3= reserved for future use
     0x001c      Compression function for blocks:
                    0= no compression
		    1= libz compression
		    2= libbz2 compression
                    3-7 reserved for future use
     0x0020      Set if this pool is intended to be read-only

   The offsets block starts at 0x100 and goes for either capacity*8 or capacity*16
    bytes.  The offset values are stored as pairs of big-endian binary representations.
    For the 32B and 64B forms, these are just straightforward integers of the same size.
    For the 40B form, which is designed to better use memory and cache, the high 8
    bits of the second word are taken as the high eight bits of a forty-byte offset.

   

*/
