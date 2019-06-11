Kno has a variety of built-in types and pre-allocated types. **Built-in** types are types which are implemented in the core implementation; pre-allocated types are types which have their type codes baked into the core header files (so that they're constants) but defined elsewhere (often in 'cmodules').

As described in [Kno Internals], lispvals have a manifest type (explicit in the pointer) of cons, immediate, fixnum, or OID. We'll describe the fixnum and OID types and then enumerate the built-in immediate and cons types.

## Manifest types

**Fixnums** are small(ish) signed integer values which use 30 or 62 bits of the lisp value to represent an integer. ints (or longs) are converted into a fixnum by:

1. stripping the sign (making it positive)
2. shifting it left 2 bits (or multiplying by 4)
3. restoring the sign (if stripped)
4. ORing in the manifest type (0x03)

Fixnums can be compared for magnitude using integer comparison (or checked for being negative). The headers provide `KNO_MAX_FIXNUM` and `KNO_MIN_FIXUM` together with `KNO_FIXNUM_BITS` (usually 30 or 62).

The macros `KNO_FIX2INT` and `KNO_FIX2UINT` return the integer value of a fixnum as either a signed or unsigned value. Unless the magnitude is known in advance, it's safest to put the results in a `long long` or `kno_long` lvalue. The macro `KNO_INT2FIX` returns a fixnum from an integral value, truncating to `KNO_MAX_FIXNUM` or `KNO_MIN_FIXNUM` as required.

There are also macros of the form `KNO_*ctype*P` (for example `KNO_USHORTP`) which check whether a fixnum value is small enough to fit in an *lvalue* of *type* where *type* can be `int`, `uint`, `long`, `short`, `ushort`, `char`, `uchar`, and (for good measure) `byte`. Note that when Kno types or names refer to **long** they are referring to 64-bit values regardless of what the platforms native `long type` may be. The type `kno_long` is typedef'd to whatever that type may be.

Kno also has a built-in arbitrary precision integer type, **bigints** which can be used for larger values. The macros `KNO_MAKEINT(*cval*)` and `KNO_GETINT(*lispval*)` convert to and from fixnums and bigints automatically. `KNO_GETINT(*lispval*)` does not do type-checking on its argument, so it's wise to call `KNO_INTEGERP` before using it.

## OIDs

**OID**s are object identifiers which represent 64-bit object ids as
  described in [OIDs and pools]. OIDs represent unique objects
  typically stored in external databases. Kno provides an KNO_OID type
  which represents the 64 bit address itself and an alternative
  *lispval* representation. Generally, the code refers to the KNO_OID
  value as the "address" and the lispval itself as the OID.

OIDs use a bucketed representation where each OID lispval consists of
a bucket ID and a bucket offset. Typically, though this can be
configured, the bucket offset is between 0x00 and 0x100000 (a little
over 1,000,000) and the bucket ID is between 0x00 and either 0x10000
(about 64,000) or 0x400 (about 1,000) depending on whether the target
architecture is 64-bit or 32-bit.
