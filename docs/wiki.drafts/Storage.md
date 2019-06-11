Persistent storage in Kno is organized around three key concepts: OIDs, pools, and indices

* OIDs are persistent object identifiers;
* Pools are tables which map a range of OIDs into individual lisp objects;
* Indexes are tables which map single lisp value into a set of other lisp values.

OIDs are currently all 64-bit integers which are allocated serially
within particular ranges or domains, called *pools*.

The term *pool* is used interchangeably for ranges of OIDs and the
persistent storage units they may arrive in. Pools are generally used
for storing a description of some domain or application object,
typically as a *slotmap*.

Indexes are external persistent hashtables which map singleton
(non-choice) lisp objects (called keys) into a set of other objects
(called values). The intention is that adding a key/value pair is
inexpensive and that the mappings of keys to values is
accumulative. In particular, dropping of values (whether explicit or
by setting the mapping wholesale) is rare.

Indexes are especially used to support *search* in the object
database, where the keys indicate features of the object and values
are generally OIDs. In this scenario, the keys are typically PAIRs of
a slotid (usually a symbol or OID) and a value (singleton). Keys can
be either **literal** meaning they describe an actual value stored on
a slot of an OID or **virtual** meaning they describe values derived
for the slot. For example, when indexing a slot which contains
strings, the **literal** keys are the actual string values on the slot
which are compared by string equality, including sensitivity to
capitalization and whitespace (though unicode normalization is mostly
handled). In parallel, the slot may be indexed with virtual keys,
where the values are transformed by:
* case normalization (downcasing them)
* whitespace standardization (turning whitepspace runs into single spaces)
* soundex or metaphone coding (which catches common variations and misspellings)
* or others.

Pools and indexes are implemented by **drivers** which manage their
persistent storage, either to external databases, the local file
system, or networked pool/index servers. Both pools and indexes
implement a *commit cycle* where all changes are local until the pool
or index is explicitly committed. Drivers try to be as ACIDic as
possible and there isn't support for either rollbacks to past commits
or nested commits.
