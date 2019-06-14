# Kno Concepts

Quick ref: [OIDs and the Object Database]  [Persistent Indices]  [Frames in
Kno]  [Searching for Frames]  [The DTYPE Protocol]

* * *

Architecture | Kno has a layered architecture  
---|---  
  

## OIDs and the object database

Kno provides a simple object database which associates numeric
**object identifers** (OIDs) with DType structures. DType structures
can contain these object identifiers so that one DType structure can
point to the location in the object database where another DType
structure
is stored.
  
One of the major components of Kno is a distributed persistent object
database. Objects in the database are designated by unique object identifiers
or OIDs. The allocation of OIDs is managed so that different users, projects,
or programs will not use the same OID for different purposes.

An OID is a 64-bit address pointing into a virtual memory space whose
locations are associated with complex structured objects. These objects can
also contain pointers to other OIDs. The idea is that one program can
associate an OID with a particular value (for instance, the integer `3` or the
list `("Three" THREE 3)`). Other programs can retrieve this value (or whatever
value replaces it) by just knowing the OID to which it was assigned. OIDs can
be included in other structures, written to files, and accessed through
indices, so that application users need not typically worry about the exact
numerical value of an OID.

OID's numeric IDs are further organized into continuous ranges called
**pools**. Pools are used to organize the storage and management of the values
associated with OIDs. There are currently two kinds of pools: file pools are
disk files containing DTYPE representations for the values associated with a
particular range of OIDs; network pools are network servers using the DTYPE
protocol which likewise provide access to a particular range of OIDs.

## OID values

OIDs are associated with values which applications may access or modify. When
an application makes changes to an OID's value, those changes are local to the
application until they are **committed** , at which point they are saved to
the appropriate file or network pool. When this occurs, the new value will be
visible to other applications which request it.

Access and revision control is managed by locking particular OIDs. When an
application locks an OID, two things are guaranteed: that the value which the
application sees for the OID is the latest value; and that the value will not
be changed by any other application until either the OID is committed or it is
unlocked. Locking of OIDs is normally invisible to applications. When an
application sets an OID's value locally, it locks it. When the value is
committed, it unlocks it.

For file pools, locking any OID in the pool locks the entire pool; for network
pools, OIDs are locked on an individual basis.

Pools reflect both implementational and administrative properties: the values
associated with the OIDs in a particular pool are generally provided by a
particular file or server; the allocation of new OIDs and the modification of
the value for existing OIDs is generally the responsibility of one
administrative entity.

## External Representations

OIDs have two different external representations. A literal OID reference has
the form @hi/lo where hi is the high 32 bits of the address and lo is the low
32 bits, both in hex. For example, an OID that looks like this `@1/1b54e`
refers to the address `0x000000010001b54e`. A logical OID reference has the
form `@/ _pool-id_ / _offset_` and indicates an OID in a particular pool.
Thus, the reference ` @/brico/1b54e` refers to the same object shown above if
the pool `brico` starts at `@1/0`. In addition, when the system can figure out
a name for an OID, it appears after the @-expression identifying its address,
e.g.:
```
@/brico/1b54e"NOUN.COGNITION synset for example, illustration, instance, and representative"
```
to make it less cryptic to the user.

The high 32 bits of an object identifer indicate the super pool of the OID.
The OID above, for instance, is in the super pool `0x1`. The allocation of new
object identifiers is based on the division of the 64 bit address space into
smaller **pools**. The first such division is into super pools and subsequent
divisions divide super pools into smaller chunks. Each chunk consists of 2 i
object identifiers.

## Getting Pools, OIDs, and their values

> _The Lisp programmer knows the value of everything but the cost of nothing._  
>  Alan Perlis

The Kno procedure `use-pool` arranges for the current session to access a
particular pool of OIDs. It's single argument is either a filename or a
network specification of the sort used for remote evaluation (e.g.
`brico@framerd.org'). Once `use-pool` has been called, the value of an OID in
the pool can be extracted by the procedure `OID-VALUE` e.g.
```

          #|fdconsole>|# **(USE-POOL "brico@framerd.org")**
          [11:57:27 Session id=Kno haase@eliza.media.mit.edu /OS:DIGITAL UNIX /Compiled:Feb 10 1997 /Started:Tue Feb 11 11:57:27 1997]
          [11:57:27 Added pool brico@framerd.org]
          [#POOL brico@framerd.org @1/0+1048576 {}]
          #|fdconsole>|# **(OID-VALUE @1/33334)**
          [SPELLING: "cause_to_tip" %ID: "cause_to_tip" 
          SENSES: @/brico/32f96"VERB.MOTION synset for tip, cause_to_tip, and cause_to_tilt"
          RANKED-SENSES:
          (V 1 @/brico/32f96"VERB.MOTION synset for tip, cause_to_tip, and cause_to_tilt")]

```
        

One can use the procedure `random-oid` to pick a random OID from a pool, e.g.

    
    
          #|fdconsole>|# **(RANDOM-OID (USE-POOL "brico@framerd.org"))**
          @/brico/58e1"ADJ.ALL synset for functional"
          #|fdconsole>|# **(RANDOM-OID (USE-POOL "brico@framerd.org"))**
          @/brico/21a5e"alpha_particle"
          
        

Pools can be stored in variables and used as arguments, e.g.

    
    
          #|fdconsole>|# **(define WORDNET (USE-POOL "brico@framerd.org"))**
          #|fdconsole>|# **(RANDOM-OID wordnet)**
          @/brico/148d3"NOUN.ARTIFACT synset for stoup and stoop"
          
        

Also, procedures which expect pools as arguments (like `ALLOCATE-OID`) will
also take strings and automatically interpret them as pool specifications),
for example:

    
    
          #|fdconsole>|# **(RANDOM-OID "brico@framerd.org")**
          @/brico/1d9a7"NOUN.COMMUNICATION synset for hillbilly_music"
          
        

## Allocating and changing OIDs

The procedure `ALLOCATE-OID` returns an OID from a pool; pools can be either
stored in files on disk or servers on a network. So we can say:

    
    
          #|fdconsole>|# **(ALLOCATE-OID (USE-POOL "temp@framerd.org"))**
          [11:59:57 Added pool temp@framerd.org]
          @/temp/40001
          
        

to allocate an OID in the network pool `"temp@framerd.org"`. (This pool is
maintained for demo purposes at the Media Laboratory. Values stored in this
pool are subject to erasure whenever our disk space gets tight). If we call
ALLOCATE-OID again, we get a different oid:

    
    
          #|fdconsole>|# **(ALLOCATE-OID (USE-POOL "temp@framerd.org"))**
          @/temp/40002
          
        

When an OID is newly allocated it has no values assigned to it, E.G. a call to
`OID-VALUE`

    
    
          #|fdconsole>|# **(OID-VALUE @/temp/40001)**
          {}
          
        

fails by returning no results. One can set the value of an OID with the `SET-
OID-VALUE!` procedure:

    
    
          (SET-OID-VALUE! @/temp/40001 "My first OID")
          
        

allowing us to get the value we put there:

    
    
          #|fdconsole>|# **(OID-VALUE @a/40001)**
          "My first OID"
          
        

## Saving and Reverting OID values

When you modify the value associated with an OID, the value is only changed
locally until the changes are _committed_ to the pool containing the object.
There are three basic `commitment' procedures for OIDs and pools:

**`(COMMIT-OID _oid_ )`**

    saves the changes associated with a particular OID.
**`(COMMIT-POOL _pool_ )`**

    saves all the changes to a particular pool;
**`(COMMIT-POOLS)`**

     saves all the changes to OIDs in the current session, and

**All changes are automatically committed if your Kno session exits
normally (and lost if it exits abnormally).**

Not all OIDs can be changed or modified. If you change an OID in a file pool,
the system attempts to lock the corresponding file before making the change.
The file stays locked until your process exits or the file is explicitly
unlocked. If you cannot lock the file --- because someone else has locked it
or the file is read-only --- you cannot change the value associated with the
OID stored in the file.

If you change an OID in a network pool, the system attempts to lock the
individual OID before making the change. If this succeeds, the local value for
the OID is changed and the remote OID will be unlocked when your changes are
committed. If this fails, (for instance, the network server will not permit
you to lock the OID), the OID cannot be modified. A server might refuse to
lock an OID because another user has locked the OID or because the OID is
declared read-only.

Before you've committed your changes, it is possible to back out of the
changes by _reverting_ the modifications:

**`(REVERT-OID _oid_ )`**

    undoes your changes to the individual OID _oid_.
**`(REVERT-POOL _pool_ )`**

    undoes all the changes to OIDs in a particular pool
**`(REVERT-POOLS)`**

     undoes all the changes to OIDs in the current session that have not yet been committed.

Two caveats: remember that committed changes cannot be reverted in this way;
also, only changes that directly affect the OID or its value can be reverted.
If you store a pointer to an OID in a hashtable, for instance, reverting the
OID will not remove the hash table entry.

## Making file pools

The frames created above were allocated and stored on a demonstration server
at MIT. Though the values might stay around for a few days, they are unlikely
to last longer for administrative reasons. To create objects with more
presistence, one has to either use **file pools** maintained in files on the
local file system or use **network pools** maintained by oneself or others
with more persistence. Actually, the network servers you would use will rely
on pools maintained in files on their local file system, so eventually someone
has to worry about how OIDs live on disk in file pools. Fortunately, dealing
with file pools is straightforward.

The procedure `MAKE-FILE-POOL` creates a file pool in the local file system,
E.G.

    
    
          #|fdconsole>|# **(MAKE-FILE-POOL "test.pool" 32)**
          [#POOL test.pool @a/140000+0/32 {}]
          
        

creates a file pool containing 32 possible and no actual objects and and whose
first object will be @a/140000. We can allocate that first object with
`allocate-oid`:

    
    
          #|fdconsole>|# **(allocate-oid "test.pool")**
          @/test/0 ; _or @a/140000 if printed "literally"_
          
        

Creating a pool automatically causes the pool to be _used_ , just as if you
had called `use-pool` on the argument.

The command-line program `make-file-pool` also creates file pools; it looks
just like the Kno call, but without the parentheses, e.g.

    
    
          sh% make-file-pool test.pool 32
          Created test.pool with space for 32 OIDs starting at @a/100
          
        

This command line program is written in and invokes Kno, but it doesn't
require that the user deal with the listener and associated parentheses. It
also generates a message describing what it has done.

Frames in Kno | Frames are at the heart of Kno, providing a way to
build complex extensible descriptions and to share pointers to these
descriptions between different databases and applications.  
---|---  
  
The word "frame" in Kno comes from its support for frames, a popular AI
representation model. A frame in Kno is an OID whose value consists of a
set of slots. Each slot consists of a slotid \--- either a symbol or an OID
--- and a set of values. Slots divide the description of objects into several
labelled parts. For instance, the following frame from the Kno port of
WordNet represents the concept (synset) of an "example":

    
    
            SYNSET-ID:  3946824 **[[[ _Slot values may be numbers or strings._ ]]]**
            SYNSET-TYPE:  "n"
            WORDS: (4 values) **[[[ _A slot may have any number of values_ ]]]**
            "representative"
            "instance"
            "illustration"
            "example"
            DESCRIPTION:  "a single item that is representative of a type"
            %ID:  "NOUN.COGNITION synset for example, illustration, instance, and representative"
            SENSE:  NOUN.COGNITION
            HYPERNYM:  @/brico/b709"NOUN.COGNITION synset for information"
            HYPONYM: (5 values) **[[[ _The values can also be pointers to other frames_ ]]]**
            @/brico/1b56e"NOUN.COGNITION synset for exception"
            @/brico/1b56f"NOUN.COGNITION synset for precedent and case_in_point"
            @/brico/1b570"NOUN.COGNITION synset for quintessence"
            @/brico/1b571"NOUN.COGNITION synset for sample"
            @/brico/1b572"NOUN.COGNITION synset for specimen"
            A:  @/brico/b709"NOUN.COGNITION synset for information"
            SYNSET-DEPTH:  4
            SYNSET-HEIGHT:  3
            SYNSET-TOTAL-HYPONYMS:  8
            SYNSET-TOTAL-BRANCHING:  2
            
          

The description above lists a number of slots, each of which associates a
particular slotid (e.g. `HYPERNYM`) with a set of slot values (which are
abitrary objects). This set of associations is called a `slotmap`. A frame is
an OID whose value in the object database is a slotmap. Using OIDs allows
different programs and databases to keep separate references to a frame while
managing to share changes and augmentations.

Operations on slots include getting their value(s), adding a value, removing a
value, and checking whether they contain a particular value. The value may be
non-deterministic (a choice), in which case we may refer to the _values_ of
the slot.

When the slotid is a symbol, operations on the value of the slot are just
operations on the slot's data. Getting the slot's value retrieves the slot
data and adding a value to a slot adds an element to the slot's data (which is
represented as a non-deterministic value). Except for this last point, frames
whose slotids are symbols function much as property lists of symbols in
languages like LISP or associative arrays in Perl. (From a computational
standpoint, slotmaps are not a very efficient way to store large numbers of
associations. For that, it is better to use a hashtable or an external index.)

When the slotid is itself a frame (i.e. an OID whose value is a slotmap),
operations on the slot are more complicated. Instead of just operating on the
slot's data, operations evaluate methods for computing or testing for values,
adding or removing elements, or checking that the given frames or values are
correct. The methods are expressions in Kno, the Kno scripting
language, which can access the variables frame, slotid, data, and value which
contain the details of the particular slot and (when appropriate) the value
being added, removed, or tested for. The expressions are stored in the
following slots of the slotid:

  * `GET-METHODS` evaluated to get the value of the slot
  * `TEST-METHODS` evaluated to determine whether or not the slot has a particular value; the slot "has a value" whenever any of these methods returnst true.
  * `VALIDATE-METHODS` evaluated whenever a value is added to a slot; an error is signalled (and the value is not added) whenever any of these methods returns false.
  * `ADD-EFFECTS` evaluated whenever a new value is added to a slot
  * `REMOVE-EFFECTS` evaluated whenever a value is removed from a slot

Methods can operate on slots of the same or other frames and normally this
simply invokes the corresponding methods, except when this is likely to recur
infinitely. This can occur when the inference relations between slots are
implicitly circular. For example, slots describing the width, height, and area
of a rectangle might be defined in terms of one another. However, this could
lead to an infinite recursion if, for instance, by the following path:

  * To know the area of X, figure out the width and height of X
  * To know the width of X, figure out the area and height of X
  * To know the area of X, figure out the width and height of X....

To avoid this recursion, operations on a complex slots always check whether an
identical operation (getting the same slot, adding, removing, or testing for
the same value) is already being performed. If it is already being performed,
the recursive operation either:

  * returns the empty set (for getting a value)
  * returns false (for testing for a value)
  * does nothing (for adding or removing a value)

This allows slots to freely refer to one another without concern for infinite
recursion.

## Getting at Frames from Kno

The procedure `FRAME-CREATE` creates a frame and requires specifying a pool in
which the frame's OID will be allocated. The value returned by `FRAME-CREATE`
is this OID. If the pool argument is `#f` (false), a raw slotmap (rather than
an OID whose value is a slotmap) is returned. All of the frame procedures will
take a raw slotmap as an argument, but the unique identity of the slotmap will
not be preserved between Kno sessions.

This is a subtle but important point. If you save a raw slotmap, subsequent
changes to the slotmap will not be shared. If you save a frame (an OID whose
value is a slotmap), subsequent changes will be changed. In general, it is
important to use frames for descriptions which may persist beyond the current
session.

In addition to the pool argument, `frame-create` takes a number of other
arguments. If it is given one argument, it should be a slotmap to which the
corresponding OID's value is initialized. If it is given more than one
argument, they are intepreted as a series of slots and values initially
assigned to the frame, e.g.

    
    
            #|fdconsole>|# (frame-create "temp@framerd.org"
            '%id "test" 'test-slot 'test-value
            'another-test-slot "another-value")
            @a/40010"test" ;<- Prints out using the %id slot
            
          

the slot `%ID` is used by Kno when displaying the frame's OID, as
you can see in the printed value above.

The procedure `GET` extracts a slot from a frame given a slotid, E.G.

    
    
            #|fdconsole>|# **(get @a/40010"test" 'test-slot)**
            TEST-VALUE
            
          

When the slotid is a symbol, `GET` simply retrieves whatever was initially or
subsequently stored in the slot. The rules are different when the slotid is
itself a frame, when certain inferences may happen, but we discuss that below.

The procedure `ASSERT!` adds a new value to a frame for a given slotid, E.G.

    
    
            #|fdconsole>|# **(assert! @a/40010"test" 'test-slot 'another-value)**
            #|fdconsole>|# **(get @a/40010"test" 'test-slot)**
            {TEST-VALUE ANOTHER-VALUE}
            
          

Since frames are OIDs, changes to frames are not permanent until either the
session exits normally or the changes are explicitly committed. The procedures
`COMMIT-OID`, `COMMIT-POOL`, and `COMMIT-POOLS` all work to save changes to
frames and the additional procedure `COMMIT-FRAME` is just another name for
`COMMIT-OID`.

The procedure `FRAME-SLOTS` returns all the slotids associated with a frame,
E.G.

    
    
            #|fdconsole>|# (frame-slots @a/40010)
            {%id test-slot another-test-slot}
            
          

For example, The following code converts a frame into an association list
(i.e. a list of key value pairs):

    
    
            #|fdconsole>|# (define (get-alist-entry unit slotid)
            (cons slotid (set->list (frame-get unit slotid))))
            #|fdconsole>|# (define (frame->alist frame)
            (set->list (get-alist-entry frame (frame-slots frame))))
            (frame->alist @a/40010)
            ((ANOTHER-TEST-SLOT "another-value") 
            (TEST-SLOT ANOTHER-VALUE TEST-VALUE)
            (%ID "test"))
            
          

## Inferences over slots

When a slotid is another frame, procedures like `get` and `assert!` act a
little differently based on the slots of that frame. Each operation has a
special slot associated with it and that slot contains a set of Kno
expressions which may be evaluated when the operation is performed:

OPERATION | SLOT | VARIABLES BOUND  
---|---|---  
get | GET-METHODS | `**frame, slotid, data**`  
test | TEST-METHODS | **`frame, slotid, data, value`**  
assert! | ADD-EFFECTS | **`frame, slotid, data, value`**  
drop! | DROP-EFFECTS | **`frame, slotid, data, value`**  
  
when these values are evaluated, certain variables are bound:

`frame`

    is bound to the frame being operated upon
`slotid`

    is bound to the slot being operated upon
`data`

    is bound to primitive values associated with the slotid
`value`

    is bound to the value being tested for, added, or removed

allowing the procedure to compute extra values, make additional changes, or
notify users of changes. For instance, the following definition..

    
    
            #|fdconsole>|# (frame-create test-pool 
            '%id "n-hyponyms"
            'get-methods (choice-size (GET frame 'hyponyms)))
            @1/3338"n-hyponyms"
            
          

so that we can ask:

    
    
            #|fdconsole>|# (get @/brico/1b54e @/brico/3338"n-hyponyms")
            5
            
        

Persistent Indices | Indices are a way of storing and manipulating
associations between objects and descriptions. They can be used to go from
words to their roots (e.g. "flew" to "fly") or from features to findings (e.g.
"BROTHER-OF Ken" to "Bruce"). Kno's indices are persistent **incremental**
data structures designed to support millions of keys.  
---|---  
  
## Indices are persistent

The mappings described by an index are maintained outside of the application
using the index. An application can change an index and those changes will be
automatically available to later instantiations of the application or even of
other applications. Changes are not visible at once, however; an application
must explicitly _commit_ its changes to make them persistent and shared.

Because indices are persistent, they are typically used as resources by
programs and may represent the accumulation of results from many computations
and many different processes. A suite of applications may have a set of
indices which they share and update.

## Indices are demand-driven

When an application uses an index, it normally only uses resources for a
fraction of the index. The rest of the index is maintained --- on a remote
server or in the file system \--- for when it is needed. When neccessary, it
goes to the server or file and retrieves the needed mappings. A quite small
application can access a huge index through this method.

Kno has two sorts of built-in indices. File indices store mappings in a
file on a local or remote disk and retrieve mappings by random access in the
file. Network indices use a simple protocol to access DType servers which
provide mappings to their clients. These two types are interchangable.

## Indices are incremental

Indices are designed to efficiently support incremental changes. In
particular, it is supposed to be efficient to both add new values to existing
keys and to add initial values to new keys. This makes index files useful for
storing _inverted indices_ used in many information retrieval applications. It
is also easy to get the _number_ of values associated with a particular key,
which is important for certain kinds of statistical retrieval algorithms.

Indices also cache values in two directions. When an index goes to a file or
server to get a mapping, it caches the result locally so that a subsequent
request can proceed much faster. For this reason, some Kno applications
may speed up over time as they cache commonly referenced index keys.

When an index is modified, the modifications are stored locally with the
application making the modifications. Only when the index is _committed_ do
the changes go into the files on disk or across the network.

## Indices can index objects

Indices are the basis of a general object indexing facility. This uses the
convention that keys of the form: `(slot . value)` refer to frames whose
`slot` contains `value`. For example, the key `(year-of-birth . 1961)` would
be associated with all the frames whose `year-of-birth` slot was `1961`. These
indices can be used to find frames with particular properties or combinations
of properties as well as for more flexible "fuzzy" searches.

## Using indices from Kno

Indices are accessed by the function **`USE-INDEX`** :

    
    
            #|fdconsole>|# **(USE-INDEX "/local/test-index")**
            [#INDEX "/local/test-index"]
            
          

which can also take a network server specification, as in:

    
    
            #|fdconsole>|# **(USE-INDEX "testi@somehost")**
            [#INDEX "testi@somehost"]
            
          

The functions for accessing an index are just like the functions for accessing
a hashtable:

**`(INDEX-GET index key)`**

    gets the values associated with key in index.
**`(INDEX-ADD! index key new)`**

    adds new to the values associated with key in index.
**`(INDEX-SET! index key new)`**

    makes new be the only value associated with key in index
**`(INDEX-DROP! index key)`**

    ensures that no values will be associated with key in index.
**`(INDEX-KEYS index)`**

    returns all of the keys stored in an index.

Changes to indices are like changes to pools in that they need to be
**committed** to be permanent. Changes are committed automatically if the
Kno session exits normally. Changes can also be committed manually:

**`(COMMIT-INDEX index)`**

    saves the changes made all the keys in index
**`(COMMIT-INDICES)`**

     saves the changes made to all the keys in all of the indices

As with pools, changes to indices can be reverted:

**`(REVERT-INDEX index)`**

    removes all the changes made to keys in index.
**`(REVERT-INDICES)`**

     removes all the changes made to keys in all indices.

As with pools, changes cannot be reverted once they are committed and
reversion applies only to the associations in the index and not to other
relations involving the keys or values.

## File indices

There are a number of special functions for dealing with file indices.

**`(make-file-index filename minsize)`**

    Creates a new file index in the file filename that contains at least minsize slots. Note that file indices have a fixed size and need to be explicitly grown using maintenance tools built into Kno or available from the command prompt. This is because growing a file index with millions of keys can be very time consuming and couldn't be "transparent" even if you wanted it to be. 
**`(cache-index index)`**

    When index is a file index, this keeps a copy of the files offset table in memory, which improves access time by avoiding one disk seek. This is especially useful when disk seeks are particularly slow (for instance with CD-ROM drives).
**`(auto-cache-file-indices)`**

     Arranges for file indices to be automatically cached when referenced. This is a good idea for programs which do a lot of index accesses or for speed when the index file is on a slow device (such as a CD-ROM or network disk).

In addition, there are several command line utilities for dealing with file
indices:

`make-file-index filename minsize`

    Creates a file index with (at least) a particular size.
`analyze-index filename --keys | --values | --stats`

    Returns information about the index file in filename. The options can be combined and have the following interpretation: 

  * `--stats` reports hashtable statistics (number of keys, average and max misses per key, most common keys, etc)
  * `--keys` lists all of the keys with the number of values for each
  * `--values` lists all of the values

With no arguments, `analyze-index` simply prints the number of slots in the
index.

File indices have standard sizes at roughly powers of 2 and user specified
sizes are rounded up to the nearest standard size.

Searching for Frames | Kno builds an object indexing facility on top of
the general association facility described above. Indexing a frame stores
inverse pointers from its properties to the frame. This allows the frame to be
found later based on a set of properties or to find objects similar (i.e. with
common properties) to one particular frame.  
---|---  
  
Kno builds an object indexing facility on top of the general association
facility described above. Indexing a frame stores inverse pointers from its
properties to the frame. This allows the frame to be found later based on a
set of properties or to find objects similar (i.e. with common properties) to
one particular frame.

There are three main ways of indexing frames:

**`(index-frame _index_ _frame_ )`**

    Indexes all the properties actually stored on _frame_ in _index_.
**`(index-frame _index_ _frame_ _slot1_ _slot2_... _slot n_)`**

    Indexes all the specified slots ( _slot1_ , _slot2_ , through _slot n_) on frame. If some of the slots are frames, they may be computed even if they aren't actually stored on _frame_.
**`(index-slot-value _index_ _frame_ _slot_ _value_ )`**

    Indexes frame as having SLOT with VALUE (whether it does or not).

Once a frame has been indexed, one can search for it either strictly --- based
on certain combinations of slots and values --- or "fuzzily" based on
similarity to an instance or set of instances.

## Strict Searching

_I'll have the french onion soup, without the cheese on top,  
the green salad with honey-mustard dressing on the side,  
and a decaffeinated coffee with one sugar._  
Sally, in **When Harry Met Sally**

Strict searching uses the _FIND-FRAMES_ procedure:

    
    
            (find-frames _index_ 
            _slot 1_ _value 1_
            ... _slot n_ _value n_)
            
          

which finds all objects that have all the specified slots and values. If any
of the _value i_ are non-deterministic sets, the slot need only have one of
the specified values, allowing some variations. For example, in the WordNet
database, the expression:

    
    
            #|fdconsole>|# **(find-frames "brico@framerd.org" 'words {"hack" "chop"})**
            {@1/31813"VERB.COMPETITION synset for hack and kick_on_the_arm" 
            @1/151a5"NOUN.ARTIFACT synset for cab, hack, taxi, and taxicab" 
            @1/316ff"VERB.COMPETITION synset for chop and hit_sharply" 
            @1/23e1b"NOUN.PERSON synset for machine_politician, ward-heeler, political_hack, and hack" 
            @1/321a4"VERB.CONTACT synset for chop and strike_sharply" 
            @1/303a5"VERB.CHANGE synset for hack and hack_on" 
            @1/1299a"NOUN.ANIMAL synset for hack, jade, nag, and plug" 
            @1/cdc6"NOUN.ACT synset for chop and chop_shot" 
            @1/31c45"VERB.CONTACT synset for hack and clear" 
            @1/20578"NOUN.PERSON synset for hack, hack_writer, and literary_hack" 
            @1/31c3d"VERB.CONTACT synset for chop and hack" 
            @1/2ee62"VERB.BODY synset for hack and whoop" 
            @1/32cfe"VERB.MOTION synset for chop and move_suddenly" 
            @1/31812"VERB.COMPETITION synset for hack and kick_on_the_shins" 
            @1/129cd"NOUN.ANIMAL synset for hack" 
            @1/1ed62"NOUN.FOOD synset for chop" 
            @1/2262a"NOUN.PERSON synset for hack, drudge, and hacker" 
            @1/31c3f"VERB.CONTACT synset for chop, chop_up, and cut_into_pieces" 
            @1/2f7e3"VERB.CHANGE synset for hack and cut_up" 
            @1/12999"NOUN.ANIMAL synset for hack" 
            @1/bc17"NOUN.ACT synset for chop and chopper"}
            
          

finds all the synsets containing either the words "hack" or "chop", while

    
    
            #|fdconsole>|# (find-frames "brico@framerd.org" 'words "hack" 'words "chop")
            @1/31c3d"VERB.CONTACT synset for chop and hack"
            
          

finds only the synsets containing both the words "hack" and "chop". One way to
think about this visually, is the search performed by `find-frames` is
conjunctive horizontally (along the list of arguments) and disjunctive
vertically (within each argument).

## Fuzzy Searching

_I'll have a burger with fries and a chocolate shake._  
Harry in **When Harry met Sally**

A fuzzy search does not require an exact match, but returns the best possible
match measured by the number of overlapping properties. There are a variety of
fuzzy search functions as well as a set of tools for writing your own fuzzy
search routines. The chief function find-best, looks just like find-frames:

    
    
            (find-frames _index_ 
            _slot 1_ _value 1_
            ... _slot n_ _value n_)
            
          

but returns those objects with the largest number of matching properties.

## Similarity Searching

_I'll have what she's having._  
from **When Harry Met Sally**

One of the most powerful search mechanisms in Kno is "similarity
searching" which begins with an object or set of objects and finds
objects which have the same properties as those objects, weighting as
higher those which are more in common among the set of initial
descriptions.

**`(find-similar _indices_ _frame_ )`**

    Returns the frames indexed in _indices_ which have the most number of slot values in common with _frame_.
**`(find-similar _indices_ _frame_ _slots_ )`**

    limits the search for similarity to _slots_. Returns the frames indexed in _indices_ which have the most number of slot values in common with _frame_.

For example, the following search finds words with similar meanings to the
common sense of "hack" and "chop":

    
    
            #|fdconsole>|# (find-similar "brico@framerd.org"
            @1/31c3d"VERB.CONTACT synset for chop and hack")
            {@1/31be9"VERB.CONTACT synset for shave, trim, and cut_closely" 
            @1/31c3f"VERB.CONTACT synset for chop, chop_up, and cut_into_pieces" 
            @1/31e95"VERB.CONTACT synset for mow and cut_down" 
            @1/325d4"VERB.CONTACT synset for rebate and cut_a_rebate_in" 
            @1/325d7"VERB.CONTACT synset for saw and cut_with_a_saw"}
            
          

**Fuzzy searching and non-determinism.** When the _frame_ argument to ` find-
similar` is non-deterministic, the search mechanism does a single search but
uses features from each of the frames in combination. As a consequence, the
search weighs properties common between the frames more heavily. For example,
in this retrieval,

    
    
            #|fdconsole>|# (find-similar "brico@framerd" 
            (amb @1/31c3f"VERB.CONTACT synset for chop, chop_up, and cut_into_pieces" 
            @1/31e95"VERB.CONTACT synset for mow and cut_down"))
    	@1/31c3d"VERB.CONTACT synset for chop and hack"
          

the properties common to the two synsets selects the search pattern which put
them together in the first place.

## The DTYPE Protocol

DTypes are a binary data format and communications protocol underlying
Kno. DTypes allow the storage and transmission of complex
recursively structured objects. DTypes can be extended to encode
application-specific data while still using generic facilities for
storage and search.

DTypes are a portable data representation used throughout the Kno suite of
libraries, tools, and applications. There are two basic levels to the DType
representation: a minimal set of "core data types" and an extensible external
binary representation for those types and their extensions. Applications and
libraries use and extend the native data types, taking advantage of
communication, persistence, and indexing facilities which use the external
representation.

DTypes differ from other data sharing approaches in not depending on explicit
data declarations for sharing complex structures. Distributed processing
models like CORBA rely on shared data declarations for communicating complex
data structures. DTypes allow a `sloppier' approach where basic data types
include nested and labelled heterogenous structures. This allows for fast
prototyping of applications and protocols as well as their on-line extension.

The minimal set of native types include fixed and floating point numbers,
ASCII strings and symbols, vectors and pair structures. It also includes some
special types, especially the OID (Object IDentifier) pointers used by the
Kno object database. The external binary representation is used in three
primary ways:

  * in communicating among clients and servers using a remote procedure call protocol;
  * in storing the values associated global "Object IDentifiers" (OIDs) by Kno's [object database](odb.html);
  * in associating structured objects to one another through [Kno indices](indices.html).

The DType core representation includes the following types:

  * literal constants for truth, falsity, the empty list, and a void value
  * small signed integers (32 bits)
  * limited precision floating point numbers (again, 32 bits)
  * ASCII strings
  * Symbols ("interned" ascii strings)
  * Pairs (of any two other types, which can be composed into lists)
  * Vectors (of any mixture of types)
  * Packets (arbitrary vectors of bytes)
  * OIDs (object identifiers) identifying in a virtual 64-bit address space
  * Exceptions and Errors (representing unusual conditions)

Native applications can provide their own types and use Kno storage and
indexing facilities by implementing extensions to the external binary
representation for their types. These extensions can either be in the form of
compounds or package types.

A compound representation consists of a type name (a tag) and a "canonical
form" which can be used to identify and regenerate the object. Logically,
neither the tag nor the the canonical form can include the object being
described or else the translation would recur indefinitely.

A packaged representation consists of a two byte type code followed by 1 or 4
bytes of size information and some amount of additional data. The value of the
first byte identifies a "package" for the extension; the value of the second
byte (which is defined by the package maintainer) specifies a more precise
type code and also specifies the format of the subsequent data in a fashion
which Kno facilities can manipulate without interpreting.

The core Kno libraries introduce new datatypes with packaged data
representations for:

  * **choices** used to represent unordered sets of alternative results and values
  * **slotmaps** providing simple associations between keys and values
  * various **numerical types** , including: 
    * arbitrary precision integers
    * rational numbers
    * complex numbers
  * extended **character types** , including: 
    * unicode and ascii characters
    * unicode strings
    * unicode symbols (interned unicode strings)

