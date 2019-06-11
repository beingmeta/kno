# It's all about OIDs

OIDs are a central part of Kno's architecture. The important thing about OIDs are:
* they are globally unique IDs (or trivially mappable into globally unique IDs)
* they are zero-cost, so there is no allocated memory associated with an OID reference
* they have values, which are usually tables of some sort (especially slotmaps)
* their values are loaded on-demand from *pools* mediated by *drivers*
* some important operations can be performed on them without loading them

Load-free operations include:
* identity comparison
* set operations
* storage in other structures
* indexing of other objects based on the OID

In addition, pool storage allows the declaration of *adjuncts* for the values of particular slots (fields).
The **adjunct** for a slot is a separate storage entity (service, index, or *adjunct pool*) which is used for saving, fetching, and testing values for that particular slot. These slots can then be accessed without loading the OID's primary value.

These properties make them ideal for representing large numbers of individuals of varying types. The usual approach to this kind of data model is to use a zero-cost value (such as an integer id) to identify individuals. But this requires either that all individual types share a common ID space (complex and often impossible) or that the application keep track of the the ID space from which a value derives. This can either be done implicitly (by the application logic) or explicitly (by creating a structure for both the ID and it's ID space).
