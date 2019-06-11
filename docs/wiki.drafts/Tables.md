KNO provides a generic table type and operations on that type. Native table types include:
* slotmaps: relatively small tables
* schemaps: tables which have a fixed set of slots
* hashtables: large tables which scale well
* hashsets: hash 'tables' which only map to one value, #t, and essentially represent sets. 

In addition, table operations can be applied to OIDs, but with slightly different semantics.

Operations on tables include:

(get table key [default])

(store! table key value)

(add! table key value)

(test table key [value])

(getkeys table)

(getvalues table)

(getassocs table)

(modified? table)

(set-modified! table flag)

(writable? table)

(set-writable! table flag)

## Numeric values tables

(table-max table)

(table-maxval table)

(table-skim table thresh)

(table-increment! table key [value])

(table-increment-existing! table key [value])

(table-multiply! table key [value])

(table-multiply-existing! table key [value])

(table-maximize! table key [value])

(table-maximize-existing! table key [value])

(table-minimize! table key [value])

(table-minimize-existing! table key [value])

(table-map-size table)

(table-size table)




