# Tables

Kno provides primitive hashtables and "hashsets" to support
efficient operations with large heterogenous data sets. These are
analogous to Perl's associative arrays or Python's dictionaries.
  
Kno provides fast implementations of sets and association tables using an
internal hashing implementation. These functions are similar to those provided
by other programming environments, so our descriptions here will be brief.

(make-hashtable)

    returns an empty hash table.
(hashtable-get hashtable key)

    gets the value(s) associated with key in hashtable.
(hashtable-add! hashtable key new)

    adds new to the values associated with key in hashtable.
(hashtable-set! hashtable key new)

    makes new be the only values associated with key in hashtable.
(hashtable-zap! hashtable key)

    removes any associations with key in hashtable.

For example, the following code stores the squares of the integers from 0 to
199 in a hashtable:

    
    
          #|kno>|# **(define square-table (make-hashtable))**
          #|kno>|# **squares-table**
          [#hashtable 0/19]
          #|kno>|# **(dotimes (i 200) (hashtable-add! square-table i (* i i)))**
          #|kno>|# **squares-table**
          [#hashtable 200/271]
          #|kno>|# **(hashtable-get square-table 20 #f)**
          400
          #|kno>|# **(hashtable-zap! square-table 20)**
          #|kno>|# **(hashtable-get square-table 20 #f)**
          {}
          #|kno>|# **(hashtable-add! square-table 30 300)** ; Not true!
          #|kno>|# **(hashtable-get square-table 30 #f)**
          {900 300} ;  < Note multiple values
        
    

Kno also provides a "hashset" facility for maintaining large sets of
objects with fast tests for membership

(make-hashset)

    returns an empty hashset.
(hashset-get hashset elt)

    returns true if elt is in hashset.
(hashset-add! hashset elt)

    adds elt to hashset.
(hashset-zap! hashset elt)

    removes elt from hashset.
(hashset-elts hashset)

    returns the elements of hashset as a non-deterministic set.
For example, the following code stores some number of primes in a hashset:

    
    
          #|kno>|# **(define primes-table (make-hashset))**
          #|kno>|# **primes-table**
          [#hashset 0/19]
          #|kno>|# **(hashset-add! primes-table (amb 1 2 3 5 7 11 13 17 19 23 29))**
          #|kno>|# **(hashset-get primes-table 15)**
          #F
          #|kno>|# **(hashset-get primes-table 17)**
          #T
          #|kno>|# **(hashset-get primes-table 2)**
          #T
          #|kno>|# **(hashset-zap! primes-table 2)**
          #|kno>|# **(hashset-get primes-table 2)**
          #F
          #|kno>|# **(hashset-elts primes-table)**
          {1 2 3 5 7 11 13 17 19 23 29}
        
    

