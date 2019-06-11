# Sequences

Kno provides a number of generic "sequence" functions based on
similar functions in Common Lisp. These functions operate on lists,
vectors, strings, and packets, uniformly, attempting to reduce the
cognitive overload of all these extra data types.
  
Sequences include lists, vectors, strings, packets, and others. Generic functions on
sequences include:

(elt sequence index)
    returns the indexth element of sequence. For strings, this will be a character, for packets, it will be an integer in the range 0-255, and for vectors and lists it could be any object. This procedure **fails** (returns the empty choice) if  sequence has fewer than index elements.

(reverse sequence)
     returns a sequence of the same type with its elements in reverse order.

(length sequence)

    returns the number of elements in sequence
(find key sequence)

    returns an element of sequence which is `EQUAL?` to key or `#F` otherwise.
(position key sequence [start])

    returns the position of the first element of sequence after start which is `EQUAL?` to key or `#F` otherwise. If start (an integer) is not provided, the absolute first occurence is returned.
(count key sequence)

    returns the number of elements of sequence which are `EQUAL?` to key.
(subseq sequence start [end])

    returns the subsequence of sequence starting at start and ending at end (or the end of sequence if end is not specified).
(remove key sequence)

    returns a copy of sequence with all elements `EQUAL?` to key removed.
(search sub-sequence sequence [start])

    returns an offset into sequence where sub-sequence starts, or `#f` otherwise. sequence and sub-sequence need not be the same type. If start is specified, the search starts at the offset start in sequence (but still returns an offset relative to the beginning of sequence).
(mismatch sequence1 sequence2 [start1] [start2])

    returns the offset at which sequence1 and sequence2 begin to differ. If start1 and start2 are specified, they indicate starting places in sequence1 and sequence2 respectively.
(doseq (var sequence [index]) body...)

    evaluates body repeatedly with each element (in order) bound to var. If the variable index is provided, it is bound to the position in the sequence where the element is found.
(first sequence)

    returns the first element of sequence
(second sequence)

    returns the second element of sequence
(third sequence)

    returns the third element of sequence
(fourth sequence)

    returns the fourth element of sequence
(fifth sequence)

    returns the fifth element of sequence

