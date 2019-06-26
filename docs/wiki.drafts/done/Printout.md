<!-- <title>PRINTOUT for formatted output</title> -->

Kno includes a formatted output library recovered from the depths of
history in the form of InterLisp's PRINTOUT expressions. PRINTOUT can
be used to create formatted user messages or to generate textual data
files.  The PRINTOUT model is also used by the HTML generation
procedures in the `webtools` library.
  
Kno provides a simple and elegant way of generating formatted output
much as other languages use `printf` or its equivalents.


A *printout* body consists of a set of sub expressions which are
evaluated in sequence. If the result is a literal string, it is output
without any enclosing quotes; if it is the void value (such as is
returned by iteration functions), it does nothing; if it is any other
value it generates a LISP-like representation (the equivalent of
`WRITE`, aiming for reversibility). For example:
````console
#|kno>|# **(printout "Two plus three is " (+ 2 3) "\n")**
Two plus three is 5
````

Strings inside of structures are enclosed in quotes, so:
````console
#|kno>|# (printout "Strings at top level are " "in the raw" "\n"
                  "But embedded in structures " '("they wear quotes"))
Strings at top level are in the raw
But embedded in structures ("they wear quotes")
````

The PRINTOUT facility is extended by defining procedures which are
used in printout sub-expressions. Because of PRINTOUT's evaluation
rules, this is especially helpful when procedures return either
strings or `VOID`. For example, suppose we want to define a primitive
for displaying percentages. We can write `SHOWPCT` to return a string:
````scheme
(define (showpct numerator denominator (precision 2))
  (string-append (inexact->string (/ (* numerator 100.0) denominator) precision)
  "%"))
````
And then use:
````console
#|kno>|# (printout "Your task is " (showpct n-processed n-total) " done.")
Your task is 29% done.
````

Alternatively, `SHOWPCT` could be defined to call `PRINTOUT`
recursively and return `VOID`. This would emit the output but the void
result would be ignored. For example:
````scheme
(define (showpct numerator denominator (precision 2))
  (printout (inexact->string (/ (* numerator 100.0) denominator) precision) "%"))
````
Which generates the same behavior:
````console
#|kno>|# (printout "Your task is " (showpct n-processed n-total) " done.")
Your task is 29% done.
````

For some formatting operations, the second method may be more natural,
but if you want to be able to use the operation outside of `PRINTOUT`,
having it return a string may make more sense.

However, you can use `STRINGOUT` to simply convert the output
`PRINTOUT` directly into a string, e.g.
````console
#|kno>|# (STRINGOUT "Your task is " (showpct n-processed n-total) " done.")
#|=>| "Your task is 29% done."
````

`PRINTOUT` can be used with iterative expressions like `DOLIST` or
`DO-CHOICES` (which return VOID) to make it easy to display sets of
results or values:
````console
#|kno>|# (define table '((1 2 3) (4 5 6) (7 8 9)))
#|kno>|# (dolist (row table)
           (printout (dolist (column table) (printout "\t" column)) "\n"))
	1       2       3
	4       5       6
	7       8       9
````

In addition to `STRINGOUT`, many other operators use printout's logic
when processing their parameters. This includes the
[KNO logging functions](incomplete.html) and the XML/HTML generation in
the `webtools` module. In the KNO core, there are three common
variants on `PRINTOUT`:
  * `(LINEOUT *printexprs...*)` is just `PRINTOUT` but puts a newline on the end of the output.
  * `(PRINTOUT-TO *port*` *printexprs...*)` outputs to *port* rather than the standard output.
  * `(FILEOUT *filename*` *printexprs...*)` saves the output in *filename*.

