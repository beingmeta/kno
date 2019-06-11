# PRINTOUT for formatted output

Kno includes a formatted output library modelled (and named)
after InterLisp's PRINTOUT. PRINTOUT can be used to create formatted
messages for the user or to generate textual data files.  The PRINTOUT
model is also used by the HTML generation procedures in the
`fdweb` library.
  
Kno provides a simple and elegant way of generating formatted output.
Most other Lisp dialects provide `FORMAT` commands descended in spirit from
Fortran's `FORMAT` directive. In Kno, we instead take InterLisp's
`PRINTOUT` expression and use it as our model. Each formatted output procedure
takes an arbitrary number of arguments and evaluates each one. If it is
string, it is output without enclosing quotes; if it is the void value (such
as is returned by iteration functions), it does nothing; and for any other
value, it calls the procedure `WRITE` to display it, which produces a LISP-
like representation of the object. E.G.

    
    
        #|fdconsole>|# **(printout "Two plus three is " (+ 2 3) "\n")**
        Two plus three is 5
      
    

Strings inside of structures are enclosed in quotes, so:

    
    
        #|fdconsole>|# (printout "Strings at top level are " "in the raw" "\n"
        "But embedded in structures " '("they wear quotes"))
        Strings at top level are in the raw
        But embedded in structures ("they wear quotes")
      
    

The procedure `PRINTOUT` processes its arguments and sends the results to the
standard output. The function `LINEOUT` does the same but appends a newline to
the end of the output.

The procedure `STRINGOUT` does its output to a string and returns the result
without doing any external output, E.G.

    
    
        #|fdconsole>|# **(stringout "Two plus three is " (+ 2 3))**
        "Two plus three is 5"
      
    

If one of the arguments to a `PRINTOUT` function is an iterative expression
(like `DOLIST`) its arguments can call PRINTOUT themselves. Since the
iteration expression returns void, only the generated output will be seen.
E.G.

    
    
        #|fdconsole>|# (define table '((1 2 3) (4 5 6) (7 8 9)))
        #|fdconsole>|# (dolist (row table)
        (lineout (dolist (column table) (printout "\t" column)) "\n"))
        1       2       3
        4       5       6
        7       8       9
      
    

The procedure `printout-to` takes an initial argument of an output stream,
followed by printout args. Generated output is sent to the designated stream.
For example

    
    
        #|fdconsole>|# (define ofile (open-output-file "temp"))
        #|fdconsole>|# (printout-to ofile "Two plus three is " (+ 2 3))
        #|fdconsole>|# (close-output-port ofile)
        #|fdconsole>|# (filestring "temp")
        "Two plus three is 5"
      
    

Kno support for [generating HTML](www.html#Generating%20formatted%20HTML)
is based on this formatted output model.

## Useful Input/Output Functions

Kno provides a number of special functions for input and
output. These include forms and procedures for binding the default
input and output streams, working with "virtual streams" writing
to strings, and doing binary input and output.
  
Kno implements Scheme **ports** as an input and output abstraction. The
function ` open-input-file` opens an external file for input; the function
`open-output-file` opens an external file for output. The results of these
functions can be used as second arguments to functions like `write`,
`display`, and `newline` or as the first argument to `printout-to`.

The ports returned by these functions can also be made the _default_ port for
input or output. The form ` (WITH-INPUT port ...body...)` evaluates body with
a default input port of port. Similarly, the form `(WITH-OUTPUT port
...body...)` evaluates body with a default output port of port.

Variants of this function can take filenames as arguments and implicitly open
an input or output file. The form `(WITH-INPUT-FROM-FILE filename ...body...)`
evaluates body with a default input port reading data from filename.
Similarly, the form `(WITH-OUTPUT-TO-FILE filename ...body...)` evaluates body
with a default output port writing data to filename.

In addition to file ports, string ports allow programs to read from and write
to strings. A string input port reads from a literal string as though it were
a file; a string output port accumulates its output in a string which can be
extracted along the way. The function `(open-string-input-stream string)`
opens a string input port for reading, e.g.

    
    
          (define p1 (open-string-input-port "(first) (second)"))
          (read p1)
          (first)
          (read p1)
          (second)
          (read p1)
          #EOF
        
    

while the form `(open-string-output-stream)` creates a stream for output whose
"output thus far" can be extracted with `STRING-STREAM-CONTENTS`, e.g.

    
    
          (define p2 (open-string-output-stream))
          (write '(first) p2)
          (write '(second) p2)
          (string-stream-contents p2)
          "(FIRST)(SECOND)"
        
    

String streams can also be used implicitly with the form `(WITH-OUTPUT-TO-
STRING ...body...)` which evaluates `body` with output going (by default) to a
string whose value is returned. Thus, we can say:

    
    
          (with-output-to-string (write '(first)) (write '(second)))
          "(FIRST)(SECOND)"
        
    

or with the form `(WITH-INPUT-FROM-STRING string ...body...)` which evaluates
forms given default input from the string string, e.g.

    
    
          (with-input-from-string "33+5i 44.5"
          (list (read) (read)))
          (33+5i 44.5)
        
    

