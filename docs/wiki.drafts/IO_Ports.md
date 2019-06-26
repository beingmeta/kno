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
        
    

