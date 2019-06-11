# Exceptions

Since the world is an uncertain place, programs can often
encounter unexpected conditions and situations. One tool for building robust
but understandable programs is to separate out the routine execution of
procedures from the handling of unexpected conditions. Kno has several
tools for supporting this sort of horizontal modularization.
  
The Kno error model is based on the idea of user procedures or primitives
_raising exceptions_ to indicate an unexpected condition. In the current
model, there is no way to handle the error where it occurred (by, for
instance, trying an operation again). Instead, programs can set up contexts
for catching and handling these errors.

The easiest way to catch errors is with the procedure `SIGNALS-ERROR?` which
takes a single argument. The function returns false (`#F`) if the argument was
evaluated without raising any exceptions (and thus discards the return value);
otherwise, the function returns an _error object_ describing the signalled
error. For example,

    
    
    #|kno>|# (signals-error? (+ 2 3))
    #f
    #|kno>|# (signals-error? (+ 2 'a))
    [#ERROR ("Type Error" "+: not an integer" A)]
    

The error object, which may also be commonly returned by remote function
evaluations, can be tested for with the predicate `ERROR?` and its components
can be accessed with the primitives `ERROR-EXCEPTION`, `ERROR-DETAILS`, and
`ERROR-IRRITANT`. E.G.

    
    
    #|kno>|# (define errobj (signals-error? (+ 2 'a)))
    #f
    #|kno>|# errobj
    [#ERROR ("Type Error" "+: not an integer" A)]
    #|kno>|# (error? errobj)
    #T
    #|kno>|# (error-exception errobj)
    "Type Error"
    #|kno>|# (error-details errobj)
    "+: not an integer"
    #|kno>|#(error-irritant errobj)
    A
    

The return value from normal evaluation is accessible by using `SIGNALS-
ERROR+?`, which returns multiple values (not choices) indicating the values
returned the evaluation. E.G.

    
    
    #|kno>|# (signals-error+? (+ 2 3))
    #f
    ;;+1: 5
    

These additional values can be accessed using `multiple-value-bind`, as in:

    
    
    #|kno>|# (define (test-eval expr)
                 (multiple-value-bind (error? result) (signals-error+? (eval expr))
                   (if error? (lineout "Evaluating " expr " signalled " error?)
                       (lineout "Evaluating " expr " returned " result))))
    #|kno>|# (test-eval '(+ 2 3))
    Evaluating (+ 2 3) returned 5
    #|kno>|# (test-eval '(+ 2 a))
    Evaluating (+ 2 A) signalled [#ERROR ("Variable is unbound" "EVAL" A)]
    #|kno>|# (test-eval '(+ 2 'a))
    Evaluating (+ 2 'A) signalled [#ERROR ("Type Error" "+: not an integer" A)]
    

More sophisticated processing can be done with the special form `ON-ERROR`
which evaluates its first argument and returns its value if no exceptions were
raised. If exceptions were raised however, the remaining expressions in the
`ON-ERROR` form are evaluated in an environment with the following bindings:

EXCEPTION

    a string identifying the signalled error;
EXCEPTION-DETAILS

    a string providing additional information about the error (for instance a filename)
IRRITANT

    the lisp object whose character caused the error; for instance, the object which happens to be the wrong type for an operation;
BACKTRACE

    a string containing the backtrace of program execution, which may be quite long, but can be parsed to extract call context information

Another option, between these two possibilities, is the `CATCH-ERRORS`
procedure which evaluates its body and returns the result of the final
expression. If any exceptions are raised during the execution of the body, the
`CATCH-ERRORS` form returns an _error object_ describing the raised exception,
its details, and the irritant.

User Kno code can signal an error with the form `RAISE-EXCEPTION`. It
takes one to three arguments: an exception name (a string or symbol), a
details description (a string), and an irritant (a lisp object).

