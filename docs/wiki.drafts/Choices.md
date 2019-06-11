# Choices

One of the key data and control structures of Kno is the *choice*. Operations over choices allow the natural expression of many
search, set, and iteration operations. Choices are based on John
McCarthy's AMB operator [original paper] where execution automatically
explores multiple compute paths.

The key idea is that when an argument to a procedure is a *choice*,
the procedure automatically iterates over the alternatives in the
choice and returns the alternative results as a choice. With multiple
arguments, the procedure iterates over all the combinations of the
values in the passed arguments (the cross product). The printed
representation of a choice consists of the alternatives separated by
spaces and surrounded by curly braces ({}). Here's an example of a
choice in Scheme:
```(list {1 2 3} {"one" "two"}
{(1 "one")
 (2 "one")
 (3 "one")
 (1 "two")
 (2 "two")
 (3 "three")}
```

Like a mathematical set, choices are unordered collections of values
without duplicates. Unlike a set, a choice can't be an element in a
choice. In Kno, normal data values are treated as choices with one
alternative.

Compound structures (pairs, vectors, tables, etc) can have choices as
values. As a knowledge representation language, choices are used to
represent *multi-valued slots*. As a database foundation, choices are
used to represent mappings of keys to the objects they characterize.

Certain procedures are declared to be **non-deterministic** meaning
that they pass their choice arguments directly without iterating over
them. In Scheme, you can create a non-deterministic procedure using
the special forms `AMBDA` instead of `LAMBDA` and `DEFAMBDA` instead
of `DEFINE`.

The C implementation of choices is optimized for performance. Choices
are immutable objects sorted by memory ordering to provide `O(n)`
performance on set operations and `O(log n)` performance on membership
tests. From C, there is also a special `PRECHOICE` type which holds a
collection of values (and choices) which have not yet been sorted and
reduced.

## Choices: Non-Deterministic Values

One of the key data and control structures of Kno is the *choice*
which supports **non-deterministic programming** patterns which
simplify the expression of many search, set, and iteration operations.

A **choice** represents an unordered set of alternative values, which
can be any Lisp object *except* another choice. When Kno encounters a
choice, it automatically explores different possible outcomes based on
each element of the choice. This makes it very simple to describe
certain kinds of processes and operations by characterizing the inputs
and outputs of procedures as _choices_ rather than single values.

Choices in Kno are descended from the AMB operator discussed by John
McCarthy and various versions of this idea implemented by David Chapman, Ramin
Zabih, David McAllester, and Jeff Siskind. They first entered Kno in its
predecessor language, `Kno`, as a way of regularizing functions involving
multi-valued and single-valued slots.

Choices are distinct from the *multiple values* provided by Common
Lisp and the R5RS Scheme standard. Mutiple value implementations allow
a procedure to return _structured_ multiple values, where different
value _positions_ have different semantics (e.g. the first value might
be an x coordinate and the second value might be a y
coordinate). Choices in Kno, on the other hand, represent unstructured
sets of values.

Curly braces represent literal choices, so evaluating a choice between 3, 4,
and 5 just returns a choice between the three numbers.
````console
#|kno>|# {3 4 5}
{3 4 5}
````

However, adding 10 to the set of choices returns a different set of choices:
````console
                #|kno>|# (+ {3 4 5} 10)
                {13 14 15}
````

while multiplying the set of choices by itself produces even more options:
````console
                #|kno>|# (* {3 4 5} {3 4 5})
                {9 12 15 16 15 20 25}
````
				
Whenever Kno applies a procedure to a set of choices, it picks each of
the choices, applies the procedure, and combines the results; thus, if we
define SQUARE as:
````console
                (define (square x) (* x x))
````

and apply it to the same set of choices as above, we get only three choices
back, since square is called three times on each single input and that single
input is then multipled by itself:
````console
                #|kno>|# (square {3 4 5})
                {9 16 25}
````

When a procedure returns a non-deterministic value, we can apply another
procedure to it, as in:
````console
                #|kno>|# (+ (square {3 4 5}) 10)
                {19 26 35}
````

Most Kno procedures work in exactly this way when given non deterministic
sets for arguments, passing on any non-determinism in their arguments to their
results. However, some procedures work differently by either returning
deterministic results for non-deterministic arguments into a single result or
taking deterministic arguments and returning a set of choices (a non-
deterministic result).

When a procedure returns a non-deterministic result consisting of one choice,
that is the same as a deterministic result. This means that a regular
procedure can return a deterministic result from non-deterministic argument,
as in:
````console
                #|kno>|# (square {3 -3})
                9
````

## Deterministic results from non-deterministic inputs

Built-in procedures for generating deterministic results from non-
deterministic inputs include:

  * `(pick-one set)` randomly selects one of the choices in set
  * `(choice->list set)` returns the choices as a list of elements
  * `(choice-size _set_ )` returns the number of choices in _set_
  * ` (empty? expr)` or `(fail? expr)` returns true if evaluating expr returns no values

  * `(exists? expr)` returns true if evaluating expr returns any values at all
  * `(contains? val expr)` returns true the result of evaluating expr includes val

    
````console
      #|kno>|# **(PICK-ONE (CHOICE 2 3 4))**
      3
      #|kno>|# **(PICK-ONE (CHOICE 2 3 4))**
      2
      #|kno>|# **(CHOICE-SIZE (CHOICE 2 3 4))**
      3
      #|kno>|# **(CHOICE-SIZE 8)**
      1
      #|kno>|# **(CHOICE-SIZE {})**
      0
      #|kno>|# **(FAIL? (CHOICE))**
      #T
      #|kno>|# **(FAIL? 3)**
      #F
      #|kno>|# **(EMPTY? (CHOICE 3 4))**
      #F
      #|kno>|# **(DEFINE (EVEN? x) (if (zero? (remainder x 2)) x (CHOICE)))**
      #|kno>|# **(EXISTS? (CHOICE))**
      #F
      #|kno>|# **(EXISTS? 3)**
      #T
      #|kno>|# **(EXISTS? (even? (CHOICE 3 5 9)))**
      #F
      #|kno>|# **(EXISTS? (even? (CHOICE 2 3 5 9)))**
      #T
      #|kno>|# **(CONTAINS? 2 (CHOICE 2 3 4))**
      #t
      #|kno>|# **(CONTAINS? 5 (CHOICE 2 3 4))**
      #f
      #|kno>|# **(CONTAINS? 8 (+ (CHOICE 2 3 4) (CHOICE 4 5 6)))**
      #t
````

## Non-deterministic results from deterministic inputs

Other built-in procedures generate non-deterministic results from
deterministic arguments. The most basic such procedure is `CHOICE`, which
returns its arguments non-deterministically, e.g.
````console
        #|kno>|# (choice 3 4 5)
        {3 4 5}
        #|kno>|# (+ (CHOICE 3 4 5) 10)
        {13 14 15}
````

while another important one is `ELTS` which returns the elements of a sequence
non-deterministically, e.g.:
````console
        #|kno>|# (elts '(a b c))
        {A B C}
        #|kno>|# (elts "def")
        {#\d #\f #\e}
````

## Failure and Pruning

A procedure can also return no choices at all. This "return value" is called a
failure and is indicated by pair of empty curly braces "`{}`", e.g.
````console
        #|kno>|# **(CHOICE)**
        {}
      
````

when a procedure is called on a failure, the procedure itself returns a
failure, so:
````console
        #|kno>|# **(+ (CHOICE 10 8) (CHOICE))**
        {}
````
This special result, indicating no returned choices, is called a failure
because of the way that choices are used in searching by non-deterministic
programming. If you think of a given procedure as doing some `search' given
the constraints of its arguments, returning the empty choice can be considered
as "failing" in the part of the search.

The early termination on failure is called "pruning." We say that the call to
`+` was **pruned** because the second call to CHOICE _failed_. Note that if a
subexpression fails in this way, none of the remaining arguments are
evaluated, E.G.
````console
        #|kno>|# **(+ (CHOICE) (begin (lineout "last argument") 3))**
        {}
````
doesn't produce the output line ``last argument`' because the whole expression
is pruned before the final form is evaluated.

## Using Choices to Represent Sets

Non-deterministic return values can be used to represent sets, as in the
following definition of set intersection, which specifies the base case and
naturally generalizes:
````console
        #|kno>|# **(define (intersect x y) (if (equal? x y) x (fail)))**
        #|kno>|# **(intersect (CHOICE 3 4 5 6) (CHOICE 5 6 7 8))**
        {5 6}
````
We can see the value combination process in action by adding trace statements
to the `INTERSECT` procedure, as in:
````console
        #|kno>|# **(define (intersect x y) 
          (lineout "INTERSECT " x " = " y " is " (equal? x y))
          (if (equal? x y) x {}))**
        #|kno>|# **(intersect (CHOICE 3 4 5) (CHOICE 5 6 7))**
        INTERSECT 3 = 5 is #f 
        INTERSECT 3 = 6 is #f
        INTERSECT 3 = 7 is #f
        INTERSECT 4 = 5 is #f
        INTERSECT 4 = 6 is #f
        INTERSECT 4 = 7 is #f
        INTERSECT 5 = 5 is #t 
        INTERSECT 5 = 6 is #f
        INTERSECT 5 = 7 is #f
        5
````
Of course, this is an inefficient way to compute intersections. Kno
provides a number of special forms for dealing with non-deterministic values,
which we describe in the next section.

## Combining Choices

There are a variety of Kno special forms for dealing with non-
deterministic values. They are called "special" forms because they do not
follow the normal rules for non-deterministic procedure combination.

`(INTERSECTION expr1 expr2)`
    evaluates expr1 and expr2 and returns only the values returned by both expressions. E.G. 
    ````console
              #|kno>|# **(INTERSECTION {3 4 5} {2 4 6})**
              4
    ````
            
    

On very large choices, operations like intersection can be very time
consuming. Kno provides a special flavor of choice, the _sorted choice_
which can be optimized for these sorts of operations. The function ` sorted-
choice` returns such a choice.

`(UNION expr1 expr2)`
    evaluates expr1 and expr2 and returns the results from both. E.G. 
    ````console
                    #|kno>|# **(UNION {3 4 5} {2 4 6})**
                    {2 3 4 5 6}
    ````

` (DIFFERENCE expr1 expr2)`
    evaluates expr1 and expr2 and returns the results of expr1 which are **not** returned by  expr2. E.G. 
    ````console
                        #|kno>|# **(DIFFERENCE {3 4 5} {2 4 6})**
                        {3 5}
    ````

` (TRY expri...)`
    Evaluates each expri in order, returning the first one which doesn't fail (e.g. which produces any values at all), E.G. 
    ````console
                            #|kno>|# **(TRY (INTERSECTION (CHOICE 3 4 5) (CHOICE 6 7 8)) ; _This one fails_
                              (INTERSECTION (CHOICE 3 4 5) (CHOICE 1 2 3))  ; _This one doesn't_
                              (INTERSECTION (CHOICE 3 4 5) (CHOICE 4 5 6))) ; _This one doesn't get a chance_**
                            3
    ````

### Pruning and Special Forms

You may have figured out that non-deterministic evaluation and pruning can't
apply to the definitions above or else an expression like:
````console
        (UNION (CHOICE) (CHOICE 3 4))
````

would automatically be pruned. Some other special forms also break the default
rules for combination and pruning. For instance, the formatted output
functions such as `LINEOUT` don't do automatic enumeration and pruning, so you
get the following behavior:
````console
        #|kno>|# **(LINEOUT "This is empty: " (CHOICE) " but this isn't: " (CHOICE 2 3))**
        This is empty: {} but this isn't: {2 3}
````

## Choices and User Procedures

User procedures (like the procedure `INTERSECT` which we defined above)
automatically invoke the interpreter's search and combination mechanisms. For
instance, the following fragment generates possible sentences:
````console
        #|kno>|# **(DEFINE (sentence subject verb object) (list subject verb object))**
        #|kno>|# **(sentence (CHOICE "Moe" "Larry" "Curly") 
          (CHOICE "hit" "kissed")
          (CHOICE "Huey" "Dewey" "Louie"))**
        {("Moe" "hit" "Huey") ("Larry" "hit" "Huey") ("Curly" "hit" "Huey") 
        ("Moe" "kissed" "Huey") ("Larry" "kissed" "Huey") 
        ("Curly" "kissed" "Huey") ("Moe" "hit" "Dewey") 
        ("Larry" "hit" "Dewey") ("Curly" "hit" "Dewey") 
        ("Moe" "kissed" "Dewey") ("Larry" "kissed" "Dewey") 
        ("Curly" "kissed" "Dewey") ("Moe" "hit" "Louie") 
        ("Larry" "hit" "Louie") ("Curly" "hit" "Louie") 
        ("Moe" "kissed" "Louie") ("Larry" "kissed" "Louie") 
        ("Curly" "kissed" "Louie")}
````

The only caveat to the non-deterministic application of user procedures was
mentioned above. If a user procedure takes a dotted or optional argument, the
argument is bound to a list of the remaining choices rather than a choice
among the lists that they would generate. So, this definition calls `LINEOUT`
once on the choice `{3 4}`:
````console
        #|kno>|# **(define (list-choices . x) (lineout "Results are: " (car x)))**
        #|kno>|# **(list-choices (CHOICE 3 4))**
        Results are: {3 4}
````
while this definition calls `list-choices` separately on the returned values:
````console
        #|kno>|# **(define (list-choices x) (lineout "Results are: " x))**
        #|kno>|# **(list-choices (CHOICE 3 4))**
        Results are: 3
        Results are: 4
````
calls `list-choices` separately on the returned values.

The key point is that if a procedure is expecting a choice as an argument and
needs the choice to remain a choice (rather than having its elements
enumerated), the argument should be extracted from a "dotted" argument. For
instance, suppose we wanted to define a function which returned twice the size
of a choice, we might try to write it this way:
````console
        #|kno>|# **(DEFINE (BAD-DOUBLE-SIZE x) (* 2 (choice-size x)))**
        #|kno>|# **(BAD-DOUBLE-SIZE 3)** ;  <== this works fine
        2
        #|kno>|# **(BAD-DOUBLE-SIZE {3 4 5 6})** ;  <== this doesn't
        2
````
but that won't work because the `x` argument is bound to each of the numbers
in the choice individually, rather than as an entire choice at once. A correct
definition would be:
````console
        #|kno>|# **(DEFINE (DOUBLE-SIZE . ARGS) (* 2 (SET-SIZE (CAR ARGS))))**
        #|kno>|# **(DOUBLE-SIZE 3)** ;  <== this still works fine
        2
        #|kno>|# **(DOUBLE-SIZE {3 4 5 6})** ;  <== and so does this...
        8
````

## Choices and variables

Choices can be stored and saved in a variety of ways. For instance, the
special form `SET!` sets a variable to contain a set of possible values, so
one can say:
````console
        #|kno>|# **(SET! small-primes (CHOICE 2 3 5 7 11 13 17 19))**
        #|kno>|# **(define (divides? x y) (if (zero? (remainder x y)) y {}))**
        #|kno>|# **(divides? 15 small-primes)**
        {3 5}
````

The `SET+!` adds a set of values non-deterministically to a variable. For
example,
````console
        #|kno>|# **(SET! small-odd-numbers (CHOICE 1 3 5 7))**
        #|kno>|# **small-odd-numbers**
        {5 1 7 3}
        #|kno>|# **(SET+! small-odd-numbers (CHOICE 9 17))**
        #|kno>|# **small-odd-numbers**
        {5 9 1 7 17 3}
````

The binding special forms `LET` and `LET*` can be used to store non-
deterministic values in the same way as `set!`. E.G.
````console
        #|kno>|# **(define (divides? x y) (if (zero? (remainder x y)) y {}))**
        #|kno>|# **(let ((small-primes (CHOICE 2 3 5 7 11 13 17 19)))
          (divides? 15 small-primes))**
        {3 5}
````

(For old-time Scheme aficianados, this interpretation of `LET` breaks the
equivalence of `LET` and `LAMBDA`, since writing the above as an application
of a lambda would automatically iterate through the choices..)

## Iterating over choices

Sometimes it is important to be able to process each element of a choice
separately. Kno provides three special forms supporting this kind of
processing, `DO-CHOICES, FOR-CHOICES,` and `FILTER-CHOICES`:

`(DO-CHOICES (var val-expr) expr1 expr2...)`
    Evaluates all of the expri with var bound to each of the values returned by val-expr. E.G. 
    ````console
    
            #|kno>|# **(DO-CHOICES (x (CHOICE 3 4)) (lineout "I saw a " x))**
            I saw a 4
            I saw a 3
    ````

` DO-CHOICES` can be used to unpack a set of values to pass to forms which
don't automatically unpack their arguments (such as `LINEOUT`), as in the
following definition which puts each of the values returned by `FGET` on a
different line:
````
            (define (print-slot-values frame slot)
            (let ((values (fget frame slot)))
            (lineout "The " slot " of " frame " is:")
            (do-choices (value values)
            (lineout "            " value))))
````
    

`(FOR-CHOICES (var val-expr) expr1 expr2...)`
    Like DO-CHOICES, but combines the results of evaluating the last expri for each value, E.G. 
	````console
                #|kno>|# **(FOR-CHOICES (x (CHOICE 3 4 5 6)) (if (zero? (remainder x 2)) (+ x 3)))**
                {7 9}
   ````

` (FILTER-CHOICES (var value-expr) test-expri...)`
    Evaluates value-expr and and binds var to each element and returning those elements for which every test-expri returns true given the binding. E.G. 
    ````console
          #|kno>|# **(DEFINE (EVEN? x) (zero? (remainder x 2)))**
          #|kno>|# **(FILTER-CHOICES (num (CHOICE 1 2 3 4 5 6)) 
            (EVEN? x))**
          {2 4 6}
          #|kno>|# **(FILTER-CHOICES (num (CHOICE 1 2 3 4 5 6)) 
            (EVEN? num)
            ( < num 6))**
          {2 4}
    ````
