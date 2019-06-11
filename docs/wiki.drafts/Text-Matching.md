# Text Matching

## The Pattern Matcher

The TX pattern matcher recognizes and extracts structure from arbitrary
strings. TX is organized around matching patterns (which are LISP objects)
against strings (which are linear sequences of characters). Since Kno
strings can include any Unicode character, these strings may contain the
characters of any human language and most machine languages.

Taken by itself, a pattern specifies a set of strings; for instance, the
pattern `(isalnum+)` matches any sequence of alphanumeric characters, so that:
````console
    #|kno>|# (textmatch '(isalnum+) "haase")
    #t
````
but:
````console
    #|kno>|# (textmatch '(isalnum+) "haase@media")
    #f
````
since ``@'` isn't a letter or number. The pattern `(isalnum+)` also matches
letters and numbers in other languages, so
````console
    #|kno>|# (textmatch '(isalnum+) "häse")
    #t
````
`(isalnum+)` is called a matching operator. Strings and matching operators are
the "basis level" for matching and searching: any search or match eventually
gets down to either strings or matching operators. However, the matcher
provides two general and powerful ways to combine these primitives.

## Vector Patterns match Sequences

A vector pattern combines several patterns into a sequence, matching all
strings consisting of a substring matched by the vector's first element
followed by a substring matching the vector's second element, and so on. For
example, the following vector pattern matches the string `"haase@media"`:
````console
    #|kno>|# (textmatch '#((isalnum+) "@" (isalnum+))  "haase@media")
    #t
````
since the first `(isalnum+)` matches `"haase"`, the string `"@"` matches `"@"`
(strings always match themselves), and the second `(isalnum+)` matches
`"media"`. Note that this pattern would not, however, match a string like
"haase%prep.ai.mit.edu".

## Choices can be used as Patterns

Alternatives like this can be described by using Kno choices to represent
different patterns which can be matched. For example, we can extend the
pattern above to also match `"haase%prep.ai.mit.edu"`:
````console
    #|kno>|# (textmatch '#((isalnum+) {"@" "%"} (isalnum+))  "haase%prep.ai.mit.edu")
    #t
````
The choices in a pattern like this need not be strings; any pattern can be
recursively included, e.g.
````console
    #|kno>|# (textmatch '#((isalnum+) {"@" "%" (ispunct)} (isalnum+))  "haase-media")
    #t
````
## Named Patterns

When a symbol is used as a pattern, the value of that symbol is used for the
matching, allowing complex patterns to be broken into smaller pieces. The
procedure `textclosure` (with abbreviation `txc`) takes a pattern and
associates it with the current environment, so that symbol references within
the pattern will be resolved in the corresponding environment. An example may
make things clearer:
````console
    (define user-name '(isalnum+))
    (define host-name
      {(isalnum+)
       #((isalnum+) "." (isalnum+) ".edu")
       #((isalnum+) "." (isalnum+) "." (isalnum+) ".edu")
       #((isalnum+) "." (isalnum+) "." (isalnum+) "." (isalnum+) ".edu")})
    #|kno>|# (textmatch (textclosure '#(user-name "@" host-name))
                         "haase@media.mit.edu")
    #t
````
The use of symbols as patterns is mostly meant to provide a way of reducing
the complexity of individual patterns and enchancing their readability.
Technically, however, it also makes the matcher more powerful because it
allows the specification of _recursive_ patterns.

## How To Do Things With Patterns

We now know enough about patterns to look at the different ways patterns can
be used in the TX package. Patterns can be used for more than matching against
strings. As we saw above, the function `textextract` extracts the structure of
the match:
````console
    #|kno>|# (textextract '#((isalnum+) "@" (isalnum+))  "haase@media.mit.edu")
    #("haase" "@" "media.mit.edu")
````
`textextract` treats named patterns as "atoms" and doesn't expand the internal
structure of their match. This allows something like this:
````console
    #|kno>|# (textextract (txc #(user-name "@" host-name)) "haase@media.mit.edu")
    #("haase" "@" "media.mit.edu")
````
where simple substitution would extract the substructure of the hostname
`"media.mit.edu"`, rather than treating it as a single chunk:
````console
    #|kno>|# (textextract (vector user-name "@" host-name))
    #("haase" "@" #("media" "." "mit" ".edu"))
````
Note that in this example, we use `vector` to construct the pattern on the
fly.

The function `textsearch` locates the first substring which matches a pattern,
returning the integer position at which the substring starts. For example,
````console
    #|kno>|# (textsearch '(isdigit+) "My name is 007, JAMES 007")
    11
````
The function `textmatcher` returns the length of the substring which a pattern
does match, for example
````console
    #|kno>|# (textmatcher '(isdigit+) "123ABC")
    3
````
The function `gather` returns the substrings of a string which match a
pattern, as in
````console
    #|kno>|# (gather '(isdigit+) "There were 12 grapes and 66 apples")
    ;; There are 2 results
    {"12" "66"}
````
The matches are returned as a choice and can then be operated on by other
procedures. For example, using `read-from-string` would return the actual
numeric values:
````console
    #|kno>|# (read-from-string
                 (gather '(isdigit+) "There were 12 grapes and 66 apples"))
    ;; There are 2 results
    {12 66}
````
The function `textslice` breaks a larger string into smaller substrings at
separators designated by a particular pattern. For instance, we can get
substrings separated by vowels as follows:
````console
    (define vowels '(+ {"a" "e" "i" "o" "u"}))
    #|kno>|# (textslice "How long has it been?" vowels)
    ("H" "w l" "ng h" "s " "t b" "n?")
````
which we could glue back together with `string-append`:
````console
    #|kno>|# (apply string-append (textslice "How long has it been?" vowels))
    "Hw lng hs t bn?"
````
The function `textfragment` works much like `textslice`, but it keeps the
separating strings, so we would have:
````console
    #|kno>|# (textfragment "How long has it been?" vowels)
    ("" "H" "o" "w l" "o" "ng h" "a" "s " "i" "t b" "ee" "n?")
````
Applying `string-append` to the results of `textfragment` will restore the
original string, as in:
````console
    #|kno>|# (apply string-append
                  (textfragment "How long has it been?" vowels))
    "How long has it been?"
````

## Parsing Files with Record Streams

Finally, we can take files and use patterns to divide them into records
without having to load the whole file into a string. This can be useful with
large data files used in other databases or applications. One starts by
creating a record stream with the function `open-record-stream`, which takes a
filename, a pattern, and (optionally) a text encoding (e.g. iso-8859/1 or
BIG5).

Once a record stream has been created, the function `read-record` sequentially
returns chunks of text from the file which match the record pattern. The
function `read-spacing` can read the spacing between records.

## Review

As we've seen, patterns in TX are built out of five simple elements:

  * strings match themselves
  * vectors of patterns match one pattern after another
  * choices match one of many patterns
  * symbols match patterns defined by global variables
  * operators (like (isalnum+)) match certain kinds of substrings

Knowing how these simple pieces work and what operators are available, you can
write and read patterns in TX. The following sections list the available
operators. This pattern language was designed to more readable than standard
regular expression languages such as those provided by the POSIX regex library
or Perl.

## Simple Operators

Simple operators are built-in primitives for identifying syntactic points
(beginnings and end of lines), character properties (spacing, case,
puncutation, etc), and some common patterns (mail ids, markup, etc).

(bol)

    matches either the beginning of a string or the beginning of a new line
(eol)

    matches either the end of a string or the end of a line
(isalpha)

    matches any alphabetic character
(isalpha+)

    matches any string of alphabetic characters
(isdigit)

    matches any base 10 digit character
(isdigit+)

    matches any sequence of base 10 digits
(isalnum)

    matches any alphanumeric character
(isalnum+)

    matches any string of alphanumeric characters
(ispunct)

    matches any punctuation character
(ispunct+)

    matches any string of punctuation characters
(isupper)

    matches any upper-case character
(isupper+)

    matches any string of upper-case characters
(islower)

    matches any lower-case character
(islower+)

    matches any string of lower-case characters
(isspace)

    matches any whitespace characters
(isspace+)

    matches any sequence of whitespace characters
(spaces)

    matches any sequence of whitespace characters
(lsymbol)

    matches any LISP symbol
(csymbol)

    matches any valid C identifier
(mailid)

    matches any email address or message reference

The primitive match operators which match more than a single character are
_maximizing_ ; this means that they match the longest string possible. In
particular, they will not match any substrings of a string they match. This
means that an operator like `(isalpha+)` will match the substring "abc" in the
string "abc3", but will _not_ match the substring "ab". This makes the
matching a lot faster and the more general sort of matching can be done by
using the compound ` *` and `+` operators (e.g. as `(+ (isalpha))`.

## Parameterized Operators

`(char-not chars)` matches any string that does not contain any of the
characters in chars (which is a string). E.G.
````console
    #|kno>|# (textmatch '(char-not "+-") "333.5")
    #t
    #|kno>|# (textmatch '(char-not "+-") "333.5+5i")
    #f
````

`(char-range first-char last-char)` matches any character whose Unicode code
point lies between the characters first-char and last-char (inclusive). For
example, we could rewrite `(islower)` with
````console
    #|kno>|# (textmatch '(char-range #\a #\z) "a")
    #t
    #|kno>|# (textmatch '(char-range #\a #\z) "m")
    #t
````
though this would only work for ASCII characters `(islower)` works for any
Unicode character.

## Compound operators

A compound operator takes another pattern as a parameter. Three of the most
useful compound operators are `(* pat)`, `(+ pat)`, `(NOT pat)`, and `(NOT>
pat)`. `(* pat)` matches any number (including zero) of consecutive sustrings
matching pat; `(+ pat)` matches any number (excluding zero) of consecutive
substrings matching pat; `(not pat)` matches all the substrings that do not
contain pat; and `(not> pat)` matches the longest possible string consisting
of anything BUT **pat**.

For example, we can recognize certain nonsense words:
````console
    #|kno>|# (textmatch '(* {"hum" "dum" "doo" "de"}) "humdumdoodedum")
    #t
````
which uses a choices as the repeated pattern. We can even extract structure
from this nonsense:
````console
    (textextract '(* {"hum" "dum" "doo" "de"}) "humdumdoodedum")
    (* "hum" "dum" "doo" "de" "dum")
````
More interestingly, we can use the `(* pat)` operator to match lists of items
whose length may vary, e.g.
````console
    #|kno>|# (textextract '(* #((isalnum+) {(eol) (isspace+) #("," (isspace+))}))
                           "foo bar baz")
    (* #("foo" " ") #("bar" " ") #("baz" ""))
    #|kno>|# (textextract '(* #((isalnum+) {(eol) (isspace+) #("," (isspace+))}))
                           "foo, bar, baz, quux")
    (* #("foo" #("," " ")) #("bar" #("," " "))
       #("baz" #("," " ")) #("quux" ""))
````
The `(* pat)` operator successfully matches no occurences of its pattern, so
we get the somewhat confusing:
````console
    #|kno>|# (textmatch '(* #((isalnum+) {(eol) (isspace+) #("," (isspace+))}))
                         "")
    #t
````
though it does have some standards:
````console
    #|kno>|# (textmatch '(* #((isalnum+) {(eol) (isspace+) #("," (isspace+))}))
                         ",")
    #f
````
We can use the operator `(+ pat)` for cases where there will always be at
least one instance of the pattern. So, we get
````console
    #|kno>|# (textmatch '(+ #((isalnum+) {(eol) (isspace+) #("," (isspace+))}))
                         "")
    #f
````
but can still handle the single case:
````console
    #|kno>|# (textmatch '(+ #((isalnum+) {(eol) (isspace+) #("," (isspace+))}))
                         "cook")
    #t
````
The `(NOT pat)` operator is apparently simple but hides some complexity. In
its top level usage, it just reverses the behaviour of textmatch:
````console
    #|kno>|# (textmatch '(not (isalpha+)) "good")
    #f
````
## Matching character case

Normally the matcher ignores case when comparing strings, so you have
````console
    (textmatch "Good" "good")
    #t
````
however, the compound operator `(MATCH-CASE pat)` causes a pattern to pay
attention to case, so that you have
````console
    #|kno>|# (textmatch '(match-case "Good") "good")
    #f
````
`(MATCH-CASE pat)` (which can be abbreviated MC) turns on case comparison; the
complementary procedure `(IGNORE-CASE pat)` (which can be abbreviated IC)
turns it back off. So, we can have:
````console
    #|kno>|# (textmatch
                '(match-case #("Good" ", " (ignore-case "BAD") ", " "Ugly"))
                "Good, bad, Ugly")
    #t
````

