# String functions

## Simple but Handy

In addition to the facilities above, the text library (or Kno itself)
include some handling functions for text strings:

(has-suffix suffix string)

    returns true if string ends in suffix
(has-prefix prefix string)

    returns true if string starts with prefix
(uppercase? string)

    returns true `#t` if string has no lowercase characters
(lowercase? string)

    returns true `#t` if string has no uppercase characters
(capitalized? string)

    returns true `#t` if the first character of string is uppercase
(multi-line? string)

    returns true `#t` if string contains newlines
(numeric? string)

    returns true `#t` if string contains only numeric or punctuation characters
(empty-string? string)

    returns true `#t` if string has no characters
(whitespace% string)

    returns the percentage (an integer from 0 to 99) of characters in string which are whitespace
(alphabetic% string)

    returns the percentage (an integer from 0 to 99) of characters in string which are alphabetic characters

