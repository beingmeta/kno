;;; -*- Mode: Scheme; text-encoding: utf-8 -*-

(load-component "common.scm")

(applytest "a b c" (glom "a " "b " "c"))
(applytest "a b c" (glom "a " #f "b " "c"))

(applytest "foo" (trim-spaces "  	foo	\n"))

(applytest "I loathe ice cream"
	   string-subst "I love ice cream" "love" "loathe")
(applytest "I loathe ice fishing"
	   string-subst* "I love ice cream"
	   "love" "loathe"
	   "cream" "fishing")

(define ilic "I love ice cream")
(applytest  #"I love ice cream" string->packet ilic)
(applytest  "I love ice cream" packet->string (string->packet ilic))
(applytest #t equal? (->secret ilic) (->secret ilic))

(applytest "Foo bar" capitalize1 "foo bar")
(applytest "Foo Bar" capitalize "foo bar")
(applytest "FOO BAR" upcase "foo bar")
(applytest "foo bar" downcase "FOO BAR")
(applytest #t compound-string? "FOO BAR")
(applytest #t compound-string? "foo bar")

(applytest #t uppercase? "FOO BAR")
(applytest #f lowercase? "FOO BAR")
(applytest #f uppercase? "foo bar")
(applytest #t lowercase? "foo bar")
(applytest #t capitalized? "Foo bar")

(applytest 3 rposition #\- "3–8-s")
(applytest #t empty-string? "")
(applytest #t empty-string? " ")
(applytest #t empty-string? " \t ")

(test-finished "FILEPRIMS")
