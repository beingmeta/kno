;;; -*- Mode: Scheme; Text-encoding: latin-1; -*-

(load-component "common.scm")

(use-module 'texttools)

(applytest #t has-suffix "foo.scm" ".scm")
(applytest #f has-suffix "foo.scm" ".lsp")
(applytest "foo" strip-suffix "foo.scm" ".scm")
(applytest "foo.scm" strip-suffix "foo.scm" ".lsp")
(applytest "foo" strip-suffix "foo.scm" {".scm" ".lsp"})

(applytest #t has-prefix "foo.scm" "foo")
(applytest #f has-suffix "foo.scm" "bar")
(applytest "Darcy" strip-prefix "Mr. Darcy" "Mr. ")
(applytest "Mr. Darcy" strip-prefix "Mr. Darcy" "Mrs. ")
(applytest "Darcy" strip-prefix "Mr. Darcy" {"Mr. " "Mrs. "})
(applytest "Darcy" strip-prefix "Mrs. Darcy" {"Mr. " "Mrs. "})

(applytest #t empty-string? "")
(applytest #t empty-string? "  ")
(applytest #t empty-string? " 	 ")
(applytest #t empty-string? " 	 \n   \n	")
(applytest #f empty-string? "full")

(applytest #t lowercase? "foo")
(applytest #f lowercase? "Foo")

(applytest #t uppercase? "ÇÖØ")
(applytest #f uppercase? "çöø")

(applytest #t uppercase? "FOO")
(applytest #f uppercase? "Foo")

(applytest #t uppercase? "ÇÖØ")
(applytest #f uppercase? "çöø")

(applytest #t capitalized? "FOO")
(applytest #t capitalized? "Foo")
(applytest #f capitalized? "foo")

(applytest #t capitalized? "ÇOO")
(applytest #f capitalized? "çoo")

;(applytest #t numeric? "33")
;(applytest #f numeric? "33.5")
;(applytest #f numeric? "$33.50")
;(applytest #f numeric? "thirty-three")

;;; Suffix tests



(define sample-string
  "The book was over here by the bookcase
which was brightly colored. 3345-9877703-33333
is the other number")

;(applytest #t multi-line? sample-string)
;(applytest #f multi-line? "what's my line")

(applytest 66 isalpha% sample-string)
(applytest 15 isspace% sample-string)

;(applytest "xbary" string-subst "xfooy" "foo" "bar")

(applytest "walk" porter-stem "walked")
(applytest "comput" porter-stem "computation")
(applytest "comput" porter-stem "computing")
(applytest "comput" porter-stem "computer")

(define test-text
  "Bill J. Clinton met with Bill Gates, III in New Orleans, LA and San Diego.")
(define time-text
  "The squad travelled from last Friday to July 20, 1969.")
(define i18n-test-text
  "Danes smuggled Jews over the Öersund to Malmö.")
; (applytest {"Bill J. Clinton" "Bill Gates, III" "New Orleans, LA" "San Diego"}
; 	   refpoints test-text)
; (applytest {"Danes" "Jews" "Öersund" "Malmö"}
; 	   refpoints i18n-test-text)

;(applytest {"Friday" "July 20, 1969"} get-times time-text))

(applytest '("Bill" "J" "Clinton" "met" "with" "Bill" "Gates" "III"
	     "in" "New" "Orleans" "LA" "and" "San" "Diego")
	   getwords test-text)
(applytest '("The" "book" "was" "over" "here" "by" "the"
	     "bookcase" "which" "was" "brightly" "colored"
	     "3345-9877703-33333" "is" "the" "other" "number")
	   getwords sample-string)
(applytest '("Bill" "J" "." "Clinton" "met" "with" "Bill" "Gates" "," "III" 
	     "in" "New" "Orleans" "," "LA" "and" "San" "Diego" ".")
	   getwords test-text #t)
(applytest '("The" "book" "was" "over" "here" "by" "the"
	     "bookcase" "which" "was" "brightly" "colored" "."
	     "3345-9877703-33333" "is" "the" "other" "number")
	   getwords sample-string #t)

(define unicode-boundary-case
  "mud when dry weighed\nonly 6¾ ounces; I kept it covered")

(applytest '("mud" "when" "dry" "weighed" "only" "6¾" "ounces" "I"
	     "kept" "it" "covered")
	   getwords unicode-boundary-case)
(applytest '("mud" "when" "dry" "weighed" "only" "6¾" "ounces" ";"
	     "I" "kept" "it" "covered")
	   getwords unicode-boundary-case #t)

(applytest #t textmatch "goo" "goo")
(applytest #f textmatch "goo" "good")
(applytest 3 textmatcher "goo" "good")

(applytest 4 textmatcher '(greedy (isalpha+)) "good3+")
(applytest 5 textmatcher '(greedy (isalnum+)) "good3+")

(applytest 11 textmatcher
	   '#((isalnum+) (isspace+) "foo") "baz3    foobar")
(applytest #t textmatch
	   '#((isalnum+) (isspace+) "foo") "baz3    foo")

(applytest #t
	   textmatch
	   '#("(define" (isspace+) (lsymbol) (isspace+) (isdigit+) ")")
	   "(define foo-bar3 8)")

(applytest #t textmatch '#("foo" (* "xy") "bar") "fooxybar")
(applytest #t textmatch '#("foo" (* "xy") "bar") "fooxyxybar")
(applytest #f textmatch '#("foo" (* "xy") "bar") "fooxxyybar")
(applytest #t textmatch '#("foo" (* "xy") "bar") "foobar")
(applytest #f textmatch '#("foo" (+ "xy") "bar") "foobar")
(applytest #t textmatch '#("foo" (+ "xy") "bar") "fooxyxybar")
(applytest #t textmatch '#("foo" (not "xy") "xy") "fooabcdefgxy")
(applytest #f textmatch '#("foo" (not "xy") "xy") "fooabcdefg")

(define expr-pat
  (textclosure
   '{(LSYMBOL)
     #("(" (* {#(expr-pat (isspace+)) expr-pat}) ")")
     }))
(applytest #t textmatch expr-pat "(foo bar baz)")
(applytest #f textmatch expr-pat "(foo bar baz")
(applytest #t textmatch expr-pat "(+ 33 44)")
(applytest #t textmatch expr-pat "(+ (* 11 3) 44)")
;; !! (applytest #f textmatch expr-pat "(+ (* 11 3 44)")

;;; Case sensitivity tests

(applytest #f textmatch "abc" "ABC")
(applytest #t textmatch '(IC "abc") "ABC")
(applytest #f textmatch  #("abc" "def") "ABCdef")
(applytest #t textmatch #((ignore-case "abc") "def") "ABCdef")

; (applytest
; 	 '(" " "" "somethhing" "" " " "" "m" "" "a" ""
; 	   "de" "" " " "" "from" "" " " "" "words" "")
; 	 tx-fragment " somethhing made from words"
; 		       '(* (char-not " a\n\r\t")))
; (applytest
; 	 '(" " "" "somethhing" "" " " "" "m" "" "\u00e4"
; 	   "" "de" "" " " "" "from" "" " " "" "words" "")
; 	 tx-fragment " somethhing mäde from words"
; 		       '(* (char-not " ä\n\r\t")))

; (applytest
; 	 '("" " somethhing made from " "http://words.com" "")
; 	 tx-fragment " somethhing made from http://words.com"
; 		       '#({"http://" "ftp://"}
; 			  (+ {(isalnum+) "."})
; 			  { "" #("/" (* (char-not "\"<> \t\n\r"))) }))

; (applytest
; 	 '("na sowas \u00dcberfall")
; 	 tx-fragment "na sowas Überfall"
; 		       '#((isupper) (* (isalnum)) (isupper)))

; (applytest
; 	 '("" "na sowas " "\u00dcberfall" "")
; 	 tx-fragment "na sowas Überfall"
; 		       '#((isupper) (* (isalnum))))

(applytest 0 textsearch '(word (isalpha+)) "word")
(applytest 1 textsearch '(word (isalpha+)) " word")
(applytest 1 textsearch '(word (isalpha+)) " word ")
(applytest #f textsearch '(word (isalpha+)) " word9 ")

(applytest 1 textsearch '(word (isalnum+)) " word9")
(applytest 0 textsearch '(word (isalpha+)) "wörd")
(applytest 1 textsearch '(word (isalpha+)) " åerö")
(applytest 1 textsearch '(word (isalpha+)) " åerö ")
(applytest #f textsearch '(word (isalpha+)) " åerö9 ")
(applytest #f textsearch '(word (isalpha+)) " åerö9")

(applytest #t textmatch '(ID "foobar") "foöbár")
(applytest 3 textmatcher '(ID "foo") "foöbár")
(applytest 3 textsearch '(ID "bar") "foöbár")

(applytest #t textmatch '(IS "George Washington  Carver") "George 
  Washington 	Carver")
(applytest 28 textmatcher
	   '(IS "George Washington  Carver") "George 
  Washington 	Carver was a great man")
(applytest 8 textsearch
	   '(IS "George Washington  Carver") "who was George 
  Washington 	Carver really?")
(applytest "George \n  Washington"
	   gather '(IGNORE-SPACING "George Washington") "who was George 
  Washington 	Carver really?")

(applytest "g\u00e9\u00f6Rg\u00ea WASH\u00cd\u00d1gt\u00f6n"
	   gather '(CANONICAL "George Washington")
	   "who was géöRgê WASHÍÑgtön anyway")

;;; Extraction tests

(define url-pattern
  #("http://"
    (label host (char-not ":/"))
    (label port {"" #(":" (isdigit+))}) 
    (label dir (chunk #("/" (* #((char-not "/") "/")))))
    (label name {"" (char-not "./")})
    (label suffix (chunk {"" #("." (not> (eol)))}))))

(applytest #("" "" "" "foo" "" "")
	   textract #("" "" "" "foo" "" "") "foo")

(applytest #("http://" 
	     (LABEL HOST "www.framerd.org") 
	     (LABEL PORT "") (LABEL DIR "/docs/") 
	     (LABEL NAME "index") 
	     (LABEL SUFFIX ".html"))
	   textract url-pattern "http://www.framerd.org/docs/index.html")
(applytest #("http://" 
	     (LABEL HOST "www.framerd.org") 
	     (LABEL PORT "") (LABEL DIR "/docs/") 
	     (LABEL NAME "index") 
	     (LABEL SUFFIX "."))
	   textract url-pattern "http://www.framerd.org/docs/index.")
(applytest #("http://" 
	     (LABEL HOST "www.framerd.org") 
	     (LABEL PORT "") (LABEL DIR "/docs/") 
	     (LABEL NAME "index") 
	     (LABEL SUFFIX ""))
	   textract url-pattern "http://www.framerd.org/docs/index")	 
(applytest #("http://" 
	     (LABEL HOST "www.framerd.org") 
	     (LABEL PORT "") (LABEL DIR "/docs/") 
	     (LABEL NAME "") 
	     (LABEL SUFFIX ""))
	   textract url-pattern "http://www.framerd.org/docs/")

;;; Text rewrite and substition tests

(applytest "X" textrewrite '(subst (isdigit) "X") "1")
(applytest "X" textrewrite '(subst (isdigit+) "X") "1")
(applytest {} textrewrite '(subst (isdigit) "X") "123")
(applytest "abXXcdefXghiXjXkls t u"
	   textsubst "ab12cdef3ghi4j5kls t u"
	   '(isdigit) "X")

(applytest {} textrewrite '(subst (isalpha) "X") "1")
(applytest "XXX" textrewrite '(* (subst (isdigit) "X")) "123")
(applytest "XXX-YYYY" textrewrite
	   #((* (subst (isdigit) "X")) "-" (* (subst (isdigit) "Y")))
	   "123-4567")
(applytest {} textrewrite
	   #((* (subst (isdigit) "X")) "-" (* (subst (isdigit) "Y")))
	   "123")

(applytest "X" textsubst "1" '(subst (isdigit+) "X"))
(applytest "XXXX" textsubst "1234" '(subst (isdigit) "X"))
(applytest "aXbcXdeXfXgh" textsubst "a1bc2de3f4gh" '(subst (isdigit) "X"))

;;; Try with second arg
(applytest "X" textsubst "1" '(subst (isdigit+) "X"))
(applytest "X" textsubst "1" '(isdigit+ )'(subst (isdigit+) "X"))
(applytest "X" textsubst "1" '(isdigit+) "X")

(applytest "XXXX" textsubst "1234" '(subst (isdigit) "X"))
(applytest "XXXX" textsubst "1234" '(isdigit) '(subst (isdigit) "X"))
(applytest "XXXX" textsubst "1234" '(isdigit) '(subst (isdigit) "X"))
(applytest "XXXX" textsubst "1234" '(isdigit) "X")

(applytest "aXbcXdeXfXgh" textsubst "a1bc2de3f4gh" '(subst (isdigit) "X"))
(applytest "aXbcXdeXfXgh" textsubst "a1bc2de3f4gh" '(isdigit) '(subst (isdigit) "X"))
(applytest "aXbcXdeXfXgh" textsubst "a1bc2de3f4gh" '(isdigit) "X")

(define ndtestsubst (ambda (s pat) (textsubst s (qc pat))))
(define testsubst (lambda (s pat) (textsubst s (qc pat))))

(applytest {"aXbcX" "aYbcY"}
	   textsubst "a1bc2"
	   '{(subst (isdigit) "X") (subst (isdigit) "Y")})
(applytest {"aXbcX" "aYbcY"}
	   testsubst "a1bc2"
	   '{(subst (isdigit) "X") (subst (isdigit) "Y")})
(applytest {"aXbcX" "aXbcY" "aYbcX" "aYbcY"}
	   ndtestsubst "a1bc2"
	   '{(subst (isdigit) "X") (subst (isdigit) "Y")})

(applytest "COOkIng Is An ExcItIng ActIvIty"
	   textsubst "Cooking is an exciting activity"
	   '(IC (subst {"A" "E" "I" "O" "U"} upcase)))
(applytest "COOkIng Is An ExcItIng ActIvIty"
	   textsubst "Cooking is an exciting activity"
	   `(IC (subst {"A" "E" "I" "O" "U"} ,upcase)))

(applytest "Co!o!ki!ng i!s a!n e!xci!ti!ng a!cti!vi!ty"
	   textsubst "Cooking is an exciting activity"
	   '(IC (subst {"A" "E" "I" "O" "U"} string-append "!")))
(applytest "Co!o!ki!ng i!s a!n e!xci!ti!ng a!cti!vi!ty"
	   textsubst "Cooking is an exciting activity"
	   `(IC (subst {"A" "E" "I" "O" "U"} ,string-append "!")))

;;; Unicode tests

(applytest 3 textmatcher "xöx" "xöxöy")
(applytest 4 textsearch "xöx" "fooÿxöxöy")

(applytest '("föö" "bár" "bèç") getwords "föö bár bèç")

;; This used to signal bad utf-8 string because it got lost in the
;; middle of the string.
(applytest #t textmatch '(* (not "(")) "ab\u00d1cd")

(define msg-text (filestring (get-component "rfc822.txt")))

(applytest #X"c0bd4ec937cfd6780ea29c4ba6cf3074" md5 sample-string)
(applytest #X"445cb4036c4dfac7ecf51f540aaff47f" md5 msg-text)

(define msg-headers (car (textslice msg-text "\n\n")))
(define msg-body
  (apply append (cdr (textslice msg-text "\n\n" #t))))
(applytest 185 textsearch "\n\n" msg-text)

(define badpat 
  `#((label ndashes #("--" (+ "-")) length) (not> "-") #("--" (+ "-"))))
(define goodpat 
  `#((label ndashes #("--" (+ "-")) ,length) (not> "-") #("--" (+ "-"))))
(evaltest 'error (onerror (text->frames badpat "----FOO-----")
		   (lambda (ex) 'error)))
(applytest #[NDASHES 4] text->frames goodpat "----FOO-----")

; (testing 'rfc822-tx '(get msg 'content) msg-body)

(define header-pat '(* {(char-not "\n") #("\n" (hspace))}))
(applytest {"Date: Sun, 22 Apr 90 15:10:14 EDT"
	    "Received: by media-lab (5.57/4.8)  id AA10965; Sun, 22 Apr 90 15:10:16 EDT"
	    "From: alanr"
	    "To: mt, dfr, kwh"
	    "Subject: funny line in a .sig\n   and in subject"}
	   gather header-pat msg-headers)
(applytest '{(* "From: alanr")
	     (* "To: mt, dfr, kwh")
	     (* "Subject: funny line in a .sig" #("\n" " ") "  and in subject")
	     (* "Date: Sun, 22 Apr 90 15:10:14 EDT")
	     (* "Received: by media-lab (5.57/4.8)  id AA10965; Sun, 22 Apr 90 15:10:16 EDT")}
	   textract header-pat (gather header-pat msg-headers))

(applytest "the story began in a little town by the name of of san diego"
	   stdstring "\t\tThe story began in a little town by the name of of\n\tSan Diego")
(applytest "The story began in a little town by the name of of San Diego"
	   stdspace
	   "\t\tThe story began in a little town by the name of of\n\tSan Diego")

;;; STDCAP testing

(applytest "find my phone" stdcap "find my phone")
(applytest "Find My Phone" stdcap "Find my phone")
(applytest "Find My Phone" stdcap "Find my Phone")
(applytest "Find My Phone" stdcap "find my Phone")
(applytest "find my iPhone" stdcap "find my iPhone")
(applytest "Find My iPhone" stdcap "Find my iPhone")

;;;; Mime parsing

; (define msg (read-mime msg-text))

; (testing 'parse-rfc822 '(equal? msg (read-dtype-from-file (get-component "rfc822.dtype")))
; 	 #t)
; (testing 'fget-rfc822 '(get msg 'date) "Sun, 22 Apr 90 15:10:14 EDT")

; (testing 'parse-timestring
; 	 '(breakup-timestamp (parse-timestring (get msg 'date)))
; 	 #[TYPE TIMESTAMP
; 		YEAR 1990
; 		MONTH 4
; 		DAY 22
; 		HOUR 15
; 		MINUTE 10
; 		SECOND 14
; 		TZOFF -14400]
; 	 )
; (testing 'parse-timestring
; 	 '(write-to-string (parse-timestring (get msg 'date)))
; 	 "#<1990-04-22T15:10:14-4:00>")
; (testing 'iso-timestring
; 	 '(iso-timestring (parse-timestring (get msg 'date)) 'utc)
; 	 "1990-04-22T19:10:14UTC")
; (testing 'iso-timestring
; 	 '(breakup-timestamp (parse-timestring
; 			      (iso-timestring (parse-timestring (get msg 'date))
; 					      'utc)))
; 	 #[TYPE TIMESTAMP
; 		YEAR 1990
; 		MONTH 4
; 		DAY 22
; 		HOUR 19
; 		MINUTE 10
; 		SECOND 14
; 		TZOFF 0]
; 	 )

;; This should avoid a leak warning
(define expr-pat #f)

(test-finished "TEXTTEST")

