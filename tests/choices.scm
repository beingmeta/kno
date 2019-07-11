;;; -*- Mode: scheme; text-encoding: utf-8; -*-

(load-component "common.scm")

(optimization-leaks)

(define-tester (random-choice n (numrange))
  (default! numrange (config 'fixmax (* n 8)))
  (when (and (config 'int_max) (> numrange (config 'int_max)))
    (set! numrange (config 'int_max)))
  (let ((answer {}))
    (dotimes (i n) (set+! answer (random numrange)))
    (while (< (choice-size answer) n)
      (dotimes (i (* 2 (- n (choice-size answer))))
	(set+! answer (random numrange))))
    (pick-n answer n)))

(define-tester (nrange start end)
  (let ((answer {}))
    (dotimes (i (- end start))
      (set+! answer (+ start i)))
    answer))

(define-tester (nrange-r start end)
  (if (= start end) {}
      (choice start (nrange-r (+ start 1) end))))

(define-tester (srange cstart cend)
  (let ((start (char->integer cstart)) (end (char->integer cend)))
    (let ((answer {}))
      (dotimes (i (- end start))
	(set+! answer (string (integer->char (+ start i)))))
      answer)))

(evaltester 100 (choice-size (intersection (nrange 0 300) (nrange 200 800))))
(applytester (choice 20 21 22 23 24 25 26 27 28 29) intersection
	     (nrange 0 30) (nrange 20 40))

(applytester (choice 0 1 2 3 4 10 11 12 13 14) union (nrange 0 5) (nrange 10 15))

(evaltester 500 (choice-size (difference (nrange 200 800) (nrange 0 300))))
(applytester (choice 0 1 4 5 6 7 8 9) difference (nrange 0 10) (nrange 2 4))

(define (pairup x) (cons x x))

(evaltester 10 (choice-size (pairup (nrange 0 10))))
(applytester '{(0 . 0) (1 . 1) (2 . 2) (3 . 3) (4 . 4)}
	     pairup (nrange 0 5))

(let ((bigrange (nrange 1000 2000)))
  (evaltester 1000 (choice-size bigrange)))

(evaltester 19 (choice-size (nrange 0 (nrange 0 20))))
(applytester {0 1 2 3} nrange 0 (nrange 0 5))

(evaltester 6 (choice-size (elts (elts '((a b c) (d e f) (a b c))))))
(applytester '{a b c d e f}
	     elts (elts '((a b c) (d e f) (a b c))))

(applytester '{10 11 12 13 14 15 16 17 18 19}
	     nrange 10 20)
(applytester '{"a" "b" "c" "d" "e" "f" "g" "h" "i" "j" "k" "l" "m" "n" "o"
	       "p" "q" "r" "s" "t" "u" "v" "w" "x" "y"}
	     srange #\a #\z)
(applytester '{"0" "1" "2" "3" "4" "5" "6" "7" "8"}
	     srange #\0 #\9)
(applytester '{10 11 12 13 14 15 16 17 18 19} nrange-r 10 20)

(applytester '{a b c d e} elts '(a b c d e))
(applytester '{x y z} elts (choice->list (choice 'x 'y 'z)))

(applytester #t empty? (choice))
(evaltester {"three" "four" "five"} (intersection {"three" "four" "five"}))
(evaltester {"three" "four" "five"} (union {"three" "four" "five"}))
(evaltester "three" (difference {"three" "four" "five"}  {"four" "five"}))
(evaltester {"three" "six"} (difference {"three" "four" "five" "six"}  {"four" "five"}))
(applytester #t empty? (intersection (choice 1 2 3) (choice 4 5 6)))
(applytester #f empty? (intersection (choice 1 2 3 4) (choice 4 5 6 7)))
(applytester #t exists? (intersection (choice 1 2 3 4) (choice 4 5 6 7)))
(applytester #f exists? (intersection (choice 1 2 3) (choice 4 5 6)))
(applytester #t < (pick-one (choice 1 2 3)) 4)

(begin (define state-list-var '())
  (define state-set-var (choice)))

(do-choices (each (nrange 0 200))
  (set! state-list-var (cons each state-list-var))
  (set+! state-set-var each))
(do-choices (each (nrange 0 50))
  (set! state-list-var (cons each state-list-var))
  (set+! state-set-var each))

;;; Test homogenous/heterogenous merges
(define homogenous-choices
  #({1 2 3} {"ONE" "TWO" "THREE"} {une deux trois}))
(define hresult {})
(evaltester #t
	    (begin (set! hresult (elt homogenous-choices (choice 0 1 2)))
	      (stringout hresult)
	      #t))

(define (convert-arg x)
  (if (number? x) (* x 2)
      (if (string? x) (string->symbol x)
	  (if (symbol? x) (symbol->string x)
	      x))))
(evaltest
 '{1 2 3 une deux trois "ONE" "TWO" "THREE"
   2 4 6 |ONE| |TWO| |THREE| "une" "deux" "trois"}
 (begin (do-choices (x hresult)
	  (set+! hresult (convert-arg x)))
   hresult))

(evaltester 250 (length state-list-var))
(evaltester 200 (choice-size state-set-var))

(evaltester {0 2 4 6 8 10 12 14 16 18}
	    (filter-choices (v (nrange 0 20)) (even? v)))
(evaltester {0 2 4 6 8 10 12 14 16 18 20 22 24 26 28 30 32 34 36 38}
	    (for-choices (v (nrange 0 20)) (* 2 v)))
(evaltester {4 6 8 9 12 16} (let ((x (choice 2 3 4))) (* x x)))

;;; Subset tests

(define choicelist '())

(do-subsets (x {1 2 3 4 5 6 7 8 9} 3)
  (set! choicelist (cons (qc x) choicelist)))
(applytester 3 length choicelist)
(applytester '(3 3 3) map choice-size choicelist)

(set! choicelist '())

(define fnlist '())

(do-subsets (x (number->string {1 2 3 4 5 6 7 8 9}) 3)
  (set! fnlist (cons (lambda () x) fnlist)))

(applytester 3 choice-size ((car fnlist)))
(applytester 3 choice-size ((cadr fnlist)))

(set! fnlist '())

;;; QCHOICE GC regressions

;;; This addresses a bug where the binding of an argument is modified
;;; during a call. The issue was that lambdas don't copy their args
;;; into the environment, so modifying a value (at least in a way
;;; which causes the existing value to be freed) ends up freeing that
;;; value twice: once on modify and once when the call ends and the
;;; arguments are freed. The fix was indirect; making schemap
;;; modifications (which are used for environments) be careful when
;;; the schemap has schemap_stackvals==1.

(define (getcars values)
  (set! values (car values))
  (cons 'car values))

(define (qc-wrapper1)
  (let* ((vals (cons '{y z} '{a b}))
	 (carred (getcars vals)))
    (pprint carred)))
(define (qc-wrapper2)
  (let* ((vals (cons '{y z} '{a b}))
	 (carred (getcars (qc vals))))
    (pprint carred)))

(qc-wrapper1)
(qc-wrapper2)

;;; Pick tests

(applytester {10 11 12} pick> (choice 5 6 7 10 11 12) 9)
(applytester {10.5 11 12} pick> (choice 5 6.3 7 10.5 11 12) 9)
(applytester {10.5 11 12} pick> (choice "string" 5 6.3 7 10.5 11 12) 9)
(applytester {10.5 11 12} pick> (choice "string" 5 6.3 7 10.5 11 12) {8 9})
(errtest (pick> {1 2 "three"} "one"))
(errtest (pick> (choice "string" 5 6.3 7 10.5 11 12) 9 #t))

(applytester @1/0 pickoids (choice "foo" 8 @1/0))
(applytester {@1/0 @1/88} pickoids (choice "foo" 8 @1/0 @1/88))
(applytester {@1/0 @1/88} pickoids (choice @1/0 @1/88))

(applytester '{foo bar} picksyms (choice 'bar "foo" 8 'foo))
(applytester '{foo bar} picksyms (choice 'bar (fail) 'foo))

(applytester '{"foo" "baz"} pickstrings (choice 'bar "foo" 8 'foo "baz"))
(applytester '{"foo" "baz"} pickstrings (choice "foo" (fail) "baz"))
(applytester '{foo bar} picksyms (choice 'bar (fail) 'foo))

(applytester #(a "b") pickvecs (choice #(a "b") 'foo "foo" 8))
(applytester {#(a "b") #("c" d)} pickvecs (choice #(a "b") 'foo "foo" #("c" d) 8))
(applytester {#(a "b") #("c" d)} pickvecs (choice #(a "b") (fail) #("c" d) (fail)))

(applytester '(a . "b") pickpairs (choice '(a . "b") 'foo "foo" 8))
(applytester {'(a . "b") '("c" . d)} pickpairs (choice '(a . "b") 'foo "foo" '("c" . d) 8))
(applytester {'(a . "b") '("c" . d)} pickpairs (choice '(a . "b") (fail) '("c" . d) (fail)))

(applytester 8 picknums (choice "foo" 8 @1/0))
(applytester {8 1.5 1/2} picknums (choice "foo" 8 @1/0 1.5 1/2))
(applytester {8 1.5 1/2} picknums (choice (fail) 8 (fail) 1.5 1/2))

(define z1 #[b "b"])
(applytester z1 pickmaps (choice #(a "b") z1 'foo "foo" 8))
(applytester {z1 #[a "five"]} pickmaps (choice z1 #[a "five"]))
(applytester {z1 #[a "a"]} pickmaps (choice #(a "b") z1 'foo "foo" #("c" d) #[a "a"] 8))

;;; Reduce-choice tests

(applytester 15 reduce-choice + {1 2 3 4 5})
(applytester 21 reduce-choice + {1 2 3 4 5} 6)
(applytester 21 reduce-choice + {1 2 3 4 5 6})

(applytester 2432902008176640000
	     reduce-choice * (nrange 1 21))
(applytester 362880 reduce-choice * (nrange 1 10))

(applytester 1 reduce-choice min {1 2 3 4 5 6})
(applytester 6 reduce-choice max {1 2 3 4 5 6})

(applytester 12 reduce-choice + {3 4 5 5 4})
(applytester 12 reduce-choice + {"3" "4" "5"} 0 string->number)
(applytester 12 reduce-choice + {"3" "4" "5" "5"} 0 string->number)
(applytester 17 reduce-choice + {"3" "4" "5" "+5"} 0 string->number)

;;; CHOICE-MAX tests

(applytester {} choice-max (nrange 1 10) 5)
(applytester (nrange 1 5) choice-max (nrange 1 5) 5)
(applytester 8 choice-max 8 5)

(applytester (nrange 1 10) choice-min (nrange 1 10) 5)
(applytester {} choice-min (nrange 1 4) 5)
(applytester {} choice-min 8 5)

;;; ND apply

(define-amb-tester (lnd x y) (list (qc x) (qc y)))
(applytester '({3 4} 5) apply lnd (qc {3 4}) (list 5))

;;; Combo apply

(define-tester (makepair x y)
  (cons (qc x) (qc y)))
(evaltester '(3 . 4) (makepair 3 4))
(evaltester '{} (makepair 3 {}))
(evaltester '(3 . {}) (makepair 3 '#{}))

;;; Set operations

(define-tester (set-same? x y) (identical? (elts x) (elts y)))
(define-tester (set-overlaps? x y) (overlaps? (elts x) (elts y)))

(applytester #t set-same? '(a b c) #(a b c))
(applytester #t set-same? '(a b c) #(b a c))
(applytester #f set-same? '(a b d) #(a b c))
(applytester #f set-same? '(a b d) '(a b c))

(applytester #t set-overlaps? '(a b c) #(a b c d))
(applytester #t set-overlaps? '(a b) #(b a c))
(applytester #f set-overlaps? '(a b c) #(x y z))
(applytester #f set-overlaps? '(a c d) '(p q r))
(applytester #t set-overlaps? '(a c d) '(p q r d))

;;; Just try

(evaltester {} (try))
(errtest (try (fail) (+ 2 "3")))
(errtest (try (fail) (if (zero? 1) #t)))

(applytester #t satisfied? (= 3 3))
(applytester #f satisfied? (= 3 4))
(applytester #f satisfied? (= 3 (+ {} 1)))

(applytester {} pick-one {})

;;; Try-choices

(evaltester 9 (try-choices (e {4 6 8 9} i) (tryif (odd? e) e)))
(evaltester {} (try-choices (e {} i) (tryif (odd? e) e)))
(evaltester {} (try-choices (e {4 6 8 9} i) (if (> e 5) (break) (if (odd? e) e (fail)))))

(errtest (try-choices))
(errtest (try-choices spec))
(errtest (try-choices (e)))
(errtest (try-choices (e . cdr)))
(errtest (try-choices (e {4 6 8 9}) . noexprs))
(errtest (try-choices (e {9 nosuchvalue}) e))
(errtest (try-choices (e {4 6 8 9} . cdr) (tryif (odd? e) (string->symbol e))))
(errtest (try-choices (e {4 6 8 9} "i") (tryif (odd? e) (string->symbol e))))
(errtest (try-choices (e {4 6 8 9} i) (tryif (odd? e) (string->symbol e))))

(errtest (do-choices))
(errtest (do-choices spec))
(errtest (do-choices ()))
(errtest (do-choices (e)))
(errtest (do-choices (e . cdr)))
(errtest (do-choices (e {4 6 8 9}) . noexprs))
(errtest (do-choices (e {9 nosuchvalue}) e))
(errtest (do-choices (e {4 6 8 9} . cdr) (tryif (odd? e) (string->symbol e))))
(errtest (do-choices (e {4 6 8 9} "i") (tryif (odd? e) (string->symbol e))))
(errtest (do-choices (e {4 6 8 9} i) (tryif (odd? e) (string->symbol e))))

(errtest (for-choices))
(errtest (for-choices spec))
(errtest (for-choices ()))
(errtest (for-choices (e)))
(errtest (for-choices (e . cdr)))
(errtest (for-choices (e {4 6 8 9}) . noexprs))
(errtest (for-choices (e {9 nosuchvalue}) e))
(errtest (for-choices (e {4 6 8 9} . cdr) (tryif (odd? e) (string->symbol e))))
(errtest (for-choices (e {4 6 8 9} "i") (tryif (odd? e) (string->symbol e))))
(errtest (for-choices (e {4 6 8 9} i) (tryif (odd? e) (string->symbol e))))

(errtest (filter-choices))
(errtest (filter-choices spec))
(errtest (filter-choices ()))
(errtest (filter-choices (e)))
(errtest (filter-choices (e . cdr)))
(errtest (filter-choices (e {4 6 8 9}) . noexprs))
(errtest (filter-choices (e {9 nosuchvalue}) e))
(errtest (filter-choices (e {4 6 8 9} . cdr) (tryif (odd? e) (string->symbol e))))
(errtest (filter-choices (e {4 6 8 9} "i") (tryif (odd? e) (string->symbol e))))
(errtest (filter-choices (e {4 6 8 9} i) (tryif (odd? e) (string->symbol e))))

(evaltester {101 102 103} (for-choices (e {1 2 3} i) (+ e 100)))

(evaltester {"foobar" "zbar" "dbar"}
	    (let ((string {"foo" "z" "d"}))
	      (for-choices string (glom string "bar"))))
(errtest (let ((string {"foo" "z" "d"}))
	   (for-choices xyzzys (glom string "bar"))))

(errtest (do-choices (e {1 2 3} i) (+ e "delta")))
(errtest (do-choices (e {1 2 3}) . err))

(evaltester {2 3 4} (for-choices (e {1 2 3} i) (+ e 1)))
(evaltester {4 6} (for-choices (e {4 6 8 9} i) (if (> e 6) (break) e)))
(errtest (for-choices (e {4 6 'eight 9} i) (+ e 1)))

(errtest (do-choices (e {1 2 3}) . err))
(errtest (do-choices (e {3 unbound}) . err))
(errtest (do-choices (e)))
(errtest (do-choices ("e" {3 "four"}) e))
(errtest (do-choices))
(errtest (do-choices ("e" {1 2 3}) . err))
(errtest (do-choices (e {1 2 3} "i" ) . err))
(errtest (do-choices (e (+ 2 "three") ) e))

(errtest (for-choices (e {1 2 3}) . err))
(errtest (for-choices (e {3 unbound}) . err))
(errtest (for-choices (e {3 unbound}) e))
(errtest (for-choices (e {3 5}) . err))
(errtest (for-choices (e)))
(errtest (for-choices ("e" {3 "four"}) e))
(errtest (for-choices (e {1 2 3} "i" ) . err))
(errtest (for-choices))

(errtest (filter-choices (e {1 2 3}) (string->symbol e)))
(errtest (filter-choices (e {1 2 3}) . err))
(errtest (filter-choices (e {3 unbound}) . err))
(errtest (filter-choices (e {3 unbound}) e))
(errtest (filter-choices (e {3 5}) . err))
(errtest (filter-choices ("e" {3 "four"}) e))
(errtest (filter-choices (e)))
(errtest (filter-choices (e {1 2 3} "i" ) . err))
(errtest (filter-choices))

(evaltest {} (filter-choices (s (difference "foo" "foo")) s))

(evaltester 2 (filter-choices (e {1 2 3} i) (even? e)))
(evaltester {1 3} (filter-choices (e {1 2 3} i) (odd? e)))

(evaltester 1 (filter-choices (e {1 2 3} i) (if (> e 2) (break) (odd? e))))

(evaltester {4 6} (for-choices (e {4 6 8 9} i) (if (> e 6) (break) e)))
(errtest (for-choices (e {4 6 'eight 9} i) (+ e 1)))

;;; Choice refs

(applytest 2 %choiceref {0 1 2 3 4} 2)
(errtest (%choiceref {} 5))
(errtest (%choiceref {0 1 2 3 4} "two"))
(errtest (%choiceref {0 1 2 3 4} 11))

;;; Smallest and largest

(applytest "eleven" largest {"one" "two" "eleven" "eight"} length)
(applytest {"one" "two"} smallest {"one" "two" "eleven" "eight"} length)

(applytest {"thirteen" "fourteen"} 
	   largest {"one" "two" "eleven" "eight" "thirteen" "fourteen"} 
	   length)
(define (bad-magnitude string)
  (elt (length string) 3))

(errtest (largest {"one" "two" "eleven" "eight" "thirteen" "fourteen"} 
		  bad-magnitude))
(errtest (smallest {"one" "two" "eleven" "eight" "thirteen" "fourteen"} 
		   bad-magnitude))

;;; QChoices

(evaltest #t (eval `(qchoice? #{a b c})))
(evaltest #{} (qchoice (fail) (fail) (fail)))
(evaltest {} (qchoicex (fail) (fail) (fail)))
(evaltest "string" (qchoicex "string"))
(evaltest #{"foo" "bar" "baz"} (qchoicex "foo" {"bar" "baz"} "foo"))

(errtest (qchoice?))

;;; IFEXISTS

(evaltest 3 (ifexists 3))
(evaltest {3 4} (ifexists (choice 3 4)))
(evaltest 'void (ifexists (fail)))
(errtest (ifexists))
(errtest (ifexists (* 3 "four")))
(errtest (ifexists (* 3 "four") "nine"))

(evaltest 3 (whenexists 3))
(evaltest {3 4} (whenexists (choice 3 4)))
(evaltest 'void (whenexists (fail)))
(errtest (whenexists))
(evaltest 'void (whenexists (* 3 "four")))

(define (exists-tests (existsfn exists))
  (applytest #t existsfn even? {3 4 5})
  (applytest #f existsfn odd? {2 4 6})
  (applytest #f existsfn odd? {2 4 6})
  (applytest #f existsfn even? {})

  (applytest #t existsfn empty-string? {3 "four" " "})
  (applytest #f existsfn empty-string? {3 "four" "five"})

  (errtest (existsfn if {3 "four" "five"}))
  (errtest (existsfn 3 {3 "four" "five"}))

  (applytest #t existsfn > {3 4 5} 4)
  (applytest #f existsfn > {3 4 5} 7)
  (errtest (existsfn > {3 4 5} (2+ 3)))

  (applytest #t existsfn string>=? {"zebra" "apple" "bee"} "mandolin")
  (applytest #f existsfn string>=? {"fish" "apple" "bee"} "mandolin")
  (applytest #f existsfn > {3 4 5} 7))
(exists-tests)
(exists-tests sometrue)

;;; Other stuff

(applytest {0 1 2 3 4} getrange 5)
(applytest {0 1 2 3 4} getrange 0 5)
(applytest {0 1 2 3 4} getrange 5 0)
(applytest {-5 -4 -3 -2 -1} getrange -5 0)
(errtest (getrange "x"))
(errtest (getrange "zero" "eight"))
(errtest (getrange 0  "eight"))

;;; Getrange

(applytest {0 1 2 3} getrange 0 4)
(applytest {0 1 2 3} getrange 4)
(applytest 'err getrange 0.9 4)
(applytest 'err getrange 4 0.9)

;;; Select

(applytest {115 116 117 118 119} pick-max (getrange 0 120) 5)
(applytest {0 1 2 3 4} pick-min (getrange 0 120) 5)

(applytest #(119 118  117 116 115) max/sorted (getrange 0 120) 5)
(applytest #(0 1 2 3 4) min/sorted (getrange 0 120) 5)

;;; Exists/Forall

(applytest #t exists = {3 4 5} {2 4 8})
(applytest #f forall = {3 4 5} {2 4 8})
(applytest #f forall = {3 4 5} {2 4 8})
(applytest #t forall < {3 4 5} {6 7 8})
(applytest #t forall < {} {6 7 8})
(applytest #f exists number? {})
(applytest #t forall number? {})

;; We make the numbers floating point to ensure that we process the
;; symbol argument first
(applytest 'err forall < {3.0 4.0 5.0} {6.0 "seven" 8.0})
(applytest 'err exists < {3.0 4.0 5.0} {6.0 'seven 8.0})
(applytest 'err forall "gt" {3.0 4.0 5.0} {6.0 'seven 8.0})
(applytest 'err exists "gt" {3.0 4.0 5.0} {6.0 'seven 8.0})
(applytest #t forall/skiperrs < {3 4 5} {6 "seven" 8})
(applytest #t exists/skiperrs < {3 4 5} {6 "seven" 8})

(applytester {"a" "b" "c"} simplify {"a" "b" "c"})
(applytester #t sometrue = 3 {1 2 3})
(applytester #f sometrue = 3 (fail))
(applytester #f sometrue = 3 {4 5})
(applytester #t sometrue = 3 3)

(define (float=? x y)
  (if (and (flonum? x) (flonum? y))
      (< (abs (- y x)) 0.0001)
      (if (fixnum? x)
	  (irritant y 'notfixnum)
	  (irritant x 'notfixnum))))

(applytester #t sometrue float=? 3.0 {1.0 2.0 3.0})
(applytester #f sometrue float=? 3.0 (fail))
(applytester #f sometrue float=? 3.0 {4. 5.0})
(applytester #t sometrue float=? 3.0 3.0)
;; This is only consistent because 3 comes first in the choice {3 4.0}
(applytester 'err sometrue float=? 4.0 {3 4.0})
(applytester #t sometrue/skiperrs float=? 4.0 {3 4.0})
(applytester 'err exists float=? 4.0 {3 4.0})
(applytester #t exists/skiperrs float=? 4.0 {3 4.0})


(applytester #() choice->vector {})
(applytester #("a" "b") choice->vector {"a" "b"})
(applytester #("a") choice->vector "a")
(applytest #("one" "eight" "eleven") choice->vector {"one" "eleven" "eight"} length)

;;; Pick/sample

(define a-random-choice
  '{5 "five" #(1 2 3 4 5) 3/2 6/4 11.5 symbol})

(set+! a-random-choice (list a-random-choice))
(set+! a-random-choice (vector a-random-choice))

(applytest 'err pick-n a-random-choice -5)
(applytest 'err pick-n a-random-choice 3 (* 1024 1024 1024 1024 17))
(applytest {} pick-n a-random-choice 0)
(applytest {} sample-n a-random-choice 0)
(applytest 1 choice-size (pick-one a-random-choice))
(applytest 3 choice-size (pick-n a-random-choice 3))
;; By default, this always replicates
(applytest (pick-n a-random-choice 3 0) pick-n a-random-choice 3 0)
(applytest (pick-n a-random-choice 3 2) pick-n a-random-choice 3 2)
;; These will very rarely return true, sorry :)
(applytest #f identical? (sample-n a-random-choice 2) (sample-n a-random-choice 2))
(applytest #t identical? (pick-n a-random-choice 3 0) (pick-n a-random-choice 3 0))

(applytest a-random-choice pick-n a-random-choice 100)
(applytest a-random-choice sample-n a-random-choice 100)

(applytest overlaps? (pick-n a-random-choice 3 0) (pick-n a-random-choice 3 1))
(applytest singleton? pick-n a-random-choice 1 3)
(applytest singleton? pick-n a-random-choice 1 3)
(applytest 'err pick-n a-random-choice "foo" 3)
(applytest 'err pick-n a-random-choice 3 "one")
(applytest "string" pick-n "string" 3)
(applytest 'err pick-n "string" "3")
(applytest "string" sample-n "string" 3)
(applytest 'err sample-n "string" "3")
(applytest 'err pick-n a-random-choice 3 "one")

(applytest 'err sample-n a-random-choice "five")
(applytest 'err sample-n a-random-choice 'five)
(applytest 'err sample-n a-random-choice 5.0)
(applytest 'err sample-n a-random-choice -1)

(applytest 'err pick-n a-random-choice "five")
(applytest 'err pick-n a-random-choice 'five)
(applytest 'err pick-n a-random-choice 5.0)
(applytest 'err pick-n a-random-choice -5)

(applytest {} pick-n {} 8)
(applytest {} sample-n {} 8)
(applytest "string" pick-n "string" 8)
(applytest "string" sample-n "string" 8)
(applytest "string" pick-n "string" 1)
(applytest "string" sample-n "string" 1)

;;; Sort and select

(define string-choice 
  {"max-fixnum" "config-def!" "test-u8raise" 
   "table-minimize!" "use-threadcache"})
(applytest #("config-def!" "max-fixnum" "table-minimize!" "test-u8raise" "use-threadcache")
	   sorted string-choice)
(applytest #("max-fixnum" "config-def!" "test-u8raise" "table-minimize!" "use-threadcache")
	   sorted string-choice length)
(applytest #("use-threadcache" "test-u8raise" "table-minimize!" "max-fixnum" "config-def!")
	   rsorted string-choice)
(applytest #("use-threadcache" "table-minimize!" "test-u8raise" "config-def!" "max-fixnum")
	   rsorted string-choice length)

(define (mixed-length x (len)) (set! len (length x)) (if (even? len) len (- len)))

(define (length-errfn string) (irritant string 'just-because))
(define (length-voidfn string) (if (= 3 4) 1))

(applytest "use-threadcache" largest string-choice)
(applytest { "table-minimize!" "use-threadcache" } largest string-choice length)
(applytest "config-def!" smallest string-choice)
(applytest "max-fixnum" smallest string-choice length)

(applytest "test-u8raise" largest string-choice mixed-length)
(applytest { "table-minimize!" "use-threadcache" } smallest string-choice mixed-length)

(applytest 'err largest string-choice length-errfn)
(applytest 'err smallest string-choice length-errfn)

(define length-slotmap 
  (let ((map #[]))
    (do-choices (string string-choice)
      (store! map string (length string)))
    map))

(define length-schemap 
  (let ((map #[]))
    (do-choices (string string-choice)
      (store! map string (length string)))
    (->schemap map)))
(define length-hashtable
  (let ((map (make-hashtable)))
    (do-choices (string string-choice)
      (store! map string (length string)))
    map))

(applytest 'err largest string-choice #"packet")
(applytest 'err smallest string-choice #"packet")
(applytest 'err largest string-choice length-errfn)
(applytest 'err smallest string-choice length-errfn)
(applytest 'err largest string-choice length-voidfn)
(applytest 'err smallest string-choice length-voidfn)

(applytest {} largest {})
(applytest {} smallest {})
(applytest "foo" largest "foo")
(applytest "foo" smallest "foo")

(applytest { "table-minimize!" "use-threadcache" } largest string-choice length-slotmap)
(applytest "max-fixnum" smallest string-choice length-slotmap)

(applytest { "table-minimize!" "use-threadcache" } largest string-choice length-schemap)
(applytest "max-fixnum" smallest string-choice length-schemap)

(applytest { "table-minimize!" "use-threadcache" } largest string-choice length-hashtable)
(applytest "max-fixnum" smallest string-choice length-hashtable)

(define many-strings
  (let ((result {}))
    (dotimes (i 50) (set+! result (make-string i #\x)))
    result))
(define many-strings-vec (sorted many-strings length))
(define many-strings-len (length many-strings-vec))

(applytest {} pick-min {} 4)
(applytest {} pick-max {} 4)
(applytest #() max/sorted {} 4)
(applytest #() min/sorted {} 4)
(applytest "foo" pick-max "foo" 4)
(applytest "foo" pick-max "foo" 4)
(applytest #("foo") max/sorted "foo" 4)
(applytest #("foo") min/sorted "foo" 4)

(applytest (elts many-strings-vec 0 4)
	   pick-min many-strings 4 length)
(applytest (elts many-strings-vec (- many-strings-len 4))
	   pick-max many-strings 4 length)
(applytest (slice many-strings-vec 0 4)
	   min/sorted many-strings 4 length)
(applytest (reverse (slice many-strings-vec (- many-strings-len 4)))
	   max/sorted many-strings 4 length)

(applytest 'err pick-min many-strings 4 #"packet")
(applytest 'err pick-max many-strings 4 #"packet")
(applytest 'err min/sorted many-strings 4 #"packet")
(applytest 'err max/sorted many-strings 4 #"packet")

(applytest 'err pick-min many-strings 4 if)
(applytest 'err pick-max many-strings 4 if)
(applytest 'err min/sorted many-strings 4 if)
(applytest 'err max/sorted many-strings 4 if)

(applytest 'err pick-min many-strings 4 length-errfn)
(applytest 'err pick-max many-strings 4 length-errfn)
(applytest 'err min/sorted many-strings 4 length-errfn)
(applytest 'err max/sorted many-strings 4 length-errfn)

(applytest 'err pick-min many-strings 4 length-voidfn)
(applytest 'err pick-max many-strings 4 length-voidfn)
(applytest 'err min/sorted many-strings 4 length-voidfn)
(applytest 'err max/sorted many-strings 4 length-voidfn)

;;;; Reduce-choice

(applytest 9 (reduce-choice + {2 3 4}))
(applytest 17 (reduce-choice + {2 3 4} 8))
(applytest 24 (reduce-choice * {2 3 4}))
(applytest 60 reduce-choice * {2 3 4} 1 1+)
(applytest "foobazbar" reduce-choice glom {"foo" "bar" "baz"})
(applytest "foobazbar" reduce-choice glom {"foo" "bar" "baz"} #f)
(applytest (apply glom (->list (reverse (choice->vector '{foo bar baz})))) 
	   reduce-choice glom '{foo bar baz} #f symbol->string)

(applytest 63 reduce-choice + string-choice 0 length)
(applytest 63 reduce-choice + string-choice 0 length-slotmap)
(applytest 63 reduce-choice + string-choice 0 length-schemap)
(applytest 63 reduce-choice + string-choice 0 length-hashtable)

(applytest 'err reduce-choice if string-choice 0 length)
(applytest 'err reduce-choice + string-choice 0 if)
(applytest 'err reduce-choice + string-choice 0 "string")

(define (bad-combine x y) (error 'just-because))
(define (bad-part x) (error 'just-because))

(applytest 'err reduce-choice bad-combine string-choice 0 length)
(applytest 'err reduce-choice + string-choice 0 bad-part)

(applytest 'err sorted string-choice length-errfn)
(applytest 'err rsorted string-choice length-errfn)
(applytest 'err sorted string-choice length-voidfn)
(applytest 'err rsorted string-choice length-voidfn)

;;; Bigger sets

(define big-choice-size 20000)

(message "Running bigtest #1")

(define big1 (difference (random-choice big-choice-size) {3 14 15}))

(applytester {} difference big1 big1)
(applytester {3 14 15} difference {3 14 15} big1)
(applytester big1 difference big1 {3 14 15})

(dotimes (i 5)
  (message "Running bigtest #" (+ i 2))
  (let* ((bigchoice (random-choice big-choice-size))
	 (removed (sample-n bigchoice 42))
	 (usechoice (difference bigchoice removed))
	 (removed2 (sample-n removed 17)))
    (applytest {} difference usechoice usechoice)
    (applytest removed difference removed usechoice)
    (applytest usechoice difference usechoice removed)
    (applytest removed2 difference removed2 usechoice)))

(message "CHOICETEST successfuly completed")

;;; Fix-choice should never be neccessary
(applytest {"one" #(two) #"three"} %fixchoice {"one" #(two) #"three"})


(test-finished "CHOICETEST")
