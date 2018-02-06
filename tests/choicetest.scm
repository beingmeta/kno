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

(evaltest 100 (choice-size (intersection (nrange 0 300) (nrange 200 800))))
(applytest (choice 20 21 22 23 24 25 26 27 28 29) intersection
	   (nrange 0 30) (nrange 20 40))

(applytest (choice 0 1 2 3 4 10 11 12 13 14) union (nrange 0 5) (nrange 10 15))

(evaltest 500 (choice-size (difference (nrange 200 800) (nrange 0 300))))
(applytest (choice 0 1 4 5 6 7 8 9) difference (nrange 0 10) (nrange 2 4))

(define (pairup x) (cons x x))

(evaltest 10 (choice-size (pairup (nrange 0 10))))
(applytest '{(0 . 0) (1 . 1) (2 . 2) (3 . 3) (4 . 4)}
	   pairup (nrange 0 5))

(let ((bigrange (nrange 1000 2000)))
  (evaltest 1000 (choice-size bigrange)))

(evaltest 19 (choice-size (nrange 0 (nrange 0 20))))
(applytest {0 1 2 3} nrange 0 (nrange 0 5))

(evaltest 6 (choice-size (elts (elts '((a b c) (d e f) (a b c))))))
(applytest '{a b c d e f}
	   elts (elts '((a b c) (d e f) (a b c))))

(applytest '{10 11 12 13 14 15 16 17 18 19}
	   nrange 10 20)
(applytest '{"a" "b" "c" "d" "e" "f" "g" "h" "i" "j" "k" "l" "m" "n" "o"
	     "p" "q" "r" "s" "t" "u" "v" "w" "x" "y"}
	   srange #\a #\z)
(applytest '{"0" "1" "2" "3" "4" "5" "6" "7" "8"}
	   srange #\0 #\9)
(applytest '{10 11 12 13 14 15 16 17 18 19} nrange-r 10 20)

(applytest '{a b c d e} elts '(a b c d e))
(applytest '{x y z} elts (choice->list (choice 'x 'y 'z)))

(applytest #t empty? (choice))
(applytest #t empty? (intersection (choice 1 2 3) (choice 4 5 6)))
(applytest #f empty? (intersection (choice 1 2 3 4) (choice 4 5 6 7)))
(applytest #t exists? (intersection (choice 1 2 3 4) (choice 4 5 6 7)))
(applytest #f exists? (intersection (choice 1 2 3) (choice 4 5 6)))
(applytest #t < (pick-one (choice 1 2 3)) 4)

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
  #({1 2 3} {"one" "two" "three"} {une deux trois}))
(define hresult {})
(evaltest #t
	  (begin (set! hresult (elt homogenous-choices (choice 0 1 2)))
		 (stringout hresult)
		 #t))

(define (convert-arg x)
  (if (number? x) (* x 2)
    (if (string? x) (string->symbol x)
      (if (symbol? x) (symbol->string x)
	x))))
(evaltest
 '{1 2 3 une deux trois "one" "two" "three"
     2 4 6 |one| |two| |three| "UNE" "DEUX" "TROIS"}
 (begin (do-choices (x hresult)
	  (set+! hresult (convert-arg x)))
	hresult))

(evaltest 250 (length state-list-var))
(evaltest 200 (choice-size state-set-var))

(evaltest {0 2 4 6 8 10 12 14 16 18}
	 (filter-choices (v (nrange 0 20)) (even? v)))
(evaltest {0 2 4 6 8 10 12 14 16 18 20 22 24 26 28 30 32 34 36 38}
	 (for-choices (v (nrange 0 20)) (* 2 v)))
(evaltest {4 6 8 9 12 16} (let ((x (choice 2 3 4))) (* x x)))

;;; Subset tests

(define choicelist '())

(do-subsets (x {1 2 3 4 5 6 7 8 9} 3)
  (set! choicelist (cons (qc x) choicelist)))
(applytest 3 length choicelist)
(applytest '(3 3 3) map choice-size choicelist)

(set! choicelist '())

(define fnlist '())

(do-subsets (x (number->string {1 2 3 4 5 6 7 8 9}) 3)
 (set! fnlist (cons (lambda () x) fnlist)))

(applytest 3 choice-size ((car fnlist)))
(applytest 3 choice-size ((cadr fnlist)))

(set! fnlist '())

;;; Pick tests

(applytest {10 11 12} pick> (choice 5 6 7 10 11 12) 9)
(applytest {10.5 11 12} pick> (choice 5 6.3 7 10.5 11 12) 9)

(applytest @1/0 pickoids (choice "foo" 8 @1/0))

;;; Reduce-choice tests

(applytest 15 reduce-choice + {1 2 3 4 5})
(applytest 21 reduce-choice + {1 2 3 4 5} 6)
(applytest 21 reduce-choice + {1 2 3 4 5 6})

(applytest 2432902008176640000
	   reduce-choice * (nrange 1 21))
(applytest 362880 reduce-choice * (nrange 1 10))

(applytest 1 reduce-choice min {1 2 3 4 5 6})
(applytest 6 reduce-choice max {1 2 3 4 5 6})

(applytest 12 reduce-choice + {3 4 5 5 4})
(applytest 12 reduce-choice + {"3" "4" "5"} 0 string->number)
(applytest 12 reduce-choice + {"3" "4" "5" "5"} 0 string->number)
(applytest 17 reduce-choice + {"3" "4" "5" "+5"} 0 string->number)

;;; CHOICE-MAX tests

(applytest {} choice-max (nrange 1 10) 5)
(applytest (nrange 1 5) choice-max (nrange 1 5) 5)

;;; ND apply

(define-amb-tester (lnd x y) (list (qc x) (qc y)))
(applytest '({3 4} 5) apply lnd (qc {3 4}) (list 5))

;;; Set operations

(define-tester (set-same? x y) (identical? (elts x) (elts y)))
(define-tester (set-overlaps? x y) (overlaps? (elts x) (elts y)))

(applytest #t set-same? '(a b c) #(a b c))
(applytest #t set-same? '(a b c) #(b a c))
(applytest #f set-same? '(a b d) #(a b c))
(applytest #f set-same? '(a b d) '(a b c))

(applytest #t set-overlaps? '(a b c) #(a b c d))
(applytest #t set-overlaps? '(a b) #(b a c))
(applytest #f set-overlaps? '(a b c) #(x y z))
(applytest #f set-overlaps? '(a c d) '(p q r))
(applytest #t set-overlaps? '(a c d) '(p q r d))

;;; Bigger sets

(define big-choice-size 20000)

(message "Running bigtest #1")

(define big1 (difference (random-choice big-choice-size) {3 14 15}))

(applytest {} difference big1 big1)
(applytest {3 14 15} difference {3 14 15} big1)
(applytest big1 difference big1 {3 14 15})

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

(test-finished "CHOICETEST")
