(load-component "common.scm")

(define (nrange start end)
  (let ((answer {}))
    (dotimes (i (- end start))
      (set+! answer (+ start i)))
    answer))
(test-optimize! nrange)

(define (nrange-r start end)
  (if (= start end) {}
      (choice start (nrange-r (+ start 1) end))))
(test-optimize! nrange-r)

(define (srange cstart cend)
  (let ((start (char->integer cstart)) (end (char->integer cend)))
    (let ((answer {}))
      (dotimes (i (- end start))
	(set+! answer (string (integer->char (+ start i)))))
      answer)))
(test-optimize! srange)

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
(define homogeous-choices
  #({1 2 3} {"one" "two" "three"} {une deux trois}))
(define hresult {})
(evaltest #t
	  (begin (set! hresult (elt homogeous-choices (choice 0 1 2)))
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

(message "CHOICETEST successfuly completed")
