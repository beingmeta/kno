;;; -*- Mode: Scheme; Text-encoding: latin-1; -*-

(load-component "common.scm")

(define (fact-break-1 n)
  (let ((i 1) (result 1))
    (while #t
      (if (> i n)
	  (break)
	  (begin (set! result (* result i))
	    (set! i (1+ i)))))
    result))
(define (fact-break-2 n)
  (let ((i 1) (result 1))
    (while #t
      (if (> i n)
	  (break (set! result (- result)))
	  (begin (set! result (* result i))
	    (set! i (1+ i)))))
    result))

(define (slice-break-1 seq n)
  (forseq (elt seq i)
    (if (>= i n) (break) elt)))
(define (slice-break-2 seq n)
  (forseq (elt seq i)
    (if (>= i n) (break) elt)))

(defambda (pick-some choice (n 5) (result {}))
  (do-choices (elt choice)
    (if (>= (choice-size result) n) (break)
	(set+! result elt)))
  result)

(define (sqlim n (max 1000) (square #f))
  (dotimes (i max)
    (if (>= (* i i) n) (break)
	(set! square (* i i))))
  square)

(define tenvec #(1 2 3 4 5 6 7 8 9 10))
(define tenchoice {1 2 3 4 5 6 7 8 9 10})

(applytest 120 fact-break-1 5)
(applytest -120 fact-break-2 5)
(applytest #(1 2 3) slice-break-1 tenvec 3)
(applytest #(1 2 3 4) slice-break-2 tenvec 4)
(applytest 16 sqlim 22)
(applytest 4 sqlim 5)
(applytest 81 sqlim 100)

(evaltest 3 (choice-size (pick-some tenchoice 3)))
(evaltest 8 (choice-size (pick-some tenchoice 8)))

(evaltest 33 (onbreak (* 3 11) 8 9 (* 88 9) #break (+ 17 5)))
(evaltest 88 (onbreak (* 8 11) 8 9 (* 88 9) (break) (+ 17 5)))
