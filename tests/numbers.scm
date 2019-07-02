;;; -*- Mode: scheme; text-encoding: utf-8; -*-

(load-component "common.scm")

(applytest 2 elt (shortvec 3 2 1) 1)
(applytest 3 length (shortvec 3 2 1))

(applytest 2 elt (intvec 3 2 1) 1)
(applytest 3 length (intvec 3 2 1))

(applytest 2 elt (longvec 3 2 1) 1)
(applytest 3 length (longvec 3 2 1))

(applytest 2.0 elt (doublevec 3 2 1) 1)
(applytest 3 length (doublevec 3 2 1))

(applytest 2.0 elt (floatvec 3 2 1) 1)
(applytest 3 length (floatvec 3 2 1))

(define (v+ x y) (->vector (+ x y)))
(define (v* x y) (->vector (* x y)))
(define (v⋅ x y) (* x y))

(applytest #(11 13 16) v+ (shortvec 3 4 6) (longvec 8 9 10))
(applytest #(11 13 16) v+ (intvec 3 4 6) (longvec 8 9 10))
(applytest #(11.0 13.0 16.0) v+ (shortvec 3 4 6) (floatvec 8 9 10))
(applytest #(11.0 13.0 16.0) v+ (intvec 3 4 6) (doublevec 8 9 10))
(applytest #(11.0 13.0 16.0) v+ (doublevec 3 4 6) (doublevec 8 9 10))

(applytest #(9 12 18) v* (longvec 3 4 6) 3)
(applytest #(9.0 12.0 18.0) v* (doublevec 3 4 6) 3)
(applytest #(9.0 12.0 18.0) v* (longvec 3 4 6) 3.0)
(applytest #(9.0 12.0 18.0) v* (vector 3 4 6) 3.0)
(applytest #(9.0 12.0 18.0) v* (vector 3.0 4.0 6.0) 3)
(applytest #(9 12 18) v* (vector 3 4 6) 3)

(applytest 120.0 v⋅ (intvec 3 4 6) (vector 8.0 9.0 10.0))
(applytest 120 v⋅ (intvec 3 4 6) (shortvec 8 9 10))
(applytest 120 v⋅ (intvec 3 4 6) (vector 8 9 10))
(applytest 120 v⋅ (vector 8 9 10) (intvec 3 4 6))
(applytest 120.0 v⋅ (vector 8 9 10) (floatvec 3 4 6))
(applytest 120.0 v⋅ (floatvec 3 4 6) (vector 8 9 10))

(evaltest 'error
	  (onerror (+ (intvec 1 2 3) (intvec 1 2 3 4))
	    (lambda (ex) 'error)))
(evaltest 'error
	  (onerror (v⋅ (intvec 1 2 3) (intvec 1 2 3 4))
	    (lambda (ex) 'error)))
(evaltest 'error
	  (onerror (v⋅ (floatvec 1 2 3) (intvec 1 2 3 4))
	    (lambda (ex) 'error)))

(applytest #(5 7 9) + #(1 2 3) #(4 5 6))
(applytest #(-3 -3 -3) - #(1 2 3) #(4 5 6))
(applytest 32 * #(1 2 3) #(4 5 6))

(do-choices (convert {->longvec ->shortvec ->intvec ->floatvec ->doublevec})
  (applytest (convert #(5 7 9)) + (convert #(1 2 3)) (convert #(4 5 6)))
  (applytest (convert #(-3 -3 -3)) - (convert #(1 2 3)) (convert #(4 5 6))))

(applytest 3 * 3)
(applytest 3.0 * 3.0)
(applytest 3+5i * 3+5i)
(applytest 3.0+5i * 3.0+5i)

(applytest 3 + 3)
(applytest 3.0 + 3.0)
(applytest 3+5i + 3+5i)
(applytest 3.0+5i + 3.0+5i)

(applytest -3 - 3)
(applytest -3.0 - 3.0)
(applytest -3-5i - 3+5i)
(applytest -3.0-5i - 3.0+5i)

(applytest #t complex? 3.0+5i)
(applytest #t complex? 3)
(applytest #t complex? 0.33)
(applytest #f complex? 'symbol)
(applytest #f complex? "string")

(applytest #t real? 3)
(applytest #t real? 3.33)
(applytest #t real? 1/3)
(applytest #f real? 1/3+9i)

(applytest #t rational? 1/3)
(applytest #f rational? 1/3+5i)
(applytest #t rational? 8)
(applytest #f rational? 0.333)

(applytest #(4 11) + #(1 5) #(2 5) #(1 1))
(applytest 33 + 11 11 11)
(applytest 33.0 + 11.0 11 11)
(applytest 1331 * 11 11 11)
(applytest 1331.0 * 11 11.0 11)

(applytest #(100 80) * 5 #(20 16) )
(applytest #(100.0 80.0) * 5.0 #(20 16) )
(applytest #(100.0 80) * 5 #(20.0 16) )

(define onebig
  (* (* 4 1024 1024 1024)  (* 4 1024 1024 1024)))

(applytest #t number? onebig)

(applytest #t bignum? onebig)
(applytest #f bignum? 33)
(applytest #f bignum? 1/3)
(applytest #f bignum? 3.33)
(applytest #f bignum? 3.33+5i)

(applytest #t even? onebig)
(applytest #f odd? onebig)
(applytest #f even? (1+ onebig))
(applytest #f even? (+ onebig 7))
(applytest #f even? (1+ onebig))

(applytest #t odd? (+ onebig 7))

(applytest flonum? exact->inexact onebig)
(applytest flonum? /~ onebig)
(applytest flonum? /~ 1 onebig)
(applytest flonum? * onebig 1.0)
(applytest flonum? * onebig 1.0)
(applytest flonum? + onebig 1.0)
(applytest flonum? - onebig 1.0)

(applytest 1.0 * onebig (/~ onebig))

(applytest 4.0 -1+ 5.0)
(applytest 4.0+9i -1+ 5.0+9i)
(applytest 3/5 -1+ 8/5)

(applytest 4+9i ->exact 4.0+9i)

(applytest (/~ 5) /~ 1 5)
(applytest (/~ 10) /~ 2 20)

(applytest flonum? ->flonum 1/3)

(applytest 3.0+5.0i ->inexact 3+5i)
(errtest (->flonum 3+5i))

(applytest 9 pow 3 2)
(applytest 1 pow 3 0)
(applytest (* onebig onebig) pow onebig 2)
(applytest 9.0 pow 3.0 2)

(applytest 3 nthroot 9 2)
(applytest 3 nthroot 27 3)
(applytest 3.0 nthroot 27.0 3)
(applytest 2.0 nthroot 4 2.0)

(applytest onebig abs (- 0 onebig))
(applytest 3.0 abs 3.0)
(applytest 3.0 abs -3.0)
(applytest 3-4i abs 3-4i)
(applytest 3-4i abs -3-4i)
;; (applytest 2/3 abs 2/3+4i)
;; (applytest 2/3 abs -2/3+4i)
;; (applytest 3-4i abs -3-4i)

(applytest 4.0 truncate 4.5)
(applytest -4.0 truncate -4.5)
(applytest -5.0 floor -4.5)
(applytest 4.0 floor 4.5)
(applytest -4.0 ceiling -4.5)
(applytest 5.0 ceiling 4.5)

(applytest 4.0 round 4.4)
(applytest 5.0 round 4.6)
(applytest -5.0 round -4.6)
(applytest -4.0 round -4.4)

(applytest 4 truncate 18/4)
(applytest -4 truncate -18/4)
(applytest -5 floor -18/4)
(applytest 4 floor 18/4)
(applytest -4 ceiling -18/4)
(applytest 5 ceiling 18/4)

(applytest "21" ->hex 33)
(applytest 33 ->hex "21")

(applytest 99.999 string->number "99.999")
(applytest -99.999 string->number "-99.999")
(applytest 99 string->number "99")
(applytest 10 string->number "0a" 16)
(applytest 1/3 string->number "1/3")

(errtest (string->number 99))
(errtest (string->number 99.5))

(applytest 99 ->number "99")
(applytest 10 ->number "0a" 16)

(applytest 99 ->number 99)
(applytest 10 ->number 0x0a)

