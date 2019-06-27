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
;;; TODO: aborts
;;(applytest 3+5i + 3+5i)
;;(applytest 3.0+5i + 3.0+5i)

(applytest -3 - 3)
(applytest -3.0 - 3.0)
;;; TODO: aborts
;;(applytest -3-5i - 3+5i)
;;(applytest -3.0-5i - 3.0+5i)
