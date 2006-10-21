(define (ack m n)
    (cond ((zero? m) (+ n 1))
	  ((zero? n) (ack (- m 1) 1))
	  (else (ack (- m 1) (ack m (- n 1))))))

(define (fib n)
    (cond ((< n 2) 1)
	  (else (+ (fib (- n 2)) (fib (- n 1))))))

(define (fibtr n) (fib-iter 1 1 n))
(define (fib-iter a b count)
  (if (= count 0) b
      (fib-iter (+ 1 a b) a (- count 1))))

(define (fact n)
  (if (= n 0) 1 (* n (fact (1- n)))))

(define (facttr n) (fact-iter 1 n))
(define (fact-iter f n)
  (if (= n 0) f
      (fact-iter (* n f) (1- n))))

(define (fibflt n)
  (cond ((< n 2.0) 1.0)
	(else (+ (fibflt (- n 2.0)) (fibflt (- n 1.0))))))


(define (tak x y z)
  (cond ((not (< y x)) z)
	(else (tak (tak (- x 1) y z) (tak (- y 1) z x) (tak (- z 1) x y)))))

(define (takflt x y z)
  (cond ((not (< y x)) z)
	(else (takflt (takflt (- x 1.0) y z) (takflt (- y 1.0) z x) (takflt (- z 1.0) x y)))))

;;   The Great Computer Language Shootout
;;    http://shootout.alioth.debian.org/
;;
;;    Adapted from the C (gcc) code by Sebastien Loisel
;;
;;    Contributed by Christopher Neufeld
;;    Modified by Juho Snellman 2005-10-26
;;      * Use SIMPLE-ARRAY instead of ARRAY in declarations
;;      * Use TRUNCATE instead of / for fixnum division
;;      * Rearrange EVAL-A to make it more readable and a bit faster

;; Note that sbcl is at least 10 times faster than either clisp or gcl
;; on this program, running with an argument of 500.  It would be nice
;; to know why the others are so slow.

(define (eval-AtA-times-u n u)
  (eval-At-times-u n (eval-A-times-u n u)))

;; This is our most expensive function.  Optimized with the knowledge
;; that 'n' will never be "huge".  This will break if 'n' exceeds
;; approximately half of the square root of the largest fixnum
;; supported by the implementation.  On sbcl 0.9.3,
;; 'most-positive-fixnum' is 536870911, and we can support values of
;; 'n' above 11000.
(define (eval-A i j)
  (let* ((n (+ i j))
         (n+1 (1+ n)))
    (/ 1.0 (+ (truncate  (/~ (* n n+1) 2)) i 1))))

(define (eval-A-times-u n u)
  (let ((retval (make-vector n 0.0)))
    (dotimes (i n)
      (dotimes (j n)
	(vector-set! retval i (* (eval-A i j) (elt u j)))))
    retval))

(define (eval-At-times-u n u)
  (let ((retval (make-vector n 0.0)))
    (dotimes (i n)
      (dotimes (j n)
        (vector-set! retval i (* (eval-A j i) (elt u j)))))
    retval))

(define (spectral-norm (n 2000))
  (let ((u (make-vector n 1.0))
	(v (make-vector n 0.0)))
    (dotimes (i 10)
      (set! v (eval-AtA-times-u n u))
      (set! u (eval-AtA-times-u n v)))
      (let ((vBv 0.0d0)
            (vv 0.0d0))
        (dotimes (i n)
          (set! vBv (+ vBv (* (elt u i) (elt v i))))
          (set! vv (+ vv (* (elt v i) (elt v i)))))
	;; (format t "~11,9F~%" (sqrt (the (double-float 0d0) (/ vBv vv))))
        (lineout (sqrt (/ vBv vv))))))
