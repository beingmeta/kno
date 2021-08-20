(in-module 'tests/benchmarks)

(module-export! '{ack fib fibtr fact facttr fibflt tak takflt spectral-norm xspectral-norm})

(define (do-n-inner n proc args)
   (if (> n 0)
       (begin (apply proc args)
	      (do-n-inner (- n 1) proc args))))
(define (do-n n proc . args) (do-n-inner n proc args))

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
      (fib-iter (+ a b) a (- count 1))))

(define (fact n)
  (if (= n 0) 1 (* n (fact (- n 1)))))

(define (facttr n) (fact-iter 1 n))
(define (fact-iter f n)
  (if (= n 0) f
      (fact-iter (* n f) (- n 1))))

(define (fibflt n)
  (cond ((< n 2.0) 1.0)
	(else (+ (fibflt (- n 2.0)) (fibflt (- n 1.0))))))


(define (tak x y z)
  (cond ((not (< y x)) z)
	(else (tak (tak (- x 1) y z) (tak (- y 1) z x) (tak (- z 1) x y)))))

(define (takflt x y z)
  (cond ((not (< y x)) z)
	(else (takflt (takflt (- x 1.0) y z) (takflt (- y 1.0) z x) (takflt (- z 1.0) x y)))))

;; Spectral norm implementation

(define (eval-a i j)
  (/~ 1.0 (+ (1+ i) (* (+ i j) (/~ (+ (+ i j) 1) 2)))))

(define (eval-a-times-u u)
  (let ((result (make-vector (length u))))
    (doseq (v u ukey)
      (let ((sum 0))
	(doseq (v u key)
	  (set! sum (+ sum (* v (eval-a ukey key)))))
	(vector-set! result ukey sum)))
    result))

(define (eval-at-times-u u)
  (let ((result (make-vector (length u))))
    (doseq (v u ukey)
      (let ((sum 0))
	(doseq (v u key)
	  (set! sum (+ sum (* v (eval-a key ukey)))))
	(vector-set! result ukey sum)))
    result))

(define (eval-ata-times-u u)
  (eval-at-times-u (eval-a-times-u u)))

(define (spectral-norm n)
  (let ((u (make-vector n 1))
	(v (make-vector n 1)))
    (dotimes (i 10)
      (set! v (eval-ata-times-u u))
      (set! u (eval-ata-times-u v)))
    (let ((vBv 0) (vv 0))
      (doseq (value u i)
	(set! vBv (+ vBv (* value (elt v i)))))
      (doseq (value v)
	(set! vv (+ vv (* value value))))
      (sqrt (/ vbv vv)))))

(define (xeval-a-times-u u)
  (forseq (v u ukey)
     (let ((sum 0))
       (doseq (v u key)
	 (set! sum (+ sum (* v (eval-a ukey key)))))
       sum)))

(define (xeval-at-times-u u)
  (forseq (v u ukey)
    (let ((sum 0))
      (doseq (v u key)
	(set! sum (+ sum (* v (eval-a key ukey)))))
      sum)))

(define (xeval-ata-times-u u)
  (xeval-at-times-u (xeval-a-times-u u)))

(define (xspectral-norm n)
  (let ((u (make-vector n 1))
	(v (make-vector n 1)))
    (dotimes (i 10)
      (set! v (xeval-ata-times-u u))
      (set! u (xeval-ata-times-u v)))
    (let ((vBv 0) (vv 0))
      (doseq (value u i)
	(set! vBv (+ vBv (* value (elt v i)))))
      (doseq (value v)
	(set! vv (+ vv (* value value))))
      (sqrt (/ vbv vv)))))




