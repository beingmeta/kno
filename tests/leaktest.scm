(use-module 'optimize)

(define (make-list n)
  (if (= n 0) '() (cons n (make-list (-1+ n)))))

(define (make-list-iter n (l '()))
  (if (= n 0) 
      (if (and (not (empty-list? l)) (number? (car l)))
	  l
	  l)
      (make-list-iter (-1+ n) (cons n l))))

(define (main)
  (lineout (make-list-iter 15)))

(optimize-locals!)
