#!/usr/bin/fdexec

(use-module 'optimize)

(define plist-table (make-hashtable))
(define (pget sym field)
  (try (get plist-table (cons sym field)) #f))
(define (pput sym field val)
  (store! plist-table (cons sym field) val))

(define (classq key alist)
  (if (null? alist) #f
      (if (pair? alist) (assq key alist)
	  #f)))
(define (clmember key list)
  (if (not list) #f
      (if (pair? list) (member key list)
	  #f)))
(define (nil? x)
  (or (null? x) (not x)))
(define caddr third)
(define cadddr fourth)

(define (run-benchmark benchmark-name benchmark-thunk)
  (let ((start (timestamp)))
    (dotimes (i 5) (benchmark-thunk))
    (let ((time (difftime (timestamp) start)))
      (lineout "Benchmark " benchmark-name
	       " took " time " seconds"))))

(define (main file)
  (load file))
