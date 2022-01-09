;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)

(in-module 'fifo/generators)

(use-module '{fifo reflection})

(module-export! '{make-generator generator})

(define (generator-loop loopfn fifo)
  (begin (loopfn (nlambda 'generator-yield (value) (fifo/push! fifo value)))
    (fifo/readonly! fifo)))

(define (make-generator looper (opts #f) (name))
  (default! name (getopt opts 'name (procedure-name looper)))
  (let* ((size (getopt opts 'queuesize (getopt opts 'size (* 2 (rusage 'ncpus)))))
	 (fifo (fifo/make [size size maxlen size])))
    (%watch fifo)
    (thread/call generator-loop looper fifo)
    (iterator ()
      [name name] 
      (if (fifo/exhausted? fifo)
	  #EOD
	  (fifo/pop fifo)))))

(define generator
  (macro expr
    (local len (length expr))
    (cond ((< len 2) (fail))
	  ((and (> len 3) (string? (cadr expr)) (table? (caddr expr))
		(not (pair? (table? (caddr expr)))))
	   `(,make-generator (nlambda ,(cadr expr) (yield) ,@(cddr expr))
			     ,(caddr expr)))
	  ((and (> len 2) (string? (cadr expr)))
	   `(,make-generator (nlambda ,(cadr expr) (yield) ,@(cddr expr))))
	  ((and (> len 2) (table? (cadr expr)) (not (pair? (cadr expr))))
	   `(,make-generator (nlambda ,(cadr expr) (yield) ,@(cddr expr))
			     ,(cadr expr)))
	  (else `(,make-generator (lambda (yield) ,@(cdr expr)))))))
