;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

(in-module 'findcycles)

(module-export! 'findcycles)

(define (checkstack obj stack depth)
  (if (eqv? obj (elt stack depth)) depth
      (if (and (or (and (string? obj) (string? (elt stack depth)))
		   (and (packet? obj) (packet? (elt stack depth))))
	       (equal? obj (elt stack depth)))
	  depth
	  (if (= depth 0) #f
	      (checkstack obj stack (-1+ depth))))))

(define (loopdeloop obj objstack opstack depth return (pos))
  (unless (or (oid? obj) (fixnum? obj) (immediate? obj)
	      (number? obj) (string? obj) (packet? obj))
    (default! pos (if (= depth 1) #f
		      (checkstack obj opstack (- depth 2))))
    ;; (when pos (message "POS=" pos) (dbg obj))
    (if pos (return (cons pos (subseq opstack 0 depth))))
    (unless pos
      (cond ((pair? obj)
	     (loopdeloop/push (qc (car obj)) car
			      objstack opstack depth return)
	     (loopdeloop/push (qc (cdr obj)) cdr
			      objstack opstack depth return))
	    ((vector? obj)
	     (doseq (elt obj i)
	       (loopdeloop/push (qc elt) (glom "VEC" i)
				objstack opstack depth return)))
	    ((hashtable? obj)
	     (do-choices (key (getkeys obj) i)
	       (loopdeloop/push (qc (get obj key)) `(HASHGET ,key)
				objstack opstack depth return)))
	    ((table? obj)
	     (do-choices (key (getkeys obj) i)
	       (loopdeloop/push (qc (get obj key)) `(GET ,key)
				objstack opstack depth return)))
	    ((compound? obj)
	     (dotimes (i (compound-length obj))
	       (loopdeloop/push (qc (compound-ref obj i))
				`(COMPOUND ,(compound-tag obj) ,i)
				objstack opstack depth return)))
	    (else #f)))))

(define (loopdeloop/push obj op objstack opstack depth return)
  (if (ambiguous? obj)
      (doseq (ch (sorted obj) i)
	(unless (or (oid? ch) (fixnum? ch) (immediate? ch)
		    (number? ch) (string? ch) (packet? ch))
	  (vector-set! objstack depth obj)
	  (vector-set! opstack depth op)
	  (vector-set! objstack (1+ depth) ch)
	  (vector-set! opstack (1+ depth) (glom "CHOICE" i))
	  (loopdeloop ch objstack opstack (+ depth 2) return)))
      (begin
	(vector-set! objstack depth obj)
	(vector-set! opstack depth op)
	(loopdeloop obj objstack opstack (1+ depth) return))))

(define (findcycles obj (maxdepth 16384))
  (let ((objstack  (make-vector maxdepth))
	(opstack (make-vector maxdepth)))
    (vector-set! objstack 0 obj)
    (vector-set! opstack 0 'top)
    (call/cc (lambda (return)
	       (loopdeloop obj objstack opstack 1 return)))))


