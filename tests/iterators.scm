;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

(errtest (doseq (elt #(a b c) "i")))
(errtest (let ((i 0)) (until (> i 10) (when (> i 2) (irrtant i |TooBigForMe|)) (set! i (1+ i)))))
(errtest (let ((i 0)) (while (< i 10) (when (> i 2) (irrtant i |TooBigForMe|)) (set! i (1+ i)))))
(errtest (forseq (x #(a b c)) . body))
(errtest (forseq (x #(a b c) "i") (symbol->string x)))
(evaltest #("a" "b" "c") (forseq (x #(a b c) i) (symbol->string x)))

(evaltest 3 (let ((i 0)) 
	      (while (< i 10) (when (> i 2) (break)) (set! i (1+ i)))
	      3))

(evaltest 3 (let ((i 0)) 
	      (until (>= i 10) (when (> i 2) (break)) (set! i (1+ i)))
	      3))

(evaltest {} (forseq (item (fail) i) (cons i item)))

(errtest (prog1 (glom "foo" "bar") . body))
(errtest (prog1 (glom "foo" "bar") (error |JustBecause|)))

(evaltest 3 (let ((items '())) 
	      (doseq (elt #("foo" 8 1/3 bar "baz" #t) i)
		(if (symbol? elt) (break)
		    (set! items (cons elt items))))
	      (length items)))

(evaltest 3 (let ((items '())) 
	      (dolist (elt '("foo" 8 1/3 bar "baz" #t) i)
		(if (symbol? elt) (break)
		    (set! items (cons elt items))))
	      (length items)))

(evaltest {} (let ((items '())) 
	       (tryseq (elt #("foo" 8 1/3 bar "baz" #t 3.5) i)
		 (if (symbol? elt) (break)
		     (if (flonum? elt) elt
			 (fail))))))
(evaltest 3.5 (let ((items '())) 
		(tryseq (elt #("foo" 8 3.5 1/3 bar "baz" #t) i)
		  (if (symbol? elt) (break)
		      (if (flonum? elt) elt
			  (fail))))))
