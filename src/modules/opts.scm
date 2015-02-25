;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2014 beingmeta, inc. All rights reserved.

(in-module 'opts)

(module-export! '{setopt! setopt+! mergeopts saveopt checkopts
		  printopts})

(defambda (setopt! opts opt val)
  (store! (if (pair? opts) (car opts) opts) opt val))
(defambda (setopt+! opts opt val)
  (add! (if (pair? opts) (car opts) opts) opt val))

(defambda (mergeopts . settings)
  (if (null? settings) (fail)
      (if (or (fail? (car settings)) (not (car settings)))
	  (apply mergeopts (cdr settings))
	  (if (ambiguous? (car settings))
	      (for-choices (s (car settings))
		(apply mergeopts s (cdr settings)))
	      (if (pair? (car settings))
		  (if (null? (car settings))
		      (try (cons (car (car settings))
				 (apply mergeopts (cdr settings)))
			   (car (car settings)))
		      (try (cons (car (car settings))
				 (apply mergeopts (cdr (car settings))
					(cdr settings)))
			   (car (car settings))))
		  (if (table? (car settings))
		      (try (cons (car settings)
				 (apply mergeopts (cdr settings)))
			   (car settings))
		      (if (symbol? (car settings))
			  (cons `#[,(car settings) ,(car settings)]
				(apply mergeopts (cdr settings)))
			  (apply mergeopts (cdr settings)))))))))

(define (checkopts settings table)
  (if (pair? settings)
      (or (checkopts (car settings) table)
	  (checkopts (cdr settings) table))
      (eq? settings table)))

;;; Save opt

(define (_saveopt settings opt thunk)
  (try (getopt settings opt {})
       (let ((v (thunk)))
	 (if (pair? settings)
	     (store! (car settings) opt v)
	     (store! settings opt v))
	 v)))
(define saveopt
  (macro expr
    `(,_saveopt ,(second expr) ,(third expr)
		(lambda () ,(fourth expr)))))

;;;; Pretty printing opts

(define (printopts opts (margin "  ;;+\t") (width 90))
  (let ((scan opts) (depth 0))
    (while (pair? scan)
      (print-table (car scan) (make-margin margin depth) width)
      (set! depth (1+ depth))
      (set! scan (cdr scan)))
    (when (table? scan)
      (print-table scan (make-margin margin depth) width))
    (printout "\n")))

(define (print-table table (margin "") (width 90))
  (cond ((pair? table)
	 (let ((stringval (lisp->string table)))
	   (if (> (+ (length margin) (length stringval)) width)
	       (printout margin (pprint table #t margin width))
	       (lineout margin stringval))))
	((table? table)
	 (doseq (key (lexsorted (getkeys table)) i)
	   (let* ((value (get table key))
		  (keystring (lisp->string key))
		  (valstring (lisp->string value)))
	     (if (> (+ (length margin) (length keystring) (length valstring) 3)
		    width)
		 (lineout margin " " keystring "\n"
		   margin "    " (pprint value #t (glom margin "  ")))
		 (lineout margin " " keystring " " valstring)))))
	(else (printout margin "?!?!" table))))

(define (make-margin margin depth)
  (if (string? margin)
      (glom margin (make-string #\Space (* depth 2)))
      (if (symbol? margin)
	  (glom "  ;;-" (make-string depth #\-)
	    margin ": ")
	  #f)))




