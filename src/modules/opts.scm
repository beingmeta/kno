;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc. All rights reserved.

(in-module 'opts)

(module-export! '{opt/set! opt/has opt/default! opt/add! opt/cache opt/try
		  mergeopts saveopt checkopts
		  opts/get opts/merge
		  setopt! setopt+!
		  printopts})

(defambda (opt/set! opts opt val)
  (store! (if (pair? opts) (car opts) opts) opt val))
(define setopt+ opt/set!)
(defambda (opt/add! opts opt val (head))
  (set! head (if (pair? opts) (car opts) opts))
  (if (test head opt) (add! head opt val)
      (store! head opt (choice val (getopt opts opt {})))))
(define setopt+ opt/add!)

(defambda (opt/has opts opt)
  (cond ((ambiguous? opts)
	 (try-choices opts (opt/has opts opt)))
	((pair? opts)
	 (or (opt/has (car opts) opt) (opt/has (cdr opts) opt)))
	((table? opts) (test opts opt ))
	((eq? opts opt) #t)
	(else #f)))

(defambda (opt/default! opts opt val)
  (unless (opt/has opts opt)
    (store! (if (pair? opts) (car opts) opts) opt val)))

(define (opt/try settings optnames)
  (unless (or (vector? optnames) (pair? optnames))
    (set! optnames (vector optnames)))
  (let ((found #f) (val {}))
    (doseq (optname optnames)
      (unless found
	(when (testopt settings optname)
	  (set! val (getopt settings optname))
	  (set! found #t))))
    (tryif found val)))

(defambda (optsget opt dflt optslist)
  (if (null? optslist) dflt
      (getopt (car optslist) opt
	      (optsget opt dflt (cdr optslist)))))
(defambda (opts/get opt dflt . optslist) (optsget opt dflt optslist))

(defambda (mergeopts . settings)
  (if (null? settings) (fail)
      (if (or (fail? (car settings)) (not (car settings))
	      (null? (car settings)))
	  (apply mergeopts (cdr settings))
	  (if (ambiguous? (car settings))
	      (let ((result #f))
		(do-choices (s (car settings))
		  (set! result 
			(if result (mergeopts s result)
			    (apply mergeopts s (cdr settings)))))
		result)
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
			  (cons `#[,(car settings) ,(cdr settings)]
				(apply mergeopts (cdr settings)))
			  (apply mergeopts (cdr settings)))))))))
(define opts/merge mergeopts)

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
(define opt/cache
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




