;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc.  All rights reserved.

(in-module 'varconfig)

(use-module '{logger reflection})

(module-export! '{varconfigfn varconfig! optconfigfn optconfig!})
(module-export! '{config:boolean config:boolean+ config:boolean+parse
		  config:number config:loglevel
		  config:goodstring config:symbol config:oneof
		  config:boolset config:fnset})

(define varconfigfn
  (macro expr
    (let ((varname (cadr expr))
	  (convertfn (and (> (length expr) 2) (third expr)))
	  (combinefn (and (> (length expr) 3) (fourth expr))))
      `(let ((_convert ,convertfn)
	     (_combine ,combinefn))
	 (lambda (var (val))
	   (if (bound? val)
	       (set! ,varname
		     ,(cond ((and convertfn combinefn)
			     `(_combine (_convert val) ,varname))
			    (convertfn
			     `(try (_convert val) ,varname))
			    (combinefn
			     `(_combine val ,varname))
			    (else 'val)))
	       ,varname))))))

(define varconfig!
  (macro expr
    (let ((confname (cadr expr))
	  (confbody (cddr expr)))
      `(config-def! ',confname (varconfigfn ,@confbody)))))

(define optconfigfn
  (macro expr
    (let ((varname (second expr))
	  (optname (third expr))
	  (convertfn (and (> (length expr) 3) (fourth expr))))
      (if convertfn
	  `(let ((_convert ,convertfn))
	     (lambda (var (val))
	       (if (bound? val)
		   (store! ,varname ',optname (_convert val))
		   (get ,varname ',optname))))
	  `(lambda (var (val))
	     (if (bound? val)
		 (store! ,varname ',optname val)
		 (get ,varname ',optname)))))))

(define optconfig!
  (macro expr
    (let ((confname (cadr expr))
	  (confbody (cddr expr)))
      `(config-def! ',confname (optconfigfn ,@confbody)))))


;;; Pre-packaged conversion functions

(define true-values {"1" "on" "enable" "y" "yes"})
(define false-values {"0" "off" "disable" "n" "no"})

(define (config:boolean val)
  (cond ((not val) #f)
	((and (not (string? val))
	      (or (empty? val) (not val)
		  (and (number? val) (zero? val))))
	 #f)
	((not (string? val)) #t)
	((overlaps? val true-values) #t)
	((overlaps? val false-values) #f)
	((overlaps? (downcase val) true-values) #t)
	((overlaps? (downcase val) false-values) #f)
	((has-prefix (downcase val) "y") #t)
	((has-prefix (downcase val) "n") #f)
	(else (begin (logwarn "Odd config:boolean specifier " (write val))
		(fail)))))
(define (config:boolean+ val)
  (cond ((not val) #f)
	((and (not (string? val))
	      (or (empty? val) (not val)
		  (and (number? val) (zero? val))))
	 #f)
	((not (string? val)) #t)
	((overlaps? val true-values) #t)
	((overlaps? val false-values) #f)
	((overlaps? (downcase val) true-values) #t)
	((overlaps? (downcase val) false-values) #f)
	((has-prefix (downcase val) "y") #t)
	((has-prefix (downcase val) "n") #f)
	(else val)))
(define (config:boolean+parse val)
  (cond ((not val) #f)
	((and (not (string? val))
	      (or (empty? val) (not val)
		  (and (number? val) (zero? val))))
	 #f)
	((not (string? val)) val)
	((overlaps? val true-values) #t)
	((overlaps? val false-values) #f)
	((overlaps? (downcase val) true-values) #t)
	((overlaps? (downcase val) false-values) #f)
	((has-prefix (downcase val) "y") #t)
	((has-prefix (downcase val) "n") #f)
	(else (string->lisp val))))

(config-def! 'config:true
	     (lambda (var (val))
	       (if (not (bound? val)) true-values
		   (if (and (pair? val)
			    (overlaps? (car val) '{not drop}))
		       (set! true-values (difference true-values val))
		       (set+! true-values val)))))
(config-def! 'config:false
	     (lambda (var (val))
	       (if (not (bound? val)) false-values
		   (if (and (pair? val)
			    (overlaps? (car val) '{not drop}))
		       (set! false-values (difference false-values val))
		       (set+! false-values val)))))


(define (config:number val)
  (if (string? val) (string->number val)
      (if (number? val) val
	  (begin (logwarn "Odd config:number specifier " (write val))
	    (fail)))))
(define (config:loglevel val)
  (if (number? val) val
      (try (getloglevel val)
	   (begin (logwarn "Odd config:loglevel specifier " (write val))
	     (fail)))))
(define (config:symbol val)
  (if (symbol? val)
      (if (string? val) (intern (upcase val))
	  (begin (logwarn "Odd config:symbol specifier " (write val))
	    (fail)))))
(define (config:goodstring val)
  (if (not (string? val))
      (begin (logwarn "Odd config:goodstring specifier " (write val))
	(fail))
      (if (empty-string? val) #f (stdspace val))))

(define (config:oneof . options)
  (let ((combined (for-choices (option (elts options))
		    (if (vector? option) (elts option)
			(if (and (pair? option) (pair? (cdr option)))
			    (elts option)
			    (begin
			      (logwarn "Odd config:oneof option " option)
			      (fail)))))))
    (lambda (val)
      (if (overlaps? val combined) val
	  (begin (logwarn "Invalid config value" (write val))
	    (fail))))))

;;; This combines values but treats #f as {}
(defambda (config:boolset new cur)
  (if (or (fail? cur) (not cur)) new
      (choice cur new)))

(defambda (config:fnset new cur)
  (if (or (fail? cur) (not cur)) new
      (if (and (procedure? new) (procedure-name new))
	  (choice (reject cur procedure-name (procedure-name new))
		  new)
	  (choice cur new))))

