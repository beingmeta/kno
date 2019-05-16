;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc.  All rights reserved.

;;; DON'T EDIT THIS FILE !!!
;;;
;;; The reference version of this module now in the src/libscm
;;; directory of the Kno/KNO source tree. Please edit that file
;;; instead.

(in-module 'varconfig)

(use-module '{logger reflection texttools})

(module-export! '{varconfigfn varconfig! optconfigfn optconfig!})
(module-export! '{config:boolean config:boolean/not
		  config:boolean+ config:boolean+parse
		  config:number config:loglevel config:bytes config:interval
		  config:goodstring config:symbol config:oneof
		  config:boolset config:fnset
		  config:replace config:push
		  config:dirname config:dirname:opt})

(define varconfigfn
  (macro expr
    (let ((varname (cadr expr))
	  (convertfn (and (> (length expr) 2) (third expr)))
	  (combinefn (and (> (length expr) 3) (fourth expr))))
      `(let ((_convert (and ,convertfn
			    (if (applicable? ,convertfn)
				,convertfn
				,string2lisp)))
	     (_combine (and ,combinefn
			    (if (applicable? ,combinefn)
				,combinefn
				,choice))))
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

(define (string2lisp arg)
  (if (string? arg)
      (if (has-prefix arg {":" "(" "#" "{"}) 
	  (parse-arg arg)
	  (if (textsearch '(isspace) arg)
	      (parse-arg arg)
	      (intern (upcase arg))))
      arg))

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
	((or (string? val) (symbol? val))
	 (text->boolean (downcase val)))
	((or (empty? val) (not val)
	     (and (number? val) (zero? val)))
	 #f)
	(else #t)))
(define (config:boolean/not val)
  (not (config:boolean val)))
(define (config:boolean+ val)
  (cond ((not val) #f)
	((or (string? val) (symbol? val))
	 (text->boolean (downcase val)))
	((or (empty? val) (not val)
	     (and (number? val) (zero? val)))
	 #f)
	(else val)))
(define (config:boolean+parse val)
  (cond ((not val) #f)
	((symbol? val) (try (text->boolean (downcase val)) #t))
	((string? val) 
	 (try (text->boolean (downcase val))
	      (string->lisp val)))
	((or (empty? val) (not val)
	     (and (number? val) (zero? val)))
	 #f)
	(else #t)))

(define (text->boolean val (dflt))
  (cond ((overlaps? val true-values) #t)
	((overlaps? val false-values) #f)
	((has-prefix val "y") #t)
	((has-prefix val "n") #f)
	(else (logwarn "Odd config:boolean specifier " (write val))
	      (fail))))

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

(define (config-dirname val (err #f))
  (cond ((not (string? val))
	 (if err
	     (irritant val |NotAString| config:dirname)
	     (begin (logwarn |ConfigError| "Dirname " val " is not a string.")
	       {})))
	((not (file-exists? val))
	 (if err
	     (irritant val |DirectoryDoesntExist| config:dirname)
	     (begin (logwarn |ConfigError| "Directory " val " does not exist.")
	       {})))
	((not (file-directory? val))
	 (if err
	     (irritant val |NotADirectory| config:dirname)
	     (begin (logwarn |ConfigError| "File " val " is not a directory.")
	       {})))
	(else val)))
(define (config:dirname:opt val) (config-dirname val #f))
(define (config:dirname val) (config-dirname val #t))

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

(define (config:bytes val)
  (when (string? val) (set! val (downcase val)))
  (if (string? val) 
      (cond ((has-suffix val {"k" "kib" "kb"})
	     (* 1024 
		(string->number (strip-suffix val {"m" "mib" "mb"}))))
	    ((has-suffix val {"m" "mib" "mb"})
	     (* 1024 1024 
		(string->number (strip-suffix val {"m" "mib" "mb"}))))
	    ((has-suffix val {"g" "gib" "gb"})
	     (* 1024 1024 1024 
		(string->number (strip-suffix val {"g" "gib" "gb"}))))
	    ((has-suffix val {"t" "tb" "tib"})
	     (* 1024 1024 1024 1024
		(string->number (strip-suffix val  {"t" "tb" "tib"})))))
      (if (number? val) val
	  (if (not val) val
	      (begin (logwarn "Odd config:bytes specifier " (write val))
		(fail))))))

(define interval-pats
  {#((label seconds #((isdigit+) (opt #("." (isdigit+)))) #t)
     {"s" "sec" "secs" "second" "seconds"})
   #((label milliseconds (isdigit+) #t) "ms")
   #((label microseconds (isdigit+) #t) "us")
   #((label nanoseconds (isdigit+) #t) "ns")
   #((label picoseconds (isdigit+) #t) "ps")
   #((label minutes (isdigit+) #t) {"m" "min" "minutes"})
   #((label hours (isdigit+) #t) {"h" "hr" "hrs" "hour" "hours"})
   #((label days (isdigit+) #t) {"d" "day" "days"})
   #((label hours (isdigit+) #t) ":"
     (label minutes (isdigit+) #t) ":"
     (label seconds #((isdigit+) (opt #("." (isdigit+)))) #t))
   #((label hours (isdigit+) #t) ":"
     (label minutes (isdigit+) #t))})

(define (config:interval val)
  (when (string? val) (set! val (downcase val)))
  (if (string? val)
      (let ((parses (text->frames (qc interval-pats) val)))
	(if (fail? val)
	    (let ((number (string->number val)))
	      (if number 
		  number
		  (irritant val |NotAnInterval|)))
	    (+ (try (get parses 'seconds) 0)
	       (* 60 (try (get parses 'minutes) 0))
	       (* 3600 (try (get parses 'hours) 0))
	       (* 3600 24 (try (get parses 'days) 0))
	       (* 0.001 (try (get parses 'milliseconds) 0))
	       (* 0.000001 (try (get parses 'microseconds) 0))
	       (* 0.000000001 (try (get parses 'nanoseconds) 0))
	       (* 0.000000000001 (try (get parses 'picoseconds) 0))
	       )))
      (if (number? val) 
	  val
	  (if (timestamp? val)
	      (let ((dt (difftime val)))
		(if (< dt 0) (- dt) dt))
	      (if (not val) val
		  (begin (logwarn "Odd config:interval specifier " (write val))
		    (fail)))))))

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
  (if (or (fail? cur) (not cur)) 
      new
      (if (and (procedure? new) (procedure-name new))
	  (choice (reject cur procedure-name (procedure-name new))
		  new)
	  (choice cur new))))

(defambda (config:replace new old) new)
(defambda (config:push new old)
  (if (not old)
      (list new)
      (if (null? old)
	  (list new)
	  (list new old))))

