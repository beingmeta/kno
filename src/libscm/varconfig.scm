;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)

;;; DON'T EDIT THIS FILE !!!
;;;
;;; The reference version of this module now in the src/libscm
;;; directory of the Kno/KNO source tree. Please edit that file
;;; instead.

(in-module 'varconfig)

(use-module '{logger reflection texttools defmacro})

(module-export! '{varconfigfn varconfig! optconfigfn optconfig!})
(module-export! '{config:boolean config:boolean/not
		  config:boolean+ config:boolean+parse
		  config:number config:loglevel config:bytes config:interval
		  config:goodstring config:symbol config:oneof
		  config:boolset config:fnset
		  config:replace config:push config:pushnew
		  config:dirname config:dirname:opt config:dirname:make
		  config:path config:path:exists config:path:writable
		  config:integer config:fixnum
		  config:nonzero config:non-postive config:non-negative
		  config:positive config:negative})

(module-export! 'propconfig!)

(defambda (config-combine valfn mergefn val (cur))
  (if (or (not mergefn) (not (bound? cur)))
      (if (applicable? valfn)
	  (valfn val)
	  (if (and valfn (string? val)) 
	      (string->lisp val)
	      val))
      (let ((v (if (applicable? valfn)
		   (valfn val)
		   (if (and valfn (string? val))
		       (string->lisp val)
		       val))))
	(mergefn v cur))))
(define config-combine-alias (fcn/alias config-combine))


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

(define (extract-doc body (before '()))
  (if (null? body)
      (cons #f (reverse before))
      (if (string? (car body))
	  (cons (car body) (append (reverse before) (cdr body)))
	  (extract-doc (cdr body) (cons (car body) before)))))

(define (make-docstring symbol env)
  (stringout "the variable `" symbol "`"
    (when (module? env)
      (printout " in the module **" (pick (get env '%moduleid) symbol?) "**"))))

(define (string2lisp arg)
  (if (string? arg)
      (if (has-prefix arg {":" "(" "#" "{"}) 
	  (parse-arg arg)
	  (if (textsearch '(isspace) arg)
	      (parse-arg arg)
	      (getsym arg)))
      arg))

(define varconfig!
  (macro expr
    (let* ((confspec (cadr expr))
	   (confarg (if (symbol? confspec) 
			`(quote ,confspec)
			(if (string? confspec)
			    `(quote ,(string->symbol confspec))
			    confspec)))
	   (confbody (extract-doc (cddr expr))))
      `(config-def! ,confarg (varconfigfn ,@(cdr confbody))
		    ,(or (car confbody)
			 `(,make-docstring ',(cadr confbody) (,%env)))))))

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
  (when (and (string? val) (string-starts-with? val #((isalnum+) ":")))
    (let* ((prefix-len (textmatcher '(greedy (isalnum+)) val))
	   (prefix (slice val 0 prefix-len)))
      (when (string? (config prefix))
	(set! val (mkpath (config prefix) (slice val (1+ prefix-len)))))))
  (cond ((not (string? val))
	 (if err
	     (irritant val |NotAString| config:dirname)
	     (begin (logwarn |ConfigError| "Dirname " val " is not a string.")
	       {})))
	((and (eq? err 'mkdirs) (not (file-exists? val)))
	 (logwarn |CONFIG/CreatingDirectory| val)
	 (mkdirs val)
	 val)
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
(define (config:dirname:make val) (config-dirname val 'mkdirs))
(define (config:dirname val) (config-dirname val #t))

(defambda (config-path path (opts {}))
  (let* ((parsed (text->frame #((label prefix (isalnum+)) ":" (label path (rest))) path))
	 (path (if (and (exists? parsed) (string? (config (get parsed 'prefix))))
		   (mkpath (config (get parsed 'prefix)) (try (get parsed 'path) "")))))
    (if (or (fail? opts) (not opts))
	path
	(begin
	  (when (and (overlaps? opts 'exists) (not (file-exists? path)))
	    (irritant path |PathDoesNotExist|))
	  (when (and (overlaps? opts 'notdir) (file-directory? path))
	    (irritant path |PathIsDirectory|))
	  (when (overlaps? opts 'writable)
	    (when (or (and (file-exists? path) (not (file-writable? path)))
		      (not (file-directory? (dirname path)))
		      (not (file-writable? (dirname path))))
	      (irritant path |NotWritable|)))
	  (cond ((overlaps? opts 'absolute) (abspath path))
		((overlaps? opts 'realpath) (realpath path))
		(else path))))))
(define (config:path val) (config-path val #f))
(define (config:path:exists val) (config-path val 'exists))
(define (config:path:writable val) (config-path val 'writable))

(define (config:number val)
  (if (string? val) (string->number val)
      (if (number? val) val
	  (begin (logwarn "Odd config:number specifier " (write val))
	    (fail)))))
(define (config:integer val (num))
  (if (string? val) 
      (set! num (string->number val))
      (set! num val))
  (if (integer? num) num
      (begin (logwarn "Invalid config:integer specifier " (write val))
	(fail))))
(define (config:fixnum val (num))
  (if (string? val) 
      (set! num (string->number val))
      (set! num val))
  (if (fixnum? num) num
      (begin (logwarn "Invalid config:fixnum specifier " (write val))
	(fail))))

(define (config:positive val (num))
  (if (string? val) 
      (set! num (string->number val))
      (set! num val))
  (if (and (number? num) (> num 0)) num
      (begin (logwarn "Invalid config:positive " (write val))
	(fail))))
(define (config:negative val (num))
  (if (string? val) 
      (set! num (string->number val))
      (set! num val))
  (if (and (number? num) (< num 0)) num
      (begin (logwarn "Invalid config:negative specifier " (write val))
	(fail))))
(define (config:nonzero val (num))
  (if (string? val) 
      (set! num (string->number val))
      (set! num val))
  (if (and (integer? num) (not (= num 0))) num
      (begin (logwarn "Invalid config:nonzero specifier " (write val))
	(fail))))

(define (config:non-negative val (num))
  (if (string? val) 
      (set! num (string->number val))
      (set! num val))
  (if (and (integer? num) (>= num 0)) num
      (begin (logwarn "Invalid config:nonneg specifier " (write val))
	(fail))))
(define (config:non-positive val (num))
  (if (string? val) 
      (set! num (string->number val))
      (set! num val))
  (if (and (integer? num) (<= num 0)) num
      (begin (logwarn "Invalid config:nonpos specifier " (write val))
	(fail))))

(define (config:loglevel val)
  (if (number? val) val
      (try (getloglevel val)
	   (begin (logwarn "Odd config:loglevel specifier " (write val))
	     (fail)))))

(define (config:symbol val)
  (if (symbol? val)
      (if (string? val) (getsym val)
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
		(string->number (strip-suffix val  {"t" "tb" "tib"}))))
	    ((string->number val))
	    (else (irritant val |BadByteValue| config:bytes)))
      (if (integer? val) 
	  (if (>= val 0)
	      val 
	      (irritant val |BadByteValue| config:bytes))
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
  (cond ((not old) (list new))
	((null? old) (list new))
	((pair? old) (cons new old))
	(else (list new old))))
(defambda (config:pushnew new old)
  (cond ((not old) (list new))
	((null? old) (list new))
	((pair? old)
	 (if (position new old) old
	     (cons new old)))
	((equal? old new) old)
	(else (list new old))))

;;; Prop config

(define (add-quote expr)
  (if (and (pair? expr) (eq? (car expr) 'quote))
      expr
      (list 'quote expr)))

(defmacro (propconfig! cfgname object propname (|OPT| valfn) (|OPT| mergefn))
  `(config-def! ,(add-quote cfgname)
     (lambda (var (val))
       (if (not (bound? val))
	   (get ,object ,(add-quote propname))
	   (store! ,object ,(add-quote propname)
	     ,(if (or valfn mergefn)
		  `(if (test ,object  ,(add-quote propname))
		       (,config-combine-alias ,valfn ,mergefn
			val (,get ,object ,(add-quote propname)))
		       (,config-combine-alias ,valfn ,mergefn val))
		  'val))))))

;;; Hook configs (to be finished)

#|
(define (config:initfn inits)
  (if (and (pair? inits) (>= (length inits) 2))
      (slambda (var (val))
	(if (unbound? val) (cddr inits)
	    (if (and (applicable? val) (zero? (procedure-min-arity val)))
		(let ((fn-name (procedure-name val))
		      (fn-module (procedure-module val))
		      (scan (cddr inits))
		      (name (first inits))
		      (run? (second inits)))
		  (unless (position val scan)
		    (when (and fn-name fn-module))
		    (let ((name (procedure-name))
			  (scan (cddr inits)))
		  )

	      )
	))
))))

(define (addfn fn list (replace #f))
  "Returns #f if fn wasn't added to list, otherwise returns "
  "the updated list."
  (if (position fn list) #f
      (let ((name (procedure-name fn))
	    (module (procedure-module fn))
	    (scan list)
	    (found #f))
	(while (and (not found) (pair? scan))
	  (if (and (eq? name (procedure-name (car scan)))
		   (eq? module (procedure-module (car scan))))
	      (set! found scan)
	      (set! scan (cdr scan))))
	(if (and found replace)
	    (begin (set-car! found fn) list)
	    (and (not found) (cons fn list))))))

(define (brico:onload-configfn var (val))
  (cond ((unbound? val) brico-onload)
	((and (applicable? val) (zero? (procedure-min-arity val)))
	 (if brico.pool
	     (let ((edited (addfn val brico-onload #f)))
	       (when edited
		 (set! brico-onload edited)
		 (val))
	       (if edited #t #f))
	     (let ((edited (addfn val brico-onload #t)))
	       (when edited (set! brico-onload edited))
	       (if edited #t #f))))
	(else (irritant val |NotAThunk| config-brico:onload))))
|#
