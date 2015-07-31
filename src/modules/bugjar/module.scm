;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2015 beingmeta, inc. All rights reserved

(in-module 'bugjar)

(use-module '{fdweb xhtml texttools xhtml/tableout})
(use-module '{varconfig stringfmts getcontent gpath logger})
(define %used_modules '{varconfig})

(module-export! 'bugjar!)

(define-init %loglevel %notify!)
;;(define %loglevel %debug!)

(define bugjar-css (get-component "bugjar.css"))
(define bugjar-js (get-component "bugjar.js"))

;;; Configurable

(define-init saveroot
  (if (config 'logdir)
      (glom (mkpath (mkpath (config 'logdir) "framerd")
		    "bugjar") "/")
      "/tmp/bugjar/"))
(define-init webroot #f)

(define (urish? string)
  (and (string? string)
       (has-prefix string {"http:" "https:" "ftp:" "s3:"})))

(define (bugjar-config var (val))
  (cond ((not (bound? val)) (cons saveroot webroot))
	((not val)
	 (set! saveroot #f)
	 (set! webroot #f))
	((and (pair? val) (string? (car val)) (urish? (cdr val)))
	 (set! saveroot (car val))
	 (set! webroot (cdr val)))
	((and (pair? val) (string? (cdr val)) (urish? (car val)))
	 (set! saveroot (cdr val))
	 (set! webroot (car val)))
	((not (string? val))
	 (error BADLOGROOT BUGJAR-CONFIG
		"Not a valid logroot specification: " val))
	((position #\Space val)
	 (let ((split (segment val " ")))
	   (if (urish? (first split))
	       (begin (set! webroot (first split))
		 (set! saveroot (second split)))
	       (begin (set! webroot (second split))
		 (set! saveroot (first split))))))
	((and (urish? val) (has-prefix val {"https:" "http:"})
	      (has-suffix (urihost val) ".s3.amazonaws.com"))
	 (set! webroot val)
	 (set! saveroot
	       (glom "s3://" (string-subst (urihost val) ".s3.amazonaws.com" "")
		 "/" (uripath val))))
	((urish? val) (set! webroot val))
	(else (set! saveroot val))))
(config-def! 'bugjar bugjar-config)

(define bughead #f)
(varconfig! bughead bughead)

(define (getlogbase uuid)
  (let* ((date (uuid-time uuid))
	 (daily (stringout "Y0" (get date 'year) "/"
		  "M" (padnum (1+ (get date 'month)) 2) "/"
		  "D" (padnum (get date 'date) 2)))
	 (dir (mkpath daily (stringout "BJ" (uuid->string uuid)))))
    dir))
(define (makelogbase uuid (root saveroot))
  (if (or (not (string? root)) (has-prefix root "s3:")
	  (file-directory? root)
	  (file-directory? (dirname root)))
      (let* ((date (uuid-time uuid))
	     (root (->gpath root))
	     (yname (gp/mkpath root (glom "Y0" (get date 'year))))
	     (mname (gp/mkpath yname (glom "M" (padnum (1+ (get date 'month)) 2))))
	     (dname (gp/mkpath mname (glom "D" (padnum (get date 'date) 2))))
	     (dir (gp/mkpath dname (stringout "BJ" (uuid->string uuid)))))
	(when (string? root)
	  (unless (file-directory? root) (mkdir root #o777))
	  (unless (file-directory? yname) (mkdir yname #o777))
	  (unless (file-directory? mname) (mkdir mname #o777))
	  (unless (file-directory? dname) (mkdir dname #o777))
	  (unless (file-directory? dir) (mkdir dir)))
	dir)
      (error "Root doesn't exist: " root)))

(define (bugjar! spec exception . more)
  (if (uuid? spec) (set! spec `#[uuid ,spec]) (set! spec #[]))
  (when (getopt spec 'sections)
    (set! more (append more (getopt spec 'sections))))
  (let* ((uuid (getopt spec 'uuid (getuuid)))
	 (saveroot (makelogbase uuid saveroot))
	 (webroot (and webroot (mkpath webroot (getlogbase uuid))))
	 (reqdata (try (getopt spec 'reqdata)
		       (and (req/get 'SCRIPT_FILENAME)
			    (or (req/get 'reqdata #f) (req/data)))))
	 (reqlog (req/getlog))
	 (head (getopt spec 'head))
	 (detailsblock #f)
	 (irritantblock #f)
	 (sections '()))
    (debug%watch spec uuid saveroot webroot reqdata)
    (when (string? saveroot) (mkdirs (mkpath saveroot "example")))
    (when reqdata
      (dtype->gpath reqdata (gp/mkpath saveroot "request.dtype")))
    (when reqlog
      (gp/save! (gp/mkpath saveroot "request.log") reqlog))
    (let ((scan more))
      (while (and (pair? scan)
		  (or (string? (car scan)) (symbol? (car scan)))
		  (pair? (cdr scan)))
	(let ((data (cadr scan))
	      (filename  (gp/mkpath saveroot (->string (car scan)))))
	  (if (or (string? data) (packet? data))
	      (gp/save! filename data)
	      (dtype->gpath data filename)))
	(set! scan (cddr scan)))
      (when (pair? scan)
	(dtype->gpath (gp/mkpath saveroot "more.dtype") (car more))))
    (gp/writeout (gp/mkpath saveroot "backtrace.html")
      (with/request/out
       (title! "Error " (uuid->string uuid) ": "
	       (error-condition exception)
	       (when (error-context exception)
		 (printout " (" (error-context exception) ") "))
	       (when (and (error-details exception)
			  (< (length (error-details exception)) 40))
		 (printout " \&ldquo;" (error-details exception) "\&rdquo;")))
       (htmlheader
	(xmlblock STYLE ((type "text/css"))
	  (xhtml "\n" (getcontent bugjar-css))))
       (htmlheader
	(xmlblock SCRIPT ((type "text/javascript") (language "javascript"))
	  (xhtml "\n" (getcontent bugjar-js))))
       (stylesheet! "https://s3.amazonaws.com/beingmeta/static/fdjt/fdjt.css")
       (body! 'class "fdjtbugreport")
       (htmlheader
	(xmlelt "META" http-equiv "Content-type"
	  content "text/html; charset=utf-8"))
       (xmlblock HGROUP ((class "head") (id "HEAD"))
	 (when reqdata
	   (let ((base (uribase (get reqdata 'request_uri))))
	     (h3* ((class "uri"))
		  (unless (equal? base (get reqdata 'script_name))
		    (span ((class "scriptname")) (get reqdata 'script_name)))
		  (if (test reqdata 'https) "https:" "http:")
		  "//" (get reqdata 'http_host)
		  (if (or (and (test reqdata 'https)
			       (not (test reqdata 'server_port 443)))
			  (and (not (test reqdata 'https))
			       (not (test reqdata 'server_port 80))))
		      (xmlout ":" (get reqdata 'server_port)))
		  base
		  (when (and (exists? (get reqdata 'query_string))
			     (not (empty-string? (get reqdata 'query_string))))
		    (span ((class "query"))
		      (span ((class "qmark")) "?") (get reqdata 'query_string))))))
	 (when head
	   (onerror (xmleval head)
	     (lambda (x)
	       (h3* ((class "errerrerr"))
		    "Recursive error rendering head") #f)))
	 (h1 (span ((class "condition"))
	       (->string (error-condition exception)))
	   (when (error-context exception)
	     (span ((class "context")) " (" (error-context exception) ") ")))
	 (if (and (error-details exception)
		  (< (length (error-details exception)) 120))
	     (h2* ((class "detail"))
	       (xmlout " \&ldquo;" (error-details exception) "\&rdquo;"))
	     (set! detailsblock #t))
	 (let* ((irritant (error-irritant exception))
		(stringval (and (bound? irritant)
				(exists? irritant) irritant
				(lisp->string (qc irritant)))))
	   (if (and stringval (< (length stringval) 50))
	       (h2* ((class "irritant")) stringval)
	       (set! irritantblock stringval)))
	 (when bughead (req/call bughead)))
       (div ((class "navbar"))
	 (span ((class "buginfo"))
	   (strong "Bug ") (uuid->string uuid)
	   (strong " reported ") (get (gmtimestamp) 'string))
	 (span ((class "links"))
	   (anchor "#HEAD" "Top") " "
	   (let ((scan more) (leftover '()))
	     (while (pair? scan)
	       (if (and (pair? (cdr scan))
			(or (string? (car scan)) (symbol? (car scan))))
		   (let* ((label (car scan))
			  (name (if (symbol? label) (symbol->string label)
				    ;;(textsubst label label2id)
				    (stringout label)))
			  (title (if (symbol? label) (capitalize name)
				     name)))
		     (anchor (glom "#" name) title)
		     (set! sections (vector title name (cadr scan)))
		     (set! scan (cddr scan)))
		   (begin (set! leftover (cons (car scan) leftover))
		     (set! scan (cddr scan))))))
	   (if detailsblock (anchor "#DETAILS" "Details")) " "
	   (if irritantblock (anchor "#IRRITANT" "Irritant")) " "
	   (anchor "#REQDATA" "Request") " "
	   (anchor "#RESOURCES" "Resources") " "
	   (anchor "#BACKTRACE" "Backtrace")))
       (div ((class "main"))
	 (when reqdata
	   (h2* ((id "REQDATA")) "Request data")
	   (tableout reqdata #[skipempty #t class "fdjtdata reqdata"]))
	 (doseq (section (reverse sections))
	   (h2* ((id (second section))) (first section))
	   (if (and (not (pair? (third section))) (table? (third section)))
	       (tableout (third section)
			 #[skipempty #t class "fdjtdata reqdata"])
	       (if (and (string? (third section))
			(search "</" (third section)))
		   (xmlblock "div" ((class (downcase (second section))))
		     (xhtml (third section)))
		   (xmlblock "PRE" ((class (downcase (second section))))
		     (pprint (third section))))))
	 (h2* ((id "RESOURCES")) "Resource data")
	 (tableout (rusage) #[skipempty #t class "fdjtdata rusage"])
	 (h2* ((id "BACKTRACE")) "Full backtrace")
	 (div ((class "backtracediv") (onclick "expandTBODY(event);"))
	   (void (backtrace->html exception)))
	 (when detailsblock
	   (h2* ((id "DETAILS")) "Details")
	   (xmlblock PRE ((class "errdetails"))
	     (xmlout (error-details exception))))
	 (when irritantblock
	   (h2* ((id "IRRITANT")) "Irritant")
	   (xmlblock PRE ((class "irritant"))
	     (xmlout (stringout (pprint (error-irritant exception)))))))))
    (if webroot
	(mkpath webroot "backtrace.html")
	(glom "file://" (gp/mkpath saveroot "backtrace.html")))))












