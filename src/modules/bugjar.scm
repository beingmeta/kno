;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc. All rights reserved

(in-module 'bugjar)

(use-module '{fdweb xhtml xhtml/tableout texttools})
(use-module '{varconfig stringfmts logger rulesets crypto
	      getcontent})
(module-export! 'bugjar!)

(define-init %loglevel %notify!)
;;(define %loglevel %debug!)

(define bugjar-css (get-component "bugjar.css"))

;;; Configurable

(define-init fileroot "/tmp/bugs/")
(define-init webroot #f)

(define (urish? string)
  (and (string? string)
       (has-prefix string {"http:" "https:" "ftp:" "s3:"})))

(define (bugjar-config var (val))
  (cond ((not (bound? val)) (cons fileroot webroot))
	((not val)
	 (set! fileroot #f)
	 (set! webroot #f))
	((and (pair? val) (string? (car val)) (urish? (cdr val)))
	 (set! fileroot (car val))
	 (set! webroot (cdr val)))
	((not (string? val))
	 (error BADLOGROOT BUGJAR-CONFIG
		"Not a valid logroot specification: " val))
	((urish? val) (set! webroot val))
	(else (set! fileroot val))))
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

(define (bugjar! uuid exception . more)
  (let* ((uuid (if uuid (getuuid uuid) (getuuid)))
	 (logbase (getlogbase uuid))
	 (fileroot (abspath (mkpath fileroot logbase)))
	 (webroot (and webroot (mkpath webroot logbase)))
	 (reqdata (and (req/get 'HTTP_HOST)
		       (or (req/get 'reqdata #f) (req/data))))
	 (sections '()))
    (mkdirs (mkpath fileroot "example"))
    (when reqdata
      (dtype->file reqdata (mkpath fileroot "request.dtype")))
    (let ((scan more))
      (while (and (pair? scan)
		  (or (string? (car scan)) (symbol? (car scan)))
		  (pair? (cdr scan)))
	(let ((data (cadr scan))
	      (filename  (mkpath fileroot (->string (car scan)))))
	  (if (or (string? data) (packet? data))
	      (write-file filename data)
	      (dtype->file data filename)))
	(set! scan (cddr scan)))
      (when (pair? scan)
	(dtype->file (mkpath fileroot "more.dtype") (car more))))
    (fileout (mkpath fileroot "backtrace.html")
      (with/request/out
       (title! "Error " (uuid->string uuid) ": "
	       (error-condition exception)
	       (when (error-context exception)
		 (printout " (" (error-context exception) ") "))
	       (when (error-details exception)
		 (printout " \&ldquo;" (error-context exception) "\&rdquo;")))
       (htmlheader
	(xmlblock STYLE ((type "text/css"))
	  (xhtml "\n" (filestring bugjar-css))))
       (stylesheet! "http://static.beingmeta.com/static/fdjt.css")
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
	 (h1 (span ((class "condition")) (->string (error-condition exception)))
	   (when (error-context exception)
	     (span ((class "context")) " (" (error-context exception) ") ")))
	 (when (error-details exception)
	   (h2* ((class "detail"))
	     (printout " \&ldquo;" (error-details exception) "\&rdquo;")))
	 (let* ((irritant (error-irritant exception))
		(stringval (and (exists? irritant) irritant
				(lisp->string (qc irritant)))))
	   (when (and stringval (< (length stringval) 50))
	     (h2* ((class "irritant")) stringval)))
	 (when bughead (req/call bughead)))
       (div ((class "navbar"))
	 (span ((class "buginfo"))
	   (strong "Bug ") (uuid->string uuid)
	   (strong " reported ") (get (gmtimestamp) 'string))
	 (anchor "#HEAD" "Top") " "
	 (let ((scan more))
	   (while (pair? scan)
	     (if (and (pair? (cdr scan))
		      (or (string? (car scan)) (symbol? car scan)))
		 (let ((label (car scan))
		       (name (if (symbol? label) (symbol->string label)
				 (textsubst label label2id)))
		       (title (if (symbol? label) (capitalize name)
				  name)))
		   (anchor (glom "#" name) title)
		   (set! sections (vector title name (cadr scan)))
		   (set! scan (cddr scan)))
		 (begin (set! leftover (cons (car scan) leftover))
		   (set! scan (cddr scan))))))
	 (anchor "#REQDATA" "Request") " "
	 (anchor "#RESOURCES" "Resources") " "
	 (anchor "#BACKTRACE" "Backtrace"))
       (div ((class "main"))
	 (when reqdata
	   (h2* ((id "REQDATA")) "Request data")
	   (tableout reqdata #[skipempty #t class "fdjtdata reqdata"]))
	 (doseq (section (reverse sections))
	   (h2* ((id (second section))) (first section))
	   (if (and (not (pair? (third section))) (table? (third section)))
	       (tableout (third section)
			 #[skipempty #t class "fdjtdata reqdata"])
	       (xmlblock "PRE" ()
		 (pprint (third section)))))
	 (h2* ((id "RESOURCES")) "Resource data")
	 (tableout (rusage) #[skipempty #t class "fdjtdata rusage"])
	 (h2* ((id "BACKTRACE")) "Full backtrace")
	 (void (backtrace->html exception)))))
    (if webroot
	(mkpath webroot "backtrace.html")
	(glom "file://" (mkpath fileroot "backtrace.html")))))



