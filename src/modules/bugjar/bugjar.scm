;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2015 beingmeta, inc. All rights reserved

(in-module 'bugjar)

(use-module '{fdweb xhtml texttools xhtml/tableout})
(use-module '{varconfig stringfmts getcontent mimetable gpath logger})
(define %used_modules '{varconfig})

(module-export! 'bugjar!)

(define-init %loglevel %notice%)

(define bugjar-css (get-component "bugjar.css"))
(define bugjar-js (get-component "bugjar.js"))

(define datetime markup*fn)

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
	 (dir (mkpath daily (glom (padnum (get date 'hours) 2) ":"
			      (padnum (get date 'minutes) 2) ":"
			      (padnum (get date 'seconds) 2)
			      "-" (uuid->string uuid)))))
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
	     (dir (gp/mkpath dname (glom (padnum (get date 'hours) 2) ":"
				     (padnum (get date 'minutes) 2) ":"
				     (padnum (get date 'seconds) 2)
				     "-" (uuid->string uuid)))))
	(when (string? root)
	  (unless (file-directory? root) (mkdir root #o777))
	  (unless (file-directory? yname) (mkdir yname #o777))
	  (unless (file-directory? mname) (mkdir mname #o777))
	  (unless (file-directory? dname) (mkdir dname #o777))
	  (unless (file-directory? dir) (mkdir dir)))
	dir)
      (error "Root doesn't exist: " root)))

(define (bugjar! spec exception . sections)
  (if (uuid? spec) (set! spec `#[uuid ,spec]) (set! spec #[]))
  (when (getopt spec 'sections)
    (set! sections (append sections (getopt spec 'sections))))
  (let* ((uuid (getopt spec 'uuid (getuuid)))
	 (saveroot (makelogbase uuid saveroot))
	 (webroot (and webroot (mkpath webroot (getlogbase uuid))))
	 (reqdata (or (getopt spec 'reqdata)
		      (req/get 'bugjar:reqdata #f)
		      (req/get 'reqdata #f)
		      (req/data)))
	 (reqlog (req/getlog))
	 (head (getopt spec 'head))
	 (detailsblock #f)
	 (irritantblock #f))
    (debug%watch spec uuid saveroot webroot reqdata)
    (when (string? saveroot) (mkdirs (mkpath saveroot "example")))
    (when reqdata
      (dtype->gpath reqdata (gp/mkpath saveroot "request.dtype")))
    (when reqlog
      (gp/save! (gp/mkpath saveroot "request.log") reqlog))
    (doseq (s sections)
      (when (test s 'filename)
	(let* ((filename (get s 'filename))
	       (saveto (gp/mkpath saveroot filename))
	       (data (get s 'data)))
	  (if (or (string? data) (packet? data))
	      (gp/save! saveto data (getopt s 'type (path->mimetype filename)))
	      (dtype->gpath data filename)))))
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
	 (body! 'class "bugjar")
	 (htmlheader
	  (xmlelt "META" http-equiv "Content-type"
	    content "text/html; charset=utf-8"))
	 (xmlblock DIV ((class "bughead") (id "HEAD"))
	   (when (and reqdata (test reqdata '{request_uri taskid}))
	     (h3* ((class (if (test reqdata 'request_uri) "uri" "taskid")))
		  (datetime ((class "fdjttime")) (get (uuid-time uuid) 'string))
		  (if (test reqdata 'request_uri)
		      (let ((base (uribase (get reqdata 'request_uri))))
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
			    (span ((class "qmark")) "?") (get reqdata 'query_string))))
		      (get reqdata 'taskid))))
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
	     (doseq (s sections)
	       (cond ((test s 'filename)
		      (anchor (get s 'filename)
			(try (get s 'title) (get s 'filename))))
		     ((test s 'id)
		      (anchor (glom "#" (get s 'id))
			(try (get s 'title) (get s 'filename))))))
	     (if detailsblock (anchor "#DETAILS" "Details")) " "
	     (if irritantblock (anchor "#IRRITANT" "Irritant")) " "
	     (when reqdata (anchor "#REQDATA" "Request")) " "
	     (anchor "#RESOURCES" "Resources") " "
	     (anchor "#BACKTRACE" "Backtrace")))
	 (div ((class "main"))
	   (when reqdata
	     (h2* ((id "REQDATA")) "Request data")
	     (tableout reqdata
		       #[skipempty #t class "fdjtdata reqdata"
			 skip parts maxdata 1024
			 sortfn #t recur 3]))
	   (h2* ((id "RESOURCES")) "Resource data")
	   (tableout (rusage) #[skipempty #t class "fdjtdata rusage"])
	   (h2* ((id "BACKTRACE")) "Full backtrace")
	   (div ((class "backtracediv"))
	     (void (backtrace->html exception)))
	   (when detailsblock
	     (h2* ((id "DETAILS")) "Details")
	     (xmlblock PRE ((class "errdetails"))
	       (xmlout (error-details exception))))
	   (when irritantblock
	     (h2* ((id "IRRITANT")) "Irritant")
	     (xmlblock PRE ((class "irritant"))
	       (xmlout (stringout (pprint (error-irritant exception))))))
	   (doseq (section (reverse sections))
	     (when (test section 'id)
	       (h2* ((id (get section 'id)))
		 (try (get section 'title) (get section 'id)))
	       (cond ((test section 'html)
		      (xmlblock "div" ((class (downcase (get section 'id))))
			(xhtml (get section 'html))))
		     ((test section 'table)
		      (tableout (get section 'table)
				#[skipempty #t class "fdjtdata reqdata"]))
		     ((test section 'text)
		      (xmlblock "PRE" ((class (downcase (get section 'id))))
			(get section 'text)))
		     ((test section 'expr)
		      (xmlblock "PRE" ((class (downcase (get section 'id))))
			(pprint (get section 'expr))))))))))
    (if webroot
	(mkpath webroot "backtrace.html")
	(glom "file://" (gp/mkpath saveroot "backtrace.html")))))




