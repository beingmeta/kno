;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc. All rights reserved

(in-module 'xhtml/buglog)

(use-module '{fdweb xhtml xhtml/tableout texttools})
(use-module '{varconfig stringfmts logger rulesets crypto getcontent ezrecords})
(module-export! 'buglog)

(define-init %loglevel %notify!)
;;(define %loglevel %debug!)

(define buglog-css (get-component "buglog.css"))

;;; Configurable

(define fileroot #f)
(define webroot #f)

(define (urish? string)
  (and (string? string)
       (has-prefix string {"http:" "https:" "ftp:" "s3:"})))

(define (buglog-config var (val))
  (cond ((not (bound? val)) (cons fileroot webroot))
	((not val)
	 (set! fileroot #f)
	 (set! webroot #f))
	((and (pair? val) (string? (car val)) (urish? (cdr val)))
	 (set! fileroot (car val))
	 (set! webroot (cdr val)))
	((not (string? val))
	 (error BADLOGROOT BUGLOG-CONFIG
		"Not a valid logroot specification: " val))
	((urish? val) (set! webroot val))
	(else (set! fileroot val))))
(config-def! 'buglog buglog-config)

(define bughead #f)
(varconfig! bughead bughead)

(define (getlogbase uuid)
  (let ((date (uuid-time uuid))
	(dir (stringout "Y0" (get date 'year) "/"
	       "M" (padnum (1+ (get date 'month)) 2) "/"
	       "D" (padnum (get date 'date) 2))))
    (mkdirs dir)
    (mkpath dir (stringout "E" (uuid->string uuid)))))

(define (buglog uuid exception)
  (let* ((uuid (getuuid uuid))
	 (logbase (getlogbase uuid))
	 (fileroot (abspath (mkpath fileroot logbase)))
	 (webroot (and webroot (mkpath webroot logbase)))
	 (reqdata (req/data)))
    (dtype->file reqdata (mkpath fileroot ".request"))
    (fileout (mkpath fileroot ".html")
      (withrequest/out
       (title! "Error " (uuid->string uuid) ": "
	       (exception-condition exception)
	       (when (exception-context exception)
		 (printout " (" (exception-context exception) ") "))
	       (when (exception-detail exception)
		 (printout " \&ldquo;" (exception-context exception) "\&rdquo;")))
       (xmlblock STYLE ((type "text/css"))
	 (xhtml (getcontent buglog-css)))
       (stylesheet! "http://static.beingmeta.com/static/fdjt.css")
       (body! "fdjtbugreport")
       (xmlblock HGROUP ((class "head"))
	 (h1 (span ((class "bugid")) (uuid->string uuid))
	   (span ((class "condition")) (exception-condition exception))
	   (when (exception-context exception)
	     (span ((class "context")) " (" (exception-context exception) ") ")))
	 (when (exception-detail exception)
	   (h2 ((class "detail"))
	     (printout " \&ldquo;" (exception-detail exception) "\&rdquo;")))
	 (let ((base (uribase (get reqdata 'request_uri))))
	   (h3 ((class "uri"))
	     "//" (get reqdata 'host) base
	     (unless (equal? base (get reqdata 'script_name))
	       (xmlout " " (span ((class "scriptname"))
			     (get reqdata 'script_name))))))
	 (when (and (exists? (get reqdata 'query_string))
		    (not (empty-string? (get reqdata 'query_string))))
	   (h3 ((class "query"))
	     (span ((class "qmark")) "?") (get reqdata 'query_string)))
	 (when bughead (req/call bughead)))
       (h2 "Request data")
       (tableout reqdata "fdjtdata cgidata" #t)
       (h2 "Error backtrace")
       (backtrace->html exception)))
    (if webroot (mkpath webroot (glom logbase ".html"))
	(stringout "file:/" fileroot ".html"))))







