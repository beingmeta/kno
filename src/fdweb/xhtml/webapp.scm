;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-

;;; Copyright (C) 2005-2017 beingmeta, inc. All rights reserved

(in-module 'xhtml/webapp)

;;; This provides a simple web application framework

(define version "$Id$")

(use-module '{fdweb xhtml texttools reflection})
(use-module '{varconfig rulesets logger})
(use-module '{xhtml/auth xhtml/openid})

(define-init %loglevel %notice!)

(module-export!
 '{req/url
   app/url
   app/setup
   app/sitepath
   app/set-cookie! app/clear-cookie!
   app/redirect
   app/needlogin})

(define-init apphost #f)
(define-init approot "/")
(define-init appid "WEBAPP")
(define-init userelpaths #t)
(varconfig! app:hostname apphost)
(varconfig! app:root approot)
(varconfig! app:id appid)

(define-init secure-site #f)
(define-init secure-roots {})
(define-init insecure-roots {})
(varconfig! app:secure secure-site)
(ruleconfig! app:https secure-roots)
(ruleconfig! app:justhttp insecure-roots)

(define-init loginform #f)
(define-init loginproc #f)
(varconfig! app:asklogin loginform)
(varconfig! app:dologin loginproc)

;;; REQ/URL

(define (req/url (w/query #f))
  (stringout (if (or (req/test 'https "on")
		     (= (req/get 'SERVER_PORT) 443))
		 "https://"
		 "http://")
    (req/get 'SERVER_NAME)
    (when (req/test 'SERVER_PORT)
      (unless (or (and (= (req/get 'SERVER_PORT) 80)
		       (not (req/test 'https "on")))
		  (= (req/get 'SERVER_PORT) 443))
	(printout ":" (req/get 'SERVER_PORT))))
    (try (req/get 'script_name) "")
    (try (req/get 'path_info) "")
    (when (and w/query (req/get 'query_string #f)
	       (or (not (empty-string? (req/get 'query_string #f)))
		   (position #\? (req/get 'request_uri ""))))
      (printout "?" (req/get 'query_string)))))

;;;; SITEURL

(define (app/sitepath (app "") (userel userelpaths))
  (let* ((hostname (req/get 'server_name))
	 (cursecure (req/test 'server_port 443))
	 (curpath  (req/get 'request_uri))
	 (curdir  (dirname curpath))
	 (basepath (mkpath (req/get 'appbase approot) app))
	 (secure (if secure-site
		     (not (textsearch (qc insecure-roots) basepath))
		     (textsearch (qc secure-roots) basepath))))
    (if (or (not userel) (not (eq? secure cursecure))
	    (not (eq? hostname apphost)))
	(stringout (if secure "https:" "http:") "//"  apphost basepath)
	(if (has-prefix basepath curdir)
	    (subseq basepath (length curdir))
	    basepath))))

(define (app/url (app "") . args)
  (if (null? args)
      (if app (app/sitepath app) (req/url))
      (apply scripturl (app/sitepath app) args)))

;;; Parsing hierarchical app paths major/minor

(define apprule
  '(GREEDY #("/" (label major (isalnum+))
	     {(eos) #("/" (eos))
	      #("/"
		(label minor (+ {(isalnum) "."}))
		{(eos) #("/" (eos))
		 (label rest #("/" (rest)))})})))

(define appminor-defaults #[])
(config-def! 'app:minor
	     (lambda (var (val))
	       (cond ((not (bound? val)) appminor-defaults)
		     ((pair? val)
		      (store! appminor-defaults (car val) (cdr val)))
		     ((and (string? val) (position #\/ val))
		      (let ((split (position #\/ val)))
			(store! appminor-defaults
				(subseq val 0 split)
				(subseq val (1+ split)))))
		     ((table? val) (set! appminor-defaults val))
		     (else (error "Bad appminor config " val)))))

(define (app/setup (appmajor #f) (appminor #f) (path_info #f)
		   (forceapp #f) (request_uri #f))
  (debug%watch "app/setup" appmajor appminor path_info forceapp)
  (when (ambiguous? (req/get 'appmajor)) (req/drop! appmajor))
  (when (ambiguous? (req/get 'appminor)) (req/drop! appminor))
  (let* ((parsed (text->frame apprule (or forceapp path_info {})))
	 (newmajor (try (get parsed 'major)
			(req/get 'appmajor)
			"about")))
    (debug%watch "app/setup" appmajor newmajor parsed path_info)
    (set! appmajor (get parsed 'major))
    (req/set! 'appmajor appmajor)
    (set-cookie! 'appmajor appmajor apphost "/")
    ;; The minor app can be either explicitly expressed or determined
    ;;  based on a cookie and default tables
    (let ((newminor (try (get parsed 'minor)
			 (req/get (string->lisp (stringout appmajor ".minor")))
			 (get appminor-defaults appmajor)
			 (req/get 'appminor)
			 #f)))
      (debug%watch "app/setup" appminor newminor)
      (set! appminor newminor)
      (req/set! 'appminor newminor)
      (if newminor
	  (app/set-cookie! (stringout appmajor ".minor") appminor)
	  (app/clear-cookie! (stringout appmajor ".minor"))))
    ;; The apprest arg is assigned here.  We don't want it to default from
    ;;  cookies or tables
    (if (and (test parsed 'rest)
	     (not (empty-string? (get parsed 'rest))))
	(req/set! 'apprest (get parsed 'rest))
	(req/drop! 'apprest (get parsed 'rest))))
  (unless (or path_info forceapp) (req/drop! 'apprest))
  (when request_uri
    (cond ((has-prefix request_uri "/fb/") (req/set! 'appbase "/fb"))
	  ((has-prefix request_uri "/app/") (req/set! 'appbase "/app"))
	  (else (req/set! 'appbase "/app")))))

;;; App cookies

(define (app/set-cookie! var val (domain apphost) (root approot) (expires #f) (secure #f))
  (set-cookie! var val (or domain apphost) (or root "/")
	       (and expires (if (number? expires) (timestamp+ expires) expires))
	       secure))
(define (app/clear-cookie! var (domain apphost) (root approot) (secure #f))
  (set-cookie! var "expired" (or domain apphost) (or root approot)
	       (timestamp+ (* -17 24 3600))
	       secure))

;; Doing redirection

(define (app/redirect uri (status 303))
  (debug%watch "app/redirect" uri)
  (req/set! 'doctype #f)
  (req/set! 'status status)
  (httpheader "Location: " uri))

(define (loginheader message)
  (app/set-cookie! 'nextstop (req/get 'request_uri))
  (when message
    (if (string? message)
	(div ((class "loginmsg"))
	  (xmleval message))
	(div ((class "loginmsg"))
	  "You need to log in to see this"))))

(define (app/needlogin (message #t))
  (cond ((exists? (auth/getinfo)) #f)
	((and loginproc (req/call loginproc)) #f)
	(else
	 (loginheader message)
	 (cond ((and (string? loginform) (not (position #\< loginform)))
		(app/redirect loginform))
	       ((applicable? loginform) (req/call loginform))
	       ((string? loginform) (xhtml loginform))
	       ((table? loginform) (xmleval loginform))
	       (else (div ((class "loginmsg"))
		       "The login form doesn't seem to be configured!  Sorry!")
		     #t)))))


