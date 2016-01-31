;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

(in-module 'facebook)

;;; Basic access to facebook, including authorization

(use-module '{fdweb texttools reflection varconfig logger})
(use-module '{xhtml xhtml/auth facebook/fbcall})

(define-init %loglevel %notice%)
;;(set! %loglevel %debug!)

(module-export! '{appname appid apikey apisecretkey fb/authorized})
(module-export! '{fb/session! fb/sessions fb/auth fb/connected?})
(module-export! '{fb/embedded? fb/incanvas? fb/added?})

(define applock #f)
(define appname #f)
(define apphost #f)
(define approot #f)
(define appid #f)
(define apikey #f)
(define apisecretkey #f)
(define callback "/fb/auth")
(define info-cookie 'fbinfo)

(config-def! 'fb:appinfo
	     (lambda (var (val))
	       (if (bound? val)
		   (if applock
		       (error "Facebook appinfo is locked")
		       (if (and (vector? val) (= (length val) 4))
			   (begin (set! appname (first val))
				  (set! appid (second val))
				  (set! apikey (third val))
				  (set! apisecretkey (fourth val)))
			   (error "Invalid Facebook application info")))
		   (vector appname appid apikey))))

(config-def! 'fb:applock
	     (lambda (var (val))
	       (if (bound? val)
		   (if applock
		       (error "Facebook appinfo is locked")
		       (set! applock val))
		   appname)))
(config-def! 'fb:appname
	     (lambda (var (val))
	       (if (bound? val)
		   (if applock
		       (error "Facebook appinfo is locked")
		       (set! appname val))
		   appname)))
(config-def! 'fb:appid
	     (lambda (var (val))
	       (if (bound? val)
		   (if applock
		       (error "Facebook appinfo is locked")
		       (set! appid val))
		   appid)))
(config-def! 'fb:key
	     (lambda (var (val))
	       (if (bound? val)
		   (if applock
		       (error "Facebook appinfo is locked")
		       (set! apikey val))
		   apikey)))
(config-def! 'fb:secret
	     (lambda (var (val))
	       (if (bound? val)
		   (if applock
		       (error "Facebook appinfo is locked")
		       (set! apisecretkey val))
		   apisecretkey)))
(varconfig! fb:host apphost)
(varconfig! fb:approot approot)
(varconfig! fb:infocookie info-cookie)

;;; Facebook info

(define fb/sessions (make-hashtable))
(define (fb/session! info (user #f))
  (debug%watch "FB/SESSION!" info user)
  (cond ((not info)
	 (drop! fb/sessions (get (get fb/sessions user) "session_key"))
	 (drop! fb/sessions user))
	(else (store! fb/sessions (get info "session_key") info)
	      (when user (store! fb/sessions user info))
	      (cgiset! 'fb_sig_session_key (get info "session_key"))
	      (cgiset! 'fb_sig_session_user (get info "uid")))))

(define (fb/connected? (info (try (get fb/sessions (auth/getuser)) #f)) (check #t))
  (when (cgitest 'session)
    (set! info (jsonparse (cgiget 'session)))
    (fb/session! info (auth/getuser)))
  (cond ((not (and (exists? info) info (test info "expires")))
	 (debug%watch "FB/CONNECTED? (no info) " info)
	 #f)
	((not (or (zero? (get info "expires")) (> (get info "expires") (time))))
	 (debug%watch "FB/CONNECTED? (expired) " info)
	 #f)
	(else 
	 (cgiset! 'fb_sig_session_user (get info "uid"))
	 (cgiset! 'fb_sig_session_key (get info "session_key"))
	 (cgiset! 'fbexpires (timestamp (get info "expires")))
	 (cgiset! 'fbsecret (get info "secret"))
	 (cond ((and check (not (onerror (fb/getmyid) #f)))
		(debug%watch "FB/CONNECTED? (closed) " info)
		(fb/session! #f)
		#f)
	       (else (debug%watch "FB/CONNECTED? (yes) " info)
		     info)))))

(define (fb/auth (nextstop #f) (req_perms "") (user (auth/getuser)))
  (debug%watch "FB/AUTH" nextstop user (cgiget 'session))
  (if (cgitest 'session)
      (let ((info (jsonparse (cgiget 'session))))
	(when user (fb/session! info user))
	info)
      (or (fb/connected?)
	  (begin
	    (cgiset! 'status 303)
	    (set-cookie! 'nextstop (or nextstop (cgiget 'request_uri))
			 apphost approot)
	    (httpheader
	     "Location: "
	     (scripturl "http://www.facebook.com/login.php"
	       "api_key" (config 'fb:key) "v" "1.0"
	       "fbconnect" "true" "return_session" "true"
	       "connect_display" "page"
	       "req_perms" req_perms
	       "next" (or callback "")))
	    #f))))

;;;; Doing stuff

(define (fb/incanvas?)
  (and (cgitest 'fb_sig_in_canvas)
       (cgiget 'fb_sig_in_canvas)
       (not (cgitest 'fb_sig_in_canvas 0))))

(define (fb/iniframe?)
  (and (cgitest 'fb_sig_in_iframe)
       (cgiget 'fb_sig_in_iframe)
       (not (cgitest 'fb_sig_in_iframe 0))))

(define (fb/embedded?)
  (or (cgitest 'fb/embedded)
      (fb/iniframe?)
      (fb/incanvas?)))

(define (fb/added?)
  (and (cgitest 'fb_sig_added)
       (cgiget 'fb_sig_added)
       (not (cgitest 'fb_sig_added 0))))

;;;; New login stuff

(define (atoken x)
  (try (get (text->frames #("access_token=" (label at (not> "&")) {(eos) "&"}) x) 'at)
       x))

(define (fb/authorized code reuri)
  (debug%watch "FB/AUTHORIZED" code)
  (let* ((appkey (config 'fb:key))
	 (appsecret (config 'fb:secret))
	 (reqaccess (scripturl "https://graph.facebook.com/oauth/access_token"
			"client_id" appkey "redirect_uri" reuri
			"client_secret" appsecret "code" code))
	 (accessreq (debug%watch (urlget reqaccess) reqaccess))
	 (access (atoken (get accessreq '%content)))
	 (reqinfo (scripturl "https://graph.facebook.com/me"
		      "access_token" access
		      "fields" "id,name,picture,about,bio,website"))
	 (info (jsonparse (get (debug%watch (urlget reqinfo) reqinfo) '%content)
			  24 #[id #t]))
	 (reqfriends (scripturl "https://graph.facebook.com/me/friends"
			 "access_token" access
			 "fields" "name"))
	 (reqgroups
	  (scripturl "https://graph.facebook.com/me/groups"
	      "access_token" access "fields" "name,description"))
	 (reqpages (scripturl "https://graph.facebook.com/me/likes"
		       "access_token" access
		       "fields" "name,description")))
    (debug%watch "FB/AUTHORIZED" info access accessreq)
    (store! info 'type 'user)
    (add! info 'friends
	  (elts (get (jsonparse (urlcontent reqfriends) 24 #[id #t])
		     'data)))
    (add! (get info 'friends) 'type 'user)
    (add! info 'groups
	  (elts (get (jsonparse (urlcontent reqgroups) 24 #[id #t])
		     'data)))
    (add! (get info 'groups) 'type 'group)
    (add! info 'pages
	  (elts (get (jsonparse (urlcontent reqpages) 24 #[id #t]) 'data)))
    (add! (get info 'pages) 'type 'page)
    (cgiset! 'fb/access access)
    info))

