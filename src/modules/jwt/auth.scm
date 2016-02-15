;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc. All rights reserved

(in-module 'jwt/auth)

(use-module '{fdweb texttools jwt})
(use-module '{varconfig logger crypto ezrecords})
(define %used_modules '{varconfig ezrecords})

(define-init %loglevel %warn%)
;;(define %loglevel %debug!)

;;;; Constant and configurable variables

;; The cookie/CGI var used to store the session ID
(define-init auth:id 'JWT:AUTH)
(define-init auth:id-cache '_JWT:AUTH)
(config-def! 'auth:id
	     (lambda (var (val))
	       (if (not (bound? val)) auth:id
		   (if (not (symbol? val))
		       (error "Not a symbol" val)
		       (begin (set! auth:id val)
			 (set! auth:id-cache
			       (string->symbol (glom "_" val))))))))

;; The cookie/CGI var used to store the current user
(define-init auth-user 'AUTHUSER)
(define-init user-cookie ".AUTHUSER")
(define-init requser '_AUTHUSER)
(config-def! 'auth:user
	     (lambda (var (val))
	       (if (not (bound? val)) auth-user
		   (if (not (symbol? val))
		       (error "Not a symbol" val)
		       (begin (set! auth-user val)
			 (set! user-cookie (glom "." val))
			 (set! requser (string->symbol (glom "_" val))))))))

;;; Checking tokens

(define-init tokentable (make-hashtable))
(varconfig! auth:tokens tokentable)
(define (table-checktoken identity token (op))
  (if (bound? op)
      (if op
	  (add! tokentable identity token)
	  (drop! tokentable identity token))
      (test tokentable identity token)))

;; This is the default function for checking tokens
;;  It takes an identity and a token value and an #t/#f op
(define-init checktoken table-checktoken)
(varconfig! auth:checktoken checktoken)

;;; JWT in cookies

(define-init jwt:cookie 'jwt:auth)
(varconfig! jwt:cookie jwt:cookie)

(define-init jwt:cookie:key (random-packet 16))
(varconfig! jwt:cookie:key jwt:cookie:key)

(define-init jwt:cookie:refresh 720) ;; 12 minutes
(varconfig! jwt:cookie:refresh jwt:cookie:refresh)
(define-init jwt:cookie:grace #f) ;; defaults to 1 hour (5*12)
(varconfig! jwt:cookie:grace jwt:cookie:grace)

(define-init jwt:cookie:domain #f)
(varconfig! jwt:cookie:domain jwt:cookie:domain)
(define-init jwt:cookie:path #f)
(varconfig! jwt:cookie:path jwt:cookie:path)
(define-init jwt:cookie:ssl #t)
(varconfig! jwt:cookie:ssl jwt:cookie:ssl)

(define (cookie->jwt (name jwt:cookie) (cachename))
  (default! cachename (glom "_" name))
  (try (req/get cachename)
       (tryif (req/get name)
	 (let* ((key (req/get '_jwtkey (or jwt:cookie:key jwt:key)))
		(jwt (jwt/parse (req/get name) key))
		(payload (jwt-payload jwt)))
	   (when (and jwt (jwt-expiration jwt)
		      (> (time) (jwt-expiration jwt)))
	     (if jwt:refresh
		 (set! jwt (jwt:refresh jwt))
		 (begin
		   (store! payload 'exp 
			   (timestamp+ jwt:cookie:refresh))
		   (set! jwt (jwt/make payload key))))
	     (set-jwt-cookie! name jwt))
	   (req/set! cachename jwt)
	   jwt))
       #f))
(define (jwt/set-cookie! 
	 payload 
	 (key (req/get '_jwtkey (or jwt:cookie:key jwt:key)))
	 (name jwt:cookie)
	 (cachename))
  (default! cachename (glom "_" name))
  (let* ((jwt (jwt/make payload key)))
    (req/set! cachename jwt)
    (set-jwt-cookie! name jwt)
    jwt))

(define (set-jwt-cookie! name jwt)
  (if jwt
      (set-cookie! name (jwt-text jwt) 
		   jwt:cookie:domain
		   jwt:cookie:path
		   (time+ (+ jwt:cookie:refresh
			     (or jwt:cookie:grace
				 (* 5 jwt:cookie:refresh))))
		   jwt:cookie:ssl)
      (set-cookie! name "expired" 
		   jwt:cookie:domain
		   jwt:cookie:path
		   (time- (* 7 24 3600))
		   jwt:cookie:ssl)))

(define-init jwt:lifetime 600) ;; 10 minutes
(varconfig! jwt:lifetime jwt:lifetime)

;;;; Top level auth functions

(define jwt:auth:key #f)
(varconfig! jwt:auth:key jwt:auth:key)
(define jwt:auth:alg #f)
(varconfig! jwt:auth:alg jwt:auth:alg)

(define (auth/getinfo (authid auth:id) (err #f)
		      (key (or jwt:auth:key jwt:key))
		      (alg (or jwt:auth:alg jwt:alg)) 
		      (authcache auth:id-cache)
		      (jwt))
  (if (eq? authid auth:id) 
      (set! authcache auth:id-cache)
      (set! authcache (string->symbol (glom "_" authid))))
  (req/get authcache
	   (begin
	     (set! jwt
		   (try (jwt/parse (req/get authid {}) key alg err)
			(jwt/parse (extract-bearer (req/get 'authorization {}))
				   key alg err)))
	     (if (exists? jwt) (req/set! authcache jwt))
	     jwt)))
  
(define (extract-bearer string)
  (get (text->frame #((bos) (spaces*) "Bearer" (spaces) (label token (rest)))
		    string)
       'token))


(define (auth/identify! identity (session #f)
			(expires auth-expiration)
			(authid auth:id))
  (and identity
       (let* ((payload
	       `#["sub" ,identity
		  "exp" ,(if expires
			     (if (> expires (time)) expires
				 (+ (time) expires))
			     (if auth-expiration
				 (+ (time) auth-expiration)
				 (+ (time) 3600)))
		  "sess" ,session])
	      (jwt (jwt/make payload 
			     (or jwt:auth:key jwt:key)
			     (or jwt:auth:alg jwt:alg))))
	 (detail%watch "AUTH/IDENTIFY!" identity session expires payload jwt
	   (auth->string auth))
	 ;; This adds token as a valid token for identity
	 (when checktoken
	   (lognotice |AUTH/Identify|
	     "AUTH/IDENTIFY! " authid "=" identity " w/" token)
	   (checktoken identity token #t))
	 (req/set! authid auth)
	 (set-cookies! auth)
	 identity)))

(define (authfail reason authid info signal)
  (notice%watch "AUTHFAIL" reason authid info)
  (expire-cookie! authid "AUTHFAIL"
		  (or (not info)
		      (not (token/ok? (authinfo-identity info)
				      (authinfo-token info)))))
  (req/drop! authid)
  ;; (logwarn reason " AUTHID=" authid "; INFO=" info)
  (if signal (error reason authid info))
  (fail))

;;; Top level functions

(define (auth/getuser (authid authid))
  (try (req/get requser)
       (let* ((info (auth/getinfo authid))
	      (user (jwt/get info 'sub)))
	 (when (and (exists? user) user)
	   (loginfo IDENTITY (or auth-user authid) "=" user
		    " via " authid " w/token " (authinfo-token info)
		    " issued " (get (timestamp (authinfo-issued info)) 'iso)
		    (when (authinfo-expires info)
		      (printout " expiring "
			(get (timestamp (authinfo-expires info)) 'iso)))
		    (when (authinfo-sticky? info) " across sessions"))
	   (req/set! requser user))
	 (try user #f))
       #f))

(define (auth/sticky? (_authid authid))
  (try (jwt/get (auth/getinfo _authid) 'sticky) #f))

(define (auth/update! (_authid authid))
  (freshauth (auth/getinfo _authid)))

;;;; Authorize/deauthorize API

(define (auth/deauthorize! (authid authid) (info) (uservar user-cookie))
  ;; Get the arguments sorted
  (cond ((authinfo? authid)
	 (set! info authid)
	 (set! authid (authinfo-realm info)))
	(else (default! info (req/get authid))))
  (when (string? info) (set! info (string->auth info)))
  (when info
    (when checktoken
      (checktoken (authinfo-identity info) (authinfo-token info) #f)))
  (expire-cookie! authid "AUTH/DEAUTHORIZE!")
  (expire-cookie! uservar "AUTH/DEAUTHORIZE!"))

