;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc. All rights reserved

(in-module 'jwt/auth)

(use-module '{fdweb texttools jwt})
(use-module '{varconfig logger crypto ezrecords})
(define %used_modules '{varconfig ezrecords})

(define-init %loglevel %warn%)
;;(define %loglevel %debug!)

;;;; Constant and configurable variables

(define auth:domain #f)
(define-init auth-cookie 'JWT:AUTH)
(define-init auth-cache '_JWT:AUTH)
(define-init identity-cache '__JWT:AUTH)
(config-def! 'auth:id
	     (lambda (var (val))
	       (if (bound? val)
		   (let ((info (jwt/getdomain val)))
		     (set! auth-cookie val)
		     (set! auth-cache (string->symbol (glom "_" val)))
		     (set! identity-cache (string->symbol (glom "__" val)))
		     (when info (set! auth:domain (get info 'issuer))))
		   auth-cookie)))
(config-def! 'auth:domain
	     (lambda (var (val))
	       (if (bound? val)
		   (let ((info (jwt/getdomain val)))
		     (when (not info)
		       (irritant val |NoSuchDomain|
				 "The domain " val " has not been registered with JWT"))
		     (set! auth:domain val)
		     (when (test info 'cookie)
		       (set! auth-cookie (get info 'cookie))
		       (set! auth-cache (string->symbol (glom "_" auth-cookie)))
		       (set! identity-cache (string->symbol (glom "__" auth-cookie)))))
		   auth:domain)))
	     
(define-init auth-domain #f)
(varconfig! auth:domain auth-domain)
(define-init auth-path "/")
(varconfig! auth:path auth-path)
(define-init auth-lifetime (* 7 24 3600))
(varconfig! auth:lifetime auth-lifetime)

;;;; Top level auth functions

(define (auth/getinfo (domain (or auth:domain auth-cookie)) (err #f)
		      (cookiename) (cachename))
  (cond ((eq? domain auth-cookie)
	 (set! cookiename auth-cookie)
	 (set! cachename auth-cache)
	 (set! domain auth:domain))
	((equal? domain auth:domain)
	 (set! cookiename auth-cookie)
	 (set! cachename auth-cache))
	(else (let ((info (jwt/getdomain domain)))
		(cond (info
		       (set! cookiename (getopt info 'cookie auth-cookie))
		       (set! cachename (glom "_" (getopt info 'cookie auth-cookie)))
		       (set! domain (get info 'issuer)))
		      ((symbol? domain)
		       (set! cookiename domain)
		       (set! cachename (string->symbol (glom "_" domain))))
		      (else (set! cookiename auth-cookie)
			    (set! cachename auth-cache))))))
  (req/get cachename
	   (let ((jwt (try (jwt/parse (extract-bearer (req/get 'authorization {})) domain)
			   (jwt/parse (req/get cookiename {}) domain))))
	     (if (exists? jwt) (req/set! cachename jwt))
	     jwt)))
(define (extract-bearer string)
  (get (text->frame #((bos) (spaces*) "Bearer" (spaces) (label token (rest)))
		    string)
       'token))

(define (auth/getid (domain (or auth:domain auth-cookie)) (err #f) 
		    (idcache))
  (cond ((or (eq? domain auth-cookie) (eq? domain auth:domain))
	 (set! idcache identity-cache)
	 (set! domain auth:domain))
	(else (let ((info (jwt/getdomain domain)))
		(cond (info
		       (set! idcache (glom "__" (getopt info 'cookie auth-cookie)))
		       (set! domain (get info 'issuer)))
		      ((symbol? domain)
		       (set! idcache (string->symbol (glom "__" domain)))
		       (set! domain auth:domain))
		      (else (set! cachename auth-cache))))))
  (req/get idcache
	   (let* ((jwt (auth/getinfo domain err))
		  (id (parse-arg (jwt/get jwt 'sub))))
	     (if (exists? id) (req/set! idcache id))
	     id)))

(define (auth/sticky? (arg auth:id))
  (if (jwt? arg)
      (try (jwt/get arg 'sticky) #f)
      (try (jwt/get (auth/getinfo arg) 'sticky) #f)))

;;;; Authorize/deauthorize API

(define (auth/identify! identity (domain (or auth:domain auth-cookie))
			(sticky #t) (cookiename) (cachename) (idcache))
  (cond ((eq? domain auth-cookie)
	 (set! cookiename auth-cookie)
	 (set! cachename auth-cache)
	 (set! idcache identity-cache)
	 (set! domain auth:domain))
	((equal? domain auth:domain)
	 (set! cookiename auth-cookie)
	 (set! cachename auth-cache)
	 (set! idcache identity-cache))
	(else (let ((info (jwt/getdomain domain)))
		(cond (info
		       (set! cookiename (getopt info 'cookie auth-cookie))
		       (set! cachename (glom "_" (getopt info 'cookie auth-cookie)))
		       (set! idcache (glom "__" (getopt info 'cookie auth-cookie)))
		       (set! domain (get info 'issuer)))
		      ((symbol? domain)
		       (set! cookiename domain)
		       (set! cachename (string->symbol (glom "_" domain)))
		       (set! idcache (string->symbol (glom "__" domain))))
		      (else (set! cookiename auth-cookie)
			    (set! cachename auth-cache)
			    (set! idcache identity-cache))))))
  (when (and sticky (not (number? sticky))) 
    (set! sticky auth-lifetime))
  (and identity
       (let* ((payload (if sticky `#["sub" ,identity "sticky" ,sticky]
			   `#["sub" ,identity]))
	      (jwt (jwt/make payload domain)))
	 (detail%watch "AUTH/IDENTIFY!" identity session expires payload jwt
		       (auth->string auth))
	 (req/set! cookiename (jwt-text jwt))
	 (req/set! cachename jwt)
	 (req/set! idcache identity)
	 (if sticky
	     (set-cookie! cookiename (jwt-text jwt) auth-domain auth-path
			  (time+ sticky) #t)
	     (set-cookie! cookiename (jwt-text jwt) auth-domain auth-path
			  #f #t))
	 identity)))

(define (auth/deauthorize! (authid authid))
  (set-cookie! authid "expired" auth-domain auth-path
	       (time- (* 7 24 3600))
	       #t))

(define (auth/update! (domain auth-cookie))
  (let ((jwt (jwt/refreshed (auth/getinfo domain))))
    (when (and (exists? jwt) jwt)
      (set-cookie! domain (jwt-text jwt) auth-domain auth-path
		   (and (jwt/get jwt 'sticky)
			(time+ (jwt/get jwt 'sticky)))
		   #t))))



