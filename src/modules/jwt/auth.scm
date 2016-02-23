;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc. All rights reserved

(in-module 'jwt/auth)

(use-module '{fdweb texttools jwt})
(use-module '{varconfig logger crypto ezrecords})
(define %used_modules '{varconfig ezrecords})

(define-init %loglevel %warn%)
(set! %loglevel %debug!)

(module-export! '{auth/getinfo auth/getid 
		  auth/identify! auth/update! 
		  auth/deauthorize! auth/sticky?
		  auth/maketoken})

;;;; Constant and configurable variables

(define-init jwt/auth/domain #f)
(varconfig! jwt:auth:domain jwt/auth/domain)
(define-init jwt/auth/key #f)
(varconfig! jwt:auth:key jwt/auth/key)

(define-init auth-cookie 'JWT:AUTH)
(define-init auth-cache '_JWT:AUTH)
(define-init identity-cache '__JWT:AUTH)
(config-def! 'jwt:cookie
	     (lambda (var (val))
	       (if (bound? val)
		   (begin
		     (lognotice |JWT:AUTH| "Set cookie to " val)
		     (set! auth-cookie val)
		     (set! auth-cache (string->symbol (glom "_" val)))
		     (set! identity-cache (string->symbol (glom "__" val))))
		   auth-cookie)))
	     
(define-init cookie-host #f)
(varconfig! jwt:cookie:host cookie-host)
(define-init cookie-path "/")
(varconfig! jwt:cookie:path cookie-path)
(define-init cookie-lifetime (* 7 24 3600))
(varconfig! jwt:cookie:lifetime cookie-lifetime)

(define (auth/maketoken (length 7) (mult (microtime)))
  (let ((sum 0) (modulus 1))
    (dotimes (i length)
      (set! sum (+ (* 256 sum) (random 256)))
      (set! modulus (* modulus 256)))
    (remainder (* sum mult) modulus)))

;;;; Top level auth functions

(define (auth/getinfo (cookie auth-cookie) 
		      (jwtarg (or jwt/auth/domain jwt/auth/key)) (err #f)
		      (cachename))
  (if (eq? cookie auth-cookie)
      (set! cachename auth-cache)
      (set! cachename (string->symbol (glom "_" cookie))))
  (req/get cachename
	   (let* ((bjwt (jwt/parse (extract-bearer (req/get 'authorization {})) jwtarg))
		  (jwt #f))
	     (if (fail? bjwt)
		 (set! jwt (jwt/parse (or (req/get cookie {}) {}) jwtarg))
		 (set! jwt bjwt))
	     (when (fail? jwt)
	       (loginfo |JWT/AUTH/getinfo| 
		 "Couldn't get JWT " jwt " from bearer or " cookie))
	     (when (exists? jwt)
	       (loginfo |JWT/AUTH/getinfo| 
		 "Got JWT " jwt " from " 
		 (if (exists? bjwt) "Bearer authorization" cookie))
	       (req/set! cachename jwt))
	     jwt)))
(define (extract-bearer string)
  (get (text->frame #((bos) (spaces*) "Bearer" (spaces) (label token (rest)))
		    string)
       'token))

(define (auth/getid (cookie auth-cookie) (err #f) 
		    (idcache))
  (if (eq? cookie auth-cookie)
      (set! idcache identity-cache)
      (set! idcache (string->symbol (glom "__" cookie))))
  (req/get idcache
	   (let* ((jwt (auth/getinfo cookie err))
		  (id (tryif jwt (parse-arg (jwt/get jwt 'sub)))))
	     (when (exists? id)
	       (loginfo |JWT/AUTH/getid| "Got id " id " from " jwt)
	       (req/set! idcache id))
	     (try id #f))))

(define (auth/sticky? (arg auth-cookie))
  (if (jwt? arg)
      (try (jwt/get arg 'sticky) #f)
      (try (jwt/get (auth/getinfo arg) 'sticky) #f)))

;;;; Authorize/deauthorize API

(define (auth/identify! identity (cookie auth-cookie) 
			(jwtarg (or jwt/auth/domain jwt/auth/key))
			(sticky #t))
  (when (and sticky (not (number? sticky))) 
    (set! sticky cookie-lifetime))
  (and identity
       (let* ((payload (if sticky `#[sub ,identity sticky ,sticky]
			   `#[sticky ,identity]))
	      (jwt (jwt/make payload jwtarg))
	      (text (jwt-text jwt)))
	 (lognotice |JWT/AUTH/identify!|
	   "Setting " (if (number? sticky)
			  (printout "sticky (" (secs->string sticky) ")")
			  (if sticky "sticky" ""))
	   " identity " identity " in " cookie " for " jwtarg)
	 (detail%watch "AUTH/IDENTIFY!" 
	   identity session expires payload jwt (auth->string auth))
	 (unless text
	   (logwarn |JWT/MAKE/Failed| 
	     "Couldn't sign JWT for " identity " with arg=" jwtarg
	     ", payload=" payload ", and jwt=" jwt))
	 (when text
	   (req/set! cookie text)
	   (req/set! (string->symbol (glom "_" cookie)) jwt)
	   (req/set! (string->symbol (glom "__" cookie)) identity)
	   (if sticky
	       (set-cookie! cookie text cookie-host cookie-path
			    (time+ sticky) #t)
	       (set-cookie! cookie text cookie-host cookie-path
			    #f #t)))
	 identity)))

(define (auth/deauthorize! (cookie auth-cookie))
  (when (req/test cookie)
    (set-cookie! cookie "expired" cookie-host cookie-path
		 (time- (* 7 24 3600))
		 #t)))

(define (auth/update! (cookie auth-cookie) 
		      (jwtarg (or jwt/auth/domain jwt/auth/key)))
  (when (req/test 'cookie)
    (let ((jwt (jwt/refreshed (auth/getinfo cookie jwtarg))))
      (when (and (exists? jwt) jwt)
	(lognotice |JWT/AUTH/update| "Updating JWT authorization " jwt)
	(set-cookie! cookie (jwt-text jwt) cookie-host cookie-path
		     (and (jwt/get jwt 'sticky)
			  (time+ (jwt/get jwt 'sticky)))
		     #t)))))




