;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc. All rights reserved

(in-module 'jwt/auth)

(use-module '{fdweb texttools jwt})
(use-module '{varconfig logger crypto ezrecords})
(define %used_modules '{varconfig ezrecords})

(define-init %loglevel %warn%)
;;(set! %loglevel %debug!)

(module-export! '{auth/getinfo auth/getid 
		  auth/identify! auth/update! 
		  auth/deauthorize! auth/sticky?
		  auth/maketoken})

;;;; Constant and configurable variables

(define-init jwt/auth/domain #f)
(varconfig! jwt:auth:domain jwt/auth/domain)
(define-init jwt/auth/defaults #[key "secret" alg "HS256"])
(varconfig! jwt:auth:defaults jwt/auth/defaults)

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
		      (jwtarg (or jwt/auth/domain jwt/auth/defaults)) 
		      (err #f)
		      (cachename))
  (if (eq? cookie auth-cookie)
      (set! cachename auth-cache)
      (set! cachename (string->symbol (glom "_" cookie))))
  (req/get cachename
	   (let* ((bjwt (jwt/parse (get-bearer-token) jwtarg))
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
(define (get-bearer-token)
  (extract-bearer (req/get 'authorization {})))

(define (auth/getid (cookie auth-cookie) 
		    (jwtarg (or jwt/auth/domain jwt/auth/defaults))
		    (err #f) 
		    (idcache))
  (if (eq? cookie auth-cookie)
      (set! idcache identity-cache)
      (set! idcache (string->symbol (glom "__" cookie))))
  (req/get idcache
	   (let* ((jwt (auth/getinfo cookie jwtarg err))
		  (id (tryif jwt (parse-arg (jwt/get jwt 'sub)))))
	     (when (and (exists? jwt) (fail? id))
	       (logwarn |JWT/AUTH/noid| "Couldn't get id (sub) from " jwt))
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
			(jwtarg (or jwt/auth/domain jwt/auth/defaults))
			(payload #t))
  (cond ((number? payload) 
	 (set! payload `#[sticky ,payload]))
	((eq? payload #t)
	 (set! payload `#[sticky ,cookie-lifetime]))
	((not payload) (set! payload `#[]))
	((not (table? payload)) (set! payload `#[])))
  (when (jwt? identity)
    (set! payload (jwt-payload identity))
    (set! identity (get payload 'sub)))
  (and identity
       (let* ((payload (frame-update payload 'sub identity))
	      (jwt (jwt/make payload jwtarg))
	      (text (jwt-text jwt)))
	 (lognotice |JWT/AUTH/identify!|
	   "Identity=" identity " in " cookie 
	   " signed by " jwtarg " w/payload " 
	   "\n" (pprint payload))
	 (detail%watch "AUTH/IDENTIFY!" 
	   identity session expires payload jwt (auth->string auth))
	 (unless text
	   (logwarn |JWT/MAKE/Failed| 
	     "Couldn't sign JWT for " identity " with arg=" jwtarg
	     ", payload=" payload ", and jwt=" jwt))
	 (when text
	   (let* ((info (and (table? cookie) cookie))
		  (name (if info
			    (try (get info 'name) auth-cookie)
			    cookie))
		  (domain (if info 
			      (try (get info 'domain) cookie-host)
			      cookie-host))
		  (path (if info 
			    (try (get info 'path) cookie-path)
			    cookie-path)))
	     (req/set! name text)
	     (req/set! (string->symbol (glom "_" name)) jwt)
	     (req/set! (string->symbol (glom "__" name)) identity)
	     (if (test payload 'sticky)
		 (set-cookie! name text domain path
			      (time+ (get payload 'sticky)) #t)
		 (set-cookie! name text domain path #f #t))))
	 identity)))

(define (auth/deauthorize! (cookie auth-cookie))
  (when (req/test cookie)
    (set-cookie! cookie "expired" cookie-host cookie-path
		 (time- (* 7 24 3600))
		 #t)))

(define (auth/update! (cookie auth-cookie) 
		      (jwtarg (or jwt/auth/domain jwt/auth/defaults)))
  (when (req/test 'cookie)
    (let ((jwt (jwt/refreshed (auth/getinfo cookie jwtarg))))
      (when (and (exists? jwt) jwt)
	(lognotice |JWT/AUTH/update| "Updating JWT authorization " jwt)
	(set-cookie! cookie (jwt-text jwt) cookie-host cookie-path
		     (and (jwt/get jwt 'sticky)
			  (time+ (jwt/get jwt 'sticky)))
		     #t)))))
