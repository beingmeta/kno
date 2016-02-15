;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc. All rights reserved

(in-module 'jwt)

(use-module '{ezrecords fdweb texttools jsonout crypto packetfns})
(use-module '{varconfig logger})
(define %used_modules 'varconfig)

(define %nosubst '{jwt:key jwt:alg jwt:pad
		   jwt:refresh jwt:iat jwt:jti jwt:issuer})

(module-export! '{jwt-header jwt-payload jwt-text jwt-signature
		  jwt/parse jwt/check 
		  jwt/make jwt/string 
		  jwt/refresh jwt/refreshed
		  jwt/get jwt?})

;;; Default configuration

(define-init jwt:key #f)
(varconfig! jwt:key jwt:key)

(define-init jwt:alg #f)
(varconfig! jwt:alg jwt:alg)

(define-init jwt:pad #f)
(varconfig! jwt:pad jwt:pad)

(define-init jwt:issuer #f)
(varconfig! jwt:issuer jwt:issuer)

(define-init jwt:iat #f)
(varconfig! jwt:iat jwt:iat )

(define-init jwt:jti #f)
(varconfig! jwt:jti jwt:jti)

;;; This checks the payload for validity.  It is a function
;;;  of the form (fn payload passive err)
;;; If passive is #t, it returns the payload unchanged,
;;;  otherwise it may update the payload.
;;; Err determines whether or not an error is returned if the 
;;;  check fails
(define-init jwt/checker #f)
(varconfig! jwt/checker jwt:checker)

;;; The table for configuration options

(define-init config-table (make-hashtable))
(config-def! 'jwt:config
	     (lambda (var (val))
	       (if (bound? val)
		   (store! config-table (get val 'iss) val)
		   (deep-copy (get config-table (getkeys config-table))))))

(define handle-jwt-args
  (macro (expr)
    (default! opts (try (get config-table (get payload 'iss))
			(get config-table (get payload "iss"))
			(req/get 'jwt:config)
			#f))
    (when (and (string? opts) (test config-table opts))
      (set! opts (get config-table opts)))
    (default! key (getopt opts 'key jwt:key)) 
    (default! alg (getopt opts 'alg jwt:alg))
    (default! checker (getopt opts 'checker jwt:checker))
    (default! issuer (getopt opts 'issuer jwt:issuer))
    ))

(defrecord jwt header payload signature text
  (checked #f) (expiration #f))

(define (jwt-body jwt (text) (dot1) (dot2))
  (set! text (jwt-text jwt))
  (set! dot1 (position #\. text))
  (set! dot2 (and dot1 (position #\. text (1+ dot1))))
  (if dot2 (slice text 0 dot2)
      (irritant text "Invalid JWT token string")))

;;; Utility functions

(define (b64->packet string)
  (base64->packet (uridecode (string-subst* string "-" "+"  "_" "/"))))
(define (b64->string string)
  (packet->string
   (base64->packet (uridecode (string-subst* string "-" "+"  "_" "/")))))
(define (b64->json string)
  (jsonparse
   (packet->string
    (base64->packet (uridecode (string-subst* string "-" "+"  "_" "/"))))))

;;; Working with existing tokens

(define (jwt/parse string (opts) (key) (alg) (checker) (issuer)
		   (err #f) (segs) (header) (signature))
  (handle-jwt-args)
  (set! segs (segment string "."))
  (if key
      (let ((header (b64->json (car segs)))
	    (payload (b64->json (cadr segs)))
	    (signature (b64->packet (caddr segs)))
	    (body (glom (car segs) "." (cadr segs))))
	(when (not alg) (set! alg (try (get header 'alg) "HS256")))
	(if (and (test header 'alg alg)
		 (or (not issuer) (test payload 'iss issuer))
		 (or (and (equal? "HS256" (upcase alg))
			  (equal? signature (hmac-sha256 key body)))
		     ;; Include other algorithms here someday
		     ))
	    (if (or (not checker) (checker payload #t err))
		(cons-jwt header payload signature string 
			  key (try (get payload "exp") #f))
		(and err (signal-error string key alg issuer header 
				       payload signature body)))
	    (and err (signal-error string key alg issuer header 
				   payload signature body))))
      (cons-jwt (b64->json (car segs)) (b64->json (cadr segs)) 
		(b64->packet (caddr segs))
		string)))

(define (jwt/check jwt (opts) (key) (alg) (checker) (issuer))
  (handle-jwt-args)
  (if (string? jwt)
      (jwt/parse jwt opts key alg checker issuer)
      (begin
	(handle-jwt-args)
	(and (test (jwt-header jwt) 'alg (upcase alg))
	     (packet? (jwt-signature jwt))
	     (if (or (and (equal? "HS256" (upcase alg))
			  (equal? (jwt-signature jwt) 
				  (hmac-sha256 key (jwt-body jwt))))
		     ;; Include other algorithms here someday
		     )
		 (and (or (not checker) 
			  (checker (jwt-payload jwt) #t #f))
		      (cons-jwt (jwt-header jwt) (jwt-payload jwt)
				(jwt-signature jwt) (jwt-text jwt)
				key (try (get (jwt-payload jwt) 'exp) #f))))))))

(define (signal-error string key alg issuer header payload signature body)
  (if (not (test header 'alg alg))
      (irritant header |WrongAlgorithm| 
		alg " != " (get header 'alg)
		" for JWT " string)
      (if (and issuer (not (test payload 'iss issuer)))
	  (irritant (try (get payload 'iss) #f) |Wrong issuer|
		    "The issuer did not match " issuer
		    "with payload \n" (pprint payload))
	  (irritant string |IncorrectSignature|
		    "Given " signature " != "
		    (hmac-sha256 key body)
		    "\n with header " header 
		    "\n and payload\n  " (pprint payload)))))
  
;;; Generating new tokens

(define (jwt/make payload (opts) (key) (alg) (checker) (issuer)
		  (header) (hdr64) (pay64) (sig))
  (handle-jwt-args)
  (if (testopt opts 'header)
      (set! header (keys->strings (getopt opts 'header)))
      (set! header (frame-create #f)))
  (when (table? payload) (set! payload (keys->strings payload)))
  (when (and issuer (not (test payload "iss")))
    (store! payload "iss" issuer))
  (when (and jwt:iat (not (test payload "iat")))
    (store! payload "iat" (get (gmtimestamp) 'iso)))
  (when (and jwt:jti (not (test payload "jti")))
    (if (applicable? jwt:jti)
	(store! payload "jti" (jwt:jti))
	(store! payload "jti" (uuid->string (getuuid)))))
  (set! hdr64 (->base64 (->json header) #t))
  (set! pay64 (if (string? payload) (->base64 payload #t)
		  (->base64 (->json payload) #t)))
  (set! sig (hmac-sha256 key (glom hdr64 "." pay64)))
  (cons-jwt header payload sig
	    (glom hdr64 "." pay64 "." (->base64 sig #t))
	    key #t))

(define (jwt/string payload (opts) (key) (alg) (checker) (issuer)
		    (header #f) (hdr64) (pay64) (sig))
  (handle-jwt-args)
  (if (testopt opts 'header)
      (set! header (keys->strings (getopt opts 'header)))
      (set! header (frame-create #f)))
  (when checker (set! payload (checker payload #t #t)))
  (when (table? payload) (set! payload (keys->strings payload)))
  (set! hdr64 (->base64 (->json header)))
  (set! pay64 (if (string? payload) (->base64 payload)
		  (->base64 (->json payload))))
  (set! sig (hmac-sha256 key (glom hdr64 "." pay64)))
  (glom hdr64 "." pay64 "." (packet->base64 sig)))

(define (keys->strings table (keys) (newtable))
  (set! keys (getkeys table))
  (if (exists? (reject keys string?))
      (begin (set! newtable (frame-create #f))
	(do-choices (key keys)
	  (if (string? key) 
	      (store! newtable key (get table key))
	      (if (symbol? key)
		  (store! newtable (downcase key) (get table key))
		  (store! newtable (->json key) (get table key)))))
	newtable)
      table))

;;; Utilities

(define (jwt/get jwt slotid)
  (get (jwt-payload jwt) slotid))

;;; Refreshing tokens

(define (jwt/refresh jwt (opts) (key) (alg) (checker) (issuer))
  "Refresh a JWT if needed or returns the JWT as is"
  (handle-jwt-args)
  (and (or (jwt-checked jwt) (jwt/check jwt key alg checker))
       (if (jwt-expiration jwt)
	   (if (> (time) (jwt-expiration jwt)) 
	       (let ((payload (if checker 
				  (checker (jwt-payload jwt) #t #t)
				  (deep-copy (jwt-payload jwt)))))
		 (when payload (store! payload 'exp (time+ threshold)))
		 (and payload (jwt/make payload key alg checker)))
	       jwt)
	   jwt)))

(define (jwt/refreshed jwt (opts) (key) (alg) (checker) (issuer)
		       (payload))
  "Refresh a JWT if needed or fails otherwise"
  (and (or (jwt-checked jwt) (jwt/check jwt))
       (if (jwt-expiration jwt)
	   (if (> (time) (jwt-expiration jwt)) 
	       (begin (handle-jwt-args)
		 (set! payload 
		       (if checker 
			   (checker (jwt-payload jwt) #t #t)
			   (deep-copy (jwt-payload jwt))))
		 (when payload (store! payload 'exp (time+ threshold)))
		 (and payload (jwt/make payload key alg checker)))
	       (fail))
	   jwt)))

