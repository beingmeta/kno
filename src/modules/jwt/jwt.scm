;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc. All rights reserved

(in-module 'jwt)

(use-module '{ezrecords fdweb texttools jsonout crypto packetfns})
(use-module '{varconfig logger})
(define %used_modules 'varconfig)

(define %nosubst '{jwt/key jwt/algorithm jwt/pad jwt/checker})

(module-export! '{jwt? jwt-valid jwt:domain
		  jwt-header jwt-payload jwt-text jwt-signature 
		  jwt/parse jwt/valid? jwt/check 
		  jwt/make jwt/string 
		  jwt/refresh jwt/refreshed
		  jwt/getdomain
		  jwt/get jwt?})

;;; Default configuration

(define-init jwt:domain #f)
(varconfig! jwt:domain jwt:domain)

(define-init jwt/key #f)
(varconfig! jwt:key jwt/key)

(define-init jwt/pad #f)
(varconfig! jwt:pad jwt/pad)

(define-init jwt/algorithm #f)
(varconfig! jwt:algorithm jwt/algorithm)

(define-init jwt/refresh 3600) ;; one hour
(varconfig! jwt:refresh jwt/refresh)

(define-init jwt-verbose #f)
(varconfig! jwt:verbose jwt-verbose)

;;; This checks the payload for validity.  It is a function
;;;  of the form (fn payload update err)
;;; If update is #t, it may return an updated payload,
;;;  otherwise it returns the payload or #f
;;; Err determines whether or not an error is returned if the 
;;;  check fails
(define-init jwt/checker #f)
(varconfig! jwt:checker jwt/checker)

;;; The table for configuration options

(define-init config-table (make-hashtable))
(config-def! 'jwt:config
	     (lambda (var (val))
	       (if (bound? val)
		   (begin
		     (store! config-table (get val 'issuer) val)
		     (store! config-table (get val 'cookie) val))
		   (deep-copy (get config-table (getkeys config-table))))))


(define (jwt/getdomain spec) (try (get config-table spec) #f))

;;; Common code for handling args to JWT functions

(define handle-jwt-args
  (macro expr
    `(begin
       (default! opts (and jwt:domain (get config-table jwt:domain)))
       (when (and (string? opts) (test config-table opts))
	 (set! opts (get config-table opts)))
       (when (or (packet? opts) (string? opts))
	 (set! key opts)
	 (set! alg (or jwt/algorithm "HS256"))
	 (set! opts #f))
       (default! key (getopt opts 'key jwt/key)) 
       (default! alg (getopt opts 'alg (and jwt/key jwt/algorithm)))
       (default! checker (getopt opts 'checker jwt/checker))
       (default! issuer (getopt opts 'issuer jwt:domain))
       )))

(define (jwt->string jwt (payload))
  (set! payload (jwt-payload jwt))
  (stringout (if (jwt-valid jwt) "#<JWT*" "#<JWT")
    (when jwt-verbose (printout " " (jwt-text jwt)))
    (if (table? payload)
	(do-choices (key (getkeys payload))
	  (printout " " key "=" (write (get payload key))))
	payload)
    ">"))

(defrecord (jwt #[stringfn jwt->string])
  header payload signature text
  (valid #f) (expiration #f))

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
      (let ((header (b64->json (elt segs 0)))
	    (payload (b64->json (elt segs 1)))
	    (signature (b64->packet (elt segs 2)))
	    (body (glom (car segs) "." (cadr segs))))
	(when (not alg) (set! alg (try (get header 'alg) "HS256")))
	(if (and (or (not alg) (test header 'alg alg))
		 (or (not issuer) (test payload 'iss issuer))
		 (test-signature signature key alg (elt segs 0) (elt segs 1)))
	    (cons-jwt header payload signature string 
		      key (try (get payload "exp") #f))
	    (and err (signal-error string key alg issuer header 
				   payload signature body))))
      (cons-jwt (b64->json (car segs)) (b64->json (cadr segs)) 
		(b64->packet (caddr segs))
		string)))

(define (jwt/valid? jwt (opts) (key) (alg) (checker) (issuer)
		    (header) (payload))
  (handle-jwt-args)
  (default! header (jwt-header jwt))
  (default! payload (jwt-payload jwt))
  (if key
      (and (or (not alg) (test header 'alg alg))
	   (or (not issuer) (test payload 'iss issuer))
	   (test-signature (jwt-signature jwt) key
			   (try (get header 'alg) 
				(or jwt/algorithm "HS256"))
			   (jwt-body jwt)))
      (error |JWT/NoKey| "No key was provided to validate " jwt)))

(define (jwt/check jwt (opts) (key) (alg) (checker) (issuer)
		   (header) (payload))
  (handle-jwt-args)
  (set! header (jwt-header jwt))
  (set! payload (jwt-payload jwt))
  (if key
      (and (or (not alg) (test header 'alg alg))
	   (or (not issuer) (test payload 'iss issuer))
	   (test-signature (jwt-signature jwt) key 
			   (try (get header 'alg) 
				(or jwt/algorithm "HS256"))
			   (jwt-body jwt))
	   (and (or (not checker) (checker (jwt-payload jwt)))
		(cons-jwt (jwt-header jwt) (jwt-payload jwt)
			  (jwt-signature jwt) (jwt-text jwt)
			  key (try (get (jwt-payload jwt) 'exp) #f))))
      (error |JWT/NoKey| "No key was provided to validate " jwt)))

;; With three args, the third arg is header64.payload64; with four, the first
;; is the base64 of the header and the second is the base64 of the payload
(define (test-signature sig key alg h64 (p64 #f))
  (and (packet? sig)
       (or (and (equal? "HS256" (upcase alg))
		(equal? sig (hmac-sha256 key (if p64 (glom h64 "." p64) h64))))
	   ;; Include other algorithms here someday
	   )))

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
		  (header) (nopad) (hdr64) (pay64) (sig))
  (handle-jwt-args)
  (set! nopad (not (getopt opts 'pad64 jwt/pad)))
  (if (testopt opts 'header)
      (set! header (keys->strings (getopt opts 'header)))
      (set! header `#["typ" "JWT" "alg" ,alg]))
  (when (table? payload) (set! payload (keys->strings payload)))
  (when (and issuer (not (test payload "iss")))
    (store! payload "iss" issuer))
  (set! hdr64 (->base64 (->json header) nopad))
  (set! pay64 (if (string? payload) 
		  (->base64 payload nopad)
		  (->base64 (->json payload) nopad)))
  (set! sig (and key (hmac-sha256 key (glom hdr64 "." pay64))))
  (cons-jwt header payload sig
	    (glom hdr64 "." pay64 "." (->base64 sig #t))
	    (and key #t) #t))

(define (jwt/string payload (opts) (key) (alg) (checker) (issuer)
		    (header #f) (nopad) (hdr64) (pay64) (sig))
  (handle-jwt-args)
  (set! nopad (not (getopt opts 'pad64 jwt/pad)))
  (if (testopt opts 'header)
      (set! header (keys->strings (getopt opts 'header)))
      (set! header `#["typ" "JWT" "alg" ,alg]))
  (when checker (set! payload (checker payload #t #t)))
  (when (table? payload) (set! payload (keys->strings payload)))
  (set! hdr64 (->base64 (->json header) nopad))
  (set! pay64 (if (string? payload) (->base64 payload nopad)
		  (->base64 (->json payload) nopad)))
  (set! sig (hmac-sha256 key (glom hdr64 "." pay64)))
  (glom hdr64 "." pay64 "." (packet->base64 sig nopad)))

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
  (and (or (jwt-valid jwt) (jwt/valid? jwt key alg checker issuer))
       (or (and (jwt-expiration jwt) (< (time) (jwt-expiration jwt)))
	   (and (not (jwt-expiration jwt)) (not checker))
	   (if (jwt-expiration jwt)
	       (let ((payload (if checker 
				  (checker (jwt-payload jwt) #t #f)
				  (deep-copy (jwt-payload jwt))))
		     (refresh (getopt opts 'refresh jwt/refresh)))
		 (when payload 
		   (if refresh
		       (store! payload 'exp (time+ refresh))
		       (drop! payload 'exp)))
		 (and payload 
		      (jwt/make payload opts key alg checker issuer)))
	       jwt))))

(define (jwt/refreshed jwt (opts) (key) (alg) (checker) (issuer))
  "Refresh a JWT if needed or fails otherwise"
  (and (or (jwt-valid jwt) (jwt/check jwt))
       (if (or (and (jwt-expiration jwt) (< (time) (jwt-expiration jwt)))
	       (and (not (jwt-expiration jwt)) (not checker)))
	   (fail)
	   (let ((payload (if checker 
			      (checker (jwt-payload jwt) #t #f)
			      (deep-copy (jwt-payload jwt))))
		 (refresh (getopt opts 'refresh jwt/refresh)))
	     (when payload 
	       (if refresh
		   (store! payload 'exp (time+ refresh))
		   (drop! payload 'exp)))
	     (and payload 
		  (jwt/make payload opts key alg checker issuer))))))


