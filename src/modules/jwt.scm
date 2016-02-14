;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc. All rights reserved

(in-module 'jwt)

(use-module '{ezrecords fdweb texttools jsonout crypto packetfns})
(use-module '{varconfig logger})
(define %used_modules 'varconfig)

(define %nosubst '{jwt:pad jwt:key jwt:refresh})
(set+! %nosubst '{jwt:cookie jwt:cookie:key jwt:cookie:refresh
		  jwt:cookie:grace jwt:cookie:domain
		  jwt:cookie:path
		  jwt:cookie:ssl})


(module-export! '{jwt-header jwt-payload jwt-text jwt-signature
		  jwt/parse jwt/check 
		  jwt/make jwt/string 
		  jwt/refresh jwt/refreshed
		  jwt/get jwt?})

(define-init jwt:key #f)
(varconfig! jwt:key jwt:key)

(define-init jwt:pad #f)
(varconfig! jwt:pad jwt:pad)

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

(define (->b64 s)
  (if jwt:pad (->base64 s)
      (strip-suffix (->base64 s) {"=" "==" "==="})))

;;; Working with existing tokens

(define (jwt/parse string (key (req/get '_jwtkey jwt:key)) (alg #f)
		   (err #f) (segs) (header) (signature))
  (set! segs (segment string "."))
  (if key
      (let ((header (b64->json (car segs)))
	    (payload (b64->json (cadr segs)))
	    (signature (b64->packet (caddr segs)))
	    (body (glom (car segs) "." (cadr segs))))
	(when (not alg) (set! alg (try (get header 'alg) "HS256")))
	(if (and (test header 'alg alg)
		 (or (and (equal? "HS256" (upcase alg))
			  (equal? signature (hmac-sha256 key body)))
		     ;; Include other algorithms here someday
		     ))
	    (cons-jwt header payload signature string 
		      key (try (get payload "exp") #f))
	    (and err (signal-error string key alg header payload signature body))))
      (cons-jwt (b64->json (car segs)) (b64->json (cadr segs)) 
		(b64->packet (caddr segs))
		string)))

(define (jwt/check jwt (key (req/get '_jwtkey jwt:key)) (alg #f))
  (if (string? jwt)
      (jwt/parse jwt key alg)
      (and (test (jwt-header jwt) 'alg (upcase alg))
	   (packet? (jwt-signature jwt))
	   (if (or (and (equal? "HS256" (upcase alg))
			(equal? (jwt-signature jwt) 
				(hmac-sha256 key (jwt-body jwt))))
		   ;; Include other algorithms here someday
		   )
	       (cons-jwt (jwt-header jwt) (jwt-payload jwt)
			 (jwt-signature jwt) (jwt-text jwt)
			 key (try (get (jwt-payload jwt) 'exp) #f))))))

(define (signal-error string key alg header payload signature body)
  (if (not (test header 'alg alg))
      (irritant header |WrongAlgorithm| 
		alg " != " (get header 'alg)
		" for JWT " string)
      (irritant string |IncorrectSignature|
		"Given " signature " != "
		(hmac-sha256 key body)
		"\n with header " header 
		"\n and payload\n  " (pprint payload))))
  
;;; Generating new tokens

(define (jwt/make payload key (alg #f) (header #f) (hdr64) (pay64) (sig))
  (if (not header) (set! header (frame-create #f))
      (set! header (keys->strings header)))
  (unless alg (set! alg (try (get header "alg") "HS256")))
  (unless (test header "typ") (store! header "typ" "JWT"))
  (unless (test header "alg") (store! header "alg" alg))
  (when (table? payload) (set! payload (keys->strings payload)))
  (set! hdr64 (->b64 (->json header)))
  (set! pay64 (if (string? payload) (->base64 payload)
		  (->b64 (->json payload))))
  (set! sig (hmac-sha256 key (glom hdr64 "." pay64)))
  (cons-jwt header payload sig
	    (glom hdr64 "." pay64 "." (->b64 sig))
	    key #t))

(define (jwt/string payload key (alg #f) (header #f) (hdr64) (pay64) (sig))
  (if (not header) (set! header (frame-create #f))
      (set! header (keys->strings header)))
  (unless alg (set! alg (try (get header "alg") "HS256")))
  (unless (test header "typ") (store! header "typ" "JWT"))
  (unless (test header "alg") (store! header "alg" alg))
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

(define-init jwt:refresh #f)
(varconfig! jwt:refresh jwt:refresh)

(define (jwt/refresh jwt (refresh jwt:refresh))
  "Refresh a JWT if needed or returns the JWT as is"
  (and (or (jwt-checked jwt) (jwt/check jwt))
       (if (jwt-expiration jwt)
	   (if (> (time) (jwt-expiration jwt)) 
	       (refresh jwt)
	       jwt)
	   jwt)))

(define (jwt/refreshed jwt (refresh jwt:refresh))
  "Refresh a JWT if needed or fails otherwise"
  (and (or (jwt-checked jwt) (jwt/check jwt))
       (if (jwt-expiration jwt)
	   (if (> (time) (jwt-expiration jwt)) 
	       (difference (refresh jwt) jwt)
	       (fail))
	   jwt)))

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
