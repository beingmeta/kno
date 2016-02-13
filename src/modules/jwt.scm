;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc. All rights reserved

(in-module 'jwt)

(use-module '{ezrecords fdweb texttools jsonout packetfns})
(use-module '{varconfig logger})
(define %used_modules 'varconfig)

(module-export! '{jwt-header jwt-payload jwt-text jwt-signature
		  jwt/parse jwt/check 
		  jwt/make jwt/string})

(defrecord jwt header payload signature text body)

(define (parse-element string)
  (jsonparse (packet->string (b64->string string))))

(define (b64->packet string)
  (base64->packet (uridecode (string-subst* string "-" "+"  "_" "/"))))
(define (b64->string string)
  (packet->string
   (base64->packet (uridecode (string-subst* string "-" "+"  "_" "/")))))
(define (b64->json string)
  (jsonparse
   (packet->string
    (base64->packet (uridecode (string-subst* string "-" "+"  "_" "/"))))))

(define (jwt/parse string (err #f)
		   (segs) (header) (signed) (signature))
  (set! segs (segment string "."))
  (set! signature (b64->packet (caddr segs)))
  (cons-jwt (b64->json (car segs)) (b64->json (cadr segs))
	    signature string (glom (car segs) "." (cadr segs))))

(define (jwt/check jwt key (alg #f))
  (when (string? jwt) (set! jwt (jwt/parse jwt)))
  (when (not alg) (set! alg (try (get (jwt-header jwt) 'alg) #f)))
  (and (test (jwt-header jwt) 'alg (upcase alg))
       (packet? (jwt-signature jwt))
       (or (and (equal? "HS256" (upcase alg))
		(equal? (jwt-signature jwt) 
			(hmac-sha256 key (jwt-body jwt))))
	   (and (equal? "RSA" (upcase alg))
		(equal? (jwt-signature jwt) 
			(hmac-sha256 key (jwt-body jwt)))))))

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

(define (jwt/make payload key (alg #f) (header #f) (hdr64) (pay64) (sig))
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
  (cons-jwt header payload sig
	    (glom hdr64 "." pay64 "." (packet->base64 sig))
	    (glom hdr64 "." pay64)))

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






	   
		 
