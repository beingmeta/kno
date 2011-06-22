;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2010-2011 beingmeta, inc. All rights reserved

(in-module 'oauth)

(use-module '{fdweb ezrecords extoids jsonout})
(use-module '{texttools logger})

(define-init %loglevel %notice!)
;;(define %loglevel %debug!)
;;(set! %loglevel  %debug!)

(define default-request-uri
  "https://api.twitter.com/oauth/request_token")
(define default-verify-uri
  "https://api.twitter.com/oauth/access_token")
(define default-consumer-key
  "eeY4hVUyOSe2IqwhHvNUw")
(define default-consumer-secret
  "PJI4IvlBf0JbPjvAvVCkwa6jRizaJFteMf3fMixeZJE")
(define default-callback
  "https://auth.sbooks.net/admin/auth")

(define oauth-defaults
  #["oauth_version" "1.0"
    "oauth_signature_method" "HMAC-SHA1"
    "oauth_consumer_key" "GDdmlQH6jhtmLUypg82g"])

(module-export! '{oauth/signature oauth/request oauth/verify oauth/call})

(define (oauth/signature method uri . params)
  (stringout method "&" (uriencode uri) "&"
    (let ((keys {}) (scan params)
	  (ptable (make-hashtable)))
      (until (null? scan)
	(if (table? (car scan))
	    (do-choices (key (difference (getkeys (car scan))
					 keys))
	      (set+! keys key)
	      (store! ptable key (get (car scan) key)))
	    (begin (set+! keys (car scan))
	      (store! ptable (car scan) (cadr scan))
	      (set! scan (cdr scan))))
	(set! scan (cdr scan)))
      (doseq (key (lexsorted keys) i)
	(if (> i 0)  (printout "%26"))
	(let ((v (get ptable key)))
	  (when (exists? v)
	    (printout (uriencode key) "%3D"
	      (if (string? v) (uriencode v)
		  (uriencode (stringout v))))))))))

(define (oauth/request spec)
  (let* ((nonce (uuid->string (getuuid)))
	 (endpoint (getopt spec 'request_uri
			   default-request-uri))
	 (callback (uriencode
		    (getopt spec 'callback
			    default-callback)))
	 (ckey (getopt spec 'key default-consumer-key))
	 (now (time))
	 (sigstring
	  (oauth/signature
	   (getopt spec 'method "POST") endpoint
	   "oauth_callback" callback
	   "oauth_consumer_key" ckey
	   "oauth_nonce" nonce
	   "oauth_signature_method" "HMAC-SHA1"
	   "oauth_timestamp" now
	   "oauth_version" (getopt spec 'version "1.0")))
	 (sig (hmac-sha1 (stringout
			   (getopt spec 'secret default-consumer-secret)
			   "&")
			 sigstring))
	 (auth-header
	  (stringout "Authorization: OAuth "
	    "oauth_nonce=\"" nonce "\", "
	    "oauth_callback=\"" callback "\", "
	    "oauth_signature_method=\"HMAC-SHA1\", "
	    "oauth_timestamp=\"" now "\", "
	    "oauth_consumer_key=\"" ckey "\", "
	    "oauth_signature=\"" (uriencode (packet->base64 sig)) "\", "
	    "oauth_version=\"" (getopt spec 'version "1.0") "\""))
	 (req (urlget endpoint (curlopen 'header auth-header 'method 'POST))))
    (and (test req 'response 200)
	 (cons (cgiparse (get req '%content)) spec))))

(define (oauth/verify spec verifier)
  (let* ((nonce (uuid->string (getuuid)))
	 (endpoint (getopt spec 'verify_uri default-verify-uri))
	 (callback (uriencode
		    (getopt spec 'callback default-callback)))
	 (ckey (getopt spec 'key default-consumer-key))
	 (now (time))
	 (sigstring
	  (oauth/signature
	   (getopt spec 'method "POST") endpoint
	   "oauth_callback" callback
	   "oauth_consumer_key" ckey
	   "oauth_token" (getopt spec "oauth_token")
	   "oauth_verifier" verifier
	   "oauth_nonce" nonce
	   "oauth_signature_method" "HMAC-SHA1"
	   "oauth_timestamp" now
	   "oauth_version" (getopt spec 'version "1.0")))
	 (sig (hmac-sha1 (stringout (getopt spec "oauth_token")
			   (getopt spec 'secret default-consumer-secret)
			   "&"
			   (getopt spec "oauth_secret"))
			 sigstring))
	 (auth-header
	  (stringout "Authorization: OAuth "
	    "oauth_nonce=\"" nonce "\", "
	    "oauth_token=\"" (getopt spec "oauth_token") "\", "
	    "oauth_verifier=\"" (getopt spec "oauth_verifier") "\", "
	    "oauth_signature_method=\"HMAC-SHA1\", "
	    "oauth_timestamp=\"" now "\", "
	    "oauth_consumer_key=\"" ckey "\", "
	    "oauth_signature=\"" (uriencode (packet->base64 sig)) "\", "
	    "oauth_version=\"" (getopt spec 'version "1.0") "\""))
	 (req (urlget endpoint (curlopen 'header auth-header 'method 'POST))))
    (and (test req 'response 200)
	 (cons (cgiparse (get req '%content)) spec))))

(define (oauth/call spec args (endpoint #f))
  (let* ((nonce (uuid->string (getuuid)))
	 (endpoint (or endpoint
		       (getopt args 'endpoint
			       (getopt spec 'endpoint
				       default-endpoint))))
	 (ckey (getopt spec 'key default-consumer-key))
	 (now (time))
	 (sigstring
	  (oauth/signature
	   (getopt spec 'method "POST") endpoint
	   "oauth_consumer_key" ckey
	   "oauth_token" (getopt spec "oauth_token")
	   "oauth_nonce" nonce
	   "oauth_signature_method" "HMAC-SHA1"
	   "oauth_timestamp" now
	   "oauth_version" (getopt spec 'version "1.0")
	   args))
	 (sig (hmac-sha1 (stringout (getopt spec "oauth_token")
			   (getopt spec 'secret default-consumer-secret)
			   "&"
			   (getopt spec "oauth_secret"))
			 sigstring))
	 (auth-header
	  (stringout "Authorization: OAuth "
	    "oauth_nonce=\"" nonce "\", "
	    "oauth_token=\"" (getopt spec "oauth_token") "\", "
	    "oauth_signature_method=\"HMAC-SHA1\", "
	    "oauth_timestamp=\"" now "\", "
	    "oauth_consumer_key=\"" ckey "\", "
	    "oauth_signature=\"" (uriencode (packet->base64 sig)) "\", "
	    "oauth_version=\"" (getopt spec 'version "1.0") "\""))
	 (req (urlget endpoint (curlopen 'header auth-header 'method 'POST))))
    req))
