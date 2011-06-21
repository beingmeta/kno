;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2010-2011 beingmeta, inc. All rights reserved

(in-module 'oauth)

(use-module '{fdweb ezrecords extoids jsonout})
(use-module '{texttools logger})

(define-init %loglevel %notice!)
;;(define %loglevel %debug!)
;;(set! %loglevel  %debug!)

(define default-request-endpoint
  "https://api.twitter.com/oauth/request_token")
(define default-access-endpoint
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

(module-export! '{oauth/signature oauth/request})

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

(define (oauth/request service args)
  (let* ((nonce (getopt args 'nonce (uuid->string (getuuid))))
	 (endpoint (getopt args 'endpoint
			   (getopt service 'request_token
				   default-request-endpoint)))
	 (callback (uriencode
		    (getopt args 'callback
			    (getopt service 'callback
				    default-callback))))
	 (ckey (getopt args 'ckey
		       (getopt service 'ckey default-consumer-key)))
	 (now (getopt args 'timestamp (time)))
	 (sigstring
	  (oauth/signature
	   (getopt args 'method "POST") endpoint
	   "oauth_callback" callback
	   "oauth_consumer_key" ckey
	   "oauth_nonce" nonce
	   "oauth_signature_method" "HMAC-SHA1"
	   "oauth_timestamp" now
	   "oauth_version" (getopt service 'version "1.0")))
	 (sig (hmac-sha1 (stringout (getopt args 'secret
					    (getopt service 'secret
						    default-consumer-secret))
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
	    "oauth_version=\"1.0\"")))
    (%watch auth-header)
    (urlget endpoint (curlopen 'header auth-header 'method 'POST))))

