;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2010-2011 beingmeta, inc. All rights reserved

(in-module 'oauth)

(use-module '{fdweb ezrecords extoids jsonout varconfig})
(use-module '{texttools logger xhtml/auth})

(define getuser #f)
(varconfig! oauth:getuser getuser)

(module-export!
 '{oauth
   oauth/request oauth/authurl oauth/verify
   oauth/call oauth/get oauth/post oauth/put
   oauth/signature})

(define-init %loglevel %notice!)
;;(define %loglevel %debug!)
;;(set! %loglevel  %debug!)

(define oauth-servers
  #[TWITTER
    #[REQUEST "https://api.twitter.com/oauth/request_token"
      AUTHORIZE "https://api.twitter.com/oauth/authorize"
      VERIFY "https://api.twitter.com/oauth/access_token"
      KEY TWITTER_KEY SECRET TWITTER_SECRET
      VERSION "1.0"
      REALM TWITTER]
    LINKEDIN
    #[REQUEST "https://api.linkedin.com/uas/oauth/requestToken"
      AUTHORIZE "https://api.linkedin.com/uas/oauth/authorize"
      VERIFY "https://api.linkedin.com/uas/oauth/accessToken"
      KEY LINKEDIN_KEY SECRET LINKEDIN_SECRET
      VERSION "1.0"
      REALM LINKEDIN]])

(define default-callback "https://auth.sbooks.net/_appinfo")

(define (getckey spec (val))
  (default! val (getopt spec 'key))
  (if (symbol? val) (getckey spec (config val))
      (if (string? val) val
	  (if (packet? val)
	      (packet->string val)
	      (error "Can't determine consumer key: " spec)))))
(define (getcsecret spec (val))
  (default! val (getopt spec 'secret))
  (if (symbol? val) (getcsecret spec (config val))
      (if (string? val) val
	  (if (packet? val)
	      (packet->string val)
	      (error "Can't determine consumer secret: " spec)))))

(define (oauth/signature method uri . params)
  (stringout method "&" (uriencode uri) "&"
    (let ((keys {}) (scan params)
	  (ptable (make-hashtable)))
      (until (null? scan)
	(if (table? (car scan))
	    (do-choices (key (difference (getckeys (car scan))
					 keys))
	      (set+! keys key)
	      (store! ptable key (uriencode (get (car scan) key))))
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

(define (oauth/request spec (ckey) (csecret))
  (if (symbol? spec) (set! spec (get oauth-servers spec)))
  (unless (and (getopt spec 'request)
	       (getopt spec 'authorize)
	       (getopt spec 'verify))
    (error "OAUTH/REQUEST: Invalid OAUTH spec: " spec))
  (default! ckey (getckey spec))
  (default! csecret (getcsecret spec))
  (unless (and ckey csecret)
    (error "OAUTH/REQUEST: No consumer key/secret: " spec))
  (let* ((nonce (uuid->string (getuuid)))
	 (endpoint (getopt spec 'request))
	 (callback (uriencode (getopt spec 'callback default-callback)))
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
	 (sig (hmac-sha1 (stringout (uriencode csecret) "&") sigstring))
	 (auth-header
	  (stringout "Authorization: OAuth "
	    "oauth_nonce=\"" nonce "\", "
	    "oauth_callback=\"" callback "\", "
	    "oauth_signature_method=\""  "HMAC-SHA1" "\", "
	    "oauth_timestamp=\"" now "\", "
	    "oauth_consumer_key=\"" ckey "\", "
	    "oauth_signature=\"" (uriencode (packet->base64 sig)) "\", "
	    "oauth_version=\"" (getopt spec 'version "1.0") "\""))
	 (req (urlget endpoint (curlopen 'header auth-header 'method 'POST))))
    (and (test req 'response 200)
	 (cons (cgiparse (get req '%content)) spec))))

(define (oauth/authurl spec)
  (unless (and (getopt spec 'authorize) (getopt spec 'oauth_token))
    (error "OAUTH/AUTHURL: Invalid OAUTH spec: " spec))
  (scripturl (getopt spec 'authorize)
      "oauth_token" (getopt spec 'oauth_token)))

(define (oauth/verify spec (verifier) (ckey) (csecret))
  (default! verifier (getopt spec 'oauth_verifier (cgiget 'oauth_verifier)))
  (unless (and (getopt spec 'request)
	       (getopt spec 'authorize)
	       (getopt spec 'verify))
    (error "OAUTH/VERIFY: Invalid OAUTH spec: " spec))
  (default! ckey (getckey spec))
  (default! csecret (getcsecret spec))
  (unless (and ckey csecret)
    (error "OAUTH/VERIFY: No consumer key/secret: " spec))
  (unless (getopt spec 'oauth_token)
    (error "OAUTH/VERIFY: No OAUTH oauth_token: " spec))
  (unless verifier
    (error "OAUTH/VERIFY: No OAUTH oauth_verifier: " spec))
  (let* ((nonce (uuid->string (getuuid)))
	 (endpoint (getopt spec 'verify default-verify-endpoint))
	 (callback (uriencode
		    (getopt spec 'callback default-callback)))
	 (ckey (getckey spec))
	 (now (time))
	 (sigstring
	  (oauth/signature
	   (getopt spec 'method "POST") endpoint
	   "oauth_callback" callback
	   "oauth_consumer_key" ckey
	   "oauth_token" (getopt spec 'oauth_token)
	   "oauth_verifier" verifier
	   "oauth_nonce" nonce
	   "oauth_signature_method" "HMAC-SHA1"
	   "oauth_timestamp" now
	   "oauth_version" (getopt spec 'version "1.0")))
	 (sig (hmac-sha1 (stringout
			   (uriencode csecret) "&"
			   (uriencode (getopt spec 'oauth_token_secret)))
			 sigstring))
	 (auth-header
	  (stringout "Authorization: OAuth "
	    "oauth_nonce=\"" nonce "\", "
	    "oauth_token=\"" (getopt spec 'oauth_token) "\", "
	    "oauth_verifier=\"" verifier "\", "
	    "oauth_signature_method=\"" "HMAC-SHA1" "\", "
	    "oauth_timestamp=\"" now "\", "
	    "oauth_consumer_key=\"" ckey "\", "
	    "oauth_signature=\"" (uriencode (packet->base64 sig)) "\", "
	    "oauth_version=\"" (getopt spec 'version "1.0") "\""))
	 (req (urlget endpoint (curlopen 'header auth-header 'method 'POST))))
    (if (test req 'response 200)
	(cons (cgiparse (get req '%content)) (cdr spec))
	req)))

;;; Actually calling the API

(define (oauth/call spec method endpoint args (ckey) (csecret))
  (default! ckey (getckey spec))
  (default! csecret (getcsecret spec))
  (unless (and ckey csecret)
    (error "OAUTH/CALL: No consumer key/secret: " spec))
  (unless (getopt spec 'oauth_token)
    (error "OAUTH/CALL: No OAUTH token: " spec))
  (unless (getopt spec 'oauth_token_secret)
    (error "OAUTH/CALL: No OAUTH secret: " spec))
  (let* ((nonce (uuid->string (getuuid)))
	 (endpoint (or endpoint
		       (getopt args 'endpoint
			       (getopt spec 'endpoint
				       default-endpoint))))
	 (now (time))
	 (sigstring
	  (oauth/signature
	   method endpoint
	   "oauth_consumer_key" ckey
	   "oauth_token" (getopt spec 'oauth_token)
	   "oauth_nonce" nonce
	   "oauth_signature_method" "HMAC-SHA1"
	   "oauth_timestamp" now
	   "oauth_version" (getopt spec 'version "1.0")
	   args))
	 (sig (hmac-sha1 (stringout 
			   (uriencode csecret) "&"
			   (uriencode (getopt spec 'oauth_token_secret)))
			 sigstring))
	 (auth-header
	  (stringout "Authorization: OAuth "
	    "realm=\"" endpoint "\", "
	    "oauth_nonce=\"" nonce "\", "
	    "oauth_signature_method=\"" "HMAC-SHA1" "\", "
	    "oauth_timestamp=\"" now "\", "
	    "oauth_consumer_key=\"" ckey "\", "
	    "oauth_token=\"" (getopt spec 'oauth_token) "\", "
	    "oauth_version=\"" (getopt spec 'version "1.0") "\", "
	    "oauth_signature=\"" (uriencode (packet->base64 sig)) "\""))
	 (req (if (eq? method 'GET)
		  (urlget (scripturl+ endpoint args)
			  (curlopen 'header "Expect: "
				    'header auth-header
				    'method method))
		  (if (eq? method 'POST)
		      (urlpost endpoint
			       (curlopen 'header "Expect: "
					 'header auth-header
					 'method method)
			       (args->post args))))))
    (debug%watch sigstring auth-header now nonce)
    req))

(define (args->post args)
  (stringout (do-choices (key (getckeys args) i)
	       (if (> i 0) (printout "&"))
	       (printout (uriencode key) "="
		 (uriencode (get args key))))))

;;; Calling functions

(define (oauth/get spec endpoint (args #[]))
  (getreqdata (oauth/call spec 'GET endpoint args)))
(define (oauth/post spec endpoint (args #[]))
  (getreqdata (oauth/call spec 'POST endpoint args)))
(define (oauth/put spec endpoint (args #[]))
  (getreqdata (oauth/call spec 'PUT endpoint args)))

(define (getreqdata req)
  (if (and (test req 'response)
	   (test req 'content-type)
	   (number? (get req 'response))
	   (>= (get req 'response) 200)
	   (< (get req 'response) 300))
      (if (search "json" (get req 'content-type))
	  (jsonparse (get req '%content))
	  (if (search "xml" (get req 'content-type))
	      (xmlparse (get req '%content) '{data slotify})
	      req)
	  req)))

;;; For cgicall

(define oauth-pending (make-hashtable))
(define oauth-info (make-hashtable))

(define (oauth (oauth_realm #f) (oauth_token #f) (oauth_verifier #f))
  (if oauth_verifier
      (let* ((state (get oauth-pending oauth_token))
	     (verified (oauth/verify state oauth_verifier)))
	(drop! oauth-pending oauth_token)
	(store! oauth-info oauth_token (cons verified (cdr state)))
	(auth/identify! (getuser (cons verified (cdr state)))))
      (and oauth_realm
	   (let* ((state (oauth/request oauth_realm))
		  (url (oauth/authurl state)))
	     (store! oauth-pending state)
	     (cgiset! 'status 300)
	     (httpheader "Location: " (uriencode url))
	     #t))))

