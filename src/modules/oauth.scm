;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2010-2012 beingmeta, inc. All rights reserved

(in-module 'oauth)

(use-module '{fdweb ezrecords extoids jsonout varconfig})
(use-module '{texttools logger xhtml/auth})

(define-init getuser #f)
(varconfig! oauth:getuser getuser)

(define-init default-callback #f)
(varconfig! oauth:callback default-callback)

(module-export!
 '{oauth
   oauth/request oauth/authurl oauth/verify
   oauth/call oauth/call* oauth/get oauth/post oauth/put
   oauth/signature})

(define-init %loglevel %notice!)
;;(set! %loglevel  %debug!)

;;; Server info

(define oauth-servers
  `#[TWITTER
     #[REQUEST "https://api.twitter.com/oauth/request_token"
       AUTHORIZE "https://api.twitter.com/oauth/authorize"
       AUTHENTICATE "https://api.twitter.com/oauth/authenticate"
       VERIFY "https://api.twitter.com/oauth/access_token"
       KEY TWITTER_KEY SECRET TWITTER_SECRET
       VERSION "1.0"
       REALM TWITTER]
     LINKEDIN
     #[REQUEST "https://api.linkedin.com/uas/oauth/requestToken"
       AUTHORIZE "https://api.linkedin.com/uas/oauth/authorize"
       AUTHENTICATE "https://api.linkedin.com/uas/oauth/authorize"
       VERIFY "https://api.linkedin.com/uas/oauth/accessToken"
       KEY LINKEDIN_KEY SECRET LINKEDIN_SECRET
       VERSION "1.0"
       REALM LINKEDIN]
     GOOGLE
     #[REQUEST "https://www.google.com/accounts/OAuthGetRequestToken"
       AUTHORIZE "https://www.google.com/accounts/OAuthAuthorizeToken"
       VERIFY "https://www.google.com/accounts/OAuthGetAccessToken"
       KEY GOOGLE_KEY SECRET GOOGLE_SECRET
       VERSION "1.0"
       REALM GOOGLE]
     GPLUS
     #[AUTHORIZE "https://accounts.google.com/o/oauth2/auth"
       VERIFY "https://accounts.google.com/o/oauth2/token"
       KEY GPLUS:KEY SECRET GPLUS:SECRET
       SCOPE "https://www.googleapis.com/auth/plus.me"
       VERSION "2.0"
       REALM GPLUS]])

(define (oauth/provider spec) (get oauth-servers spec))
(module-export! 'oauth/provider)

(define (thisurl)
  (stringout (if (= (req/get 'SERVER_PORT) 443) "https://" "http://")
	     (req/get 'SERVER_NAME)
	     (when (req/test 'SERVER_PORT)
	       (unless (or (= (req/get 'SERVER_PORT) 80)
			   (= (req/get 'SERVER_PORT) 443))
		 (printout ":" (req/get 'SERVER_PORT))))
    (uribase (try (req/get 'request_uri) (req/get 'path_info)))))

;;; Getting keys and secrets

(define (getckey spec (val))
  (default! val (getopt spec 'key))
  (if (symbol? val) (getckey spec (config val))
      (if (string? val) val
	  (if (packet? val) val
	      (error "Can't determine consumer key: " spec)))))
(define (getcsecret spec (val))
  (default! val (getopt spec 'secret))
  (if (symbol? val) (getcsecret spec (config val))
      (if (string? val) val
	  (if (packet? val) val
	      (error "Can't determine consumer secret: " spec)))))
(define (getcallback spec)
  (getopt spec 'callback
	  (req/get 'oauth_callback
		   (getopt spec 'default_callback
			   (or default-callback (thisurl))))))

;;; Computing signatures for the OAUTH1x implementations

(define (oauth/signature method uri . params)
  (debug%watch method uri params)
  (let ((keys {}) (scan params) (args '())
	(ptable (make-hashtable)))
    (until (null? scan)
      (if (null? (car scan))
	  (begin)
	  (if (and (pair? (car scan)) (pair? (cdr (car scan))))
	      (let ((args (car scan)))
		(until (null? args)
		  (unless (overlaps? (car args) keys)
		    (set+! keys (car args))
		    (if (string? (cadr args))
			(store! ptable (car args) (uriencode (cadr args)))
			(store! ptable (car args)
				(uriencode (stringout (cadr args))))))
		  (set! args (cddr args))))
	      (if (table? (car scan))
		  (do-choices (key (difference (getkeys (car scan)) keys))
		    (set+! keys key)
		    (if (string? (get (car scan) key))
			(store! ptable key (uriencode (get (car scan) key)))
			(store! ptable key
				(uriencode (stringout (get (car scan) key))))))
		  (begin (set+! keys (car scan))
		    (if (string? (cadr scan))
			(store! ptable (car scan) (uriencode (cadr scan)))
			(store! ptable (car scan) (uriencode (stringout (cadr scan)))))
		    (set! scan (cdr scan))))))
      (set! scan (cdr scan)))
    (doseq (key (lexsorted keys) i)
      (let ((v (get ptable key)))
	(when (exists? v)
	  (set! args (cons* v "%3D" (uriencode key) "%26" args)))))
    (apply glom method "&" (uriencode uri) "&" (cdr (reverse args)))))

(define (oauth/request spec (ckey) (csecret))
  (if (and (pair? spec) (symbol? (cdr spec)))
      (set-cdr! spec (get oauth-servers (cdr spec)))
      (if (symbol? spec) (set! spec (get oauth-servers spec))))
  (debug%watch "OAUTH/REQUEST" spec)
  (default! ckey (getckey spec))
  (default! csecret (getcsecret spec))
  (unless (and ckey csecret)
    (error "OAUTH/REQUEST: No consumer key/secret: " spec))
  (if (testopt spec 'version "2.0")
      (if (and (getopt spec 'authorize) (getopt spec 'verify))
	  spec
	  (error "OAUTH/REQUEST: Invalid OAUTH spec: " spec))
      (begin
	(unless (and (getopt spec 'request)
		     (getopt spec 'authorize)
		     (getopt spec 'verify))
	  (error "OAUTH/REQUEST: Invalid OAUTH spec: " spec))
	(let* ((nonce (getopt spec 'nonce (uuid->string (getuuid))))
	       (endpoint (getopt spec 'request))
	       (callback (uriencode (getcallback spec)))
	       (now (getopt spec 'time (time)))
	       (sigstring
		(oauth/signature
		 (getopt spec 'method "POST") endpoint
		 "oauth_callback" callback
		 "oauth_consumer_key" ckey
		 "oauth_nonce" nonce
		 "oauth_signature_method" "HMAC-SHA1"
		 "oauth_timestamp" now
		 "oauth_version" (getopt spec 'version "1.0")))
	       (sig (hmac-sha1 (glom csecret "&") sigstring))
	       (sig64 (packet->base64 sig))
	       (auth-header
		(glom "Authorization: OAuth "
		      "oauth_nonce=\"" nonce "\", "
		      "oauth_callback=\"" callback "\", "
		      "oauth_signature_method=\""  "HMAC-SHA1" "\", "
		      "oauth_timestamp=\"" now "\", "
		      "oauth_consumer_key=\"" ckey "\", "
		      "oauth_signature=\"" (uriencode sig64) "\", "
		      "oauth_version=\"" (getopt spec 'version "1.0") "\""))
	       (req (urlget endpoint (curlopen 'header auth-header 'method 'POST))))
	  (debug%watch sigstring sig sig64 auth-header)
	  (if (test req 'response 200)
	      (debug%watch (cons (cgiparse (get req '%content)) spec) "OATH/REQUEST")
	      (error "Can't get request token" req))))))

(define (oauth/authurl spec (scope #f))
  (when (symbol? spec) (set! spec (get oauth-servers spec)))
  (unless (and (getopt spec 'authorize)
	       (or (getopt spec 'oauth_token) (getckey spec)))
    (error "OAUTH/AUTHURL: Invalid OAUTH spec: " spec))
  (if (testopt spec 'version "1.0")
      (scripturl (getopt spec 'authorize)
	  "oauth_token" (getopt spec 'oauth_token)
	  "redirect_uri" (uriencode (getcallback spec)))
      (scripturl (getopt spec 'authorize)
	  "client_id" (getckey spec)
	  "redirect_uri" (getcallback spec)
	  "scope"
	  (stringout (do-choices (scope (or scope (getopt spec 'scope)) i)
		       (printout (if (> i 0) " ") scope)))
	  "response_type" "code")))

(define (oauth/verify spec (verifier) (ckey) (csecret))
  (if (symbol? spec) (set! spec (get oauth-servers spec)))
  (default! verifier (getopt spec 'oauth_verifier (req/get 'oauth_verifier)))
  (if (testopt spec 'version "1.0")
      (verify1.0 spec verifier)
      (verify2.0 spec verifier)))

(define (verify1.0 spec (verifier) (ckey) (csecret))
  (unless (and (getopt spec 'request)
	       (getopt spec 'authorize)
	       (getopt spec 'verify))
    (error "OAUTH/VERIFY: Invalid OAUTH spec: " spec))
  (default! ckey (getckey spec))
  (default! csecret (getcsecret spec))
  (debug%watch "OAUTH/VERIFY/1.0" verifier spec)
  (unless (and ckey csecret)
    (error "OAUTH/VERIFY: No consumer key/secret: " spec))
  (unless (getopt spec 'oauth_token)
    (error "OAUTH/VERIFY: No OAUTH oauth_token: " spec))
  (unless verifier
    (error "OAUTH/VERIFY: No OAUTH oauth_verifier: " spec))
  (let* ((nonce (uuid->string (getuuid)))
	 (endpoint (getopt spec 'verify default-verify-endpoint))
	 (callback (uriencode (getcallback spec)))
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
	 (sig (hmac-sha1 (glom csecret "&" (getopt spec 'oauth_token_secret))
			 sigstring))
	 (sig64 (packet->base64 sig))
	 (auth-header
	  (glom "Authorization: OAuth "
	    "oauth_nonce=\"" nonce "\", "
	    "oauth_token=\"" (getopt spec 'oauth_token) "\", "
	    "oauth_verifier=\"" verifier "\", "
	    "oauth_signature_method=\"" "HMAC-SHA1" "\", "
	    "oauth_timestamp=\"" now "\", "
	    "oauth_consumer_key=\"" ckey "\", "
	    "oauth_signature=\"" (uriencode sig64) "\", "
	    "oauth_version=\"" (getopt spec 'version "1.0") "\""))
	 (req (urlget endpoint (curlopen 'header auth-header 'method 'POST))))
    (debug%watch sigstring sig sig64 auth-header)
    (if (test req 'response 200)
	(cons (cgiparse (get req '%content)) (cdr spec))
	(if (getopt spec 'noverify)
	    ((getopt spec 'noverify) spec verifier req)
	    (error "OAUTH/VERIFY failed" req)))))

(define (verify2.0 spec (code) (ckey) (csecret))
  (default! code (getopt spec 'code (req/get 'code)))
  (default! ckey (getckey spec))
  (default! csecret (getcsecret spec))
  (debug%watch "OAUTH/VERIFY/2.0" code spec)
  (unless (and ckey csecret)
    (error "OAUTH/VERIFY: No consumer key/secret: " spec))
  (unless code (error "OAUTH/VERIFY: No OAUTH code: " spec))
  (let* ((callback (getcallback spec))
	 (req (urlpost (getopt spec 'verify) 
		       "code" code "client_id" ckey "client_secret" csecret
		       "grant_type" "authorization_code"
		       "redirect_uri" callback)))
    (if (test req 'response 200)
	(cons (jsonparse (get req '%content)) spec)
	(if (getopt spec 'noverify)
	    ((getopt spec 'noverify) spec verifier req)
	    (error "OAUTH/VERIFY failed" req)))))

;;; Actually calling the API

(define (oauth/call10 spec method endpoint (args '()) (ckey) (csecret))
  (debug%watch "OAUTH/CALL1.0" method endpoint args)
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
	 (sig (hmac-sha1 (glom  csecret "&" (getopt spec 'oauth_token_secret))
			 sigstring))
	 (sig64 (packet->base64 sig))
	 (auth-header
	  (glom "Authorization: OAuth "
	    "realm=\"" endpoint "\", "
	    "oauth_nonce=\"" nonce "\", "
	    "oauth_signature_method=\"" "HMAC-SHA1" "\", "
	    "oauth_timestamp=\"" now "\", "
	    "oauth_consumer_key=\"" ckey "\", "
	    "oauth_token=\"" (getopt spec 'oauth_token) "\", "
	    "oauth_version=\"" (getopt spec 'version "1.0") "\", "
	    "oauth_signature=\"" (uriencode sig64) "\""))
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
    (debug%watch sigstring auth-header now nonce req)
    (if (test req 'response 200)
	(cons (getreqdata req) (cdr spec))
	req)))
(define (oauth/call20 spec method endpoint (args '()) (ckey) (csecret))
  (debug%watch "OAUTH/CALL2.0" method endpoint args ckey csecret)
  (unless (and ckey csecret)
    (error "OAUTH/CALL: No consumer key/secret: " spec))
  (unless (getopt spec 'access_token)
    (error "OAUTH/CALL: No OAUTH2 ACCESS token: " spec))
  (let* ((endpoint (or endpoint
		       (getopt args 'endpoint
			       (getopt spec 'endpoint
				       default-endpoint))))
	 (auth-header
	  (glom "Authorization: Bearer " (getopt spec 'access_token)))
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
    (debug%watch endpoint auth-header req)
    (if (test req 'response 200)
	(cons (getreqdata req) (cdr spec))
	req)))
(define (oauth/call spec method endpoint (args '()) (ckey) (csecret))
  (default! ckey (getckey spec))
  (default! csecret (getcsecret spec))
  (if (testopt spec 'version "1.0")
      (oauth/call10 spec method endpoint args ckey csecret)
      (oauth/call20 spec method endpoint args ckey csecret)))
(define (oauth/call* spec method endpoint . args)
  (oauth/call spec method endpoint args))

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

(define-init oauth-pending (make-hashtable))
(define-init oauth-info (make-hashtable))

(define (oauth (code #f) (oauth_realm #f) (oauth_token #f) (oauth_verifier #f))
  (debug%watch "OAUTH" oauth_realm code oauth_token oauth_verifier)
  (if oauth_verifier
      (let* ((state (get oauth-pending oauth_token))
	     (verified (oauth/verify state oauth_verifier)))
	(drop! oauth-pending oauth_token)
	(store! oauth-info oauth_token (cons verified (cdr state)))
	(let ((user (getuser verified)))
	  (debug%watch "OAUTH/complete" user verified state)
	  user))
      (if code
	  (let* ((state (get oauth-servers oauth_realm))
		 (verified (oauth/verify state code)))
	    (let ((user (getuser verified)))
	      (debug%watch "OAUTH/complete" user verified state)
	      user))
	  (and oauth_realm
	       (let* ((state (oauth/request oauth_realm))
		      (url (oauth/authurl state)))
		 (store! oauth-pending (getopt state 'oauth_token) state)
		 (cgiset! 'status 300)
		 (httpheader "Location: " url)
		 #f)))))

