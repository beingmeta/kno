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
   oauth/sigstring oauth/callsig})

(define-init %loglevel %notice!)
;;(set! %loglevel  %debug!)

;;; Server info

(define-init oauth-servers
  `#[TWITTER
     #[REQUEST "https://api.twitter.com/oauth/request_token"
       VERIFY "https://api.twitter.com/oauth/access_token"
       AUTHORIZE "https://api.twitter.com/oauth/authorize"
       AUTHENTICATE "https://api.twitter.com/oauth/authenticate"
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

(config-def! 'oauth:providers
	     (lambda (var (val))
	       (cond ((not (bound? val))
		      (get oauth-servers (getkeys oauth-servers)))
		     ((not (and (or (slotmap? val) (schemap? val))
				(exists? (get val 'realm))))
		      (error "Invalid OAUTH provider spec: " val))
		     (else
		      (when (exists? (get oauth-servers (get val 'realm)))
			(logwarn "OAUTH/REDEFINE"
				 "Redefining OAUTH specification for "
				 realm ", changes may be lost!"))
		      (store! oauth-servers (get val 'realm) val)))))

(define (thisurl)
  (stringout (if (= (req/get 'SERVER_PORT) 443) "https://" "http://")
	     (req/get 'SERVER_NAME)
	     (when (req/test 'SERVER_PORT)
	       (unless (or (= (req/get 'SERVER_PORT) 80)
			   (= (req/get 'SERVER_PORT) 443))
		 (printout ":" (req/get 'SERVER_PORT))))
    (uribase (try (req/get 'request_uri) (req/get 'path_info)))))

;;; Getting consumer/client keys and secrets

;;; The key/secret can either be literally on the provider definition or
;;;  it can be a symbol which refers to a CONFIG setting for the key/secret

;;; As a best practice, the secret should be a FramerD secret, which is a
;;;  packet whose contents are never displayed.

(define (getckey spec (val))
  (default! val (getopt spec 'key))
  (if (symbol? val) (getckey spec (config val))
      (if (string? val) val
	  (if (packet? val) val
	      (error "Can't determine consumer key: " spec)))))
(define (getcsecret spec (val))
  (default! val (getopt spec 'secret))
  (->secret
   (if (symbol? val) (getcsecret spec (config val))
       (if (string? val) val
	   (if (packet? val) val
	       (error "Can't determine consumer secret: " spec))))))
(define (getcallback spec)
  (getopt spec 'callback
	  (req/get 'oauth_callback
		   (getopt spec 'default_callback
			   (or default-callback (thisurl))))))

;;; Computing signatures for the OAUTH1x implementations

(define (oauth/sigstring method uri . params)
  "Compute the signature (for OAUTH1) of a call"
  (debug%watch method uri params)
  (let ((keys {}) (scan params) (args '())
	(ptable (make-hashtable)))
    ;; Params are considered an alternating set of key/value pairs
    ;;  with two exceptions:
    ;;   + if a key is a pair whose cdr is a pair, we assume that it is
    ;;     an embedded alternating set of key/value pairs
    ;;   + if a key is a table, we assume that it's keys and values
    ;;     are keys and values to use
    (until (null? scan)
      (if (null? (car scan))
	  (begin)
	  (if (and (pair? (car scan)) (pair? (cdr (car scan))))
	      (let ((args (car scan)))
		(until (null? args)
		  (unless (overlaps? (car args) keys)
		    (set+! keys (car args))
		    (if (or (string? (cadr args)) (packet? (cadr args)))
			(store! ptable (car args) (uriencode (cadr args)))
			(store! ptable (car args)
				(uriencode (stringout (cadr args))))))
		  (set! args (cddr args))))
	      (if (table? (car scan))
		  (do-choices (key (difference (getkeys (car scan)) keys))
		    (set+! keys key)
		    (if (or (string? (get (car scan) key))
			    (packet? (get (car scan) key)))
			(store! ptable key (uriencode (get (car scan) key)))
			(store! ptable key
				(uriencode (stringout (get (car scan) key))))))
		  (begin (set+! keys (car scan))
		    (if (or (string? (cadr scan)) (packet? (cadr scan)))
			(store! ptable (car scan) (uriencode (cadr scan)))
			(store! ptable (car scan)
				(uriencode (stringout (cadr scan)))))
		    (set! scan (cdr scan))))))
      (set! scan (cdr scan)))
    (doseq (key (lexsorted keys) i)
      (let ((v (get ptable key)))
	(when (exists? v)
	  (set! args (cons* v "%3D" (uriencode key) "%26" args)))))
    (apply glom method "&" (uriencode uri) "&" (cdr (reverse args)))))

(define (oauthspec spec)
  "Expands provider references a spec"
  (if (and (pair? spec) (symbol? (cdr spec))
	   (exists? (get oauth-servers (cdr spec))))
      (cons (car spec) (get oauth-servers (cdr spec)))
      (if (and (symbol? spec) (test oauth-servers spec))
	  (get oauth-servers spec)
	  (if (and (table? spec) (test spec 'realm)
		   (test oauth-servers (get spec 'realm)))
	      (cons spec (get oauth-servers spec))
	      spec))))

(define (oauth/request spec (ckey) (csecret))
  ;; Expand provider references in the spec
  (set! spec (debug%watch (oauthspec spec) spec))
  (default! ckey (getckey spec))
  (default! csecret (getcsecret spec))
  (unless (and ckey csecret)
    (logwarn "OAUTH/REQUEST: No consumer key/secret: " spec)
    (error "OAUTH/REQUEST: No consumer key/secret: " spec))
  (unless (and (getopt spec 'request)
	       (getopt spec 'authorize)
	       (getopt spec 'verify))
    ;; This should exit
    (logwarn "OAUTH/REQUEST: Invalid OAUTH spec: " spec)
    (error "OAUTH/REQUEST: Invalid OAUTH spec: " spec))
  ;; We allow the nonce, time, etc to come from the spec
  ;;  to allow signature debugging
  (let* ((nonce (getopt spec 'nonce (uuid->string (getuuid))))
	 (endpoint (getopt spec 'request))
	 (callback (uriencode (getcallback spec)))
	 (now (getopt spec 'time (time)))
	 (sigstring
	  (oauth/sigstring
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
	 (handle (curlopen 'header auth-header 'method 'POST))
	 (req (urlget endpoint handle)))
    (debug%watch sigstring sig sig64 auth-header)
    (if (test req 'response 200)
	(cons (cgiparse (get req '%content)) spec)
	(begin (warn%watch "Can't get request token" spec req)
	  (error "Can't get request token" req)))))


(define (oauth/authurl spec (scope #f))
  "Returns a URL for redirection and authorization and authentication. \
   If a scope is provided, we assume that we're authorizing, not \
   authenticating."
  (set! spec (oauthspec spec))
  (unless (and (getopt spec 'authenticate (getopt spec 'authorize))
	       (or (getopt spec 'oauth_token) (getckey spec)))
    (error "OAUTH/AUTHURL: Invalid OAUTH spec: " spec))
  (if (testopt spec 'version "1.0")
      (scripturl+ (if scope
		      (getopt spec 'authorize (getopt spec 'authenticate))
		      (getopt spec 'authenticate (getopt spec 'authorize)))
		  args (getopt spec 'auth_args)
		  "oauth_token" (getopt spec 'oauth_token)
		  ;; "redirect_uri" (uriencode (getcallback spec))
		  "redirect_uri" (getcallback spec))
      (scripturl+
       (if scope
	   (getopt spec 'authorize (getopt spec 'authenticate))
	   (getopt spec 'authenticate (getopt spec 'authorize)))
       (getopt spec 'auth_args)
       "client_id" (getckey spec)
       "redirect_uri" (getcallback spec)
       "scope"
       (stringout (do-choices (scope (or scope (getopt spec 'scope)) i)
		    (printout (if (> i 0) " ") scope)))
       "state" (getopt spec 'state (getuuid))
       "response_type" "code")))

(define (oauth/verify spec verifier (ckey) (csecret))
  (unless (and (getopt spec 'request)
	       (getopt spec 'authorize)
	       (getopt spec 'verify))
    (error "OAUTH/VERIFY: Invalid OAUTH spec: " spec))
  (debug%watch "OAUTH/VERIFY/1.0" verifier spec)
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
	 (callback (uriencode (getcallback spec)))
	 (ckey (getckey spec))
	 (now (time))
	 (sigstring
	  (oauth/sigstring
	   (getopt spec 'method "POST") endpoint
	   "oauth_callback" callback
	   "oauth_consumer_key" ckey
	   "oauth_token" (getopt spec 'oauth_token)
	   "oauth_verifier" verifier
	   "oauth_nonce" nonce
	   "oauth_signature_method" "HMAC-SHA1"
	   "oauth_timestamp" now
	   "oauth_version" (getopt spec 'version "1.0")))
	 (sig (hmac-sha1 (glom csecret "&" (getopt spec 'oauth_secret))
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

(define (oauth/access spec (code #f) (ckey) (csecret))
  (set! spec (oauthspec spec))
  (default! ckey (getckey spec))
  (default! csecret (getcsecret spec))
  (debug%watch "OAUTH/VERIFY/2.0" code spec)
  (unless (and ckey csecret)
    (error "OAUTH/VERIFY: No consumer key/secret: " spec))
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

(define (oauth/call10 spec method endpoint args ckey csecret)
  (debug%watch "OAUTH/CALL1.0" method endpoint args)
  (default! ckey (getckey spec))
  (default! csecret (getcsecret spec))
  (unless (and ckey csecret)
    (error "OAUTH/CALL: No consumer key/secret: " spec))
  (unless (getopt spec 'oauth_token)
    (error "OAUTH/CALL: No OAUTH token: " spec))
  (unless (getopt spec 'oauth_secret)
    (error "OAUTH/CALL: No OAUTH secret: " spec))
  (let* ((nonce (getopt spec 'nonce (uuid->string (getuuid))))
	 (endpoint (or endpoint
		       (getopt args 'endpoint
			       (getopt spec 'endpoint
				       default-endpoint))))
	 (now (getopt spec 'timestamp (time)))
	 (sigstring
	  (oauth/sigstring
	   method endpoint
	   "oauth_consumer_key" ckey
	   "oauth_token" (->string (getopt spec 'oauth_token))
	   "oauth_nonce" nonce
	   "oauth_signature_method" "HMAC-SHA1"
	   "oauth_timestamp" now
	   "oauth_version" (getopt spec 'version "1.0")
	   (if (pair? args)
	       (map uriencode args)
	       (if (table? args)
		   (let ((newtable (frame-create #f)))
		     (do-choices (key (getkeys args))
		       (store! newtable
			       (if (string? key) (uriencode key) key)
			       (if (or (string? (get args key)) (packet? (get args key)))
				   (uriencode (get args key))
				   (get args key))))
		     newtable)
		   args))))
	 (sig (hmac-sha1 (glom  csecret "&" (getopt spec 'oauth_secret))
			 sigstring))
	 (sig64 (packet->base64 sig))
	 (auth-header
	  (glom "Authorization: OAuth "
	    ;; "realm=\"" (urischeme endpoint) "://" (urihost endpoint) "\", "
	    "oauth_consumer_key=\"" ckey "\", "
	    "oauth_nonce=\"" nonce "\", "
	    "oauth_signature=\"" (uriencode sig64) "\", "
	    "oauth_signature_method=\"" "HMAC-SHA1" "\", "
	    "oauth_timestamp=\"" now "\", "
	    "oauth_token=\"" (getopt spec 'oauth_token) "\", "
	    "oauth_version=\"" (getopt spec 'version "1.0") "\""))
	 (req (if (eq? method 'GET)
		  (urlget (if (pair? args)
			      (apply scripturl endpoint args)
			      (scripturl+ endpoint args))
			  (curlopen 'header "Expect: "
				    'header auth-header
				    'method method))
		  (if (eq? method 'POST)
		      (urlpost endpoint
			       (curlopen 'header "Expect: "
					 'header auth-header
					 'method method)
			       (args->post args))))))
    (debug%watch now nonce sig64)
    ;; Keeping these watchpoints separate makes them easier to read
    ;;  (maybe %watch needs a redesign?)
    (debug%watch sigstring)
    (debug%watch auth-header)
    (if (test req 'response 200)
	(cons (getreqdata req) (cdr spec))
	req)))

(define (oauth/call20 spec method endpoint args ckey csecret)
  (debug%watch "OAUTH/CALL2.0" method endpoint args ckey csecret)
  (unless (and ckey csecret)
    (error "OAUTH/CALL: No consumer key/secret: " spec))
  (unless (getopt spec 'token)
    (error "OAUTH/CALL: No OAUTH2 ACCESS token: " spec))
  (let* ((endpoint (or endpoint
		       (getopt args 'endpoint
			       (getopt spec 'endpoint
				       default-endpoint))))
	 (auth-header
	  (glom "Authorization: Bearer " (getopt spec 'token)))
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
			       (args->post args))
		      (error "Only GET and POST are currently handled")))))
    (debug%watch endpoint auth-header req)
    (if (test req 'response 200)
	(cons (getreqdata req) (cdr spec))
	req)))
(define (oauth/refresh spec)
  )

(define (oauth/call spec method endpoint (args '()) (ckey) (csecret))
  (set! spec (oauthspec spec))
  (default! ckey (getckey spec))
  (default! csecret (getcsecret spec))
  (if (testopt spec 'version "1.0")
      (oauth/call10 spec method endpoint args ckey csecret)
      (oauth/call20 spec method endpoint args ckey csecret)))
(define (oauth/call* spec method endpoint . args)
  (oauth/call spec method endpoint args))

(define (args->post args (first #t))
  (stringout
    (if (pair? args)
	(while (and (pair? args) (pair? (cdr args)))
	  (printout
	    (if first (set! first #f) "&")
	    (uriencode (car args)) "="
	    (uriencode (cadr args)))
	  (set! args (cddr args)))
	(do-choices (key (getkeys args) i)
	  (if (> i 0) (printout "&"))
	  (printout (uriencode key) "="
	    (uriencode (get args key)))))))

;;; This is helpful for debugging OAUTH1

(define (oauth/callsig spec method endpoint . args)
  (when (and (not (getopt spec 'secret))
	     (getopt spec 'realm)
	     (exists? (oauth/provider (getopt spec 'realm))))
    (set! spec (cons spec (oauth/provider (getopt spec 'realm)))))
  (let ((ckey (getckey spec))
	(csecret (getcsecret spec)))
    (unless (and ckey csecret)
      (error "OAUTH/CALL: No consumer key/secret: " spec))
    (unless (getopt spec 'oauth_token)
      (error "OAUTH/CALL: No OAUTH token: " spec))
    (unless (getopt spec 'oauth_secret)
      (error "OAUTH/CALL: No OAUTH secret: " spec))
    (let* ((nonce (getopt spec 'nonce (uuid->string (getuuid))))
	   (endpoint (or endpoint
			 (getopt args 'endpoint
				 (getopt spec 'endpoint
					 default-endpoint))))
	   (now (getopt spec 'timestamp (time)))
	   (sigstring
	    (oauth/sigstring
	     method endpoint
	     "oauth_consumer_key" ckey
	     "oauth_token" (->string (getopt spec 'oauth_token))
	     "oauth_nonce" nonce
	     "oauth_signature_method" "HMAC-SHA1"
	     "oauth_timestamp" now
	     "oauth_version" (getopt spec 'version "1.0")
	     args))
	   (sig (hmac-sha1 (glom  csecret "&" (getopt spec 'oauth_secret))
		  sigstring))
	   (sig64 (packet->base64 sig))
	   (auth-header
	    (glom "Authorization: OAuth "
	      ;; "realm=\"" (urischeme endpoint) "://" (urihost endpoint) "\", "
	      "oauth_consumer_key=\"" ckey "\", "
	      "oauth_nonce=\"" nonce "\", "
	      "oauth_signature=\"" (uriencode sig64) "\", "
	      "oauth_signature_method=\"" "HMAC-SHA1" "\", "
	      "oauth_timestamp=\"" now "\", "
	      "oauth_token=\"" (getopt spec 'oauth_token) "\", "
	      "oauth_version=\"" (getopt spec 'version "1.0") "\"")))
      (%watch now nonce sig64)
      (%watch sigstring)
      (%watch auth-header)
      sig)))
  
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

;;; Top level authenication

(define-init oauth-pending (make-hashtable))
(define-init oauth-info (make-hashtable))

;;; For calling (via req/call) from CGI scripts
;;; Arguments come from CGI
(define (oauth (code #f) (state #f)
	       (oauth_realm #f) (oauth_token #f) (oauth_verifier #f)
	       (scope #f))
  (debug%watch "OAUTH" oauth_realm code oauth_token oauth_verifier getuser)
  (if oauth_verifier ;; 1.0
      (let* ((state (get oauth-pending oauth_token))
	     (verified (oauth/verify state oauth_verifier)))
	(drop! oauth-pending oauth_token)
	(store! oauth-info oauth_token (cons verified (cdr state)))
	(let ((user (getuser verified)))
	  (debug%watch "OAUTH1/complete" user verified state)
	  user))
      (if code ;; 2.0
	  (let* ((spec (get oauth-pending state))
		 (access (and spec (oauth/access spec code))))
	    (and access
		 (let ((user (getuser access)))
		   (debug%watch "OAUTH2/complete" user verified spec)
		   user)))
	  (let* ((spec (and oauth_realm (get oauth-servers oauth_realm)))
		 (state (if (getopt spec 'request)
			    (oauth/request spec) ;; 1.0
			    (cons `#[state ,(getuuid)
				     callback ,(getcallback)]
				  spec)))
		 (redirect (oauth/authurl state)))
	    (debug%watch "OAUTH/redirect" url state)
	    (store! oauth-pending
		    (getopt state 'oauth_token (getopt state 'state))
		    state)
	    (req/set! 'status 303)
	    (httpheader "Location: " redirect)
	    #f))))

