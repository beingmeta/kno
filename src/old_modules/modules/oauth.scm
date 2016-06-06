;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2010-2016 beingmeta, inc. All rights reserved

(in-module 'oauth)

(use-module '{fdweb reflection xhtml/auth varconfig opts})
(use-module '{texttools logger})
(define %used_modules '{varconfig xhtml/auth})
(define %volatile '{getuser oauth-sessionfn})

(define-init getuser #f)
(varconfig! oauth:getuser getuser)

(define-init default-callback #f)
(varconfig! oauth:callback default-callback)

(define-init default-endpoint #f)
(varconfig! oauth:endpoint default-endpoint)

(define-init default-verify-endpoint #f)
(varconfig! oauth:verifier default-verify-endpoint)

(define-init curlcache #f)
(varconfig! oauth:curlcache curlcache)

(module-export!
 '{oauth oauth/spec oauth/start oauth/refresh!
   oauth/request oauth/authurl oauth/verify 
   oauth/getaccess oauth/getclient
   oauth/call oauth/call* oauth/call/req
   oauth/get oauth/post oauth/put
   oauth/sigstring oauth/callsig})

(define-init %loglevel %notice!)
;;(set!  %loglevel %debug%)

(define (getcurl)
  (if curlcache
      (try (threadget 'curlcache)
	   (let ((handle (curlopen)))
	     (threadset! 'curlcache handle)
	     handle))
      (frame-create #f)))

(define (getxmldata string)
  (let ((parsed (xmlparse string '{data slotify})))
    (reject (elts parsed) string?)))

(define (getreqdata req (ctype) (content))
  (default! ctype (try (get req 'content-type) #f))
  (default! content (try (get req '%content) #f))
  (if (and (test req 'response) ctype content
	   (number? (get req 'response))
	   (>= (get req 'response) 200)
	   (< (get req 'response) 300))
      (if (search "json" ctype)
	  (jsonparse content)
	  (if (search "xml" ctype)
	      (getxmldata content)
	      ;; This comes across as text/plain from FB,
	      ;;  but should probably be something else.
	      (if (search "form" ctype)
		  (cgiparse content)
		  (if (textsearch #((bol) (spaces*) "<") content)
		      (getxmldata content)
		      (if (textsearch #((bol) (spaces*) {"{" "["})
				      content)
			  (jsonparse content)
			  (cgiparse content)))))
	  req)
      (fail)))

;;; Server info

(define oauth-servers
  `#[TWITTER
     #[REQUEST "https://api.twitter.com/oauth/request_token"
       VERIFY "https://api.twitter.com/oauth/access_token"
       AUTHORIZE "https://api.twitter.com/oauth/authorize"
       AUTHENTICATE "https://api.twitter.com/oauth/authenticate"
       KEY TWITTER:KEY SECRET TWITTER:SECRET
       VERSION "1.0"
       REALM TWITTER10
       NAME "Twitter"]
     TWITTER20
     #[ACCESS "https://api.twitter.com/oauth2/token"
       AUTHORIZE "https://api.twitter.com/oauth/authorize"
       ;; AUTHENTICATE "https://api.twitter.com/oauth/authenticate"
       KEY TWITTER:KEY SECRET TWITTER:SECRET
       ACCESS_TOKEN "oauth_token"
       VERSION "2.0"
       REALM TWITTER20
       NAME "Twitter"]
     LINKEDIN
     #[AUTHORIZE "https://www.linkedin.com/uas/oauth2/authorization"
       ACCESS "https://www.linkedin.com/uas/oauth2/accessToken"
       KEY LINKEDIN:KEY SECRET LINKEDIN:SECRET
       VERSION "2.0"
       SCOPE "r_fullprofile r_network r_emailaddress rw_groups"
       ACCESS_TOKEN "oauth2_access_token"
       REALM LINKEDIN
       NAME "LinkedIn"]
     DROPBOX
     #[AUTHORIZE "https://www.dropbox.com/1/oauth2/authorize"
       ACCESS "https://api.dropbox.com/1/oauth2/token"
       KEY DROPBOX:KEY SECRET DROPBOX:SECRET
       ;; ACCESS_TOKEN HTTP
       VERSION "2.0"
       REALM DROPBOX
       NAME "Dropbox"]
     GOOGLE
     #[AUTHORIZE "https://accounts.google.com/o/oauth2/auth"
       ACCESS "https://accounts.google.com/o/oauth2/token"
       AUTH_ARGS #["access_type" "offline"]
       KEY GOOGLE:KEY SECRET GOOGLE:SECRET
       SCOPE "openid email profile"
       VERSION "2.0"
       REALM GOOGLE
       ACCESS_TOKEN HTTP
       NAME "Google"]
     GDRIVE
     #[AUTHORIZE "https://accounts.google.com/o/oauth2/auth"
       ACCESS "https://accounts.google.com/o/oauth2/token"
       AUTH_ARGS #["access_type" "offline"]
       KEY GOOGLE:KEY SECRET GOOGLE:SECRET
       SCOPE "openid email profile https://www.googleapis.com/auth/drive.file"
       VERSION "2.0"
       REALM GOOGLE
       ACCESS_TOKEN HTTP
       NAME "Google"]
     GPLUS
     #[AUTHORIZE "https://accounts.google.com/o/oauth2/auth"
       ACCESS "https://accounts.google.com/o/oauth2/token"
       KEY GPLUS:KEY SECRET GPLUS:SECRET
       SCOPE "https://www.googleapis.com/auth/plus.me"
       VERSION "2.0"
       ACCESS_TOKEN HTTP
       REALM GPLUS
       NAME "Google+"]
     FACEBOOK
     #[AUTHORIZE "https://www.facebook.com/dialog/oauth"
       ACCESS "https://graph.facebook.com/oauth/access_token"
       KEY FB:KEY SECRET FB:SECRET
       SCOPE "publish_actions,publish_stream,user_about_me,offline_access"
       VERSION "2.0"
       REALM FACEBOOK
       NAME "Facebook"]
     AMAZON
     #[AUTHORIZE "https://www.amazon.com/ap/oa"
       ACCESS "https://api.amazon.com/auth/o2/token"
       KEY AMAZON:KEY SECRET AMAZON:SECRET
       SCOPE "profile postal_code"
       VERSION "2.0"
       REALM AMAZON
       NAME "Amazon"]
     PAYPAL
     #[AUTHORIZE "https://www.paypal.com/webapps/auth/protocol/openidconnect/v1/authorize"
       ACCESS "https://api.paypal.com/v1/identity/openidconnect/tokenservice"
       KEY PP:LOGINKEY SECRET PP:LOGINSECRET
       SCOPE "openid profile email"
       VERSION "2.0"
       REALM PAYPAL
       ACCESS_TOKEN HTTP
       NAME "Paypal"]
     PAYPALTEST
     #[AUTHORIZE
       "https://www.sandbox.paypal.com/webapps/auth/protocol/openidconnect/v1/authorize"
       ACCESS
       "https://api.sandbox.paypal.com/v1/identity/openidconnect/tokenservice"
       KEY PP:TESTKEY SECRET PP:TESTSECRET
       SCOPE "openid profile email"
       VERSION "2.0"
       REALM PAYPAL
       ACCESS_TOKEN HTTP
       NAME "Paypal Test"]
     PAYPALAPI
     #[AUTHORIZE
       "https://api.paypal.com/v1/oauth2/token"
       ACCESS
       "https://api.paypal.com/v1/oauth2/token"
       KEY PP:KEY SECRET PP:SECRET
       SCOPE ""
       VERSION "2.0"
       REALM PAYPAL
       ACCESS_TOKEN HTTP
       NAME "Paypal Test"]
     PAYPALAPITEST
     #[AUTHORIZE
       "https://api.sandbox.paypal.com/v1/oauth2/token"
       ACCESS
       "https://api.sandbox.paypal.com/v1/oauth2/token"
       KEY PP:TESTKEY SECRET PP:TESTSECRET
       SCOPE ""
       VERSION "2.0"
       REALM PAYPAL
       ACCESS_TOKEN HTTP
       NAME "Paypal Test"]
     DWOLLA
     #[AUTHORIZE "https://www.dwolla.com/oauth/v2/authenticate"
       ACCESS "https://www.dwolla.com/oauth/v2/token"
       KEY DWOLLA:KEY SECRET DWOLLA:SECRET
       SCOPE "openid profile email"
       VERSION "2.0"
       REALM DWOLLA
       NAME "Dwolla"]])

(define (oauth/provider spec) (get oauth-servers spec))
(module-export! 'oauth/provider)

(config-def! 'oauth:providers
	     (lambda (var (val))
	       (cond ((not (bound? val))
		      (get oauth-servers (getkeys oauth-servers)))
		     ((and (or (slotmap? val) (schemap? val))
			   (exists? (get val 'realm)))
		      (when (and (exists? (get oauth-servers (get val 'realm)))
				 (not (test oauth-servers (get val 'realm) val)))
			(logwarn OAUTH/REDEFINE "Redefining OAUTH specification for "
				 (get val 'realm) ", changes may be lost!"))
		      (do-choices val
			(store! oauth-servers (get val 'realm) val)))
		     (else (irritant val OAUTH:BADSPEC
				     "Invalid OAUTH provider spec")))))

(define (thisurl)
  (tryif (req/get 'request_uri #f)
    (stringout (if (= (req/get 'SERVER_PORT) 443) "https://" "http://")
      (req/get 'SERVER_NAME)
      (when (req/test 'SERVER_PORT)
	(unless (or (= (req/get 'SERVER_PORT) 80)
		    (= (req/get 'SERVER_PORT) 443))
	  (printout ":" (req/get 'SERVER_PORT))))
      (uribase (try (req/get 'request_uri) (req/get 'path_info))))))

;;; Managing state

(define-init oauth-pending (make-hashtable))
(define-init oauth-sessionfn #f)
(varconfig! oauth:sessionstore oauth-sessionfn)

(define (oauth/pending id)
  (try (get oauth-pending id)
       (tryif oauth-sessionfn
	 (let ((pending (oauth-sessionfn id)))
	   (if (exists? pending)
	       (store! oauth-pending id (oauth/spec pending)))
	   pending))))
(define (oauth/pending! id (state))
  (debug%watch "OAUTH/PENDING!" id state oauth-sessionfn)
  (if (and (bound? state) state)
      (store! oauth-pending id state)
      (drop! oauth-pending id))
  (when oauth-sessionfn
    (if (bound? state)
	(if (pair? state)
	    (let ((copied (deep-copy (car state)))
		  (realm (getopt state 'realm)))
	      (store! copied 'realm realm)
	      (oauth-sessionfn id copied))
	    (if state
		(oauth-sessionfn id state)
		(oauth-sessionfn id #f)))
	(oauth-sessionfn id #f))))

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
	      (irritant spec OAUTH:NOCKEY "Can't determine consumer key")))))
(define (getcsecret spec (val))
  (default! val (getopt spec 'secret))
  (->secret
   (if (symbol? val) (getcsecret spec (config val))
       (if (string? val) val
	   (if (packet? val) val
	       (irritant spec OAUTH:NOCSECRET
			 "Can't determine consumer secret"))))))
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

(define (oauth/spec spec)
  "Expands provider references a spec"
  (if (and (pair? spec) (symbol? (cdr spec))
	   (exists? (get oauth-servers (cdr spec))))
      (cons (car spec) (get oauth-servers (cdr spec)))
      (if (and (symbol? spec) (test oauth-servers spec))
	  (get oauth-servers spec)
	  (if (and (pair? spec) (getopt spec 'realm)) spec
	      (if (and (table? spec) (test spec 'realm)
		       (test oauth-servers (get spec 'realm)))
		  (cons spec (get oauth-servers (get spec 'realm)))
		  spec)))))

(define (oauth/request spec (ckey) (csecret))
  ;; Expand provider references in the spec
  (set! spec (debug%watch (oauth/spec spec) spec))
  (default! ckey (getckey spec))
  (default! csecret (getcsecret spec))
  (debug%watch "OAUTH/REQUEST" spec ckey cecret)
  (unless (and (getopt spec 'request)
	       (getopt spec 'authorize)
	       (getopt spec 'verify))
    ;; This should exit
    (logwarn OAUTH/REQUEST/BADSPEC "Invalid OAUTH spec: " spec)
    (irritant spec OAUTH:BADSPEC OAUTH/REQUEST
	      "Missing methods for OAuth 1.0 request"))
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
    (debug%watch "OAUTH/REQUEST"
      sigstring sig sig64 auth-header
      (get req 'response)
      (get req '%content))
    (if (test req 'response 200)
	(cons (cgiparse (get req '%content)) spec)
	(begin (warn%watch "Can't get request token" spec req)
	  (irritant req
		    OAUTH/REQFAILED OAUTH/REQUEST
		    "Can't get request token for " (getopt spec 'realm)
		    "given \n\t" spec)))))

(define (oauth/authurl spec (scope #f))
  "Returns a URL for redirection and authorization and authentication. \
   If a scope is provided, we assume that we're authorizing, not \
   authenticating."
  (set! spec (oauth/spec spec))
  (debug%watch "OAUTH/AUTHURL" spec (getcallback spec))
  (unless (and (getopt spec 'authenticate (getopt spec 'authorize))
	       (or (getopt spec 'oauth_token) (getckey spec)))
    (irritant spec OAUTH:BADSPEC OAUTH/AUTHURL "Incomplete OAUTH spec"))
  (if (testopt spec 'version "1.0")
      (scripturl+ (if scope
		      (getopt spec 'authorize (getopt spec 'authenticate))
		      (getopt spec 'authenticate (getopt spec 'authorize)))
		  (getopt spec 'auth_args)
		  "oauth_token" (getopt spec 'oauth_token)
		  "redirect_uri" (getcallback spec))
      (scripturl+
       (if scope
	   (getopt spec 'authorize (getopt spec 'authenticate))
	   (getopt spec 'authenticate (getopt spec 'authorize)))
       (get-auth-args spec)
       "client_id" (getckey spec)
       "redirect_uri" (getcallback spec)
       "scope"
       (tryif (choice (tryif scope scope) (getopt spec 'scope))
	 (stringout (do-choices (scope (or scope (getopt spec 'scope)) i)
		      (printout (if (> i 0) " ") scope))))
       "state" (getopt spec 'state (uuid->string (getuuid)))
       "response_type" "code")))

(define (get-auth-args spec)
  (let ((args (getopt spec 'auth_args))
	(req_args (req/get 'oauth_args #f)))
    (cond ((and args req_args)
	   (set! args (deep-copy args))
	   (do-choices (key (getkeys req_args))
	     (store! args key (get req_args key)))
	   args)
	  ((not args) req_args)
	  (else args))))

(define (oauth/verify spec verifier (ckey) (csecret))
  (set! spec (oauth/spec spec))
  (unless (and (getopt spec 'request)
	       (getopt spec 'authorize)
	       (getopt spec 'verify))
    (irritant spec OAUTH:BADSPEC OAUTH/VERIFY "Invalid OAUTH1.0 spec"))
  (debug%watch "OAUTH/VERIFY/1.0" verifier spec)
  (default! ckey (getckey spec))
  (default! csecret (getcsecret spec))
  (unless (getopt spec 'oauth_token)
    (logwarn OAUTH/VERIFY:NOTOKEN "No OAUTH token in " spec)
    (irritant spec OAUTH:NOTOKEN OAUTH/VERIFY "No OAUTH token"))
  (unless verifier
    (logwarn OAUTH/VERIFY:NOVERIFIER "No OAUTH verifier for " spec)
    (irritant spec OAUTH:NOVERIFIER OAUTH/VERIFY "No OAUTH verifier"))
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
	(let ((info (cgiparse (get req '%content))))
	  (store! info 'token (get info 'OAUTH_TOKEN))
	  (store! info '{oauth_secret OAUTH_TOKEN_SECRET}
		  (->secret (get info 'OAUTH_TOKEN_SECRET)))
	  (cons info (cdr spec)))
	(if (getopt spec 'noverify)
	    ((getopt spec 'noverify) spec verifier req)
	    (begin
	      (logwarn OAUTH/VERIFY:REQFAIL spec req)
	      (irritant req OAUTH:REQFAIL OAUTH:VERIFY "Web call failed"))))))

(define (oauth/getaccess spec (code) (ckey) (csecret) (verifier))
  (set! spec (oauth/spec spec))
  (default! code
    (and (or (testopt spec 'grant "authorization_code")
	     (not (testopt spec 'grant)))
	 (getopt spec 'code (req/get 'code))))
  (default! ckey (getckey spec))
  (default! csecret (getcsecret spec))
  (default! verifier (getopt spec 'verifier))
  (debug%watch "OAUTH/GETACCESS" code spec)
  (let* ((callback (getcallback spec))
	 (req (urlpost (getopt spec 'access)
		       #[content-type "application/x-www-form-urlencoded"
			 header ("Expect" . "")]
		       (args->post
			(list "code" (qc (tryif code code))
			      "fb_exchange_token"
			      (qc (tryif (and (testopt spec 'grant "fb_exchange_token")
					      (getopt spec 'token))
				    (getopt spec 'token)))
			      "client_id" ckey "client_secret" csecret
			      "grant_type"
			      (getopt spec 'grant "authorization_code")
			      "redirect_uri" (qc callback))))))
    (if (test req 'response 200)
	(let* ((parsed (getreqdata req))
	       (expires_in (->number
			    (try (get parsed 'expires_in)
				 (get parsed 'expires))))
	       (authinfo `#[token ,(get parsed 'access_token)]))
	  (debug%watch parsed spec req expires_in)
	  (when (exists? (get parsed 'token_type))
	    (unless (string-ci=? (get parsed 'token_type) "Bearer")
	      (logwarn |OAUTH/GETACESS/OddToken|
		       "Odd token type " (get parsed 'token_type) " responding to " spec
		       " response=" parsed)))
	  (when (and (exists? expires_in) expires_in)
	    (store! authinfo 'expires (timestamp+ expires_in))
	    (when (exists? (get parsed 'refresh_token))
	      (store! authinfo 'refresh
		      (->secret (get parsed 'refresh_token)))))
	  (debug%watch (cons authinfo spec) "OAUTH/GETACCESS/RESULT")
	  (cons authinfo spec))
	(begin
	  (logwarn |OATH/GETACCESS:Failure|
		   "OAUTH/GETACCESS failed " spec " ==> " req)
	  (if (getopt spec 'noverify)
	      ((getopt spec 'noverify) spec verifier req)
	      (irritant req OAUTH:REQFAIL OAUTH/GETACCESS
			"Web call failed"))))))

(define (oauth/getclient spec (ckey) (csecret))
  (set! spec (oauth/spec spec))
  (default! ckey (getckey spec))
  (default! csecret (getcsecret spec))
  (debug%watch "OAUTH/GETACCESS" spec)
  (let* ((callback (getcallback spec))
	 (req (urlpost (getopt spec 'access)
		       `#[content-type "application/x-www-form-urlencoded"
			  basicauth ,(glom ckey ":" csecret)
			  header ("Expect" . "")]
		       (args->post
			(list "grant_type" 
			      (getopt spec 'grant "client_credentials"))))))
    (if (test req 'response 200)
	(let* ((parsed (getreqdata req))
	       (expires_in (->number
			    (try (get parsed 'expires_in)
				 (get parsed 'expires))))
	       (authinfo `#[token ,(get parsed 'access_token)]))
	  (debug%watch parsed spec req expires_in)
	  (when (exists? (get parsed 'token_type))
	    (unless (string-ci=? (get parsed 'token_type) "Bearer")
	      (logwarn |OAUTH/GETACESS/OddToken|
		"Odd token type " (get parsed 'token_type) " responding to " spec
		" response=" parsed)))
	  (when (and (exists? expires_in) expires_in)
	    (store! authinfo 'expires (timestamp+ expires_in))
	    (when (exists? (get parsed 'refresh_token))
	      (store! authinfo 'refresh
		      (->secret (get parsed 'refresh_token)))))
	  (debug%watch (cons authinfo spec) "OAUTH/GETACCESS/RESULT")
	  (cons authinfo spec))
	(begin
	  (logwarn |OATH/GETACCESS:Failure|
	    "OAUTH/GETACCESS failed " spec " ==> " req)
	  (irritant req OAUTH:REQFAIL OAUTH/GETACCESS
		    "Web call failed")))))

;;; Actually calling the API

(define (oauth/call10 spec method endpoint args body ctype raw ckey csecret)
  (default! ctype
    (and body
	 (if (pair? body) (cdr body)
	     (getopt spec 'ctype
		     (if (packet? body) "application" "text")))))
  (debug%watch "OAUTH/CALL1.0" method endpoint args ctype)
  (unless (getopt spec 'token)
    (irritant spec OAUTH:NOTOKEN OAUTH/CALL
	      "No OAUTH token for OAuth1.0 call"))
  (unless (getopt spec 'oauth_secret)
    (irritant spec OAUTH:NOSECRET OAUTH/CALL
	      "No OAUTH secret for OAuth1.0 call"))
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
	   "oauth_token" (->string (getopt spec 'token))
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
	 (sig (hmac-sha1 (glom csecret "&" (getopt spec 'oauth_secret))
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
	    "oauth_token=\"" (getopt spec 'token) "\", "
	    "oauth_version=\"" (getopt spec 'version "1.0") "\""))
	 (content (if (pair? body) (car body) body))
	 (handle (curlopen 'header "Expect: "
			   'header auth-header
			   'method method))
	 (req (if (eq? method 'GET)
		  (urlget (if (pair? args)
			      (apply scripturl endpoint args)
			      (scripturl+ endpoint args))
			  handle)
		  (if (eq? method 'HEAD)
		      (urlhead (if (pair? args)
				   (apply scripturl endpoint args)
				   (scripturl+ endpoint args))
			       handle)
		      (if (eq? method 'POST)
			  (urlpost endpoint handle (args->post args))
			  (if (eq? method 'PUT)
			      (urlput (if (pair? args)
					  (apply scripturl endpoint args)
					  (scripturl+ endpoint args))
				      content ctype handle)
			      (error OAUTH:BADMETHOD OAUTH/CALL10
				     "Only GET, HEAD, PUT, and POST are allowed: "
				     method endpoint args)))))))
    (debug%watch now nonce sig64)
    ;; Keeping these watchpoints separate makes them easier to read
    ;;  (maybe %watch needs a redesign?)
    (debug%watch sigstring)
    (debug%watch auth-header)
    (if raw req
	(if (and (test req 'response) (number? (get req 'response))
		 (<= 200 (get req 'response) 299))
	    (getreqdata req)
	    (irritant req OAUTH:REQFAIL OAUTH/CALL1.0
		      "Failed to " method " at " endpoint
		      " with " args "\n\t" spec)))))

(define (oauth/call20 spec method endpoint args
		      (body #f) (ctype) (raw) (ckey) (csecret) (expires))
  (debug%watch "OAUTH/CALL2.0" method endpoint args ckey csecret)
  (unless (getopt spec 'token)
    (irritant spec OAUTH:NOTOKEN OAUTH/CALL
	      "No OAUTH token for OAuth2 call"))
  (default! expires (getopt spec 'expires))
  (default! ctype
    (and body
	 (if (pair? body) (cdr body)
	     (getopt spec 'ctype
		     (if (packet? body) "application" "text")))))
  (when (and expires (time-earlier? expires)) (oauth/refresh! spec))
  (let* ((endpoint (or endpoint
		       (getopt args 'endpoint
			       (getopt spec 'endpoint default-endpoint))))
	 (httpauth (testopt spec 'access_token 'http))
	 (auth-header
	  (if httpauth
	      (glom "Authorization: Bearer " (getopt spec 'token))
	      ""))
	 (content (if (pair? body) (car body) body))
	 (useurl (if (or (eq? method 'get) (eq? method 'put)
			 (and (eq? method 'post) body))
		     (if httpauth
			 (if (or (not args) (null? args)) endpoint
			     (if (pair? args)
				 (apply scripturl endpoint args)
				 (apply scripturl+ endpoint (list args))))
			 (if (pair? args)
			     (apply scripturl endpoint
				    (getopt spec 'access_token "access_token")
				    (getopt spec 'token)
				    args)
			     (if (or (not args) (null? args))
				 (scripturl endpoint
				     (getopt spec 'access_token "access_token")
				   (getopt spec 'token))
				 (apply scripturl+ endpoint (list args)))))
		     endpoint))
	 (handle (if httpauth
		     (curlopen 'header "Expect: " 'header auth-header
			       'method method)
		     (curlopen 'header "Expect: " 'method method)))
	 (req (if (eq? method 'GET) (urlget useurl handle)
		  (if (eq? method 'HEAD) (urlget useurl handle)
		      (if (eq? method 'POST)
			  (if body
			      (urlput useurl content ctype handle)
			      (urlpost useurl handle (args->post args)))
			  (if (eq? method 'PUT)
			      (urlput useurl content ctype handle)
			      (error OAUTH:BADMETHOD OAUTH/CALL20
				     "Only GET, HEAD, PUT, and POST are allowed: "
				     method endpoint args)))))))
    (debug%watch endpoint auth-header req)
    (if raw req
	(if (and (test req 'response) (number? (get req 'response))
		 (<= 200 (get req 'response) 299))
	    (getreqdata req)
	    (if (and (<= 400 (get req 'response) 499) (getopt spec 'refresh))
		(begin
		  (debug%watch 'OAUTH/ERROR "RESPONSE" response req)
		  (oauth/refresh! spec)
		  (oauth/call20 spec method endpoint args
				body ctype raw ckey csecret))
		(irritant req OAUTH:REQFAIL OAUTH/CALL20
			  method " at " endpoint " with " args
			  "\n\t" spec))))))

(define (args->post args (first #t) (elt #f))
  (if (not (pair? args)) (set! args (list args)))
  (stringout
    (while (pair? args)
      (set! elt (car args))
      (cond ((table? elt)
	     (do-choices (key (getkeys elt) i)
	       (printout
		 (if first (set! first #f) "&")
		 (uriencode key) "="
		 (uriencode (get elt key))))
	     (set! args (cdr args)))
	    ((and (pair? (cdr args)) (fail? (cadr args)))
	     (set! args (cddr args)))
	    ((pair? (cdr args))
	     (printout
	       (if first (set! first #f) "&")
	       (uriencode (car args))
	       "="
	       (uriencode (cadr args)))
	     (set! args (cddr args)))
	    (else
	     (printout (uriencode elt))
	     (set! args (cdr args)))))))

(define (oauth/refresh! spec)
  (unless (getopt spec 'refresh)
    (irritant spec OAUTH:NOREFRESH OAUTH/REFRESH!
	      "No OAUTH2 refresh key"))
  (unless (pair? spec) (set! spec (oauth/spec spec)))
  (let* ((endpoint (getopt spec 'access (getopt spec 'authorize)))
	 (auth-header
	  (glom "Authorization: Bearer " (getopt spec 'token)))
	 (req (urlpost endpoint
		       (curlopen 'header "Expect: "
				 'header auth-header
				 'method 'POST)
		       (scripturl+
			#f `#["client_id" ,(getckey spec)
			      "client_secret" ,(getcsecret spec)
			      "grant_type" "refresh_token"
			      "refresh_token" ,(getopt spec 'refresh)]))))
    (debug%watch endpoint auth-header req)
    (if (test req 'response 200)
	(let* ((parsed (getreqdata req))
	       (expires_in (->number (try (get parsed 'expires_in)
					  (get parsed 'expires))))
	       (newtoken (get parsed 'access_token))
	       (head (car spec)))
	  (when (exists? newtoken) (store! head 'token  newtoken))
	  (unless (equal? (get parsed 'token_type) "Bearer")
	    (logwarn |OAUTH/REFRESH!/OddToken|
		     "Odd token type " (get parsed 'token_type)
		     " responding to " spec " from " parsed))
	  (when expires_in
	    (store! head 'expires (timestamp+ expires_in))
	    (store! head 'refresh (get parsed 'refresh)))
	  (unless expires_in
	    (drop! head 'expires) (drop! head 'refresh))
	  (when getuser (getuser spec))
	  spec)
	(irritant req OAUTH:NOREFRESH OAUTH/REFRESH!
		  "Can't refresh token" spec))))

;;; Generic call function

(define (oauth/call spec method endpoint (args '())
		    (body #f) (ctype) (raw) (ckey) (csecret))
  (set! spec (oauth/spec spec))
  (default! ctype
    (and body
	 (if (pair? body) (cdr body)
	     (getopt spec 'ctype
		     (if (packet? body) "application" "text")))))
  (default! ckey (getckey spec))
  (default! csecret (getcsecret spec))
  (default! raw (getopt spec 'noparse))
  (if (testopt spec 'version "1.0")
      (oauth/call10 spec method endpoint args body ctype raw ckey csecret)
      (oauth/call20 spec method endpoint args body ctype raw ckey csecret)))
(define (oauth/call* spec method endpoint . args)
  (oauth/call spec method endpoint args))
(define (oauth/call/req spec method endpoint
			(args '()) (body #f) (ctype) (ckey) (csecret))
  (set! spec (oauth/spec spec))
  (default! ckey (getckey spec))
  (default! csecret (getcsecret spec))
  (default! ctype
    (and body
	 (if (pair? body) (cdr body)
	     (getopt spec 'ctype
		     (if (packet? body) "application" "text")))))
  (if (testopt spec 'version "1.0")
      (oauth/call10 spec method endpoint args body ctype #t ckey csecret)
      (oauth/call20 spec method endpoint args body ctype #t ckey csecret)))

;;; This is helpful for debugging OAUTH1

(define (oauth/callsig spec method endpoint . args)
  (when (and (not (getopt spec 'secret))
	     (getopt spec 'realm)
	     (exists? (oauth/provider (getopt spec 'realm))))
    (set! spec (cons spec (oauth/provider (getopt spec 'realm)))))
  (let ((ckey (getckey spec))
	(csecret (getcsecret spec)))
    (unless (getopt spec 'oauth_token)
      (irritant spec |OAUTH/Missing| OAUTH/CALLSIG "No OAUTH token"))
    (unless (getopt spec 'oauth_secret)
      (irritant spec |OAUTH/Missing| OAUTH/CALLSIG "No OAUTH secret"))
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
      ;; These are supposed to be naked %watch statements
      (always%watch now nonce sig64)
      (always%watch sigstring)
      (always%watch auth-header)
      sig)))
  
;;; Calling functions

(define (oauth/get spec endpoint (args #[]))
  (getreqdata (oauth/call spec 'GET endpoint args)))
(define (oauth/post spec endpoint (args #[]))
  (getreqdata (oauth/call spec 'POST endpoint args)))
(define (oauth/put spec endpoint (args #[]))
  (getreqdata (oauth/call spec 'PUT endpoint args)))

;;; Top level authentication

(define-init oauth-info (make-hashtable))
(define-init oauth-onaccess (make-hashtable))

(config-def! 'oauth:onaccess
	     (lambda (var (val))
	       (if (bound? val)
		   (if (and (procedure? val) (string? (procedure-name val)))
		       (let* ((name (string->symbol (procedure-name val)))
			      (cur (get oauth-onaccess name)))
			 (cond ((fail? cur) (store! oauth-onaccess name val))
			       ((identical? cur val))
			       (else (logwarn "Redefining ONACCESS handler for "
					      name)
				     (store! oauth-onaccess name val))))
		       (error |TypeError| OAUTH:ONACCESS
			      "Not a named procedure: " val))
		   (get oauth-onaccess (getkeys oauth-onaccess)))))

;;; For calling (via req/call) from CGI scripts
;;; Arguments come from CGI
(define (oauth (code #f) (state #f)
	       (oauth_realm #f) (oauth_token #f) (oauth_verifier #f)
	       (scope #f))
  (debug%watch "OAUTH" oauth_realm code oauth_token oauth_verifier scope getuser)
  (if oauth_verifier ;; 1.0 success
      (let* ((auth-state (oauth/pending oauth_token))
	     (verified (oauth/verify auth-state oauth_verifier)))
	(oauth/pending! oauth_token) ;; Drop state
	(store! oauth-info oauth_token (cons verified (cdr auth-state)))
	(let ((user (getuser verified)))
	  (loginfo |OAUTH1/complete| " Got user " user
		   " verified with\n" (printopts verified 'verified)
		   " and auth-state\n" (printopts auth-state 'oauth))
	  user))
      (if code ;; 2.0 success
	  (let* ((spec (oauth/pending state))
		 (access (and spec (oauth/getaccess spec code))))
	    (unless access
	      (logwarn |OAUTH2/noaccess|
		"Couldn't get access token given code " code
		"for spec " spec))
	    (and access
		 (let* ((handler (getopt spec 'onaccess getuser))
			(redirect (getopt spec 'redirect))
			(user (if (applicable? handler)
				  (handler access)
				  (if (and (test oauth-onaccess handler)
					   (procedure? (get oauth-onaccess handler)))
				      ((get oauth-onaccess handler) access)
				      (error |BadHandler| OAUTH
					     "Not a valid OAUTH ONACCESS handler"
					     handler)))))
		   (loginfo |OAUTH2/gotcode|
		     "Code=" code " and access " access
		     " yielding user " user " returning to " redirect)
		   (debug%watch "OAUTH2/complete" handler user access spec)
		   (oauth/pending! state) ;; Drop state
		   (when redirect
		     (req/set! 'status 303)
		     (httpheader "Location: " redirect))
		   user)))
	  (let* ((spec (and oauth_realm
			    (if (table? oauth_realm)
				oauth_realm
				(get oauth-servers oauth_realm))))
		 (state (if (getopt spec 'request)
			    (oauth/request spec) ;; 1.0
			    (cons `#[state ,(uuid->string (getuuid))
				     callback ,(getcallback spec)]
				  spec)))
		 (redirect (oauth/authurl state scope)))
	    (loginfo |OAUTH/redirect|
	      "\n   Directing user to " redirect
	      " given state:\n" (printopts state 'oauth))
	    (debug%watch "OAUTH/redirect" redirect state)
	    (oauth/pending! (getopt state 'oauth_token (getopt state 'state))
			    state)
	    (req/set! 'status 303)
	    (httpheader "Location: " redirect)
	    #f))))

(define (oauth/start spec)
  (debug%watch "OAUTH/START" spec)
  (set! spec (oauth/spec spec))
  (let* ((state (if (getopt spec 'request)
		    (oauth/request spec) ;; 1.0
		    spec))
	 (redirect (oauth/authurl state)))
    (debug%watch "OAUTH/START/redirect" redirect state)
    (oauth/pending!
     (getopt state 'oauth_token (getopt state 'state))
     state)
    (req/set! 'status 303)
    (httpheader "Location: " redirect)
    #f))
