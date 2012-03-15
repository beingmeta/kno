;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-

(in-module 'xhtml/auth)

(use-module '{fdweb texttools})
(use-module '{varconfig logger rulesets crypto ezrecords})

(define-init %loglevel %notify!)
;;(define %loglevel %debug!)

(module-export! '{auth/getinfo
		  auth/getuser
		  auth/identify!
		  auth/maketoken
		  auth/deauthorize!})

;;;; Utility functions

(define (random-signature (length %siglen)) (random-packet length))

(define-init *default-random-chars*
  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789.-!?")

(define (random-string len (chars *default-random-chars*))
  (let ((charvec (make-vector len))
	(nrandom (length chars)))
    (dotimes (i len)
      (vector-set! charvec i (elt chars (random nrandom))))
    (->string charvec)))

(define (auth/maketoken (length 7) (mult (microtime)))
  (let ((sum 0) (modulus 1))
    (dotimes (i length)
      (set! sum (+ (* 256 sum) (random 256)))
      (set! modulus (* modulus 256)))
    (remainder (* sum mult) modulus)))

;;;; Constant and configurable variables

;; The cookie/CGI var used to store the session ID
(define-init authid 'AUTH)
(varconfig! auth:id authid)

;; The CGI state var used to store the current user
(define-init userid 'AUTHUSER)
(varconfig! auth:user userid)

;; How long a key to use when signing 
(define %siglen 32)
;; The key to use in signing session ids
(define-init signature (random-signature))
;; This is used to encrypt session ids sent insecurely
(define-init secret #f)
;; This is where the credentials (signature and secret) are stored
;;  in the file system (if anywhere)
(define-init signature-file #f)

(define (signature-config var (val))
  (if (not (bound? val)) signature
      (begin
	(cond ((not val)
	       (set! signature-file #f)
	       (set! signature (random-signature)))
	      ((packet? val)
	       (set! signature val))
	      ((number? val)
	       (if (and (integer? val) (> 4096 val 16))
		   (set! signature (random-signature val))
		   (error "Invalid signature length" val)))
	      ((not (string? val))
	       (error "Invalid signature key" val))
	      ((has-prefix val "0x")
	       (set! signature (base16->packet (subseq val 2))))
	      ((has-prefix val "0z")
	       (set! signature (base64->packet (subseq val 2))))
	      ((has-prefix val "{base16}")
	       (set! signature (base16->packet (subseq val 8))))
	      ((has-prefix val "{base64}")
	       (set! signature (base64->packet (subseq val 8))))
	      (else (error "Can't interpret signature value" val)))
	(when signature-file
	  (dtype->file val (vector signature secret))))))
(config-def! 'auth:signature signature-config)

(define (secret-config var (val))
  (if (not (bound? val)) secret
      (begin
	(cond ((not val) (set! secret (random-packet 32)))
	      ((packet? val)
	       (set! secret val))
	      ((number? val)
	       (if (and (integer? val) (> 4096 val 16))
		   (set! secret (random-packet val))
		   (error "Invalid secret length" val)))
	      ((not (string? val))
	       (error "Invalid secret key" val))
	      ((has-prefix val "0x")
	       (set! secret (base16->packet (subseq val 2))))
	      ((has-prefix val "0z")
	       (set! secret (base64->packet (subseq val 2))))
	      ((has-prefix val "{base16}")
	       (set! secret (base16->packet (subseq val 8))))
	      ((has-prefix val "{base64}")
	       (set! secret (base64->packet (subseq val 8))))
	      (else (error "Can't interpret secret value" val)))
	(when signature-file
	  (dtype->file val (vector signature secret))))))
(config-def! 'auth:secret secret-config)

(define (signature-file-config var (val))
  (cond ((not (bound? val)) signature-file)
	((not val) (set! signature-file #f))
	((not (string? val)) (error "Invalid signature filename" val))
	((file-exists? val)
	 (set! signature-file val)
	 (let ((content (file->dtype val)))
	   (if (vector? content)
	       (if (= (length content) 2)
		   (begin (set! signature (first content))
			  (set! secret (second content)))
		   (error "Invalid signature file content in " (write val)))
	       (set! signature (filedata val)))))
	(else (set! signature-file val)
	      (dtype->file (vector signature secret) val))))
(config-def! 'auth:sigfile signature-file-config)

;;; Expiration intervals

(define auth-expiration (* 3600 24 14))
(define auth-refresh (* 60 15))
(define auth-grace (* 60 15))
(varconfig! auth:expires auth-expiration)
(varconfig! auth:refresh auth-refresh)
(varconfig! auth:grace auth-grace)

;;; Checking tokens

(define tokentable (make-hashtable))
(varconfig! auth:tokens tokentable)
(define (table-checktoken identity token (op))
  (if (bound? op)
      (if op
	  (add! tokentable identity token)
	  (drop! tokentable identity token))
      (test tokentable identity token)))

;; This is the default function for checking tokens
;;  It takes an identity and a token value and an #t/#f op
(define-init checktoken table-checktoken)
(varconfig! auth:checktoken checktoken)

;;;; Cookie functions

;;; Cookie parameters
;; The max duration of cookies: if #f, use session cookies, if #t, use
;; the authentication expiration
(define-init auth-cookie-expires #t)
(define-init auth-cookie-domain #f)
(define-init auth-cookie-path "/")
(define-init auth-secure #f)
;; Whether to have expiration apply to the cookie or just the
;; authentication token
(define-init auth-sticky #f)

(varconfig! auth:cookie auth-cookie-expires)
(varconfig! auth:domain auth-cookie-domain)
(varconfig! auth:sitepath auth-cookie-path)
(varconfig! auth:secure auth-secure)
(varconfig! auth:sticky auth-sticky)

;; How cookies are used:
;; If *auth-secure* is false and *secret* is false,
;;   we send a single signed authentication over both HTTP and HTTPS
;; if *auth-secure* is false but we have a (non-false) *secret*,
;;   we send a single signed and encrypted authentication
;;     over both HTTP and HTTPS
;; if *auth-secure* is true and *secret* is false,
;;   we send a single signed authentication over HTTPS
;; if *auth-secure* is true and we have a *secret*, we send two cookies:
;;    _var_ is a signed authentication in plaintext over HTTPS
;;    _var-_ is a signed and encrypted authentication

(define (expire-cookie! cookievar (cxt #f))
  (debug%watch "EXPIRE-COOKIE!" cookievar cxt)
  (set-cookie! cookievar "expired"
	       auth-cookie-domain auth-cookie-path
	       (timestamp+ (- (* 7 24 3600)))
	       #f)
  (set-cookie! cookievar "expired"
	       auth-cookie-domain auth-cookie-path
	       (timestamp+ (- (* 7 24 3600)))
	       #t)
  (set-cookie! (stringout cookievar "-") "expired"
	       auth-cookie-domain auth-cookie-path
	       (timestamp+ (- (* 7 24 3600)))
	       #f))

(define (set-cookies! auth (var authid) (uservar userid)
		      (authstring) (identity))
  (default! authstring (auth->string auth))
  (default! identity (authinfo-identity auth))
  (debug%watch "SET-COOKIES!" var authstring identity
	       auth-cookie-domain auth-cookie-path)
  (if auth-secure
      (set-cookie! var authstring
		   auth-cookie-domain auth-cookie-path
		   (and (authinfo-sticky? auth) (authinfo-expires auth))
		   #t)
      (set-cookie! var (if secret
			   (packet->base64 (encrypt authstring secret))
			   authstring)
		   auth-cookie-domain auth-cookie-path
		   (and (authinfo-sticky? auth) (authinfo-expires auth))
		   #f))
  (req/set! var
	    (if auth-secure authstring
		(if secret (packet->base64 (encrypt authstring secret))
		    authstring)))
  ;; When you have a secret but are in secure mode, store an encrypted version
  ;;  of your authorization in a variant
  (when (and auth-secure secret)
    (set-cookie! (if auth-secure (stringout var "-") var)
		 (packet->base64 (encrypt authstring secret))
		 auth-cookie-domain auth-cookie-path
		 (and (authinfo-sticky? auth) (authinfo-expires auth))
		 #f)
    (req/set! (if auth-secure (stringout var "-") var)
	      (packet->base64 (encrypt authstring secret))))
  ;; When you can encrypt it, store the identity as a cookie
  ;;  Note that this should never be used to confirm identity
  (when secret
    (set-cookie! (glom "." uservar)
		 (packet->base64
		  (encrypt (stringout (unparse-arg identity) ";"
			     (random-string 128))
			   secret))
		 auth-cookie-domain auth-cookie-path
		 (timestamp+ (* 3600 24 42))
		 #f)))

(define (getauthinfo var)
  (cond ((and auth-secure (cgiget 'https #f)) (cgiget var))
	((and auth-secure secret)
	 (packet->string
	  (decrypt (base64->packet (cgiget (stringout var "-"))) secret)))
	(auth-secure #f)
	(secret (decrypt (cgiget var) secret))
	(else (cgiget var))))

;;; AUTHINFO

(defrecord authinfo
  realm identity (token (auth/maketoken))
  (issued (time))
  (expires (and auth-expiration (+ (time) auth-expiration)))
  (sticky? #f))

(define (auth->string auth)
  (let* ((expires (authinfo-expires auth))
	 (info (stringout (authinfo-realm auth)
		 ";" (unparse-arg (authinfo-identity auth))
		 ";" (authinfo-token auth)
		 ";" (authinfo-issued auth)
		 ";" (or expires "")
		 (if (authinfo-sticky? auth) ";STICKY")))
	 (sig (hmac-sha1 info signature))
	 (result (stringout info ";" (packet->base64 sig))))
    (debug%watch "AUTH->STRING" auth info sig result)
    result))

(define (string->auth authstring (authid authid))
  (let* ((split (rposition #\; authstring))
	 (payload (and split (subseq authstring 0 split)))
	 (sig (and split (base64->packet (subseq authstring (1+ split)))))
	 (info (and split (map string->lisp (segment payload ";")))))
    (debug%watch "STRING->AUTH" 
      info payload sig (hmac-sha1 payload signature)
      authstring)
    (unless (equal? sig (hmac-sha1 payload signature))
      (logwarn "Invalid signature in " authid " authstring " (write authstring)
	       "\n\tfor " payload
	       "\n\tor " info
	       "\n\tbased on " signature
	       "\n\texpecting " sig
	       "\n\tgetting " (hmac-sha1 payload signature)))
    (and sig payload info
	 (equal? (car info) authid)
	 (equal? sig (hmac-sha1 payload signature))
	 (apply cons-authinfo info))))

(define (unpack-authinfo authstring)
  (let* ((split (rposition #\; authstring))
	 (payload (and split (subseq authstring 0 split)))
	 (sig (and split (base64->packet (subseq authstring (1+ split)))))
	 (info (and split (map string->lisp (segment payload ";"))))
	 (expected (hmac-sha1 payload signature)))
    (debug%watch "UNPACK-AUTHINFO" authstring info payload sig expected)
    (unless (equal? sig expected)
      (logwarn "Invalid signature in " authid " authstring " (write authstring)
	       "\n\tfor " payload
	       "\n\tbased on " signature
	       "\n\texpecting " expected
	       "\n\tgetting " sig))
    (and sig payload info (equal? sig computed) (->vector info))))

;;;; Core functions

(define (auth/identify! identity (sticky auth-sticky)
			(expires auth-expiration))
  (and identity
       (let* ((auth (cons-authinfo authid identity
				   (auth/maketoken) (time)
				   (if expires
				       (if (> expires (time)) expires
					   (+ (time) expires))
				       (if auth-expiration
					   (+ (time) auth-expiration)
					   (+ (time) 3600)))
				   sticky))
	      (token (authinfo-token auth)))
	 (debug%watch "AUTH/IDENTIFY!" authid identity token auth sticky
	   (auth->string auth))
	 ;; This adds token as a valid token for identity
	 (when checktoken
	   (lognotice "AUTH/IDENTIFY! " authid "=" identity " w/" token)
	   (checktoken identity token #t))
	 (cgiset! authid auth)
	 (set-cookies! auth)
	 identity)))

(define (auth/ok? auth)
  (and auth
       (or (not (authinfo-expires auth))
	   (> (authinfo-expires auth) (time))
	   (begin (logwarn "AUTH TOKEN expired: " auth) #f))
       (if (or (not auth-refresh)
	       (> (time) (+ (authinfo-issued auth) auth-refresh)))
	   (freshauth auth)
	   auth)))

(define (token/ok? identity token)
  (or (checktoken identity token)
      (begin (logwarn "TOKEN/OK? failed for "
		      identity " with " token " using " checktoken)
	#f)))

(define (freshauth auth)
  (lognotice "Refreshing auth token " auth)
  (and (or (not checktoken) ;; Valid token?
	   (token/ok? (authinfo-identity auth) (authinfo-token auth)))
       ;; Check that the authorization isn't too old to refresh
       ;;  (HTTPS tokens are always good to refresh)
       (or (req/get 'https #f)
	   (< (time) (+ (authinfo-issued auth) auth-refresh auth-grace))
	   (begin (logwarn "Auth token older than refresh grace period: "
			   "issued=" (authinfo-issued auth)
			   "; refresh=" auth-refresh "; grace=" auth-grace
			   "; auth=" auth)
	     #f))
       (let* ((realm (authinfo-realm auth))
	      (identity (authinfo-identity auth))
	      ;; The only thing changed is the issue datetime
	      (new (cons-authinfo realm identity (authinfo-token auth)
				  (time) (authinfo-expires auth)
				  (authinfo-sticky? auth))))
	 (notify "Fresh auth " new "\n\t replacing " auth)
	 (set-cookies! new)
	 new)))

;;; Checking authorization

(define (auth/getinfo (authid authid) (signal #f) (authinfo)
		      (https #f) (secret secret) (auth-secure auth-secure))
  (default! authinfo (debug%watch (getauthinfo authid) authid))
  (debug%watch "AUTH/GETINFO"
	       authid authinfo signal
	        (cgiget authid)
	        auth-secure
	        (cgiget 'https #f)
	        (not (not secret)))
  (cond ((fail? authinfo) (fail))
	((not authinfo) (authfail  "No authorization info" authid authinfo))
	;; If the info is a string, convert it and authorize that
	;; (conversion might fail if the info is invalid)
	((string? authinfo)
	 ;; If authinfo is a string, try to convert it and call
	 ;; yourself again
	 (let ((info (string->auth authinfo authid)))
	   (if info
	       (auth/getinfo authid signal info https secret auth-secure)
	       (authfail "Invalid authorization" authid info signal))))
	;; Check if the info is a valid object
	((not (authinfo? authinfo)) 
	 (authfail "Invalid authorization object" authid authinfo signal))
	((and (authinfo-issued authinfo)
	      (> (authinfo-issued authinfo) (+ (time) 60)))
	 (authfail "Authorization time paradox" authid authinfo signal))
	((and (authinfo-expires authinfo)
	      (> (time) (authinfo-expires authinfo)))
	 (authfail "Authorization expired" authid authinfo signal))
	(else (or (auth/ok? authinfo) 
		  ;; This might return a new, fresh authinfo
		  (authfail "Authorization error" authid authinfo signal)))))

(define (authfail reason authid info signal)
  (debug%watch "AUTHFAIL" reason authid info)
  (expire-cookie! authid "AUTHFAIL")
  (req/drop! authid)
  (logwarn reason " AUTHID=" authid "; INFO=" info)
  (if signal (error reason authid info))
  (fail))

;;; Top level functions

(define (auth/getuser (authid authid))
  (try (req/get userid)
       (authinfo-identity (auth/getinfo authid))
       #f))

;;;; Authorize/deauthorize API

(define (auth/deauthorize! (authid authid) (info))
  ;; Get the arguments sorted
  (cond ((authinfo? authid)
	 (set! info authid)
	 (set! authid (authinfo-realm info)))
	(else (default! info (cgiget authid))))
  (when (string? info) (set! info (string->auth info)))
  (when info
    (when checktoken
      (checktoken (authinfo-identity info) (authinfo-token info) #f)))
  (expire-cookie! authid "AUTH/DEAUTHORIZE!"))


