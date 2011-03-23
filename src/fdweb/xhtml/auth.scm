;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-

(in-module 'xhtml/auth)

(use-module '{fdweb texttools})
(use-module '{varconfig logger rulesets crypto ezrecords})

;;(define %loglevel %debug!)
(define %loglevel %notify!)

(module-export! '{auth/getinfo
		  auth/getuser
		  auth/identify!
		  auth/deauthorize!})

;;;; Utility functions

(define (random-signature (length %siglen))
  (let ((bytes '()))
    (dotimes (i %siglen) (set! bytes (cons (random 256) bytes)))
    (->packet bytes)))

(define (random-integer (length 8))
  (let ((sum 0))
    (dotimes (i length)
      (set! sum (+ (* 256 sum) (random 256))))
    sum))

;;;; Constant and configurable variables

;; The cookie/CGI var used to store the session ID
(define authid 'AUTH)
(varconfig! auth:id authid)

;; The CGI state var used to store the current user
(define userid 'AUTHUSER)
(varconfig! auth:user userid)

;; How long a key to use when signing 
(define %siglen 32)
;; The key to use in signing session ids
(define signature (random-signature))
;; This is used to encrypt session ids sent insecurely
(define secret #f)
;; This is where the credentials (signature and secret) are stored
;;  in the file system (if anywhere)
(define signature-file #f)

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
	(cond ((not val)
	       (set! secret-file #f)
	       (set! secret (random-secret)))
	      ((packet? val)
	       (set! secret val))
	      ((number? val)
	       (if (and (integer? val) (> 4096 val 16))
		   (set! secret (random-secret val))
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

(define auth-expiration #f)
(define auth-refresh (* 60 15))
(varconfig! auth:expires auth-expiration)
(varconfig! auth:refresh auth-refresh)

;;; Checking tokens

(define tokentable (make-hashtable))
(varconfig! auth:tokens tokentable)
(define (table-checktoken identity token (op))
  (if (bound? op)
      (if op
	  (add! tokentable identity token)
	  (drop! tokentable identity token))
      (test tokentable identity token)))

(define checktoken table-checktoken)
(varconfig! auth:checktoken checktoken)

;;; The blacklist

(define blacklist (make-hashset))

(define (blacklisted? string)
  (or (not string) (hashset-get blacklist string)))
(defslambda (blacklist! string) (hashset-add! blacklist string))

;;;; Cookie functions

;;; Cookie parameters
;; The max duration of cookies: if #f, use session cookies, if #t, use
;; the authentication expiration
(define auth-sticky-var 'AUTH:STICKY)
(define auth-cookie-expires #t)
(define auth-cookie-domain #f)
(define auth-cookie-path "/")
(define auth-secure #f)
(varconfig! auth:cookie auth-sticky-var)
(varconfig! auth:cookie auth-cookie-expires)
(varconfig! auth:domain auth-cookie-domain)
(varconfig! auth:sitepath auth-cookie-path)
(varconfig! auth:secure auth-secure)

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

(define (expire-cookie! cookievar)
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

(define (set-cookies! var authstring expires)
  (if auth-secure
      (set-cookie! var authstring
		   auth-cookie-domain auth-cookie-path
		   expires
		   #t)
      (set-cookie! var (if secret
			   (packet->base64 (encrypt authstring secret))
			   authstring)
		   auth-cookie-domain auth-cookie-path
		   expires
		   #f))
  (when (and auth-secure secret)
    (set-cookie! (if auth-secure (stringout var "-") var)
		 (packet->base64 (encrypt authstring secret))
		 auth-cookie-domain auth-cookie-path
		 expires
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
  realm identity (token (random-integer))
  (issued (time)) (expires (+ (time) auth-expiration)))

(define (auth->string auth)
  (let* ((info (stringout (authinfo-realm auth)
		 ";" (unparse-arg (authinfo-identity auth))
		 ";" (authinfo-issued auth)
		 ";" (authinfo-token auth)
		 (if expires ";")
		 (if expires expires)))
	 (sig (hmac-sha1 info signature))
	 (result (stringout info ";" (packet->base64 sig))))
    (debug%watch "AUTH->STRING" auth info sig result)
    result))

(define (string->auth authstring (authid authid))
  (let* ((split (rposition #\; authstring))
	 (payload (and split (subseq authstring 0 split)))
	 (sig (and split (base64->packet (subseq authstring (1+ split)))))
	 (info (and split (map parse-arg (segment payload ";")))))
    (debug%watch "STRING->AUTH" 
      info payload sig (hmac-sha1 payload signature)
      authstring)
    (unless (equal? sig (hmac-sha1 payload signature))
      (logwarn "Invalid signature in " authid " authstring " (write authstring)
	       "\n\tfor " payload
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
	 (info (and split (map parse-arg (segment payload ";")))))
    (debug%watch "STRING->AUTH" 
      info payload sig (hmac-sha1 payload signature)
      authstring)
    (debug%watch "UNPACK-AUTHINFO" authstring
		 info payload sig (hmac-sha1 payload signature))
    (unless (equal? sig (hmac-sha1 payload signature))
      (logwarn "Invalid signature in " authid " authstring " (write authstring)
	       "\n\tfor " payload
	       "\n\tbased on " signature
	       "\n\texpecting " sig
	       "\n\tgetting " (hmac-sha1 payload signature)))
    (and sig payload info (equal? sig (hmac-sha1 payload signature))
	 (->vector info))))

;;;; Core functions

(define (auth/identify! identity)
  (and identity
       (let* ((auth (cons-authinfo authid identity))
	      (authstring (auth->string auth))
	      (token (authinfo-token auth))
	      (cookie-expires
	       (and (or (not auth-sticky-var) (cgiget 'auth-sticky-var #f))
		    (authinfo-expires auth))))
	 (info%watch "AUTH/IDENTIFY!" authid identity token auth authstring)
	 (when checktoken (checktoken identity token #t))
	 (cgiset! authid auth)
	 (set-cookies! authid authstring cookie-expires)
	 identity)))

(define (auth/ok? auth)
  (and auth (> (authinfo-expires auth) (time))
       (if (> (time) (+ (authinfo-issued auth) auth-refresh))
	   (newauth auth)
	   auth)))

(define (newauth auth)
  (and (or (not checktoken) (checktoken (authinfo-identity auth) (authinfo-token auth)))
       (let ((realm (authinfo-realm auth))
	     (identity (authinfo-identity auth))
	     (oldtoken (authinfo-token auth))
	     (new (cons-auth realm identity (random-integer)
			     (time) (authinfo-expires auth))))
	 (when checktoken
	   (checktoken identity oldtoken #f)
	   (checktoken identity (authinfo-token new) #t))
	 (set-cookie! realm (auth->string new)
		      auth-cookie-domain auth-cookie-path
		      (if (or (not auth-sticky-var) (cgiget 'auth-sticky-var #f))
			  (auth-expires new)
			  #f)
		      auth-secure)
	 new)))

;;; Checking authorization

(define (auth/getinfo (authid authid) (signal #f) (authinfo)
		      (https #f) (secret secret) (auth-secure auth-secure))
  (default! authinfo (getauthinfo authid))
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
	 (let ((info (string->auth authinfo authid)))
	   (if info
	       (auth/getinfo authid signal info https secret auth-secure)
	       (authfail "Invalid authorization" authid info signal))))
	;; Check if the info is a valid object
	((not (authinfo? authinfo)) 
	 (authfail "Invalid authorization object" authid authinfo signal))
	((> (time) (authinfo-expires authinfo))
	 (authfail "Authorization expired" authid authinfo signal))
	(else (or (auth/ok? authinfo)
		  (authfail "Authorization error" authid authinfo signal)))))

(define (authfail reason authid info signal)
  (expire-cookie! authid)
  (cgidrop! authid)
  (if signal
      (error reason authid info)
      (logwarn reason authid info))
  (fail))

;;; Top level functions

(define (auth/getuser (authid authid))
  (try (cgiget userid)
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
    (when checktoken (checktoken (authinfo-identity info) (authinfo-token info) #f)))
  (expire-cookie! authid))


