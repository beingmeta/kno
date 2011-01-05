(in-module 'xhtml/auth)

(use-module '{fdweb texttools})
(use-module '{varconfig logger rulesets ezrecords})

;;(define %loglevel %debug!)
(define %loglevel %notify!)

(module-export! '{auth/getinfo
		  auth/getuser
		  auth/identify!
		  auth/deauthorize!})

(define (random-signature (length %siglen))
  (let ((bytes '()))
    (dotimes (i %siglen) (set! bytes (cons (random 256) bytes)))
    (->packet bytes)))

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
	(when signature-file (write-file val signature)))))
(config-def! 'auth:signature signature-config)

(define (signature-file-config var (val))
  (cond ((not (bound? val)) signature-file)
	((not val) (set! signature-file #f))
	((not (string? val)) (error "Invalid signature file" val))
	((file-exists? val)
	 (set! signature-file val)
	 (set! signature (filedata val)))
	(else (set! signature-file val)
	      (write-file val signature))))
(config-def! 'auth:sigfile signature-file-config)

;; Various expiration intervals
(define auth-expiration (* 3600 24 17))
(define auth-refresh (* 3600 24))
(varconfig! auth:expires auth-expiration)
(varconfig! auth:refresh auth-refresh)

;; Cookie parameters

;; The max duration of cookies, if #f, use session cookies, if #t, use
;; the authentication expiration
(define auth-cookie-expires #t)
(define auth-cookie-domain #f)
(define auth-cookie-path "/")
(define auth-secure #f)
(varconfig! auth:cookie auth-cookie-expires)
(varconfig! auth:domain auth-cookie-domain)
(varconfig! auth:sitepath auth-cookie-path)
(varconfig! auth:secure auth-secure)

;; The blacklist
(define blacklist (make-hashset))

(define (blacklisted? string)
  (or (not string) (hashset-get blacklist string)))
(defslambda (blacklist! string) (hashset-add! blacklist string))

;;; Cookie functions

(define (expire-cookie! cookievar)
  (set-cookie! cookievar "expired"
	       auth-cookie-domain auth-cookie-path
	       (timestamp+ (- (* 7 24 3600)))
	       auth-secure)
  (when auth-secure
    (set-cookie! (stringout cookievar "EXPIRES") "expired"
		 auth-cookie-domain auth-cookie-path
		 (timestamp+ (- (* 7 24 3600)))
		 #f)))

;;; AUTHINFO

(defrecord authinfo realm identity expires)

(define (auth->string auth)
  (let* ((info (stringout (authinfo-realm auth)
			  ";" (unparse-arg (authinfo-identity auth))
			  ";" (authinfo-expires auth)))
	 (sig (hmac-sha1 info signature))
	 (result (stringout info ";" (packet->base64 sig))))
    (debug%watch "AUTH->STRING" auth info sig result)
    result))

(define (string->auth authstring (authid authid))
  (let* ((segs (segment authstring ";"))
	 (info (vector (string->lisp (first segs)) (parse-arg (second segs))
		       (parse-arg (third segs))
		       (base64->packet (fourth segs))))
	 (payload (stringout (first info) ";" (unparse-arg (second info)) ";"
			     (third info)))
	 (sig (fourth info)))
    (debug%watch "STRING->AUTH" authstring
		 info payload sig (hmac-sha1 payload signature))
    (unless (equal? sig (hmac-sha1 payload signature))
      (logwarn "Invalid signature in " authid " authstring " (write authstring)
	       "\n\tfor " payload
	       "\n\tbased on " signature
	       "\n\texpecting " sig
	       "\n\tgetting " (hmac-sha1 payload signature)))
    (and sig info payload (equal? sig (hmac-sha1 payload signature))
	 (equal? (elt info 0) authid)
	 (cons-authinfo (elt info 0) (elt info 1) (elt info 2)))))

(define (unpack-authinfo authstring (checksig #f))
  (let* ((segs (segment authstring ";"))
	 (info (vector (string->lisp (first segs)) (parse-arg (second segs))
		       (parse-arg (third segs))
		       (base64->packet (fourth segs))))
	 (payload (stringout (first info) ";" (unparse-arg (second info)) ";"
			     (third info)))
	 (sig (fourth info)))
    (debug%watch "UNPACK-AUTHINFO" authstring
		 info payload sig (hmac-sha1 payload signature))
    (unless (equal? sig (hmac-sha1 payload signature))
      (logwarn "Invalid signature in " authid " authstring " (write authstring)
	       "\n\tfor " payload
	       "\n\tbased on " signature
	       "\n\texpecting " sig
	       "\n\tgetting " (hmac-sha1 payload signature)))
    info))

;;; Core functions

(define (auth/identify! identity (duration auth-expiration))
  (and identity
       (let* ((auth (cons-authinfo authid identity (+ (time) duration)))
	      (authstring (auth->string auth)))
	 (cgiset! authid auth)
	 (set-cookie! authid authstring
		      auth-cookie-domain auth-cookie-path
		      (if auth-cookie-expires
			  (if (number? auth-cookie-expires)
			      (timestamp+ (min duration auth-cookie-expires))
			      (timestamp+ duration))
			  #f)
		      auth-secure)
	 (when auth-secure
	   (set-cookie! (stringout authid "EXPIRES")
			(get (timestamp+ duration) 'tick)
			auth-cookie-domain auth-cookie-path
			(if auth-cookie-expires
			    (if (number? auth-cookie-expires)
				(timestamp+ (min duration auth-cookie-expires))
				(timestamp+ duration))
			    #f)
			#f))
	 (info%watch "AUTH/IDENTIFY!" identity auth authstring)
	 identity)))

(define (auth/ok? auth)
  (and auth (> (authinfo-expires auth) (time))
       (not (blacklisted? (auth->string auth)))
       auth))

(define (auth/refresh! info (authid) (duration auth-expiration))
  (default! authid (authinfo-realm info))
  (info%watch "AUTH/REFRESH!" info authid duration)
  (if (auth/ok? info)
      (let* ((identity (authinfo-identity info))
	     (auth (cons-authinfo (authinfo-realm info) identity
				  (+ (time) duration))))
	(cgiset! authid auth)
	(set-cookie! authid (auth->string auth)
		     auth-cookie-domain auth-cookie-path
		     (if auth-cookie-expires
			 (if (number? auth-cookie-expires)
			     (timestamp+ (min duration auth-cookie-expires))
			     (timestamp+ duration))
			 #f)
		     auth-secure)
	(blacklist! (auth->string auth))
	auth)
      (begin (cgidrop! authid)
	     (expire-cookie! authid)
	     (error "Can't refresh an invalid authorization" info))))

;;; Checking authorization

(define (auth/getinfo (authid authid) (signal #f) (authinfo))
  (default! authinfo (cgiget authid))
  (debug%watch "AUTH/GETINFO" authid authinfo signal (cgiget authid))
  (cond ((fail? authinfo) (fail))
	((not authinfo)
	 (when signal (error "No authorization info" authinfo authid))
	 (fail))
	;; If the info is a string, we haven't verified it yet
	;; This is where the real validation happens
	((string? authinfo)
	 (let ((info (string->auth authinfo authid)))
	   (if info (auth/getinfo authid signal info)
	       (begin
		 (cgidrop! authid)
		 (expire-cookie! authid)
		 (when signal (error "No authorization info" authinfo authid))
		 (fail)))))
	;; Check if the info is a valid vector
	((not (authinfo? authinfo))
	 (error "Invalid authorization info" authinfo authid)
	 (expire-cookie! authid)
	 (cgidrop! authid)
	 (fail))
	((not (auth/ok? authinfo))
	 (logwarn "Expired authorization " authinfo authid)
	 (expire-cookie! authid)
	 (cgidrop! authid)
	 (fail))
	((< (- (authinfo-expires authinfo) (time))
	    (if (inexact? auth-refresh)
		(* auth-expires auth-refresh)
		auth-refresh))
	 (logdebug "Refreshing authinfo " authinfo)
	 (auth/refresh! authinfo))
	(else 
	 (logdebug "Valid authinfo " authinfo)
	 authinfo)))

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
  (cond ((string? info)
	 ;; Unpack the string to check its expiration
	 (let ((parsed (unpack-authinfo info)))
	   ;; If it hasn't expired, blacklist it
	   (unless (> (time) (elt parsed 2)) (blacklist! info))))
	((not (authinfo? info))
	 (logwarn "Can't deauthorize invalid info" info))
	;; Don't bother
	((auth/expired? info))
	(else
	 (let ((string (auth->string info)))
	   (unless (hashset-get blacklist string)
	     (blacklist! string)))))
  (expire-cookie! authid))
