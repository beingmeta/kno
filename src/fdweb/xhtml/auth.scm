(in-module 'xhtml/auth)

(use-module '{fdweb texttools})
(use-module '{varconfig logger rulesets})

(module-export! '{auth/getinfo
		  auth/getuser
		  auth/authorize!
		  auth/deauthorize!
		  auth/validate})

;; The cookie/CGI var used to store the session ID
(define authid 'AUTH)
;; The key to use in signing session ids
(define signature #f)
;; The key to use when encrypting session ids
(define encrypt #f)
(varconfig! auth:id authid)
(varconfig! auth:signature signature)
(varconfig! auth:encrypt encrypt)

;; Various expiration intervals
(define auth-timeout 3600)
(define auth-refresh 600)
(varconfig! auth:expires auth-expiration)
(varconfig! auth:refresh auth-refresh)

;; Session functions
(define sessionfn #f)
(varconfig! auth:sessionfn sessionfn)

;; Cookie parameters
(define auth-cookie-domain #f)
(define auth-cookie-path "/")
(define auth-secure #f)
(varconfig! auth:domain auth-cookie-domain)
(varconfig! auth:sitepath auth-cookie-path)
(varconfig! auth:secure auth-secure)

;;; Here's the authorization model:
;;;  There's one cookie/cgi variable (the AUTHID) indicating authorization
;;;  Parsed authorization consists of a vector with four elements:
;;;    the AUTHID
;;;    the USER
;;;    a SESSID
;;;    an expiration timestamp
;;;    a refresh timestamp

;;; When the AUTHID is a string (how it comes from the web), it is
;;; unpacked into a parsed authorization.  This authorization process
;;; optionally decrypts and checks a signature before using it.  The parsed
;;; form is then cached
;;; An authorization is valid until its authorization timestamp
;;;  is passed and the authorization functions automatically extend
;;;  authorizations older than the refresh timestamp, so an authorization
;;;  will only be invalidated if there are no transactions between the
;;;  refresh 

;;; Top level functions

(define (auth/getinfo (authid authid) (authinfo))
  (default! authinfo (cgiget authid #f))
  (debug%watch "AUTH/GETINFO" authid authinfo)
  (cond ((not authinfo) #f)
	;; If the info is a string, we haven't verified it yet
	;; This is where the real validation happens
	((string? authinfo)
	 (auth/getinfo authid (checkinfo authinfo authid)))
	;; Check if the info is a valid vector
	((not (and (vector? authinfo)
		   (= (length authinfo) 5)
		   (eq? authid (elt authinfo 0))
		   (integer? (elt authinfo 3))
		   (integer? (elt authinfo 4))))
	 (error "Bad authorization info" authinfo authid)
	 #f)
	(else authinfo)))

(define (auth/getuser (authid authid))
  (elt (auth/getinfo authid (cgiget authid)) 2))

;;; Unpacks an authstring into a parsed representation
;;;  and updates it if neccesary
(define (checkinfo authstring authid)
  (let* ((sigsplit (position #\| authstring))
	 (payload (if sigsplit
		      (base64->packet (subseq authstring 0 sigsplit))
		      (base64->packet authstring)))
	 (packet (if encrypt (decrypt payload encrypt) payload))
	 (sigstring (and sigsplit (subseq sigsplit (1+ sigsplit))))
	 (sigok (or (and (not signature) (not sigstring))
		    (and sigstring signature
			 (equal? (base64->packet sigstring)
				 (hmac-sha1 payload signature)))))
	 (parsed (packet->dtype packet))
	 (now (time)))
    (debug%watch "CHECKINFO" authid authstring sigok parsed (time))
    (cond ((not sigok)
	   (bad-signature! parsed authstring authid))
	  ((not (and (vector? parsed) (= (length parsed) 5)
		     (eq? (elt parsed 0) authid)
		     (integer? (elt parsed 3))
		     (integer? (elt parsed 4))))
	   (bad-authinfo! parsed authstring authid))
	  (else
	   (if (> now (elt parsed 4))
	       (auth-expired! parsed authstring authid)
	       (if (> now (elt parsed 3))
		   (update-authinfo! parsed)
		   (begin (cgiset! authid parsed)
			  parsed)))))))

(define (update-authinfo! authinfo)
  (let* ((authid (elt authinfo 0))
	 (user (elt authinfo 1))
	 (cursession (elt authinfo 2))
	 (session (if sessionfn
		      (sessionfn user cursession #t)
		      user))
	 (expires (elt authinfo 3)))
    (and session
	 (let* ((now (time))
		(authvec (vector authid user session
				 (if sessionfn (+ now auth-timeout)
				     expires)
				 (+ now auth-refresh)))
		(authdata (if encrypt
			      (encrypt (dtype->packet authvec) encrypt)
			      (dtype->packet authvec)))
		(sig (and signature (hmac-sha1 authdata signature)))
		(authstring
		 (if sig (stringout (packet->base64 authdata) "|"
				    (packet->base64 sig))
		     (packet->base64 authdata))))
	   (cgiset! authid authvec)
	   (set-cookie! authid authstring
			auth-cookie-domain auth-cookie-path #f
			auth-secure)
	   authvec))))

;;;; Error procedures

(define (bad-signature! info authstring authid)
  (logwarn "Bad signature for " info " from " authid "=" authstring)
  (clear-cookie! authid)
  #f)
(define (no-signature! authstring authid)
  (logwarn "No signature in " authid "=" authstring)
  (clear-cookie! authid)
  #f)
(define (bad-authinfo! info authstring authid)
  (logwarn "Malformed authinfo " info " from " authid "=" authstring)
  (clear-cookie! authid)
  #f)
(define (auth-expired! info authstring authid)
  (loginfo "Expired authinfo " info " from " authid "=" authstring)
  (clear-cookie! authid)
  #f)

(define (clear-cookie! id)
  (set-cookie! id "expired"
	       auth-cookie-domain auth-cookie-path
	       (timestamp+ (* 3600 -24))))

;;;; Authorize/deauthorize API

(define (auth/authorize! user (authid authid))
  (let ((now (time)))
    (update-authinfo! (vector authid user #f
			      (+ now auth-timeout)
			      (+ now auth-refresh)))))

(define (auth/deauthorize! (authid authid))
  (let ((info (if (vector? authid) authid (cgiget authid))))
    (when (vector? info)
      (when sessionfn (sessionfn (elt info 1) (elt info 2) #f))
      (cgidrop! (first info))
      (set-cookie! (first info) "expired"
		   auth-cookie-domain auth-cookie-path
		   (timestamp+ (* 3600 -24))
		   auth-secure))
    (when (string? info)
      (set-cookie! authid "expired"
		   auth-cookie-domain auth-cookie-path
		   (timestamp+ (* 3600 -24))
		   auth-secure))
    #f))

;;; Simple sessions

;;; These just use a random number as a sessionid and keeps a table
;;;  of the valid session for each user.

(define sessions-file #f)
(define sessions #f)

(define (auth/ezsession user session valid)
  (if valid
      (if session
	  (and (test sessions user session) session)
	  (let ((session (random 6000000)))
	    (store! sessions user session)
	    (when session-file (save-ezsessions))
	    session))
      (drop! sessions user)))

(defslambda (save-ezsessions)
  (when session-file (dtype->file sessions session-file)))

(config-def! 'auth:ezsessions
	     (lambda (var (val))
	       (if (bound? val)
		   (cond ((string? val)
			  (set! sessions
				(if (file-exists? val)
				    (file->dtype val)
				    (make-hashtable)))
			  (set! sessions-file val)
			  (set! sessionfn auth/ezsession))
			 ((not val)
			  (set! sessions #f)
			  (set! sessions-file #f)
			  (set! sessionfn #f))
			 (else
			  (set! sessions val)
			  (set! sessionfn auth/ezsession)))
		   sessions)))

;;; Validation

;; These are methods which handle respones from various authorization
;;  sites (OpenID, Facebook, etc)
;; When a validator (a thunk) returns non-empty/non-false, it
;;  is taken to indicate that the user has been authorized.
(define validators '())
;; This takes the results of validation and makes a user
;;  who can then be authorized
(define getuser #f)
(varconfig! auth:getuser getuser)

(define (auth/validate (authid authid))
  (debug%watch "AUTH/VALIDATE" authid validators)
  (or (auth/getinfo authid)
      (do ((scan validators (cdr scan))
	   (user #f))
	  ((or user (null? scan))
	   (when user
	     (if getuser
		 (auth/authorize! (getuser user) authid)
		 (auth/authorize! user authid)))
	   user)
	(set! user (try (if (pair? (car scan))
			    ((cdr (car scan)))
			    ((car scan)))
			#f))
	(message "Validating " (car scan) " yielded " user))))

(ruleconfig! auth:validate validators)

;;; Logging in

(define (auth/login uri (authid authid))
  (or (auth/getinfo authid)
      (let ((valid (auth/valiate authid)))
	(cond ((and valid (cgitest 'next))
	       (cgiset! 'status 303)
	       (httpheader "Location: " (cgiget 'next))
	       #f)
	      (else valid)))))


