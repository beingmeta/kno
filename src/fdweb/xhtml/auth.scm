(in-module 'xhtml/auth)

(use-module '{fdweb texttools})
(use-module '{varconfig})

(define auth-prefix "AUTH/")
(define uservar "AUTH/USER")
(define sessionvar "AUTH/SESSION")
(define expiresvar "AUTH/EXPIRES")
(define loggedin (make-hashtable))

(define (prefix-config var (val))
  (cond ((bound? val)
	 (set! auth-prefix val)
	 (set! uservar (string->symbol (stringout val "USER")))
	 (set! sessionvar (string->symbol (stringout val "SESSION")))
	 (set! expiresvar (string->symbol (stringout val "EXPIRES")))
	 #t)
	(else auth-prefix)))
(config-def! 'auth:prefix prefix-config)

(define user-expiration (* 3600 24 365))
(define auth-expiration 7200)
(define auth-refresh 600)
(varconfig! auth:userexpires user-expiration)
(varconfig! auth:expiration auth-expiration)
(varconfig! auth:refresh auth-refresh)

(define auth-cookie-host #f)
(define auth-cookie-path "/")

(define-init sessions (make-hashtable))

(define (sessions-config var (val))
  (cond ((not (bound? val)) sessions)
	((or (hashtable? val) (index? val))
	 (set! sessions val)
	 #t)
	((string? val)
	 (set! sessions (open-index val))
	 #t)))
(config-def! 'auth:sessions sessions-config)

;;; Top level functions

(define (auth/getinfo (refresh #t))
  (let* ((user (cgiget uservar))
	 (session (cgiget sessionvar))
	 (expires (gmtimestamp (cgiget expiresvar)))
	 (info (get sessions session)))
    (and (exists? info)
	 (test info 'user user)
	 (and (exists? expires)
	      (%watch (test info 'expires expires)))
	 (%watch (future-time? expires))
	 ;; Now we're good
	 (if refresh
	     (dorefresh user session info (try expires (get info 'expires)))
	     info))))

;; It would be very cool to have the session ID be an encrypted
;;  string that includes the user and expiration time (at least)
;;  and a nonce.  This might avoid having to have a persistent
;;  sessions table.
(define (getnewsession user)
  (number->string
   (let ((now (gmtimestamp 'seconds)))
     (* (+ (get now 'tick)
	   (get now 'nanoseconds)
	   (random 8000000)
	   1000000)
	(1+ (random 20))))
   16))

(define (set-cookies! user session expires)
  (let ((host (or auth-cookie-host (cgiget 'HTTP_HOST)))
	(path auth-cookie-path))
    (set-cookie! uservar user host path
		 (timestamp+ (gmtimestamp 'seconds) user-expiration))
    (cgiset! uservar user)
    (set-cookie! sessionvar session host path expires)
    (cgiset! sessionvar user)
    (set-cookie! expiresvar (get expires 'iso) host path expires)
    (cgiset! expiresvar user)))

(define (dorefresh user session info expires)
  (when (time-later? expires (timestamp+ (gmtimestamp 'seconds) auth-refresh))
    (let* ((newexpiration (timestamp+ (gmtimestamp 'seconds) auth-expiration))
	   (newsession (getnewsession user)))
      (store! info 'session newsession)
      (store! info 'expires expires)
      (store! sessions newsession info)
      (store! sessions session #f)
      (set-cookies! user newsession newexpiration))))

(define (auth/authorize! user (oldsession #f) (info (frame-create #f)))
  (let ((session (getnewsession user))
	(expires (timestamp+ (gmtimestamp 'seconds) auth-expiration)))
    (if oldsession (store! sessions oldsession #f))
    (store! sessions session info)
    (store! info 'user user)
    (store! info 'session session)
    (store! info 'expires expires)
    (set-cookies! user session expires)
    info))

(define (auth/deauthorize! session)
  (let ((info (get sessions session)))
    (drop! info '{session expires})
    (store! info session #f)
    (set-cookies! user session (timestamp+ (gmtimestamp 'seconds) (* -24 3600)))
    info))

(module-export! '{auth/getinfo auth/authorize! auth/deauthorize!})

