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

(define auth-cookie-domain #f)
(define auth-cookie-path "/")
(varconfig! auth:domain auth-cookie-domain)
(varconfig! auth:sitepath auth-cookie-path)

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
    (%watch "GETINFO" user session expires info (cgiget expiresvar))
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

(define (set-cookies! info)
  (let ((user (get info 'user))
	(session (get info 'session))
	(expires (get info 'expires))
	(domain (or auth-cookie-domain (cgiget 'HTTP_HOST)))
	(path auth-cookie-path))
    (when (exists? session)
      (set-cookie! uservar user domain path
		 (timestamp+ (gmtimestamp 'seconds) user-expiration))
      (cgiset! uservar user)
      (set-cookie! sessionvar session domain path expires)
      (cgiset! sessionvar user)
      (set-cookie! expiresvar (get expires 'iso) domain path expires)
      (cgiset! expiresvar user))
    (unless (exists? session)
      (let ((thepast (timestamp+ (gmtimestamp 'seconds) 3600)))
	(set-cookie! sessionvar "" domain path thepast)
	(cgidrop! sessionvar user)
	(set-cookie! expiresvar "" domain path thepast)
	(cgidrop! expiresvar user)))))

(define (dorefresh user session info expires)
  (when (or (not (test info 'refresh))
	    (time-later? (gmtimestamp) (get info 'refresh)))
    (let* ((newexpiration (timestamp+ (gmtimestamp 'seconds) auth-expiration))
	   (newrefresh (timestamp+ (gmtimestamp 'seconds) auth-refresh))
	   (newsession (getnewsession user)))
      (store! info 'session newsession)
      (store! info 'expires newexpiration)
      (store! info 'refresh newexpiration)      
      (store! sessions newsession info)
      (store! sessions session #f)
      (set-cookies! info))))

(define (auth/authorize! user (oldsession #f) (info (frame-create #f)))
  (let ((session (getnewsession user))
	(now (gmtimestamp 'seconds)))
    (if oldsession (store! sessions oldsession #f))
    (store! sessions session info)
    (store! info 'user user)
    (store! info 'session session)
    (store! info 'expires (timestamp+ now auth-expiration))
    (store! info 'refresh (timestamp+ now auth-refresh))
    (%watch "AUTHORIZE" user session now oldsession info)
    (set-cookies! info)
    info))

(define (auth/deauthorize! session)
  (let ((info (get sessions session)))
    (drop! info '{session expires refresh})
    (store! info session #f)
    (set-cookies! info)
    info))

(module-export! '{auth/getinfo auth/authorize! auth/deauthorize!})

