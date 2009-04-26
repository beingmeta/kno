(in-module 'xhtml/auth)

(use-module '{fdweb texttools})
(use-module '{varconfig})

(define auth-prefix "AUTH/")
(define uservar "AUTH/USER")
(define sessionvar "AUTH/SESSION")
(define expiresvar "AUTH/EXPIRES")

(define (prefix-config var (val))
  (cond ((bound? val)
	 (set! auth-prefix val)
	 (set! uservar (string->symbol (stringout val "USER")))
	 (set! sessionvar (string->symbol (stringout val "SESSION")))
	 (set! expiresvar (string->symbol (stringout val "EXPIRES")))
	 #t)
	(else auth-prefix)))
(config-def! 'auth/prefix prefix-config)

(define user-expiration (* 3600 24 365))
(define auth-expiration 7200)
(define auth-refresh 600)
(varconfig! auth/userexpires user-expiration)
(varconfig! auth/expiration auth-expiration)
(varconfig! auth/refresh auth-refresh)

(define auth-cookie-host #f)
(define auth-cookie-path "/")

(define sessions (make-hashtable))

;;; Top level functions

(define (auth/getinfo (refresh #t))
  (let* ((user (cgiget uservar))
	 (session (cgiget sessionvar))
	 (expires (timestamp (cgiget expiresvar)))
	 (info (get sessions session)))
    (and (exists? info)
	 (test info 'user user)
	 (and (exists? expires) (test info 'expires expires))
	 (future-time? expires)
	 ;; Now we're good
	 (if update
	     (dorefresh user session info (try expires (get info 'expires)))
	     info))))

(define (getnewsession user)
  (number->string
   (let ((now (timestamp)))
     (* (+ (+ (get now 'tick) (get now 'nsecs))
	   (random 8000000))
	(1+ (random 20))))
   16))

(define (set-cookies! user session expires)
  (let ((host (or auth-cookie-host (cgiget 'HTTP_HOST)))
	(path auth-cookie-path))
    (set-cookie! uservar user host path
		 (timestamp+ (timestamp 'seconds) auth-user-expires))
    (set-cookie! sessionvar session host path expires)
    (set-cookie! expiresvar (get expires 'iso) host path expires)))

(define (dorefresh user session info expires)
  (when (time-later? expires (timestamp+ auth-refresh))
    (let* ((newexpiration (timestamp+ (timestamp 'seconds) auth-expiration))
	   (newsession (getnewsession user)))
      (store! info 'session newsession)
      (store! info 'expires expires)
      (store! sessions newsession info)
      (store! sessions session #f)
      (set-cookies! user newsession newexpiration))))

(define (auth/authorize! user (oldsession #f) (info (frame-create #f)))
  (let ((session (getnewsession user))
	(expires (timestamp+ (timestamp 'seconds) auth-expiration)))
    (if oldsession (store! sessions oldsession #f))
    (store! info 'user user)
    (store! info 'session session)
    (store! info 'expires expires)
    (set-cookies! user session expires)
    info))

(define (auth/deauthorize! session)
  (let ((info (get sessions session)))
    (drop! info '{session expires})
    (store! info session #f)
    (set-cookies! user session (timestamp+ (timestamp 'seconds) (* -24 3600)))
    info))

(module-export! '{auth/getinfo auth/authorize! auth/deauthorize!})

