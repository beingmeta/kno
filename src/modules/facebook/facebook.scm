(in-module 'facebook)

(define version "$Id$")

(use-module '{fdweb xhtml texttools reflection facebook/fbcall varconfig})

(module-export! '{appname appid apikey apisecretkey})
(module-export! '{fb/incanvas? fb/added? fb/getuser fb/authorize})

(define halfhour (* 60 30))
(define oneday (* 60 60 24))
(define fiveminutes (* 60 5))

(define (timeplus interval) (timestamp+ (gmtimestamp 'seconds) interval))

(define facebook-cookies
  '{"fb_sig_session_key"
    "fb_sig_session_expires"
    "fb_sig_user"
    "fb_sig_added"
    fb_sig_session_key
    fb_sig_session_expires
    fb_sig_user
    fb_sig_added})

(define applock #f)
(define appname #f)
(define apphost #f)
(define approot #f)
(define appid #f)
(define apikey #f)
(define apisecretkey #f)

(config-def! 'fb:appinfo
	     (lambda (var (val))
	       (if (bound? val)
		   (if applock
		       (error "Facebook appinfo is locked")
		       (if (and (vector? val) (= (length val) 4))
			   (begin (set! appname (first val))
				  (set! appid (second val))
				  (set! apikey (third val))
				  (set! apisecretkey (fourth val)))
			   (error "Invalid Facebook application info")))
		   (vector appname appid apikey))))

(config-def! 'fb:applock
	     (lambda (var (val))
	       (if (bound? val)
		   (if applock
		       (error "Facebook appinfo is locked")
		       (set! applock val))
		   appname)))
(config-def! 'fb:appname
	     (lambda (var (val))
	       (if (bound? val)
		   (if applock
		       (error "Facebook appinfo is locked")
		       (set! appname val))
		   appname)))
(config-def! 'fb:appid
	     (lambda (var (val))
	       (if (bound? val)
		   (if applock
		       (error "Facebook appinfo is locked")
		       (set! appid val))
		   appid)))
(config-def! 'fb:key
	     (lambda (var (val))
	       (if (bound? val)
		   (if applock
		       (error "Facebook appinfo is locked")
		       (set! apikey val))
		   apikey)))
(config-def! 'fb:secret
	     (lambda (var (val))
	       (if (bound? val)
		   (if applock
		       (error "Facebook appinfo is locked")
		       (set! apisecretkey val))
		   apisecretkey)))
(varconfig! fb:host apphost)
(varconfig! fb:approot approot)

;;;; Doing stuff

(define (fb/incanvas?)
  (and (cgitest 'fb_sig_in_canvas)
       (cgiget 'fb_sig_in_canvas)
       (not (cgitest 'fb_sig_in_canvas 0))))

(define (fb/added?)
  (and (cgitest 'fb_sig_added)
       (cgiget 'fb_sig_added)
       (not (cgitest 'fb_sig_added 0))))

(define (fb/getuser)
  (let ((session (cgiget 'fb_sig_session_key)))
    (try (get fb/sessions->users (cgiget 'fb_sig_session_key))
	 (let ((user (fbcall "users.getLoggedInUser")))
	   (store! fb/sessions->users session user)
	   user))))

;;; Authorization body

(define authorize-body #f)

(define (emit-authorize-body)
  (when (procedure? authorize-body)
    (authorize-body))
  (unless (procedure? authorize-body)
    (body! 'id "AUTHORIZE")
    (h1 "Authorizing")
    (p "Sorry, we need to authorize you with Facebook.  "
       "Please wait while we authorize your account, "
       "which may require logging into facebook.")))


;;;; Authorization stuff

(define-init fb/sessions->users (make-hashtable))
(define-init fb/closed-sessions (make-hashset))

(define (handleauthtoken (fb_sig_session_key #f)
			 (fb_sig_session_expires #f)
			 (fb_sig_in_canvas #f)
			 (fb_sig_added #f)
			 (fb_sig_user #f)
			 (pptimestamp #f)
			 (path_info #f)
			 (auth_token #f)
			 (popup #f))
  (let* ((info (fbcall/open "auth.getSession" "auth_token" auth_token))
	 (session (get info "session_key"))
	 (expires (get info "expires"))
	 (user (get info "uid"))
	 (added #f)
	 (extstamp (timestamp (if (string? expires)
				  (string->lisp expires) expires))))
    (cgiset! 'fb_sig_session_key session)
    ;; Set it for this site
    (set-cookie! 'fb_sig_session_key session
		 (or apphost (cgiget 'host_name)) "/" extstamp)
	   
    (set! added (fbcall "users.isAppAdded"))

    (%watch session user info added)

    (cgiset! 'fb_sig_session_expires expires)
    ;; Set it for this site
    (set-cookie! 'fb_sig_session_expires expires
		 (or apphost (cgiget 'host_name)) "/" extstamp)
	   
    (cgiset! 'fb_sig_user user)
    ;; Set it for this site
    (set-cookie! 'fb_sig_user user
		 (or apphost (cgiget 'host_name)) "/" extstamp)

    (cgiset! fb_sig_added added)
    ;; Set it for this site
    (set-cookie! 'fb_sig_added added
		 (or apphost (cgiget 'host_name)) "/" extstamp)

    (cgiset! 'status 303)

    (httpheader "Location: ""http:" "//"
		(or apphost (cgiget 'host_name))
		(or (cgiget 'script_name) approot)
		(or path_info "")
		(if (and (cgitest 'query_string)
			 (not (cgitest 'query_string "")))
		    "?" "")
		(if (cgitest 'query_string)
		    (textsubst (cgiget 'query_string)
			       #("auth_token=" (not> {"&" (eol)}))
			       "")))))

(define (doauthorize  (fb_sig_session_key #f)
		      (fb_sig_session_expires #f)
		      (fb_sig_in_canvas #f)
		      (fb_sig_added #f)
		      (fb_sig_user #f)
		      (pptimestamp #f)
		      (path_info #f)
		      (auth_token #f)
		      (popup #f))
  ;; Reset the cookies (fb/logout) to avoid ambiguous values
  (when fb_sig_session_key (fb/logout))
  (cgiset! 'status 303)
  (httpheader
   "Location: https://www.facebook.com/login.php?"
   (if popup "popup=yes&" "")
   "v=1.0&" "api_key=" (config 'fb:key) "&"
   "next=" (uriencode (stringout (or path_info "")
			(if (cgitest 'query_string) "?")
			(or (cgiget 'query_string) ""))))
  (emit-authorize-body)
  #f)

; (define (doaddapp (path_info #f) (query_string #f) (popup #f))
;   (cgiset! 'status 303)
;   (httpheader
;    "Location: https://www.facebook.com/add.php?"
;    (if popup "popup=yes&" "")
;    "v=1.0&" "api_key=" (config 'fb:key) "&"
;    "next=" (uriencode (stringout (or path_info "")
; 			(if query_string "?")
; 			(or query_string "")))))

;;;; FB/AUTHORIZE: External entry point

(define (fb/authorize)
  ;; There are three cases:
  ;;  1. we have an auth_token after Facebook logs us in
  ;;     In this case, we get a session id and set duplicate cookies
  ;;     for the current site.
  ;;  2. we don't have a session or we have an expired session
  ;;     In this case, we clear whatever state we have and redirect
  ;;     to the Facebook login page
  ;;  3. we have a valid session
  ;;     We just return #t after setting USER

  (if (cgitest 'auth_token)
      (begin (cgicall handleauthtoken) #f)
      (let ((session (cgiget 'fb_sig_session_key))
	    (expires (cgiget 'fb_sig_session_expires)))
	(if (or (not session) (hashset-get fb/closed-sessions session)
		(and session expires (> (time) expires)))
	    (begin (cgicall doauthorize) #f)
	    (or (cgitest 'user) 
		(begin (onerror (cgiset! 'user (fb/getuser))
				(lambda (ex)
				  (cgicall doauthorize)
				  #f))))))))

(define (fb/logout)
  (do-choices (key facebook-cookies)
    (set-cookie! key "expired"
		 (or apphost (cgiget 'host_name)) "/"
		 (timeplus (- oneday)))
    (cgidrop! key)))

(module-export! '{fb/authorize fb/logout fb/sessions->users})

