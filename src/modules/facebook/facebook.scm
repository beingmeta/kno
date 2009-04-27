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
(define info-cookie 'fbinfo)

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
(varconfig! fb:infocookie info-cookie)

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

(define (unpack-fbinfo)
  (let ((info (cgiget info-cookie)))
    (and (exists? info) (string? info)
	 (let* ((break1 (position #\; info))
		(break2 (and break1 (position #\; info (1+ break1)))))
	   (and break1 break2
		(let ((id (string->number (subseq info 0 break1)))
		      (expires (string->number (subseq info (1+ break1) break2)))
		      (session (subseq info (1+ break2))))
		  (cgiset! 'fb_sig_user id)
		  (cgiset! 'fb_sig_session_expires expires)
		  (cgiset! 'fb_sig_session_key session)
		  #t))))))

(define (save-fbinfo! (cookie #t))
  (let* ((id (cgiget 'fb_sig_user))
	 (session (cgiget 'fb_sig_session_key))
	 (expires (timestamp (cgiget 'fb_sig_session_expires)))
	 (info (stringout id ";" (get expires 'tick) ";" session))
	 (domain (or apphost (cgiget 'http_host))))
    (when cookie (set-cookie! info-cookie info domain "/" expires))
    info))

(define (cgipass! var value (force #t))
  (if force (cgiset! var value)
      (unless (cgitest var) (cgiset! var value))))

(define (get-next-uri)
  (try (cgiget 'next_uri)
       (stringout (cgiget 'request_uri "/")
		  (when (and (cgitest 'query_string)
			     (not (cgitest 'query_string "")))
		    (printout "?" (cgiget 'query_string))))))

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
	 (domain (or apphost (cgiget 'http_host)))
	 (extstamp
	  (gmtimestamp (if (string? expires)
			   (string->lisp expires) expires))))

    (%watch "HANDLEAUTHTOKEN" auth_token info session expires user)

    (cgiset! 'fb_sig_session_key session)
    (cgiset! 'fb_sig_session_expires expires)
    (cgiset! 'fb_sig_user user)

    (save-fbinfo!)

    (cgiset! 'status 303)
    
    (message (threadget 'cgidata))
    

    (httpheader "Location: "
		"http://"
		(cgiget 'HTTP_HOST)
		(let ((stripped
		       (textsubst (cgiget 'REQUEST_URI)
				  '(SUBST (GREEDY #("auth_token="
						    (not> {"&" (eol)})
						    {"&" (eol)}))
					  ""))))
		  (stringout (unless (has-prefix stripped "/") "/")
			     (if (has-suffix stripped "?")
				 (subseq stripped 0 -1)
				 stripped))))

    user))

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
   "next=" (uriencode (cgiget 'REQUEST_URI "")))
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

(define (fb/authorize (next #f) (dialog (not (fb/incanvas?))))
  ;; There are three cases:
  ;;  1. we have an auth_token after Facebook logs us in
  ;;     In this case, we get a session id and set duplicate cookies
  ;;     for the current site.
  ;;  2. we don't have a session or we have an expired session
  ;;     In this case, we clear whatever state we have and redirect
  ;;     to the Facebook login page
  ;;  3. we have a valid session
  ;;     We just return #t after setting USER

  (cond ((fb/incanvas?) #t) 
	((cgitest 'auth_token)
	 (%watch "AUTH_TOKEN" (cgiget 'auth_token))
	 (when next (cgipass! 'next_uri next))
	 (when dialog (cgipass! 'dialog #t))
	 (cgicall handleauthtoken))
	((or (cgitest 'fb_sig_session_key) (unpack-fbinfo))
	 (%watch "HAVEKEY" (cgiget 'fb_sig_session_key))
	 (when next (cgipass! 'next next))
	 (when dialog (cgipass! 'dialog #t))
	 (let ((session (cgiget 'fb_sig_session_key))
	       (expires (cgiget 'fb_sig_session_expires)))
	   (if (or (not session) (hashset-get fb/closed-sessions session)
		   (and session expires (> (time) expires)))
	       (begin (cgicall doauthorize) #f)
	       (if (cgitest 'fb/user)
		   (cgiget 'fb/user)
		   (begin (onerror (begin (cgiset! 'fb/user (fb/getuser))
					  (fb/getuser))
				   (lambda (ex)
				     (cgicall doauthorize)
				     #f)))))))
	(else (cgicall doauthorize) #f)))

(define (fb/logout)
  (unless (fb/incanvas?)
    (do-choices (key facebook-cookies)
      (set-cookie! key "expired"
		   (or apphost (cgiget 'http_host)) "/"
		   (timeplus (- oneday)))
      (cgidrop! key))))

(module-export! '{fb/authorize fb/logout fb/sessions->users})

