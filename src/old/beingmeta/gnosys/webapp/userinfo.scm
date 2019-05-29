(in-module 'gnosys/webapp/userinfo)

(use-module '{fdweb xhtml})

(module-export! '{requireuser getuser login-page userindex})

(define userinfo #f)
(define userpool #f)
(define userindex #f)

(define register
  (slambda (email password realname)
    (try (?? 'email email 'password password 'type 'user)
	 (let* ((f (frame-create userpool
		     '%id email 'type 'user
		     'email email 'password password
		     'displayname realname)))
	   (index-frame userindex f '{email password})
	   (commit userpool) (commit userindex)
	   f))))

(define (getuser (emailaddr #f))
  (if emailaddr
      (find-frames userindex 'email emailaddr)
      (cgiget 'user)))

(define (requireuser (emailaddr #f))
  (cond (emailaddr (find-frames userindex 'email emailaddr))
	((exists? (cgiget 'user)) (cgiget 'user))
	((exists? (cgiget 'email))
	 (let* ((email (or emailaddr (cgiget 'email)))
		(password (cgiget 'password))
		(realname (cgiget 'realname))
		(user (find-frames userindex 'email email)))
	   (cond ((and (fail? user) (exists? email) (exists? password)
		       (cgitest 'action "Register"))
		  (cond ((not (equal? email (cgiget 'cemail)))
			 (login-page "Email addresses don't agree"))
			((not (equal? password (cgiget 'cpassword)))
			 (login-page "Passwords don't agree"))
			(else (let ((user (register email password (qc realname))))
				(cgiset! 'user user)
				user))))
		 ((fail? user)
		  (no-such-user email))
		 ((test user 'password password)
		  (cgiset! 'user user)
		  (if (cgitest 'rememberme)
		      (set-cookie! 'user user #f #f
				   (timestamp+ (* 3600 24 365)))
		      (set-cookie! 'user user))
		  user)
		 (else (wrong-password email)))))
	(else (login-page))))

(define (login-page (email-msg #f) (password-msg #f))
  ;; This should be more generic
  (title! (if (exists? (config 'appname))
	      (stringout (config 'appname) " Login")
	      "Login"))
  (body! 'id "LOGIN")
  ;; This will make pop ups pop up.
  (htmlheader (xmlblock SCRIPT () "if (window.focus) window.focus();"))
  (div ((id "login_title"))
    (let ((image (if (cgitest 'device 'handheld)
		     (try (config 'apptinylogo)
			  (config 'appsmalllogo))
		     (config 'appsmalllogo)))
	  (name (config 'appname)))
      (if (exists? image)
	  (img src image border 0 alt (try name "Application"))
	  (try name "Application")))
    (br) "Login")
  (form (action (cgiget 'script_name))
    (table* ((class "login_form") (align "CENTER") (cellpadding "0"))
      (when (exists? (cgiget 'url))
	(input type "HIDDEN" name "URL" value (cgiget 'url)))
      (when (exists? (cgiget 'title))
	(input type "HIDDEN" name "TITLE" value (cgiget 'title)))
      (when email-msg
	(tr (th) (td* ((class "message")) email-msg)))
      (tr (th "Email")
	  (td (input type "TEXT" name "EMAIL" value "")))
      (when password-msg
	(tr (th) (td* ((class "message")) password-msg)))
      (tr (th "Password")
	  (td (input type "PASSWORD" name "PASSWORD" value "")))
      (tr (th (input type "CHECKBOX" name "REMEMBERME" value "YES"))
	  (td* ((class "smallnarrative"))
	    " remember me on this computer."))
      (tr (th (input type "SUBMIT" name "ACTION" value "Login"))
	  (td* ((class "narrative"))
	    " or register below."))
      (div ((style "background-color: lightgray;"))
	(tr* ((class "register"))
	  (th* ((style "font-size: 150%;")) "REGISTER")
	  (td* ((class "narrative"))
	    "by confirming the email and password above."))
	(tr* ((class "register"))
	  (th "Real Name")
	  (td (input type "TEXT" name "REALNAME" value "")))
	(tr* ((class "register"))
	  (th "Email" (div ((class "detail")) "(confirm)"))
	  (td (input type "TEXT" name "CEMAIL" value "")))
	(tr* ((class "register"))
	  (th "Password" (div ((class "detail")) "(confirm)"))
	  ;; This weirdness below seems to keep the Firefox password manager
	  ;; from always thinking that you are changing the password.
	  (td (input type "TEXT" name "CPASSWORD" value ""
		     onfocus "this.type='PASSWORD'")))
	(tr* ((class "register"))
	  (th)
	  (td* ((style "font-weight: bold;"))
	    (input type "SUBMIT" name "ACTION" value "Register"))))))
  (fail))

(define (no-such-user email)
  (login-page
   (stringout "Unknown email address `" email "', please `Register'")))
(define (wrong-password email)
  (login-page #f (stringout "Wrong password for '" email "'.")))

;;; Configuring the user database

(define userinfo-config
  (slambda (var (val unbound))
    (cond ((eq? val 'unbound) userinfo)
	  ((equal? val userinfo) #f)
	  (else
	   (when userinfo
	     (message "Warning: reconfiguring user info source"))
	   (set! userinfo val)
	   (set! userpool (use-pool val))
	   (set! userindex (open-index val))))))
(config-def! 'userinfo userinfo-config)
