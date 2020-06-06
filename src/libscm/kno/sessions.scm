;;; -*- Mode: Scheme; -*-

(in-module 'kno/sessions)

(use-module '{logger varconfig optimize})

(module-export! '{*session*
		  valid-session?
		  set-session!
		  load-session
		  create-session!
		  session-config!
		  session-setup!
		  session-load!
		  session.command
		  newsession.command
		  mysession.command
		  addconfig.command
		  addsetup.command
		  usemods.command
		  optmods.command
		  optuse.command
		  reopt.command
		  load.command
		  appload.command})

(define %loglevel %notice%)

(define-init *session* #f)

(define-init loaded-configs {})
(define-init loaded-setups {})

(define (load-session location)
  (lognotice |Session| "based in " (abspath location))
  (when (file-exists? (mkpath location "session.cfg"))
    (unless (overlaps? (abspath (mkpath location "session.cfg")) loaded-configs)
      (set+! loaded-configs (abspath (mkpath location "session.cfg")))
      (load-config (mkpath location "session.cfg"))))
  (when (file-exists? (mkpath location "setup.scm"))
    (unless (overlaps? (abspath (mkpath location "setup.scm")) loaded-setups)
      (set+! loaded-setups (abspath (mkpath location "setup.scm")))
      (load->env (mkpath location "setup.scm") (%appenv))))
  ;;; Initialize the history
  ;;; Run the preload
  location)

(define (set-session! location (session))
  (set! session (find-session location))
  (cond ((and *session* (equal? (realpath *session*) (realpath session)))
	 (lognotice |AlreadyCurrent| 
	   "The session " location " is already active"
	   (if (equal? location *session*)
	       (unless (equal? (realpath location) (realpath session))
		 (printout " in " (realpath session)))
	       (printout " from " *session*)))
	 #f)
	(*session*
	 (logwarn |ExistingSession|
	   "Overlaying " location " on the existing session " *session*)
	 (load-session session)
	 (set! *session* session))
	(else
	 (load-session session)
	 (set! *session* session))))

(define (create-session! location (activate #f))
  (if (file-exists? location)
      (cond ((and (file-directory? (realpath location)) (file-writable? (realpath location)))) 
	    ((file-directory? (realpath location))
	     (irritant location |BadSessionLocation| 
	       "The session location " (write location) " already exists and is not a directory"))
	    (else
	     (irritant location |ReadOnlySessionLocation| 
	       "The session location " (write location) " is read-only")))
      (unless (onerror (mkdir location) #f)
	(irritant location |SessionDirectoryFailed|
	    "Couldn't create the directory " (write location))))
  (unless (file-exists? (mkpath location "session.cfg"))
    (fileout (mkpath location "session.cfg")
      (lineout ";;; -*- Mode: Scheme; Content-encoding: utf-8; -*-")
      (lineout (write `(SESSION_LOCATION   ,location)))
      (lineout (write `(SESSION_TIMESTAMP  ,(timestamp))))
      (lineout (write `(SESSION_CREATED    ,(config 'sessionid))))))
  (unless (file-exists? (mkpath location "setup.sm"))
    (fileout (mkpath location "setup-scm")
      (lineout ";;; -*- Mode: Scheme; Content-encoding: utf-8; -*-")
      (printout "\n\n\n")))
  (when activate (set-session! location)))

(define (valid-session? location)
  (and (string? location)
       (file-directory? (realpath location))
       (file-exists? (mkpath location "session.cfg"))))

(define (session-path? path (session *session*) (dir (getcwd)))
  (and session
	   (has-prefix dir (abspath (dirname session)))
	   (has-prefix (abspath path) (abspath (dirname *session*)))))

(define (session-path path (session *session*) (dir (getcwd)))
  (if (and session
	   (has-prefix dir (abspath (dirname session)))
	   (has-prefix (abspath path) (abspath (dirname *session*))))
      path
      (abspath path dir)))

;;;; Modifying the session programmatically

(defambda (session-config! var val (exec #t) (session *session*))
  (unless (valid-session? session) (irritant session |InvalidSession|))
  (when exec (config! var val))
  (let ((out (extend-output-file (mkpath session "session.cfg"))))
    (printout-to out ";; Added " (gmtimestamp) " from " (config 'sessionid) "\n")
    (printout-to out 
      (write `(,var ,val))
      "\n")))

(define (session-setup! expr (session *session*))
  (unless (valid-session? session) (irritant session |InvalidSession|))
  (let ((out (extend-output-file (mkpath session "setup.scm"))))
    (printout-to out ";; Added " (gmtimestamp) " from " (config 'sessionid) "\n")
    (printout-to out (pprint expr) "\n")))

;;;; Configuring session

(config-def! 'session
  (slambda (var (val))
    (cond ((unbound? val) *session*)
	  ((not val) (set! *session* val))
	  ((not (string? val)) (irritant val |SessionDirectoryPath|))
	  ((not (file-directory? val))
	   (irritant val |SessionDirectoryPath|))
	  ((config 'NOSESSIONS #f) 
	   (logwarn |SessionsDisabled| "by previous config"))
	  ((and *session*
		(or (equal? val *session*)
		    (equal? (abspath val) (abspath *session*))))
	   (loginfo |SessionAlreadyLoaded| " from " *session*))
	  (else (set! *session* val)
		(load-session val)))))


;;;; KNOC Commands

(define (find-session arg (logfail #f))
  (cond ((or (not arg) (empty-string? arg)) #f)
	((file-exists? (mkpath arg "session.cfg")) 
	 (dirname (abspath (mkpath arg "session.cfg"))))
	((file-exists? (mkpath (mkpath ".kno_sessions" arg) "session.cfg"))
	 (dirname (abspath (mkpath (mkpath ".sessions" arg) "session.cfg"))))
	((file-exists? (mkpath (mkpath "~/.kno_sessions" arg) "session.cfg"))
	 (dirname (abspath (mkpath (mkpath "~/.kno_sessions" arg) "session.cfg"))))
	(logfail (logerr |UnknownSession|
		   "Couldn't find session for " (write arg) ", tried:"
		   "\n\t" (write (abspath arg))
		   "\n\t" (write (abspath (mkpath ".kno_sessions" arg)))
		   "\n\t" (write (abspath (mkpath "~/.kno_sessions" arg))))
		 #f)
	(logfail #f)))

(define (session.command arg)
  (let ((session (find-session arg #t)))
    (when session (load-session session))))

(define (newsession.command (arg #f))
  (let ((session (and arg (find-session arg #t))))
    (cond (session
	   (logwarn |ExistingSession| session)
	   (load-session session))
	  ((or (not arg) (empty-string? arg))
	   (create-session! (abspath ".knoc") #t))
	  (else 
	   (unless (file-directory? ".kno_sessions") (mkdir ".kno_sessions"))
	   (unless (file-directory? (mkpath ".kno_sessions" (glom arg)))
	     (mkdir (mkpath ".kno_sessions" (glom arg))))
	   (create-session! (abspath (mkpath ".kno_sessions" (glom arg))) #t)))))

(define (mysession.command (arg "default"))
  (unless (file-directory? (realpath "~/.kno_sessions"))
    (mkdir "~/.kno_sessions"))
  (if (file-exists? (mkpath (mkpath "~/.kno_sessions" arg) "session.cfg"))
      (load-session (mkpath (mkpath "~/.kno_sessions" arg) "session.cfg"))
      (create-session! (mkpath "~/.kno_sessions" arg) #t)))

(define (addconfig.command var val)
  (session-config! var val *session*))
(define (addsetup.command expr)
  (session-setup! expr *session*))

(define (session-load! file)
  (if (file-exists? file)
      (session-setup! `(load ,(session-path file)) *session*)
      (irritant file |NoSuchFile|)))

(defambda (get-good-mods modnames (warn #f))
  (filter-choices (name {(picksyms modnames)
			 (string->symbol (pickstrings modnames))})
    (or (get-module name)
	(begin (when warn (logwarn |UnknownModule| name)) #f))))

(define (usemods.command . modules)
  "Add modules to the current application session"
  (let ((good-mods (get-good-mods (elts modules) #t)))
    (if (exists? good-mods)
	(session-config! 'appmods good-mods *session*)
	(logwarn |UseMods| "No modules specified"))))
(define usemod.command usemods.command)
(define (optmods.command . modules)
  "Optimize the specified modules for the current session"
  (let ((good-mods (get-good-mods (elts modules) #t)))
    (cond ((exists? good-mods) (optimize-module! good-mods)
	   (session-setup! `((importvar 'optimize 'optimize!) ',good-mods) *session*))
	  (else (logwarn |OptMods| "No modules specified")))))
(define optmod.command optmods.command)
(define (optuse.command . modules)
  "Use (and optimize) the specified modules for the current session"
  (let ((good-mods (get-good-mods (elts modules) #t)))
    (cond ((exists? good-mods)
	   (session-config! 'appmods good-mods *session*)
	   (optimize-module! (get-module (elts modules)))
	   (session-setup! `((importvar 'optimize 'optimize!) ',good-mods) *session*))
	  (else (logwarn |OptMods| "No modules specified")))))

;;; Other commands

(define-init *reopt-arg* #f)

(define (reopt.command (module *reopt-arg*) . more)
  (if module
      (let ((good-mods (get-good-mods {module (elts more)} #t)))
	(cond ((exists? good-mods)
	       (do-choices (module good-mods)
		 (reload-module module)
		 (optimize-module! module))
	       (set! *reopt-arg* good-mods))
	      (else (logwarn |OptMods| "No modules specified"))))
      (logwarn |ReOpt| "Reload and optimize modules")))

(define load.command
  (macro expr `(load ,(get-arg expr 1))))

(define (appload.command file (force #f))
  "Add modules to the current application session"
  (if (file-exists? file)
      (if (position (session-path file) (config 'appload))
	  (if force
	      (logwarn |Reloading| (write file))
	      (logwarn |AlreadyLoaded| "Skipping reload of " (write file)))
	  (session-config! 'appload (session-path file)))
      (logwarn |MissingFile| "The file " (write file) " does not exist")))

