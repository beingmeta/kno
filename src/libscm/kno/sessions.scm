;;; -*- Mode: Scheme; -*-

(in-module 'kno/sessions)

(use-module '{logger varconfig text/stringfmts knodb optimize})
(use-module '{binio})

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
		  config.command
		  addsetup.command
		  loadmods.command
		  loadmod.command
		  optmods.command
		  optmod.command
		  optuse.command
		  usemods.command
		  usemod.command
		  use.command
		  reflect.command
		  reopt.command
		  load.command
		  appload.command
		  usedb.command
		  commit.command
		  =.command})

(define %loglevel %notice%)

(define-init *session* #f)

(define-init verbose #f)
(varconfig! sessions:verbose verbose config:boolean)
(varconfig! session:verbose verbose config:boolean)

(define-init loaded-configs {})
(define-init loaded-setups {})

(define (return-exception ex) ex)

(define ($exception ex (with-irritant #f))
  (printout (if (symbol? (exception-condition ex)) 
		(symbol->string (exception-condition ex))
		(exception-condition ex))
    (if (exception-caller ex) (printout " [" (exception-caller ex) "]"))
    (if (exception-details ex) (printout " (" (exception-details ex) ")"))
    (if (and with-irritant (exception-irritant? ex))
	(printout " " ex))))

(define (load-session location)
  (lognotice |Session| "Loaded configuration from " (abspath location))
  (when (file-exists? (mkpath location "session.cfg"))
    (unless (overlaps? (abspath (mkpath location "session.cfg")) loaded-configs)
      (set+! loaded-configs (abspath (mkpath location "session.cfg")))
      (load-config (mkpath location "session.cfg"))))
  (when (file-exists? (mkpath location "setup.scm"))
    (unless (overlaps? (abspath (mkpath location "setup.scm")) loaded-setups)
      (set+! loaded-setups (abspath (mkpath location "setup.scm")))
      (load->env (mkpath location "setup.scm") (%appenv))))
  (when (file-directory? (mkpath location "inits"))
    (let ((files (getfiles (mkpath location "inits")))
	  (console-env (req/get 'console_env))
	  (restored '()))
      (doseq (file (sorted (reject (reject files has-prefix {"#" "."})
			     has-suffix {"#" "~"})
			   file-modtime))
	(let* ((size (file-size file))
	       (modtime (file-modtime file))
	       (val (onerror (read-session-val file) return-exception))
	       (name (basename file #t))
	       (sym (string->symbol name)))
	  (cond ((exception? val)
		 (set! restored (cons [var sym error val size size time modtime file file]
				      restored)))
		(else (store! console-env sym val)
		      (set! restored (cons [var sym type (typeof val) size size time modtime file file]
					   restored))))))
      (unless (empty-list? restored)
	(if verbose
	    (lineout ";; Restored " 
	      ($count (length restored) "init") ":"
	      (doseq (restore (reverse restored) i)
		(if (test restore 'error)
		    (printout "\n;;!!  "
		      (get restore 'var) " error restoring " ($bytes (get restore 'size))
		      " from " (get restore 'file) " (saved " (get (get restore 'time) 'rfc822))
		    (printout "\n;;    "
		      (get restore 'var) " (" (get restore 'type) ") "
		      " (set " (get (get restore 'time) 'rfc822) ")"))))
 	    (lineout ";; Restored " 
	      ($count (length restored) "init") ":"
	      (doseq (restore (reverse restored) i)
		(printout 
		  (cond ((= i 0) " ")
			(else ", "))
		  (get restore 'var)
		  (if (test restore 'error)
		      (let ((ex (get restore 'error)))
			(printout " (error:" ($exception ex) ")"))
		      (printout " (" (get restore 'type) ")")))))))))
  ;;; Initialize the history
  ;;; Run the preload
  location)

(define (read-session-val file)
  (cond ((has-suffix file {".txt" ".text"})
	 (filestring file))
	((has-suffix file {".bytes" ".packet"})
	 (filedata file))
	((has-suffix file ".xtype") (read-xtype file))
	((has-suffix file ".dtype") (file->dtype file))
	((has-suffix file {".scm" ".lisp" ".lsp" ".lst"})
	 (read (open-input-file file)))
	(else (read-xtype file))))

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

(define (read-all-configs file)
  (if (file-exists? file)
      (let* ((in (open-input-file file))
	     (def (read in))
	     (defs {}))
	(while (pair? def)
	  (set+! defs def)
	  (set! def (read in)))
	defs)
      (fail)))

;;; TODO: Add session-drop-config!
(defambda (session-config! var val (exec #t) (session *session*))
  (unless (valid-session? session) (irritant session |InvalidSession|))
  (when exec (config! var val))
  (let* ((file (mkpath session "session.cfg"))
	 (configs (read-all-configs file))
	 (confdef `(,(string->symbol (downcase var)) ,val)))
    (unless (overlaps? confdef configs)
      (let ((out (extend-output-file file)))
	(printout-to out ";; Added " (gmtimestamp) " from " (config 'sessionid) "\n")
	(printout-to out (write confdef) "\n")))))

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
	  ((or (empty-string? val) (overlaps? (downcase val) {"none" "no" "non" "nei"}))
	   (logwarn |NoSession| val)
	   (set! *session* #f))
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

(config-def! 'initsession
  (slambda (var (val))
    (cond ((unbound? val) *init-session*)
	  ((and *session* (equal? val *session*)))
	  ((file-directory? val) (config! 'session val))
	  (else (mkdir val) (config! 'session val)))))

;;;; Session variables

(define (=.command pname (val (req/get 'lastval)))
  (if (not *session*)
      (logwarn |NoSession| " for binding " pname)
      (let ((env (req/get 'console_env)))
	(unless (file-directory? (mkpath *session* "inits"))
	  (mkdir (mkpath *session* "inits")))
	(let ((path (mkpath (mkpath *session* "inits") pname))
	      (problem #f))
	  (store! env (string->symbol pname) val)
	  (cond ((has-suffix pname {".text" ".txt" ".html" ".string"})
		 (if (string? val)
		     (write-file path val)
		     (set! problem '|NotAString|)))
		((has-suffix pname {".data" ".packet" ".bytes"})
		 (if (packet? val)
		     (write-file path val)
		     (set! problem '|NotAPacket|)))
		((has-suffix pname {".lisp" ".lsp" ".scm" ".pprint"})
		 (fileout path (pprint val)))
		((has-suffix pname {".lispdata" ".scmdata" ".ldata"})
		 (listdata val #f path))
		((has-suffix pname {".expr" ".exprs" ".sexprs" ".sexpr"})
		 (write val (open-output-file path)))
		((has-suffix pname ".xtype") (write-xtype val path))
		((has-suffix pname ".dtype") (dtype->file val path))
		(else (write-xtype val path)))
	  (if problem
	      (lineout ";;; Write failed of " (typeof val) " to " path " for " pname ": " 
		problem)
	      (lineout ";;; Wrote " (typeof val) " to " path " for init " pname))
	  (void)))))

;;;; Default KNOC Commands

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
(define (config.command var (val))
  (if (bound? val)
      (session-config! var val *session*)
      (config var)))
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

(define (loadmods.command . modules)
  "Add modules to the current application session"
  (let ((good-mods (get-good-mods (elts modules) #t)))
    (if (exists? good-mods)
	(if *session*
	    (session-config! 'appmods good-mods *session*)
	    (config! 'appmods good-mods))
	(logwarn |LoadMods| "No modules specified"))))
(define loadmod.command (fcn/alias loadmods.command))

(define (optmods.command . modules)
  "Optimize the specified modules for the current session"
  (let ((good-mods (get-good-mods (elts modules) #t)))
    (cond ((exists? good-mods) (optimize-module! good-mods)
	   (session-setup! `((importvar 'optimize 'optimize*) ',good-mods) *session*))
	  (else (logwarn |OptMods| "No modules specified")))))
(define optmod.command (fcn/alias optmods.command))

(define (optuse.command . modules)
  "Use (and optimize) the specified modules for the current session"
  (let ((good-mods (get-good-mods (elts modules) #t)))
    (cond ((exists? good-mods)
	   (if *session*
	       (session-config! 'appmods good-mods *session*)
	       (config! 'appmods good-mods))
	   (optimize-module! (get-module (elts modules)))
	   (session-setup! `((importvar 'optimize 'optimize!) ',good-mods) *session*))
	  (else (logwarn |OptMods| "No modules specified")))))

(define optimize-usemods #t)
(varconfig! usemods:optimize optimize-usemods config:boolean)

(define (usemods.command . modules)
  "Add modules to the current application session"
  (let ((good-mods (get-good-mods (elts modules) #t)))
    (if (exists? good-mods)
	(if *session*
	    (begin
	      (session-config! 'appmods good-mods *session*)
	      (when optimize-usemods (session-config! 'optmods good-mods *session*)))
	    (begin (config! 'appmods good-mods)
	      (when optimize-usemods (config! 'optmods good-mods))))
	(logwarn |UseMods| "No modules specified"))))
(define usemod.command (fcn/alias usemods.command))
(define use.command (fcn/alias usemods.command))

;;; Some standard modules

(define (reflect.command)
  (config! 'appmods 'reflection))

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

;;;; DB functions

(define (usedb.command spec)
  (let ((db (onerror (knodb/ref spec)
		(lambda (ex)
		  (stringout "Error using db '" spec "', "
		    (exception-summary ex))))))
    (if (string? db)
	(logwarn |UseDBFailed| db)
	(if *session*
	    (session-config! 'usedb spec *session*)
	    (config! 'usedb spec)))))

(define (commit.command . args)
  (knodb/commit!))



