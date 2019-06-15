;;; -*- Mode: Scheme; -*-

(in-module 'kno/sessions)

(use-module '{logger})

(define %loglevel %notice%)

(define-init *session* #f)

(define loaded-configs {})
(define loaded-setups {})

(define (start-session location)
  (lognotice |Session| "based in " (abspath location))
  (when (file-exists? (mkpath location "session.cfg"))
    (unless (overlaps? (abspath (mkpath location "session.cfg")) loaded-configs)
      (set+! loaded-configs (abspath (mkpath location "session.cfg")))
      (load-config (mkpath location "session.cfg"))))
  (when (file-exists? (mkpath location "setup.scm"))
    (unless (overlaps? (abspath (mkpath location "setup.scm")) loaded-setups)
      (set+! loaded-setups (abspath (mkpath location "setup.scm")))
      (load (mkpath location "setup.scm"))))
  ;;; Initialize the history
  ;;; Run the preload
  location)

(config-def! 'session
  (slambda (var (val))
    (cond ((unbound? val) *session*)
	  ((not val) (set! *session* val))
	  ((not (string? val)) (irritant val |SessionDirectoryPath|))
	  ((config 'NOSESSIONS #f) 
	   (logwarn |SessionsDisabled| "by previous config"))
	  ((and *session*
		(or (equal? val *session*)
		    (equal? (abspath val) (abspath *session*))))
	   (loginfo |SessionAlreadyLoaded| " from " *session*))
	  ((not (file-directory? val))
	   (irritant val |SessionDirectoryPath|))
	  (else (set! *session* val)
		(start-session val)))))

(unless *session*
  (cond ((file-directory? (abspath ".knoc"))
	 (config! 'session (abspath ".knoc")))
	((file-directory? (abspath "_knoc"))
	 (config! 'session (abspath "_knoc")))))


