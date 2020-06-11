;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.

(in-module 'logctl)

;;; Provides module-level logging control together with the LOGGER file

(use-module '{logger reflection})

(module-export! '{logctl! set-loglevel!
		  log/debug! log/info! log/detail! log/notice!
		  log/reset!})

(define-init saved-levels {})
(define-init levels {})

(define-init %loglevel %notice%)

(define (convert-log-arg arg)
  (if (and (pair? arg) (eq? (car arg) 'quote) (pair? (cdr arg))
	      (symbol? (cadr arg)))
      (getloglevel (cadr arg))
      (getloglevel arg)))

(define (record-loglevel! modname level)
  (let ((entry (pick levels car modname)))
    (if (exists? entry)
	(set-cdr! entry level)
	(set+! levels (cons modname level)))))
(define (save-loglevel! modname level)
  (let ((entry (pick saved-levels car modname)))
    (unless (exists? entry)
      (set+! saved-levels (cons modname level)))))


(define (logctl! id (level #f) (mod #f))
  (cond ((module? id) (set! mod id))
	((symbol? id) (set! mod (get-module id)))
	((string? id)
	 (set! mod (or (get-module id) (get-module (string->lisp id))))))
  (unless (module? mod)
    (error "No module from " id ": " mod))
  (if level
      (logwarn LOGCTL "Setting loglevel of " id " to " level)
      (logwarn LOGCTL "Resetting loglevel of " id " to "
	       (get saved-levels id)))
  (set! id (try (pick (get mod '%moduleid) symbol?)
		(get mod '%moduleid)))
  (when level
    (save-loglevel! id (get mod '%loglevel))
    (store! mod '%loglevel level)
    (record-loglevel! id level))
  (unless level
    (let ((restore-level (get saved-levels id)))
      (when (exists? restore-level)
	(store! mod '%loglevel restore-level)
	(record-loglevel! id restore-level)))))

(define (log/deluge! id) (logctl! id %deluge%))
(define (log/detail! id) (logctl! id %detail%))
(define (log/debug! id) (logctl! id %debug%))
(define (log/info! id) (logctl! id %info%))
(define (log/notice! id) (logctl! id %notice%))
(define (log/reset! id) (logctl! id #f))

(define (logctl-config var (val))
  (cond ((not (bound? val)) levels)
	((pair? val) (logctl! (car val) (cdr val)))
	((string? val)
	 (let ((split (position #\: val)))
	   (if (not position)
	       (error "Invalid LOGCTL spec" val)
	       (let ((module (or (get-module (slice val 0 split))
				 (get-module (string->lisp (slice val 0 split)))))
		     (level (getloglevel (string->lisp (slice val (1+ split))))))
		 (logctl! module level)))))
	(else (irritant val "Invalid specification"))))
(config-def! 'logctl logctl-config)

(define (make-logconfigfn level)
  (lambda (var (val))
    (cond ((not (bound? val))
	   (car (pick levels cdr level)))
	  ((not val)
	   (let* ((llevel (getloglevel level))
		  (matches (pick levels cdr llevel)))
	     (do-choices (restore-mod (car matches))
	       (let ((mod (get-module restore-mod)))
		 (if mod
		     (logctl! mod #f)
		     (logerr |BadModuleToRestore| restore-mod))))
	     (sset! levels (difference saved-levels matches))))
	  (val (logctl! val (getloglevel level)))
	  (else (irritant val "Bad module argument")))))

(config-def! 'logdeluge (make-logconfigfn %deluge%))
(config-def! 'logdetail (make-logconfigfn %detail%))
(config-def! 'logdebug (make-logconfigfn %debug%))
(config-def! 'loginfo (make-logconfigfn %info%))
(config-def! 'lognotice (make-logconfigfn %notice%))
(config-def! 'logwarn (make-logconfigfn %warn%))
(config-def! 'logerr (make-logconfigfn %error%))
(config-def! 'logcrit (make-logconfigfn %critical%))
(config-def! 'logalert (make-logconfigfn %alert%))
(config-def! 'logpanic (make-logconfigfn %panic%))

