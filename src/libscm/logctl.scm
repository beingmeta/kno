;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.

(in-module 'logctl)

;;; Provides module-level logging control together with the LOGGER file

(use-module 'logger)

(module-export! '{logctl! set-loglevel!
		  logdebug! loginfo! logdetail! lognotice!
		  logreset!})

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
  (if (string? id)
      (set! mod (get-module (string->lisp id)))
      (if (symbol? id)
	  (set! mod (get-module id))))
  (unless (and (environment? mod)
	       (exists? (get mod '%moduleid)))
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
    (store! mod '%loglevel (get saved-levels id))
    (record-loglevel! id (get saved-levels id))))

(define (logdeluge! id) (logctl! id %deluge%))
(define (logdetail! id) (logctl! id %detail%))
(define (logdebug! id) (logctl! id %debug%))
(define (loginfo! id) (logctl! id %info%))
(define (lognotice! id) (logctl! id %notice%))
(define (logreset! id) (logctl! id #f))

(define (logctl-config var (val))
  (if (not (bound? val)) levels
      (if (pair? val)
	  (logctl! (car val) (cdr val))
	  (if (string? val)
	      (let ((split (position #\: val)))
		(if (not position)
		    (error "Invalid LOGCTL spec" val)
		    (let ((module (get-module (string->lisp (slice val 0 split))))
			  (level (getloglevel (string->lisp (slice val (1+ split))))))
		      (logctl! module level))))))))
(config-def! 'logctl logctl-config)

(define (make-logconfigfn level)
  (lambda (var (val))
    (if (bound? val)
	(when val 
	  (logctl! (if (string? val) (string->lisp val) val)
		   (getloglevel level)))
	(car (pick levels cdr level)))))

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

