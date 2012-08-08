;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2012 beingmeta, inc.  All rights reserved.

(in-module 'logger)

;;; Provides lightweight logging functions with levels, module-level
;;; control, etc
(define version "$Id$")
(define revision "$Revision$")

(module-export!
 '{logger
   getloglevel %loglevel
   logdetail loginfo lognotice logwarn logdebug
   %debug %detail})
(module-export!
 '{%emergency! %emergency%
   %alert! %alert%
   %critical! %danger! %critical% %danger%
   %error! %err! %error% %err%
   %warning! %warn! %warning% %warn%
   %notice! %notify! %note! %notice% %notify% %note%
   %informration! %info! %informration% %info%
   %debug! %detail! %debug% %detail%})

(module-export!
 '{detail%watch debug%watch info%watch notice%watch warn%watch})

(define %nosubst '%loglevel)

(define %emergency! 0)
(define %alert! 1)
(define %critical! 2)
(define %danger! 2)
(define %error! 3)
(define %err! 3)
(define %warning! 4)
(define %warn! 4)
(define %notice! 5)
(define %notify! 5)
(define %info! 6)
(define %debug! 7)
(define %detail! 8)

(define %emergency% 0)
(define %alert% 1)
(define %critical% 2)
(define %danger% 2)
(define %error% 3)
(define %err% 3)
(define %warning% 4)
(define %warn% 4)
(define %notice% 5)
(define %notify% 5)
(define %info% 6)
(define %debug% 7)
(define %detail% 8)

(define %loglevel 4)

(define loglevel-init-map
  '{(DETAIL . 8) (DETAILS . 8)
    (DEBUG . 7) (DBG . 7) (INFO . 6)
    (NOTICE . 5) (NOTE . 5) (NOTIFY . 5)
    (WARN 4) (WARNING 4)
    (ERROR 3) (ERR 3)
    (ERROR 3) (ERR 3)
    (CRITICAL 2) (CRIT 2)
    (ALERT 1) (EMERGENCY 0) (EMERG 0)})

(define loglevel-table
  (let ((table (make-hashtable)))
    (do-choices (map loglevel-init-map)
      (add! table
	    (choice (car map)
		    (symbol->string (car map))
		    (downcase (symbol->string (car map)))
		    (string->symbol (string-append "%" (symbol->string (car map))))
		    (string->symbol (string-append "%" (symbol->string (car map)) "!")))
	    (cdr map)))
    table))

(define (getloglevel arg)
  (if (number? arg) arg (get loglevel-table arg)))

(define logger
  (macro expr
    `(logif+ (>= %loglevel ,(cadr expr)) ,(cadr expr) ,@(cddr expr))))

(define logdetail
  (macro expr `(logif+ (>= %loglevel ,%detail%) 7 ,@(cdr expr))))
(define logdebug
  (macro expr `(logif+ (>= %loglevel ,%debug%) 7 ,@(cdr expr))))
(define loginfo
  (macro expr `(logif+ (>= %loglevel ,%info%) 6 ,@(cdr expr))))
(define lognotice
  (macro expr `(logif+ (>= %loglevel ,%notice%) 5 ,@(cdr expr))))
(define logwarn
  (macro expr `(logif+ (>= %loglevel ,%warn%) 4 ,@(cdr expr))))
(define %debug
  (macro expr `(logif+ (>= %loglevel ,%debug%) 7 ,@(cdr expr))))
(define %detail
  (macro expr `(logif+ (>= %loglevel ,%debug%) 7 ,@(cdr expr))))

(define detail%watch
  (macro expr `(if (>= %loglevel ,%detail%) (,%watch ,@(cdr expr)) ,(cadr expr))))
(define debug%watch
  (macro expr `(if (>= %loglevel ,%debug%) (,%watch ,@(cdr expr)) ,(cadr expr))))
(define info%watch
  (macro expr `(if (>= %loglevel ,%info%) (,%watch ,@(cdr expr)) ,(cadr expr))))
(define notice%watch
  (macro expr `(if (>= %loglevel ,%notice%) (,%watch ,@(cdr expr)) ,(cadr expr))))
(define warn%watch
  (macro expr `(if (>= %loglevel ,%warn%) (,%watch ,@(cdr expr)) ,(cadr expr))))

