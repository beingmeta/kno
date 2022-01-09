;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu).

(in-module 'logger)

;;; Provides lightweight logging functions with levels, module-level
;;; control, etc

(module-export!
 '{logger
   getloglevel %loglevel
   logwsamp logdeluge logdetail logdebug loginfo logmessage
   lognotice logwarn logerr logerror logcrit logalert logpanic
   logswamp! logdeluge! logdetail! logdebug! loginfo!
   lognotice! logwarn! logerr! logcrit! logalert! logpanic!
   %debug %detail %deluge %swamp})
(module-export!
 '{%emergency! %emergency% %panic%
   %alert! %alert%
   %critical% %critical! %danger! %danger% %crit% %crit!
   %error! %err! %error% %err%
   %warning! %warn! %warning% %warn%
   %notice% %notice! %notify% %notify! %note% %note!
   %information% %information! %status% %status! %info% %info!
   %debug% %debug! 
   %detail% %detail! %details%
   %deluge% %deluge! %swamp% %swamp!})
(module-export! '{swamp%wc deluge%wc detail%wc debug%wc
		  info%wc notice%wc warn%wc})
(module-export! '{logswamp? logdeluge? logdetail? logdebug?
		  loginfo? lognotice? logwarn?
		  logerr? logcrit? logalert? logpanic?
		  log>? log>=?})

(module-export!
 '{swamp%watch deluge%watch detail%watch debug%watch
   info%watch notice%watch warn%watch
   always%watch})

(define %nosubst '%loglevel)

(define %emergency% 0)
(define %emergency! 0)
(define %panic% 0)
(define %attention% 1)
(define %attention! 1)
(define %alert% 1)
(define %alert! 1)
(define %critical% 2)
(define %critical! 2)
(define %danger% 2)
(define %danger! 2)
(define %crit! 2)
(define %crit! 2)
(define %error% 3)
(define %err% 3)
(define %error! 3)
(define %err! 3)
(define %warning% 4)
(define %warn% 4)
(define %warning! 4)
(define %warn! 4)
(define %notice% 5)
(define %notify% 5)
(define %notice! 5)
(define %notify! 5)
(define %info% 6)
(define %info! 6)
(define %debug% 7)
(define %debug! 7)
(define %detail% 8)
(define %detail! 8)
(define %details% 8)
(define %deluge% 9)
(define %deluge! 9)
(define %swamp% 9)
(define %swamp! 9)

(define-init %loglevel 4)

(define loglevel-init-map
  '{(SWAMP . 9) (DELUGE . 9) (VERYDETAILED 9)
    (DETAIL . 8) (DETAILS . 8) (DETAILED . 8)
    (DEBUG . 7) (DBG . 7)
    (INFO . 6) (STATUS . 6) (INFORMATION . 6)
    (NOTICE . 5) (NOTE . 5) (NOTIFY . 5)
    (WARN . 4) (WARNING . 4)
    (ERROR . 3) (ERR . 3)
    (ERROR . 3) (ERR . 3)
    (CRITICAL . 2) (DANGER . 2) (CRIT . 2)
    (ALERT . 1) (ATTENTION . 1)
    (EMERGENCY . 0) (EMERG . 0) (PANIC . 0)})

(define loglevel-table
  (let ((table (make-hashtable)))
    (do-choices (map loglevel-init-map)
      (add! table
	    (choice (car map)
		    (symbol->string (car map))
		    (upcase (symbol->string (car map)))
		    (string->symbol
		     (glom "%" (symbol->string (car map))))
		    (string->symbol
		     (glom "%" (symbol->string (car map)) "!"))
		    (string->symbol
		     (glom "%" (symbol->string (car map)) "%")))
	    (cdr map)))
    table))

(define (getloglevel arg)
  (if (number? arg) arg (get loglevel-table arg)))

(define logmessage
  (macro expr `(,logmsg #f ,@(cdr expr))))

(define logger
  (macro expr
    `(logif+ (>= %loglevel ,(cadr expr)) ,(cadr expr) ,@(cddr expr))))

;; These all call the regular log function but will ignore the loglevel (both local and global)

(define logswamp! (macro expr `(logmsg -9 ,@(cdr expr))))
(define logdeluge! (macro expr `(logmsg -9 ,@(cdr expr))))
(define logdetail! (macro expr `(logmsg -8 ,@(cdr expr))))
(define logdebug! (macro expr `(logmsg -7 ,@(cdr expr))))
(define loginfo! (macro expr `(logmsg -6 ,@(cdr expr))))
(define lognotice! (macro expr `(logmsg -5 ,@(cdr expr))))
(define logwarn! (macro expr `(logmsg -4 ,@(cdr expr))))
(define logerr! (macro expr `(logmsg -3 ,@(cdr expr))))
(define logcrit! (macro expr `(logmsg -2 ,@(cdr expr))))
(define logalert! (macro expr `(logmsg -1 ,@(cdr expr))))
(define logpanic! (macro expr `(logmsg %panic% ,@(cdr expr))))

;;; These all check the local %loglevel, except if the priority is
;;;  worse than an error.
(define logswamp
  (macro expr `(logif+ (>= %loglevel ,%swamp%) 9 ,@(cdr expr))))
(define logdeluge
  (macro expr `(logif+ (>= %loglevel ,%deluge%) 9 ,@(cdr expr))))
(define logdetail
  (macro expr `(logif+ (>= %loglevel ,%detail%) 8 ,@(cdr expr))))
(define logdebug
  (macro expr `(logif+ (>= %loglevel ,%debug%) 7 ,@(cdr expr))))
(define loginfo
  (macro expr `(logif+ (>= %loglevel ,%info%) 6 ,@(cdr expr))))
(define lognotice
  (macro expr `(logif+ (>= %loglevel ,%notice%) 5 ,@(cdr expr))))
(define logwarn
  (macro expr `(logif+ (>= %loglevel ,%warn%) 4 ,@(cdr expr))))
(define logerr
  (macro expr `(logmsg %error% ,@(cdr expr))))
(define logcrit
  (macro expr `(logmsg %critical% ,@(cdr expr))))
(define logalert
  (macro expr `(logmsg %alert% ,@(cdr expr))))
(define logpanic
  (macro expr `(logmsg %panic% ,@(cdr expr))))
(define logerror logerr)

;;; Useful aliases
(define %debug
  (macro expr `(logif+ (>= %loglevel ,%debug%) 7 ,@(cdr expr))))
(define %detail
  (macro expr `(logif+ (>= %loglevel ,%detail%) 8 ,@(cdr expr))))
(define %deluge
  (macro expr `(logif+ (>= %loglevel ,%deluge%) 9 ,@(cdr expr))))
(define %swamp
  (macro expr `(logif+ (>= %loglevel ,%swamp%) 9 ,@(cdr expr))))

;;; %loglevel checking watchpoints
(define swamp%watch
  (macro expr
    `(if (>= %loglevel ,%swamp%)
	 (,%watch ,@(cdr expr))
	 ,(cadr expr))))
(define deluge%watch
  (macro expr
    `(if (>= %loglevel ,%deluge%)
	 (,%watch ,@(cdr expr))
	 ,(cadr expr))))
(define detail%watch
  (macro expr
    `(if (>= %loglevel ,%detail%)
	 (,%watch ,@(cdr expr))
	 ,(cadr expr))))
(define debug%watch
  (macro expr
    `(if (>= %loglevel ,%debug%)
	 (,%watch ,@(cdr expr))
	 ,(cadr expr))))
(define info%watch
  (macro expr
    `(if (>= %loglevel ,%info%)
	 (,%watch ,@(cdr expr))
	 ,(cadr expr))))
(define notice%watch
  (macro expr
    `(if (>= %loglevel ,%notice%)
	 (,%watch ,@(cdr expr))
	 ,(cadr expr))))
(define warn%watch
  (macro expr
    `(if (>= %loglevel ,%warn%)
	 (,%watch ,@(cdr expr))
	 ,(cadr expr))))
(define always%watch
  (macro expr `(,%watch ,@(cdr expr))))

(define swamp%wc
  (macro expr
    `(if (>= %loglevel ,%swamp%)
	 (,%wc ,@(cdr expr))
	 ,(cdr expr))))
(define deluge%wc
  (macro expr
    `(if (>= %loglevel ,%deluge%)
	 (,%wc ,@(cdr expr))
	 ,(cdr expr))))
(define detail%wc
  (macro expr
    `(if (>= %loglevel ,%detail%)
	 (,%wc ,@(cdr expr))
	 ,(cdr expr))))
(define debug%wc
  (macro expr
    `(if (>= %loglevel ,%debug%)
	 (,%wc ,@(cdr expr))
	 ,(cdr expr))))
(define info%wc
  (macro expr
    `(if (>= %loglevel ,%info%)
	 (,%wc ,@(cdr expr))
	 ,(cdr expr))))
(define notice%wc
  (macro expr
    `(if (>= %loglevel ,%notice%)
	 (,%wc ,@(cdr expr))
	 ,(cdr expr))))
(define warn%wc
  (macro expr
    `(if (>= %loglevel ,%warn%)
	 (,%wc ,@(cdr expr))
	 ,(cdr expr))))

;;; Local loglevel predicates

(define log>=?
  (macro expr `(>= %loglevel ,(get-arg expr 1))))
(define log>?
  (macro expr `(> %loglevel ,(get-arg expr 1))))

(define logswamp?
  (macro expr `(>= %loglevel ,%swamp%)))
(define logdeluge?
  (macro expr `(>= %loglevel ,%deluge%)))
(define logdetail?
  (macro expr `(>= %loglevel ,%detail%)))
(define logdebug?
  (macro expr `(>= %loglevel ,%debug%)))
(define loginfo?
  (macro expr `(>= %loglevel ,%info%)))
(define lognotice?
  (macro expr `(>= %loglevel ,%notice%)))
(define logwarn?
  (macro expr `(>= %loglevel ,%warn%)))
(define logerr?
  (macro expr `(>= %loglevel ,%err%)))
(define logcrit?
  (macro expr `(>= %loglevel ,%crit%)))
(define logalert?
  (macro expr `(>= %loglevel ,%alert%)))
(define logpanic?
  (macro expr `(>= %loglevel ,%panic%)))

