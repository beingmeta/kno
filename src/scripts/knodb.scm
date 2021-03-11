;;; -*- Mode: Scheme -*-

(use-module '{varconfig text/stringfmts optimize logger})

(define %loglevel (config 'loglevel %notice%))

(define-init knodb-commands
  #[pack {"<dbfile> [into] [type]"
	  "pool <poolfile> [into] [type]"
	  "index <indexfile> [into] [type]"}
    packpool "<poolfile> [into]"
    packindex "<indexfile> [into]"])
(varconfig! knodb:commands knodb-commands #t choice)

(config! 'cachelevel 2)
(config! 'optlevel 4)
(config! 'logprocinfo #t)
(config! 'logthreadinfo #t)
(config! 'thread:logexit #f)

(define (main (op #f) . args)
  (when (not op) (usage) (exit))
  (let* ((modname (string->symbol (glom "knodb/actions/" op)))
	 (module (get-module modname)))
    (cond ((not module)
	   (logerr |UnknownCommand| op ", the module " modname " doesn't exist"))
	  (else (optimize-module! (get module '%optimize))
		(apply (get module 'main) args)))))

(define (usage)
  (lineout "Usage: knodb <op> <args...>")
  (do-choices (command (getkeys knodb-commands))
    (let ((sig (get knodb-commands command)))
      (cond ((string? sig)
	     (lineout "   " command " " sig))
	    ((ambiguous? sig)
	     (do-choices sig (lineout "   " command " " sig)))
	    ((sequence? sig)
	     (doseq (sig sig) (lineout "   " command " " sig)))
	    (else (lineout "   " command " args..."))))))



