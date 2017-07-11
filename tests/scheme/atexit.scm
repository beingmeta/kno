(use-module '{logger varconfig})

(config! 'logprocinfo #t)

(define target-prefix (get-component "_atexit"))
(varconfig! prefix target-prefix)

(define quiet #f)
(varconfig! quiet quiet)

(define (delete-pid-file exitarg (file (glom target-prefix ".pid")))
  (unless quiet (logwarn |ExitArg| "Is " exitarg))
  (if (file-exists? file)
      (remove-file file)
      (logwarn "No PID file" file)))

(define (old-save-note arg (file (glom target-prefix ".oldout")))
  (unless quiet (loginfo |SavingAtExit| "Saving elapsed to " file))
  (fileout file (elapsed-time)))

(define (save-note arg (file (glom target-prefix ".out")))
  (unless quiet (loginfo |SavingAtExit| "Saving elapsed to " file))
  (fileout file (elapsed-time)))

(config! 'atexit delete-pid-file)
(config! 'atexit (cons 'save-note old-save-note))
(config! 'atexit (cons 'save-note save-note))

(define (main)
  ;; (logwarn |Started| "With " target-prefix)
  (fileout (glom target-prefix ".pid") (config 'pid))
  (config! 'atexit delete-pid-file)
  (unless quiet (logwarn |Sleeping| "For " (config 'SLEEP4 20)))
  (sleep (config 'SLEEP4 20)))






