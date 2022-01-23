;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-

(in-module 'kno/exec)

(use-module '{procprims})

(module-export! '{proc/run proc/open run->file run->string})

(module-export! '{knox/fork/wait
		  fork/cmd/wait
		  fork/wait
		  knox/fork
		  fork/cmd
		  fork
		  knox
		  exec/cmd
		  exec})

;;; Wrapper to get more functionality

(define default-knox-configs
  #[quiet #t loglevel 3])

(define (proc-runner addopts spec args)
  (local invoke #f opts #f)
  (cond ((and (opts? spec) 
	      (pair? args) 
	      (or (string? (car args)) (symbol? (car args))))
	 (set! opts spec)
	 (set! invoke (car args))
	 (set! args (cdr args)))
	((and (or (string? spec) (symbol? spec))
	      (pair? args) (opts? (car args)))
	 (set! opts (car args))
	 (set! invoke spec)
	 (set! args (cdr args)))
	((or (string? spec) (symbol? spec))
	 (set! invoke spec))
	(else (irritant spec |InvalidProcSpec|)))
  (let* ((shell (getopt opts 'shell
		  (getopt opts 'interpreter
		    (and (symbol? invoke) (config 'KNOX "knox")))))
	 (isknox (getopt opts 'isknox 
		   (or (symbol? invoke) (testopt opts 'configs)
		       (knox-shell? shell))))
	 (addopts (if addopts (deep-copy addopts) #[]))
	 (configs '()))
    ;; Assemble config arguments
    (when isknox
      (when (not shell) (set! shell (config 'KNOX "knox")))
      (do-choices (config (getopt opts 'configs {}))
	(cond ((pair? config)
	       (set! configs (cons (stringout (car config) "=" (cdr config)) configs)))
	      ((table? config) 
	       (do-choices (key (getkeys config))
		 (set! configs (cons (stringout key "=" (get config key)) configs))))
	      ((and (string? config) (position #\= config))
	       (set! configs (cons config configs)))
	      ((or (string? config) (symbol? config))
	       (set! configs (cons (stringout config "=" (config config))
				   configs)))))
      (do-choices (config (getopt addopts 'configs {}))
	(cond ((pair? config)
	       (set! configs (cons (stringout (car config) "=" (cdr config)) configs)))
	      ((table? config) 
	       (do-choices (key (getkeys config))
		 (set! configs (cons (stringout key "=" (get config key)) configs))))
	      ((and (string? config) (position #\= config))
	       (set! configs (cons config configs)))
	      ((or (string? config) (symbol? config))
	       (set! configs (cons (stringout config "=" (config config))
				   configs))))))
    (unless (getopt opts 'environment) (add! addopts 'environment #f))
    (when (exists? (getkeys addopts))
      (if opts 
	  (set! opts (cons addopts opts))
	  (set! opts addopts)))
    (cond (shell
	   (if (> (length configs) 0)
	       (apply %proc/run opts shell (->vector configs) invoke args)
	       (apply %proc/run opts shell invoke args)))
	  (else (apply %proc/run opts invoke args)))))

(define (proc/open spec . args)
  (proc-runner #f spec args))
(define proc/run (fcn/alias proc/open))

(define (run->file outfile spec . args)
  (proc-runner `#[stdout ,outfile stderr temp wait #t
		  configs [quiet #t loglevel 3]]
	       spec args))
(define (run->string spec . args)
  (let ((job (proc-runner `#[stdout file stderr temp wait #t
			     configs [quiet #t loglevel 3]]
			  spec args)))
    (config! 'lastjob job)
    (if (file-exists? (proc-stdout job))
	(let ((result (filestring (proc-stdout job))))
	  (remove-file (proc-stdout job))
	  result)
	(error |MissingOutput| job))))

(define-init *knox-wrappers* {})

(define (knox-shell? path)
  (and (string? path)
       (or (has-suffix path {"knox" ".scm"})
	   (overlaps? (basename path) *knox-wrappers*))))

;;; direct calls, no options

(define (fork/cmd prog . args)
  (apply proc/open prog #[fork #t lookup #t] args))
(define (fork/cmd/wait prog . args)
  (apply proc/open prog #[fork #t lookup #t wait #t] args))

(define (fork prog . args)
  (apply proc/open prog #[fork #t] args))
(define (fork/wait prog . args)
  (apply proc/open prog #[fork #t wait #t] args))

(define (knox prog . args)
  (apply proc/open prog #[fork #f knox #t] args))
(define (knox/fork prog . args)
  (apply proc/open prog #[fork #t] args))
(define (knox/fork/wait prog . args)
  (apply proc/open prog #[fork #t knox #t wait #t] args))

(define (exec/cmd prog . args)
  (apply proc/open prog #[fork #f lookup #t] args))
(define (exec prog . args)
  (apply proc/open prog #[fork #f] args))
