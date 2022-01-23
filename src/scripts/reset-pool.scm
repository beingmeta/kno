;;; -*- Mode: Scheme -*-

(use-module '{optimize kno/mttools varconfig text/stringfmts logger})

(define %loglevel %notice%)
(config! 'cachelevel 2)
(config! 'optlevel 4)
(config! 'logprocinfo #t)

(define newflags {})
(varconfig! FLAGS newflags #t choice)

(define (writable? file)
  (if (file-exists? file)
      (file-writable? file)
      (and (file-directory? (dirname file)) 
	   (file-writable? (dirname file)))))

(define (main (poolfile))
  (cond ((not (bound? poolfile)) (usage))
	((not (file-exists? poolfile))
	 (logwarn |MissingInput| "Can't locate source " (write poolfile))
	 (exit))
	((not (file-writable? poolfile))
	 (logwarn |MissingInput| "Can't write to " (write poolfile))
	 (exit))
	(else (reset-pool (abspath poolfile)))))

(define (check-poolfile poolfile)
  (let* ((opts (frame-create #f
		 'adjunct #t 'register #f shared #f
		 'type (config 'pooltype (or (config 'type) {}))
		 'module (or (config 'dbmodule) {}))))
    (onerror (begin (open-pool poolfile opts) #t)
	(lambda (ex)
	  (logcrit |BadPool| "Couldn't open the pool " poolfile ": " ex)
	  (exit 1)))))

(define (reset-pool poolfile)
  (let* ((bakfile (config 'BAKFILE (CONFIG 'BACKUP (glom poolfile ".bak")))))
    (config! 'appid (glom "reset(" poolfile ")"))
    (when (and bakfile (not (writable? bakfile)))
      (logcrit |NoBackup| "Can't write backup file " (write bakfile))
      (exit))
    (when (check-poolfile poolfile)
      (onerror (move-file poolfile bakfile)
	  (lambda (ex) 
	    (logwarn |MoveFailed| "Falling back to shell 'mv " poolfile " " bakfile "'")
	    (system "mv " poolfile " " bakfile)))
      (onerror
	  (let* ((read-opts (frame-create #f
			      'adjunct #t
			      'type (config 'pooltype (or (config 'type) {}))
			      'module (or (config 'dbmodule) {})))
		 (old (open-pool bakfile read-opts))
		 (pooltype (config 'NEWTYPE (try (poolctl old 'metadata 'type) 'kpool)))
		 (base (pool-base old))
		 (capacity (config 'NEWCAP (pool-capacity old)))
		 (old-metadata (poolctl old 'metadata))
		 (make-opts (frame-create #f
			      'base base 'capacity capacity 'type pooltype 'load 0
			      'metadata old-metadata
			      'compression (config 'compression (poolctl old 'metadata 'compression) #t)
			      'offtype (config 'offtype (poolctl old 'metadata 'offmode) #t)
			      'dtypev2 (config 'dtypev2 (get (poolctl old 'metadata 'opts) 'dtypev2)))))
	    (drop! old-metadata (get old-metadata '_READONLY_PROPS))
	    (make-pool poolfile make-opts))
	  (lambda (ex)
	    (logcrit |MakePoolFailed| "With exception " ex "\n  Restoring original from backup")
	    (onerror (move-file bakfile poolfile)
		(lambda (ex)
		  (logwarn |MoveFailed| "Falling back to shell 'mv " bakfile " " poolfile "'")
		  (system "mv " bakfile " " poolfile))))))))

(define (usage)
  (lineout "Usage: reset-pool <from> [to]")
  (lineout "    Empties (resets) the pool stored in <from>.  The new file ")
  (lineout "    pool either replaces <from> or is written into [to].")
  (lineout "    if specified, [to] must not exist unless OVERWRITE=yes.")
  (lineout "    POOLTYPE=kpool|filepool"))

(optimize-locals!)

