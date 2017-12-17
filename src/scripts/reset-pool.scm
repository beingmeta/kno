;;; -*- Mode: Scheme -*-

(use-module '{optimize mttools varconfig stringfmts logger})

(define %loglevel %notice%)
(config! 'cachelevel 2)
(config! 'optlevel 4)
(config! 'logprocinfo #t)

(defambda (newflags (current {}))
  (let* ((configured
	  (choice (tryif (config 'dtypev2 #f) 'dtypev2)
		  (tryif (config 'B32 #f) 'B32)
		  (tryif (config 'B40 #f) 'B40)
		  (tryif (config 'B64 #f) 'B64)
		  (tryif (config 'ZLIB #f) 'ZLIB)
		  (tryif (config 'SNAPPY #f) 'SNAPPY)
		  (tryif (config 'ADJUNCT #f) 'ISADJUNCT)
		  (tryif (config 'ISADJUNCT #f) 'ISADJUNCT)))
	 (choice configured
		 (difference current
			     (tryif (overlaps? configured '{B32 B40 B64})
			       '{B32 B40 B64})
			     (tryif (overlaps? configured '{zlib snappy})
			       '{zlib snappy})
			     (tryif (overlaps? configured 'dtypev2)
			       'dtypev2)
			     (tryif (config 'noadjunct) '{adjunct isadjunct}))))))

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
		 'adjunct #t 'register #f
		 'type (config 'pooltype (config 'type {}))
		 'module (config 'module {})))
	 (pool (onerror (open-pool poolfile opts)) #f)))
    (cond (pool #t)
	  (else (logcrit |BadPool| "Couldn't open the pool " poolfile)
		(exit 1))))

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
	  (let* ((opts (frame-create #f
			 'adjunct #t
			 'type (config 'pooltype (config 'type {}))
			 'module (config 'module {})))
		 (old (open-pool bakfile opts))
		 (pooltype (config 'NEWTYPE (try (poolctl old 'metadata 'type) 'bigpool)))
		 (base (pool-base old))
		 (capacity (config 'NEWCAP (pool-capacity old)))
		 (opts `#[base ,base capacity ,capacity type ,pooltype load 0
			  metadata ,(poolctl old 'metadata)
			  flags ,(newflags (poolctl old 'metadata 'flags))]))
	    (make-pool poolfile opts))
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
  (lineout "    POOLTYPE=bigpool|oidpool|filepool"))

(optimize!)

