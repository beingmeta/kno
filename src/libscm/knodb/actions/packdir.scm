;;; -*- Mode: Scheme -*-

(in-module 'knodb/actions/packdir)

(module-export! '{flexpack main})

(use-module '{varconfig logger texttools text/stringfmts binio fifo optimize})
(use-module '{knodb knodb/indexes knodb/flexindex knodb/actions/flexpack})

(define %loglevel (config 'loglevel %notice%))
(define %optmods '{knodb/actions/splitindex
		   knodb/indexes
		   knodb/hashindexes
		   ezrecords
		   engine
		   fifo})

(define (get-packinputs dir)
  (reject (pick (getfiles dir) has-suffix {".index" ".pool" ".flexindex"})
    string-contains? #("." (isdigit+) ".index" (eos))))

(define (main dir)
  (default-configs)
  (debug%watch "main" in head tail)
  (if (file-directory? dir)
      (let* ((inputs (get-packinputs dir))
	     (flexpacks (flexpack-tasks (pick inputs has-suffix ".flexindex")))
	     (outputs (for-choices (task flexpacks)
			{(or (< (length task) 3) (elt task 2) {})
			 (or (< (length task) 4) (elt task 3) {})}))
	     (packtasks (list 'pack (difference (reject inputs has-suffix ".flexindex")
						outputs)))
	     (tasks {flexpacks packtasks})
	     (fifo (->fifo tasks))
	     (maxjobs (config 'maxjobs (1+ (quotient (rusage 'ncpus) 2))))
	     (proc-opts [lookup #t stdout 'temp stderr 'temp])
	     (active {})
	     (done {})
	     (failed {}))
	(while (or (fifo-live? fifo) (exists? active))
	  (while (and (fifo-live? fifo) (< (|| active) maxjobs))
	    (let* ((task (fifo/pop fifo))
		   (job (cond ((not (pair? task)) (logwarn |BadTask| task) #f)
			      ((eq? (car task) 'pack)
			       (proc/open "knodb" proc-opts "pack" (cadr task)))
			      ((eq? (car task) 'merge)
			       (proc/open "knodb" proc-opts
					  (->vector (cons "splitindex" (cdr task)))))
			      (else  (logwarn |BadTask| task)))))
	      (when job
		(logwarn |Launched| job)
		(set+! active job))))
	  (sleep 3)
	  (let ((exited (reject active proc/live?)))
	    (do-choices (job exited)
	      (logwarn |Finished| job "\t " (proc/status job))
	      (if (test (proc/status job) 'error)
		  (set+! failed job)
		  (set+! done job)))
	    (set! active (difference active exited)))))
      (logwarn |Usage| "knodb packdir *dir*")))

(define configs-done #f)

(define (default-configs)
  (unless configs-done
    (config! 'cachelevel 2)
    (config! 'optlevel 4)
    (config! 'logprocinfo #t)
    (config! 'logthreadinfo #t)
    (config-default! 'logelapsed #t)
    (config! 'thread:logexit #f)
    (set! configs-done #t)))

