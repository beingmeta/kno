;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2016-2017 beingmeta, inc.  All rights reserved.

(in-module 'batch)

(use-module '{logger stringfmts varconfig slotindex})

(define %volatile '{maxtime maxmem maxvmem maxload maxcount 
		    saving last-save save-frequency
		    pre-save post-save
		    batch-state init-state})

(define %loglevel %notice%)

(define start-time (elapsed-time))

(define-init maxtime #f)
(varconfig! maxtime maxtime config:interval)

(define-init maxmem (->exact (* 0.9 (physmem))))
(varconfig! maxmem maxmem config:bytes)

(define-init maxvmem (physmem))
(varconfig! maxvmem maxvmem config:bytes)

(define-init maxload #f)
(varconfig! maxload maxload)

(define-init maxcount #f)
(varconfig! maxcount maxcount)

(define-init last-log (elapsed-time))
(define-init log-frequency 30)
(varconfig! logfreq log-frequency config:interval)

(define-init saving #f)
(define-init last-save (elapsed-time))
(define-init save-frequency 300)
(varconfig! savefreq save-frequency config:interval)

(define-init pre-save #f)
(varconfig! PRESAVE pre-save)
(define-init post-save #f)
(varconfig! POSTSAVE post-save)

(define %nosusbt '{pre-save post-save})

(define-init batch-state #[])
(define-init init-state #f)

(defambda (nstore! table slot value)
  (unless (fail? value) (store! table slot value)))

(defambda (batch/read-state file . defaults)
  (let* ((init (if (and file (file-exists? file))
		   (begin
		     (logwarn |BatchRead| 
		       "Reading batch state from " file)
		     (file->dtype file))
		   (apply frame-create #f defaults)))
	 (state (deep-copy init)))
    (store! state 'init init)
    (when file (store! state 'statefile file))
    (nstore! state 'pools (use-pool (get state 'pools)))
    (nstore! state 'indexes (open-index (get state 'indexes)))
    (nstore! state 'adjpools (open-pool (get state 'adjpools) #[adjunct #t]))
    (nstore! state 'objects
	     (for-choices (obj (get state 'objects))
	       (if (and (string? obj) (file-exists? obj))
		   (cons obj (file->dtype obj))
		   (if (and (pair? obj) (string? (car obj)))
		       (if (file-exists? (car obj))
			   (cons (car obj) (file->dtype (car obj)))
			   (begin (dtype->file (cdr obj) (car obj))
			     obj))
		       (irritant obj |BadObjectStateReference|)))))
    (nstore! state 'slotindex
	     (slotindex/init (get state 'slotindex)))
    (unless (test state 'begun) (store! state 'begun (timestamp)))
    (store! state 'count (1+ (try (get state 'count) 0)))
    (store! state 'words (1+ (try (get state 'words) 0)))
    (store! state 'sentences (1+ (try (get state 'sentences) 0)))
    (unless (file-exists? file) (dtype->file init file))
    state))

(define (batch/start! (state batch-state) . defaults)
  (when (string? state) 
    (set! state (batch/read-state state)))
  (unless init-state 
    (set! init-state (try (get state 'init) (deep-copy state))))
  (set! batch-state state)
  (set! start-time (elapsed-time))
  (store! state 'cycle-count (1+ (try (get state 'cycle-count) 0)))
  (unless (test state 'begun)
    (store! state 'begun (gmtimestamp 'seconds)))
  (lognotice |BatchStart| 
    "Cycle "  (get state 'cycle-count) 
    " of batch task " (get state 'statefile)
    (when (test state 'summary) (printout " (" (get state 'summary) ")")))
  state)

(module-export! '{batch/read-state batch/start! batch-state})

;;; Are we done yet?

(define (batch/finished? (state batch-state))
  (or finished
      (exists batch/threshtest "Time" 
	      (elapsed-time start-time) maxtime secs->string)
      (exists batch/threshtest "Memory" (memusage) maxmem $bytes)
      (and maxvmem (> (vmemusage) maxvmem)
	   (begin (release-memory)
	     (exists batch/threshtest "VMEM" (vmemusage) maxvmem $bytes)))
      (exists batch/threshtest "Progress" (get state 'progress) maxcount)
      (exists batch/threshtest "LOAD" (get (rusage) 'load) maxload)
      (exists test-limit state tasklimits)))

(define-init finished #f)
(define (batch/threshtest condition cur max (format #f))
  (or finished
      (and cur max (> cur max) 
	   (begin
	     (logwarn |Finishing| "Due to " condition ": " 
		      (if format (format cur) cur) " > "
		      (if format (format max) max))
	     (set! finished #t)
	     #t))))

(define tasklimits {})
(varconfig! tasklimit tasklimits)

(define (test-limit state limit)
  (and (test state (car limit))
       (number? (cdr limit))
       (number? (get state (car limit)))
       (> (get state (car limit)) (cdr limit))))

(module-export! '{batch/finished? batch/threshtest})

;;; Regular checkins

(define (batch/checkin delta (state batch-state))
  (info%watch "BATCH/CHECKIN" delta state)
  (let ((result (docheckin delta state)))
    (if (applicable? result)
	(result)
	result)))

(defslambda (docheckin delta state)
  (if (table? delta)
      (do-choices (key (getkeys delta))
	(let ((dv (get delta key)))
	  (if (number? dv)
	      (store! state key (+ dv (try (get state key) 0)))
	      (add! state key dv))))
      (if (number? delta)
	  (store! state 'progress 
		  (+ delta (try (get state 'progress) 0)))
	  (add! state 'checkins delta)))
  (info%watch "BATCH/CHECKIN/changed" state)
  (unless finished
    (when (batch/finished? batch-state)
      (set! finished #t))
    (when (and log-frequency (> (elapsed-time last-log) log-frequency))
      (set! last-log (elapsed-time))
      (batch/log! state)))
  (if (and (not finished) save-frequency (not saving) 
	   (not (zero? save-frequency))
	   (> (elapsed-time last-save) save-frequency) 
	   (getsavelock))
      (lambda () (batch/save! state))
      state))

(module-export! '{batch/checkin})

(define (log-resources)
  (let ((u (rusage)))
    (lognotice |Resources|
      "MEM=" ($bytes (get u 'memusage)) 
      ", VMEM=" ($bytes (get u 'vmemusage))
      (when maxmem (printout ", MAXMEM=" ($bytes maxmem)))
      (when maxvmem (printout ", MAXMEM=" ($bytes maxvmem)))
      ", PHYSMEM=" ($bytes (get u 'physical-memory)))))

;;; Checkpointing

(defslambda (batch/save! (state batch-state) (started (elapsed-time)))
  (cond ((not saving))
	((and pre-save (not (pre-save)))
	 (logwarn |BatchSaveAbort| "PRESAVE function aborted save")
	 (set! saving #f))
	(else
	 (lognotice |BatchSave| "Saving task databases")
	 (log-resources)
	 (set! last-save (elapsed-time))
	 (let ((threads
		(choice
		 (for-choices (pool (get state 'pools))
		   (threadcall commit-pool pool))
		 (for-choices (index (get state 'indexes))
		   (threadcall commit index))
		 (for-choices (pool (get state 'adjpools))
		   (threadcall commit pool))
		 (for-choices (index (get (get state 'slotindex)
					  (get (get state 'slotindex) 'slots)))
		   (threadcall commit index))
		 (for-choices (file.object (get state 'objects))
		   (threadcall dtype->file 
			       (deep-copy (cdr file.object))
			       (car file.object))))))
	   (lognotice |BatchSave| 
	     "Using " (choice-size threads) " threads to commit task state")
	   (thread/join threads)
	   (lognotice |BatchSave| 
	     "Writing out current state to " (get state 'statefile))
	   (when post-save (post-save state-copy))
	   (let* ((state-copy (deep-copy state))
		  (totals (get state-copy 'totals)))
	     (store! state-copy 'pools
		     (pool-source (get state-copy 'pools)))
	     (store! state-copy 'indexes
		     (index-source (get state-copy 'indexes)))
	     (store! state-copy 'adjpools
		     (pool-source (get state-copy 'adjpools)))
	     (store! state-copy 'objects (car (get state-copy 'objects)))
	     (store! state-copy 'elapsed 
		     (+ (elapsed-time start-time) 
			(try (get state 'elapsed) 0)))
	     (drop! (get state-copy 'slotindex)
		    (get (get state-copy 'slotindex) 'slots))
	     (do-choices (slot (getkeys totals))
	       (store! totals slot
		       (try (+ (get state slot )(get totals slot))
			    (get totals slot)
			    (get state slot))))
	     (store! state-copy (getkeys totals) 0)
	     (drop! state-copy 'init)
	     (onerror
		 (begin
		   (dtype->file state-copy (get state-copy 'statefile))
		   (lognotice |BatchSave| 
		     "Finished saving batch state to " (get state 'statefile) 
		     " in " (elapsed-time started) "s")
		   (log-resources))
		 (lambda (ex)
		   (logwarn |SaveFailed| 
		     "Couldn't write state file " (get state-copy 'statefile) ":\n"
		     (pprint state-copy)))))
	   (set! saving #f)))))

(defslambda (batch/finish! (state batch-state) (sleep-for 4))
  (while saving
    (sleep sleep-for)
    (logwarn |BatchFinished| "Waiting for current commit to clear")
    (set! sleep-for (+ sleep-for 4)))
  (when pre-save
    (unless (pre-save)
      (logwarn |BatchSaveAbort| "PRESAVE function would have aborted")
      (set! saving #f)))
  (logwarn |BatchFinished| "Starting to save current state")
  (log-resources)
  (let ((started (elapsed-time))
	(threads
	 (choice
	  (for-choices (pool (get state 'pools))
	    (threadcall commit-pool pool))
	  (for-choices (index (get state 'indexes))
	    (threadcall commit index))
	  (for-choices (pool (get state 'adjpools))
	    (threadcall commit pool))
	  (for-choices (index (get (get state 'slotindex)
				   (get (get state 'slotindex) 'slots)))
	    (threadcall commit index))
	  (for-choices (file.object (get state 'objects))
	    (threadcall dtype->file 
			(deep-copy (cdr file.object))
			(car file.object))))))
    (logwarn |BatchSave| 
      "Saving state using " (choice-size threads) " threads")
    (thread/join threads)
    (logwarn |BatchSave| 
      "Writing out processing state to " (get state 'statefile))
    (when post-save (post-save state-copy))
    (do-choices (slot (getkeys (get state 'totals)))
      (when (test state slot)
	(store! (get state 'totals) slot 
		(+ (get (get state 'totals) slot)
		   (get state slot)))
	(store! state slot 0)))
    (let ((state-copy (deep-copy state)))
      (store! state-copy 'pools
	      (pool-source (get state-copy 'pools)))
      (store! state-copy 'indexes
	      (index-source (get state-copy 'indexes)))
      (store! state-copy 'adjpools
	      (pool-source (get state-copy 'adjpools)))
      (store! state-copy 'objects (car (get state-copy 'objects)))
      (store! state-copy 'elapsed 
	      (+ (elapsed-time start-time) 
		 (try (get state 'elapsed) 0)))
      (drop! (get state-copy 'slotindex)
	     (get (get state-copy 'slotindex) 'slots))
      (drop! state-copy 'init)
      (dtype->file state-copy (get state-copy 'statefile)))
    (lognotice |BatchFinished| 
      "Finished saving current state to " (get state 'statefile) 
      " in " (elapsed-time started) "s")
    (log-resources)))

(defslambda (getsavelock)
  (and (not saving) 
       (begin (set! saving (threadid)) #t)))

(define (batch/savelock)
  (and (not saving) (getsavelock)))

(module-export! '{batch/save! batch/finish! batch/savelock})

;;; Logging

(define (batch/log! (state batch-state))
  (lognotice |#| (make-string 120 #\■))
  (lognotice |Batch|
    "After " (secs->string (elapsed-time start-time)) 
    (if (and (test state 'logging) (test state (get state 'logging)))
	": ") 
    (do-choices (log (get state 'logging))
      (if (symbol? log)
	  (when (test state log)
	    (printout " " log "=" (get state log)))
	  (if (and (pair? log) (symbol? (car log))
		   (test state (car log))
		   (applicable? (cdr log)))
	      (printout " " (car log) "=" 
		((cdr log) (get state (car log))))))))
  (when (test state 'logcounts)
    (lognotice |Progress|
      (do-choices (count (get state 'logcounts) i)
	(let ((slot (if (symbol? count) count (if (pair? count) (car count) #f))))
	  (when (and slot (test state slot))
	    (when (> i 0) (printout "; "))
	    (printout ($num (get state slot)) " " (downcase slot))
	    (when (pair? count)
	      (printout " (" (show% (get state slot) (cdr count)) ")")))))))
  (when (test state 'logrates)
    (let ((elapsed (elapsed-time start-time)))
      (lognotice |Progress|
	(do-choices (rate (get state 'logrates) i)
	  (when (test state rate)
	    (printout (printnum (/ (get state rate) elapsed) 1) " "
	      (downcase rate) "/sec; "))))))
  (when (and (test state 'totals) (test state 'logcounts))
    (let ((totals (get state 'totals)))
      (lognotice |Overall|
	(do-choices (count (get state 'logcounts) i)
	  (let ((slot (if (symbol? count) count (if (pair? count) (car count) #f))))
	    (when (and slot (test state slot) (test totals slot))
	      (when (> i 0) (printout "; "))
	      (printout ($num (+ (get state slot) (get totals slot)))
		" " (downcase slot))
	      (when (pair? count)
		(printout " (" (show% (+ (get state slot) (get totals slot))
				      (cdr count)) ")"))))))))
  (when (and init-state (test state 'totals)
	     (test state 'logrates) (test init-state 'elapsed)
	     (test (get state 'totals) (get state 'logrates)))
    (let ((totals (get state 'totals))
	  (elapsed (+ (get init-state 'elapsed)
		      (elapsed-time start-time))))
      (lognotice |Overall|
	(do-choices (rate (get state 'logrates) i)
	  (when (and (test state rate) (test totals rate))
	    (printout 
	      (printnum (/ (+ (get state rate) (get totals rate))
			   elapsed) 1) " "
	      (downcase rate) "/sec; "))))))
  (let ((u (rusage)))
    (lognotice |Resources|
      "CPU=" (printnum (get u 'cpu%) 2) 
      " (" (printnum (/~ (get u 'cpu%) (get u 'ncpus)) 2)
      " * " (get u 'ncpus) " cpus) "
      (when (test state 'nthreads)
	(printout " (" (printnum (/~ (get u 'cpu%) (get state 'nthreads)) 2) " * "
	  (get state 'nthreads) " threads) "))
      " load= "
      (let ((load (loadavg)))
	(printout (first load) "  ⋯  " (second load) "  ⋯  " (third load))))
    (lognotice |Resources|
      (when (test u 'mallocd)
	(printout "HEAP="  
	  (when (test u 'heap) 
	    (printout ($bytes (get u 'mallocd))))
	  " "))
      "MEM=" ($bytes (get u 'memusage)) 
      ", VMEM=" ($bytes (get u 'vmemusage))
      (when maxmem (printout ", MAXMEM=" ($bytes maxmem)))
      (when maxvmem (printout ", MAXMEM=" ($bytes maxvmem)))
      ", PHYSMEM=" ($bytes (get u 'physical-memory))))
  (lognotice |#| (make-string 120 #\#)))

(module-export! '{batch/log!})
