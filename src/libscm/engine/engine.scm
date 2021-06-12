;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)

(in-module 'engine)

(use-module '{fifo varconfig kno/threads text/stringfmts binio reflection 
	      bugjar bugjar/html logger kno/statefiles})
(use-module '{knodb knodb/registry knodb/branches})

(define %loglevel %notice%)

(module-export! '{engine/run
		  engine/checkpoint
		  engine/getopt
		  engine/test
		  engine/stop!})

(module-export! '{engine/showrates engine/showrusage
		  engine/log engine/logrusage engine/logrates})

(module-export! '{engine/fetchoids engine/fetchkeys
		  engine/poolfetch engine/indexfetch engine/swapout
		  engine/savepool engine/saveindex engine/savetable
		  engine/lockoids})

(module-export! '{engine/stopfn engine/callif
		  engine/interval engine/maxitems
		  engine/usage
		  engine/delta})

(define %optmods '{fifo kno/threads text/stringfmts logger kno/statefiles knodb knodb/branches})

;;; TODO: Have engine/log be consistent in batch/cycle/task wording
;;; TODO: Have tasklog file which contains cumulative cycle end task state together with timing/progress/rate stats

;;;; Configurable

(define-init engine-bugjar #f)
(varconfig! engine:bugjar engine-bugjar)

(define-init log-frequency 60)
(varconfig! engine:logfreq log-frequency)

(define-init check-frequency 15)
(varconfig! engine:checkfreq check-frequency)

(define-init check-spacing 60)
(varconfig! engine:checkspace check-spacing)

(define-init engine-threadcount #t)
(varconfig! engine:threads engine-threadcount)

(define-init use-configs #t)
(varconfig! engine:useconfigs use-configs)
(varconfig! engine:useconfig use-configs)

(define (getconfopt opts prop (dflt #f))
  (getopt opts prop 
	  (if (getopt opts 'useconfigs use-configs)
	      (config prop dflt)
	      dflt)))

(defimport fifo-condvar 'fifo)

(define ($showrate rate (term #f))
  (if term
      (if (> rate 50)
	  ($count (->exact rate 0) term)
	  ($count rate term))
      (if (> rate 50)
	  ($num (->exact rate 0))
	  ($num rate))))

(define *standard-counters*
  '{items batches cycles
    threadtime clocktime coretime
    filltime checktime
    systime usertime})

;;; This is a task loop engine where a task applies a function to a
;;;  bunch of data items organized into batches.

;;; Currently, this engine supports thread parallelism but not job
;;; (process) parallelism. There are three levels:

;;; 1. Task (outermost, cross-process)
;;; 2. Loop (single process, single invocation of engine/run, often one invocation per process)
;;; 3. Batch (single thread, set of items)

;;; While running, a table (slotmap) is associated with each of the
;;; levels, called *state*, *loop-state*, and *batch_state*
;;; respectively.


;;; The engine uses a FIFO of batches and a pool of threads pulling
;;; batches from the FIFO and processing them. For a single batch,
;;; there can be serveral phases (this is implemented by ENGINE-THREADFN).

#|Markdown

1. BEFOREFN is called on the batch, the batch-state, the loop-state, and the task state)

2. PROCESSFN is the item function itself. It is called on each element of the batch. If it takes two
arguments, it is called on the item and the batch state. If it takes 4 arguments, it is called on
the item, batch state, loop state, and task state.

3. AFTERFN is called on the batch, the batch-state, the loop-state, and the task state)

4. The 'loop state' is then 'bumped'. This updates the total 'items' (number of items processed),
total 'batches' (number of batches processed), 'coretime' (how much realtime was spent in PROCESSFN)
and 'threadtime' (how much realtime this thread spent on the batch).

5. LOGFN is then called on:
* the items processed;
* the clock time spent inside of processfn (CORETIME)
* the clock time spent on the combined BEFORE+CORETIME+AFTER
* the batch state
* the loop state
* the task state

5. The STOPFNs are then called and if any return non-false, the loop 'stops'. This means that the
current thread stops processing batches and all of the other threads stop when they're done with
their current batch.

6. If the loop hasn't stopped, the loop's *monitors* are called. A monitor is either a function or a
pair of functions. In the first case, the function is simply called on the loop-state; in the
second, the CAR is called on the loop-state and the CDR is called if the CAR returns #t.

The monitors can stop the loop by storing a value in the 'stopped slot of the loop state.

|#

(define (checkpointing? loop-state)
  (getopt (get loop-state 'opts) 'checkpointing
	  (or (and (exists? (get loop-state 'checkpoint)) (get loop-state 'checkpoint))
	      (and (exists? (getopt (get loop-state 'opts) 'statefile))
		   (getopt (get loop-state 'opts) 'statefile)))))

;;; We make this handler unique right now, but it could be engine specific.
(define-init bump-loop-state
  (slambda (batch-state loop-state (count #f)
			(proc-time #f) (thread-time #f)
			(sys-time #f) (user-time #f))
    (store! loop-state 'batches (1+ (getopt loop-state 'batches 0)))
    (when count
      (store! loop-state 'items 
	(+ (try (get loop-state 'items) 0) count))
      (store! loop-state 'total
	(+ (try (get loop-state 'total) 0) count)))
    (when proc-time
      (store! loop-state 'coretime 
	(+ (try (get loop-state 'coretime) 0.0) proc-time)))
    (when thread-time
      (store! loop-state 'threadtime 
	(+ (try (get loop-state 'threadtime) 0.0) thread-time)))
    (when sys-time
      (store! loop-state 'systime
	(+ (try (get loop-state 'systime) 0.0) sys-time)))
    (when user-time
      (store! loop-state 'usertime
	(+ (try (get loop-state 'usertime) 0.0) user-time)))
    (do-choices (counter (get loop-state 'counters))
      (when (test batch-state counter)
	(store! loop-state counter
	  (+ (try (get loop-state counter) 0) (get batch-state counter)))))
    (store! loop-state 'clocktime (elapsed-time (get loop-state 'started)))))

(defambda (engine/getopt loop-state optname (default #f))
  (getopt loop-state optname
	  (getopt (getopt loop-state 'task) optname
		  (getopt (get loop-state 'opts) optname
			  default))))

(define (thread-error ex batch-state loop-state (handler) (opts)
		      (now (timestamp)) (saved #f))
  (default! opts (get loop-state 'opts))
  (default! handler (get loop-state 'onerror))
  (store! batch-state 'error ex)
  (logwarn |EngineError| 
    "Unexpected error on " (get loop-state 'fifo) ": "
    (exception-condition ex) " @" (exception-caller ex)
    (when (exception-details ex) (printout " (" (exception-details ex) ")"))
    (when handler (printout " (handling with " handler ") "))
    (when saved (printout "\n  exception dumped to " (write saved)))
    (when (exception-irritant? ex)
      (printout "\n" (lisp->string (exception-irritant ex) 120))))
  ;;(dump-bug ex)
  (when (exception-irritant? ex)
    (loginfo |EngineError/irritant| 
      "For " (get loop-state 'fifo) " "
      (exception-condition ex) " @" (exception-caller ex) ":\n	"
      (lisp->string (exception-irritant ex) 120)))
  (cond ((or (fail? handler) (overlaps? handler '{stopall stop}))
	 (add! loop-state 'errors ex)
	 (store! loop-state 'stopped now)
	 (store! batch-state 'aborted now)
	 #f)
	((or (not handler) (overlaps? handler '{continue ignore}))
	 (add! loop-state 'errors ex)
	 #f)
	((overlaps? handler 'reraise) (reraise ex))
	((applicable? handler)
	 (let ((hv (handler ex batch-state loop-state)))
	   (thread-error ex batch-state loop-state hv opts now)))
	(else
	 (add! loop-state 'errors ex)
	 (store! loop-state 'stopped now)
	 (store! batch-state 'aborted now)
	 #f)))

(define (get-saveroot arg)
  (cond ((pair? arg) (get-saveroot (car arg)))
	((string? arg) (abspath arg))
	((symbol? arg) (config arg))
	(else #f)))

(define (get-refroot arg saveroot)
  (cond ((pair? arg) (cdr arg))
	((string? arg) saveroot)
	((symbol? arg) (config arg saveroot))
	(else #f)))

(define (dump-error ex bugroot (saveroot) (refroot))
  (default! saveroot (get-saveroot bugroot))
  (default! refroot (get-refroot bugroot saveroot))
  (let* ((now (timestamp 'days))
	 (uuid (getuuid))
	 (bugdir (mkpath saveroot (get now 'isobasic)))
	 (refdir (mkpath refroot (get now 'isobasic)))
	 (bugpath (mkpath bugdir (glom (downcase (uuid->string uuid)) ".html")))
	 (refpath (mkpath refdir (glom (downcase (uuid->string uuid)) ".html")))
	 (condition (exception-condition ex))
	 (caller (exception-caller ex))
	 (details (exception-details ex)))
    (unless (file-directory? bugdir) (mkdir bugdir))
    (fileout bugpath (exception.html ex))
    (logerr |Bugjar|
      "Unexpected " condition " in " caller 
      (if details (printout " (" details ")"))
      "\nsaved to " refpath
      (when (error-irritant? ex)
	(printout "\nirritant: " (lisp->string (exception-irritant ex) 120))))
    refpath))

(define (engine-error-handler batch-state loop-state)
  (let* ((opts (getopt loop-state 'opts))
	 (bugdir (getopt opts 'bugjar engine-bugjar)))
    (lambda (ex)
      (thread-error ex batch-state loop-state 
		    (try (getopt loop-state 'onerror) #f) opts (timestamp) 
		    (and bugdir (onerror (dump-error ex bugdir)
				    (lambda (ex)
				      (logerr |ErrorDumpingError| 
					"Can't save error to " bugdir))))))))

(define-init threadfn-loglevel #f)
(varconfig! engine:threadfn:loglevel threadfn-loglevel)

(defambda ($batchsize batch count-term batchsize)
  (if batchsize
      ($count (length batch) count-term)
      ($count (|| batch) count-term)))

(define (engine-threadfn iterfn fifo opts loop-state state batchsize
			 beforefn afterfn fillfn
			 monitors
			 stopfn)
  "This is then loop function for each engine thread."
  (debug%watch "EngineThreadfn"
    iterfn batchsize fifo fillfn "\n" loop-state)
  (let ((threadid (threadid))
	(count-term (try (get loop-state 'count-term) "item"))
	(%loglevel (or threadfn-loglevel %loglevel))
	(batch-indexes (pick (getopt opts 'branchindexes {}) loop-state))
	(batchcall (if batchsize
		       (getopt opts 'batchcall #f)
		       (getopt opts 'batchcall (non-deterministic? iterfn))))
	(batchno 1))
    (let* ((batch (get-batch fifo batchsize loop-state fillfn))
	   (batch-state `#[loop ,loop-state 
			   thread ,threadid
			   batchno ,batchno
			   started ,(elapsed-time)
			   rusage ,(rusage)])
	   (start (get batch-state 'started))
	   (core-time #f))
      (while (and (exists? batch) batch)
	(logdebug |GotBatch|
	  "Processing " ($batchsize batch count-term batchsize)
	  " in batch#" batchno " of thread " threadid
	  " using " ($fn iterfn))
	(when (exists? batch-indexes)
	  (do-choices (indexslot batch-indexes)
	    (when (test loop-state indexslot)
	      (store! batch-state indexslot 
		(index/branch (get loop-state indexslot))))))
	(when (and (exists? beforefn) beforefn)
	  (beforefn (qc batch) batch-state loop-state state))
	(logdetail |EngineStepStarted|
	  "Applying " ($fn iterfn) " to " ($batchsize batch count-term batchsize)
	  " in batch#" batchno " of thread " threadid)
	(set! core-time (elapsed-time))
	(onerror
	    (if batchsize
		(if batchcall
		    (cond ((= (procedure-arity iterfn) 1)
			   (iterfn batch))
			  ((= (procedure-arity iterfn) 2)
			   (iterfn batch batch-state))
			  ((= (procedure-arity iterfn) 3)
			   (iterfn batch batch-state loop-state))
			  ((= (procedure-arity iterfn) 4)
			   (iterfn batch batch-state loop-state state))
			  (else (iterfn batch)))
		    (cond ((= (procedure-arity iterfn) 1)
			   (doseq (item batch) (iterfn item)))
			  ((= (procedure-arity iterfn) 2)
			   (doseq (item batch) (iterfn item batch-state)))
			  ((= (procedure-arity iterfn) 3)
			   (doseq (item batch) (iterfn item batch-state loop-state)))
			  ((= (procedure-arity iterfn) 4)
			   (doseq (item batch)
			     (iterfn item batch-state loop-state state)))
			  (else (doseq (item batch) (iterfn item)))))
		(if (or batchcall (singleton? batch))
		    (cond ((= (procedure-arity iterfn) 1)
			   (iterfn (qc batch)))
			  ((= (procedure-arity iterfn) 2)
			   (iterfn (qc batch) batch-state))
			  ((= (procedure-arity iterfn) 3)
			   (iterfn (qc batch) batch-state loop-state))
			  ((= (procedure-arity iterfn) 4)
			   (iterfn (qc batch) batch-state loop-state state))
			  (else (iterfn (qc batch))))
		    (cond ((= (procedure-arity iterfn) 1)
			   (dochoices (item batch) (iterfn item)))
			  ((= (procedure-arity iterfn) 2)
			   (dochoices (item batch) (iterfn item batch-state)))
			  ((= (procedure-arity iterfn) 3)
			   (dochoices (item batch) (iterfn item batch-state loop-state)))
			  ((= (procedure-arity iterfn) 4)
			   (dochoices (item batch)
				      (iterfn item batch-state loop-state state)))
			  (else (dochoices (item batch) (iterfn item))))))
	    (engine-error-handler batch-state loop-state))
	(set! core-time (elapsed-time core-time))
	(logdetail |EngineStepFinished|
	  "Took " (secs->string core-time) " to apply " ($fn iterfn) 
	  " to " ($batchsize batch count-term batchsize)
	  " in batch#" batchno " of thread " threadid)
	(when (exists? batch-indexes)
	  (do-choices (indexslot batch-indexes)
	    (branch/commit! (get batch-state indexslot)))
	  (logdebug |FinishedBranchCommits| (getopt opts 'branchindexes)))
	(unless (test batch-state 'aborted)
	  (when (and  (exists? afterfn) afterfn)
	    (afterfn (qc batch) batch-state loop-state state))
	  (let ((pre (get batch-state 'rusage)) (post (rusage)))
	    (bump-loop-state batch-state loop-state
			     (if batchsize (length batch) (|| batch))
			     core-time (elapsed-time start)
			     (- (get post 'utime) (get pre 'utime))
			     (- (get post 'stime) (get pre 'stime))))
	  (when (dolog? loop-state fifo)
	    (engine-logger (qc batch) batchsize core-time (elapsed-time start)
			   batch-state loop-state state)))
	(logdebug |FinishedBatch|
	  "Finished batch of " ($batchsize batch count-term batchsize)
	  " in " (secs->string core-time) " using " ($fn iterfn))
	;; Free some stuff up, maybe
	(set! batch #f)
	(let ((stopval (or (try (get batch-state 'stopval) #f)
			   (try (get batch-state 'error) #f)
			   (try (get loop-state 'stopval) #f)
			   (try (get loop-state 'stopped) #f)
			   (and (test loop-state 'maxitems)
				(test loop-state 'items)
				(>= (get loop-state 'items) (get loop-state 'maxitems))
				(cons 'maxitems (get loop-state 'maxitems)))
			   (and (fifo/exhausted? fifo) 'fifo-empty)
			   (and (exists? stopfn) stopfn
				(exists stopfn loop-state))))
	      (load (fifo/load fifo)))
	  (when stopval
	    (if (zero? load)
		(store! loop-state 'stopped (timestamp))
		(store! loop-state 'stopping (timestamp)))
	    (unless (test loop-state 'stopval)
	      (logwarn |EngineStopping| "Due to circumstance: " stopval)
	      (store! loop-state 'stopval stopval)))
	  (when (and monitors (not (test loop-state 'stopped)))
	    (do-choices (monitor monitors)
	      (cond ((applicable? monitor) (monitor loop-state))
		    ((and (pair? monitor) (applicable? (car monitor)))
		     (let ((testval ((car monitor) loop-state)))
		       (when testval
			 (do-choices (action (if (pair? (cdr monitor))
						 (cadr monitor)
						 (cdr monitor)))
			   (if (applicable? action)
			       (action testval loop-state)
			       (logwarn |BadMonitorAction| action))))))
		    (else (logwarn |BadMonitor| monitor)))))
	  (unless (test loop-state '{stopped stopping})
	    (when (checkpointing? loop-state)
	      (when (or (test loop-state 'checknow) (docheck? loop-state fifo))
		(when (or (test loop-state 'checknow) (check/save? loop-state))
		  (thread/wait! (thread/call engine/checkpoint loop-state fifo)))))
	    (set! batch (get-batch fifo batchsize loop-state fillfn))
	    (set! batchno (1+ batchno))
	    (set! start (elapsed-time))
	    (set! batch-state
	      `#[loop ,loop-state
		 thread ,threadid
		 batchno ,batchno
		 started ,start
		 rusage ,(rusage)])))))
    (info%watch "WorkerFinished" "iterfn" (or (procedure-name iterfn) iterfn))
    (deluge%watch "WorkerFinished" "iterfn" (or (procedure-name iterfn) iterfn)))
  (fifo/release! fifo))

(define (get-batch fifo batchsize loop-state fillfn (fillthresh))
  (default! fillthresh (try (get loop-state 'fillthresh) (quotient (fifo-size fifo) 2)))
  (when (and fillfn (not (test loop-state '{stopping stopped})) 
	     (<= (fifo/load fifo) fillthresh)
	     (fill/start! loop-state))
    (engine/fill! fifo fillfn loop-state))
  (tryif (and (not (test loop-state '{stopped done}))
	      (or fillfn (> (fifo/load fifo) 0)))
    (if batchsize
	(fifo/popvec fifo batchsize)
	(fifo/pop fifo batchsize))))

(define (pick-spacing opts nthreads)
  (let ((spacing (getopt opts 'spacing 0.1)))
    (and spacing
	 (if (number? spacing)
	     spacing
	     (and nthreads 
		  (> nthreads 1)
		  (/~ nthreads (ilog nthreads)))))))

(defambda (get-items-vec items opts (maxitems))
  (set! maxitems (getconfopt opts 'maxitems))
  (if (applicable? items) #()
      (let ((vec (if (and (singleton? items) (vector? items)) 
		     items
		     (choice->vector items))))
	(if (or (not maxitems) (>= maxitems (length vec)))
	    vec
	    (slice vec 0 maxitems)))))

(define (get-engine-threadcount opts max-items fill)
  (let* ((spec (getopt opts 'nthreads (getconfopt opts 'engine:threads)))
	 (nthreads (threadcount spec)))
    (if fill nthreads
	(if (and nthreads max-items (> max-items 0))
	    (min nthreads max-items)
	    nthreads))))

(defambda (engine/run fcn items-arg (opts #f))
  (let* ((loop-inits (getopt opts 'loop))
	 (items (get-items-vec items-arg opts))
	 (fillfn (if (applicable? items-arg) items-arg (getopt opts 'fillfn #f)))
	 (init-items (if (fail? items) 0
			 (if (vector? items) (length items) (|| items))))
	 (max-items (try (getopt opts 'maxitems (if fillfn #f init-items)) #f))
	 ;; How many threads to actually create
	 (nthreads (get-engine-threadcount
		    opts (or max-items (and (not fillfn) init-items))
		    fillfn))
	 (batchsize (get-batchsize opts fillfn init-items nthreads))
	 ;; how much to space the launching of threads to reduce racetrack problems
	 (spacing (pick-spacing opts nthreads))
	 ;; How long to make the queue
	 (queuelen (getopt opts 'queuesize
			   (getopt opts 'size
				   (cond ((and nthreads batchsize) (* nthreads batchsize))
					 (nthreads (* nthreads 16))
					 (batchsize (* 2 batchsize))
					 (else 64)))))
	 (task (getopt opts 'task (init-task-state opts)))
	 (stopfile (try (get task 'stopfile) (getopt opts 'stopfile #t)))
	 (donefile (try (get task 'donefile) (getopt opts 'donefile #t))))
    (when (and stopfile (not (string? stopfile)))
      (set! stopfile (runfile ".stop")))
    (when (and donefile (not (string? donefile)))
      (set! donefile (runfile ".done")))
    (when (and init-items (> init-items queuelen)) (set! queuelen init-items))
    (when (and max-items (> queuelen max-items)) (set! queuelen max-items))
    (when (and max-items nthreads (> nthreads max-items)) 
      (set! nthreads max-items)
      (when batchsize (set! batchsize 1)))
    (let* ((fifo-opts 
	    (frame-create #f
	      'name (getopt opts 'name (or (procedure-name fcn) {}))
	      'size queuelen
	      'maxlen (getopt opts 'maxlen queuelen)
	      'readonly (if fillfn #f #t)))
	   (fifo (->fifo items fifo-opts))
	   (name (getopt opts 'name
			 (getopt loop-inits 'name
				 (or (fifo-name fifo)
				     (procedure-name fcn)
				     (stringout fifo)))))
	   (before (getopt opts 'before #f))
	   (after (getopt opts 'after #f))
	   (stop (getopt opts 'stopfn (getopt opts 'stopfns #f)))
	   (logfns (getopt opts 'logfns {}))
	   (counters {(getopt task 'counters {}) (getopt opts 'counters {})})
	   (count-term (getopt opts 'count-term "items"))
	   (loop-state (frame-create #f
			 'name name
			 'fifo fifo
			 'fillfn (tryif fillfn fillfn)
			 'counters counters
			 ;; Opts can specify 'started to account for work before engine/run was
			 ;; called
			 'started (getopt opts 'started (elapsed-time))
			 'total (getopt opts 'total (getopt task 'total 0))
			 'totalmax (getopt opts 'totalmax {})
			 'batchsize batchsize
			 'maxitems (tryif max-items max-items)
			 'stopfile (tryif stopfile stopfile)
			 'donefile (tryif donefile donefile)
			 'nthreads nthreads
			 'threadtime 0
			 'clocktime 0
			 'fillsize (getopt opts 'fillsize {})
			 'filltime (tryif fillfn 0)
			 'logcontext (getopt opts 'logcontext {})
			 'loglevel (getopt opts 'loglevel {})
			 'logfreq (getopt opts 'logfreq log-frequency)
			 'checkfreq (getopt opts 'checkfreq check-frequency)
			 'checkspace (getopt opts 'checkspace check-spacing)
			 'checktests (getopt opts 'checktests {})
			 'checkpoint (getopt opts 'checkpoint {})
			 'checkpause (getopt opts 'checkpause {})
			 'checksync (getopt opts 'checksync {})
			 'checktime 0
			 'monitors (getopt opts 'monitors {})
			 'onerror (getopt opts 'onerror 'stopall)
			 'loopdump (getopt opts 'loopdump {})
			 'logfns logfns
			 'logcounters (getopt opts 'logcounters {})
			 'logrates (getopt opts 'logrates {})
			 'task task
			 'count-term count-term
			 'queued init-items
			 'opts opts
			 'items 0
			 'cycles 1))
	   (%loglevel (getopt opts 'loglevel %loglevel))
	   (count 0))

      (lognotice |Engine|
	"Using " (or (procedure-name fcn) fcn)
	(cond ((not nthreads) " in the current thread")
	      ((= nthreads 1) " in a single thread")
	      (else (printout " across " nthreads " threads")))
	" to process " 
	(if max-items
	    ($count max-items count-term)
	    (printout "multiple " count-term)))

      (do-choices (init loop-inits)
	(cond ((not init))
	      ((table? init)
	       (do-choices (key (getkeys init))
		 (when (test loop-state key)
		   (logwarn |LoopStateOverwrite|
		     "Overwriting loop-state item " key " from loop init "
		     init))
		 (store! loop-state key (get init key))))
	      ((and (symbol? init) (testopt opts init))
	       (when (test loop-state init)
		 (logwarn |LoopStateOverwrite|
		   "Overwriting loop-state item " init " from loop init "
		   init))
	       (store! loop-state init (get loop-inits init)))
	      ((applicable? init) (init loop-state))
	      (else (logerr |Engine/BadLoopInit|
		      "The value " init " isn't a valid init value"))))

      (when (and (test loop-state 'counters)
		 (exists? (pick (get loop-state 'counters) loop-state)))
	(irritant (pick (get loop-state 'counters) loop-state)
	    |BadCounter| "Overlaps loop state fields"))
      
    ;;; Check arguments
      (do-choices fcn
	(unless (and (applicable? fcn) (overlaps? (procedure-arity fcn) {1 2 3 4}))
	  (irritant fcn |ENGINE/InvalidLoopFn| engine/run)))
      (when (and (exists? before) before)
	(do-choices before
	  (when before
	    (unless (and (applicable? before) (= (procedure-arity before) 4))
	      (irritant before |ENGINE/InvalidBeforeFn| engine/run)))))
      (when (and (exists? after) after)
	(do-choices after
	  (unless (and (applicable? after) (= (procedure-arity after) 4))
	    (irritant after |ENGINE/InvalidAfterFn| engine/run))))
      (when (and (exists? fillfn) fillfn)
	(do-choices fillfn
	  (unless (applicable? fillfn)
	    (irritant after |ENGINE/InvalidFillFn| engine/run))))

      (when (and (exists? logfns) logfns)
	(do-choices (logfn (difference logfns #t))
	  (unless (and (applicable? logfn) 
		       (overlaps? (procedure-arity logfn) {0 1 3 7}))
	    (irritant logfn |ENGINE/InvalidLogfn| engine/run))))

      (cond ((and (<= init-items 0) (not fillfn)))
	    ((and nthreads (> nthreads 1))
	     (let ((threads {}))
	       (dotimes (i nthreads)
		 (set+! threads 
		   (thread/call engine-threadfn
		       fcn fifo opts 
		       loop-state task (or batchsize 1)
		       (qc before) (qc after) (qc fillfn)
		       (getopt opts 'monitors)
		       stop))
		 (when spacing (sleep spacing)))
	       (loginfo |Engine/Threads| fifo fcn
			(do-choices (thread threads)
			  (lineout "  " (thread-id thread) "\t" thread)))
	       (thread/wait threads)))
	    (else (engine-threadfn 
		    fcn fifo opts 
		    loop-state task (or batchsize 1)
		    (qc before) (qc after) (qc fillfn)
		    (getopt opts 'monitors)
		    stop)))

      (when (not fillfn)
	(let* ((elapsed (elapsed-time (get loop-state 'started)))
	       (rate (/ init-items elapsed)))
	  (lognotice |Engine| 
	    "Finished " ($count init-items count-term) " "
	    "in " (secs->string elapsed #t) "  "
	    "averaging " ($showrate rate count-term) "/sec")))

      (unless (test loop-state 'stopped)
	(store! loop-state 'stopped (timestamp))
	(store! loop-state 'stopval 'final))

      (when (and (or (test loop-state 'done)
		     (and (test loop-state 'taskstate)
			  (test (get loop-state 'taskstate) 'done)))
		 (test loop-state 'donefile)
		 (not (file-exists? (get loop-state 'donefile))))
	(statefile/save! (get loop-state 'taskstate) #f
			 (get loop-state 'donefile)))

      (if (getopt opts 'finalcheck #t)
	  (begin
	    (when (checkpointing? loop-state)
	      (engine/checkpoint loop-state fifo #t))
	    (when (getopt opts 'finalcommit #f) (commit)))
	  (begin
	    (lognotice |Engine| "Skipping final checkpoint for ENGINE/RUN")
	    (save-task-state! loop-state)
	    (engine-logger (qc) batchsize 0 (elapsed-time (get loop-state 'started))
			   #[] loop-state task)))

      (when (and (exists? (get loop-state 'errors))
		 (overlaps? (get loop-state 'onerror) 'signal))
	(irritant (get loop-state 'errors)
	    |EngineErrors| engine/run
	    (stringout ($count (choice-size (get loop-state 'errors)) "error")
	      " occurred running " fifo)))

      (when (test loop-state 'done)
	(begin (logwarn |TaskDone| (listdata task))
	  (when (and (test loop-state 'donefile) (not (file-exists? (get loop-state 'donefile))))
	    (fileout (get loop-state 'donefile) "Completed at " (get (timestamp) 'string)))
	  (when (and (getopt opts 'donefile) (not (file-exists? (getopt opts 'donefile))))
	    (fileout (getopt opts 'donefile) "Completed at " (get (timestamp) 'string)))
	  task))

      loop-state)))

(define (get-batchsize opts fill init-items nthreads)
  (local spec (getopt opts 'batchsize)
	 nbatches (getopt opts 'nbatches)
	 maxitems (if (not fill)
		      (min (getopt opts 'maxitems init-items) init-items)
		      (getopt opts 'maxitems)))
  (when (not nthreads) (set! nthreads 1))
  (cond ((and (number? nbatches) maxitems)
	 (->exact (ceiling (/~ maxitems (* nbatches nthreads)))))
	((and (number? spec) maxitems)
	 (min spec (->exact (ceiling (/~ maxitems nthreads)))))
	((number? spec) (->exact spec))
	((and spec maxitems)
	 (/~ (ceiling (/~ maxitems nthreads))))
	(else #f)))

(define (init-task-state opts)
  (let* ((state (or (and (getopt opts 'statefile)
			 (statefile/read (getopt opts 'statefile) opts))
		    #[])))
    (do-choices (counter {(getopt opts 'counters) (get state 'counters)
			  *standard-counters*})
      (unless (or (not counter) (not (symbol? counter)) (test state counter))
	(store! state counter 0)))
    (unless (test state 'started) (store! state 'started (timestamp)))
    (when (getopt opts 'statefile)
      (store! state 'statefile (getopt opts 'statefile))
      (statefile/save! state))
    state))

;;;; Logging

(define (dolog? loop-state (fifo) (freq))
  (default! fifo (get loop-state 'fifo))
  (default! freq (try (get loop-state 'logfreq) #f))
  (with-lock (fifo-condvar fifo)
    (cond ((not freq) #t)
	  ((not (getopt loop-state 'lastlog))
	   (store! loop-state 'lastlog (elapsed-time))
	   ;; Don't do the first log report because it's almost certainly short
	   #f)
	  ((> (elapsed-time (getopt loop-state 'lastlog)) freq)
	   (store! loop-state 'lastlog (elapsed-time))
	   #t)
	  (else #f))))

(define (engine-logger batch batchsize proc-time time batch-state loop-state state)
  (let ((logfns (get loop-state 'logfns))
	(loopdump (try (get loop-state 'loopdump) #f)))
    (when loopdump
      (listdata loop-state #f (open-output-file loopdump)))
    (when (or (fail? logfns) (overlaps? #t logfns))
      (engine/log (qc batch) batchsize proc-time time
		  batch-state loop-state state))
    (do-choices (logfn (difference logfns #t #f))
      (cond ((not (applicable? logfn))
	     (logwarn |Engine/BadLogFn| 
	       "Couldn't apply the log function " logfn)
	     (drop! loop-state 'logfns logfn))
	    ((= (procedure-arity logfn) 0) (logfn))
	    ((= (procedure-arity logfn) 1) (logfn loop-state))
	    ((= (procedure-arity logfn) 3)
	     (logfn batch-state loop-state state))
	    ((= (procedure-arity logfn) 7)
	     (logfn (qc batch) batchsize proc-time time batch-state loop-state state))
	    (else
	     (logwarn |Engine/BadLogFn| 
	       "Couldn't use the log function " logfn)
	     (drop! loop-state 'logfns logfn))))))

;;; The default log function
(define (engine/log batch batchsize coretime time batch-state loop-state state)
  (let* ((count (getopt loop-state 'items 0))
	 (count-term (try (get loop-state 'count-term) "item"))
	 (logrates (get loop-state 'logrates))
	 (loopmax (getopt loop-state 'maxitems))
	 (total (try (get loop-state 'total)  (get state 'total) #f))
	 (totalmax (try (get loop-state 'totalmax)  (get state 'totalmax) #f))
	 (items (try (get loop-state 'items) 0))
	 (elapsed (elapsed-time (get loop-state 'started)))
	 (rate (/~ items elapsed))
	 (%loglevel (getopt loop-state 'loglevel %loglevel))
	 (fifo (get loop-state 'fifo)))
    (when (exists? batch)
      (loginfo |Batch/Progress| 
	(when (testopt (fifo-opts fifo) 'static)
	  (printout "(" (show% (fifo/load fifo) (fifo-size fifo) 2)") "))
	"Processed " ($batchsize batch count-term batchsize) " in " 
	(secs->string (elapsed-time (get batch-state 'started)) 1) " or ~"
	($showrate (/~ (choice-size batch) (elapsed-time (get batch-state 'started)))
		   count-term) 
	"/second for this batch in thread " (threadid)))
    (debug%watch "ENGINE/LOG" loop-state)
    (lognotice |Engine/Progress|
      "Processed " ($count (getopt loop-state 'items 0) count-term)
      (when loopmax
	(printout " (" (show% (getopt loop-state 'items 0) loopmax) " of "
	  ($count loopmax count-term) ")"))
      " in " (secs->string (elapsed-time (get loop-state 'started)) 1) 
      " or ~" ($showrate rate count-term) " per second"
      (doseq (counter (get-log-counters loop-state) i)
	(let ((key (get-log-key counter)))
	  (when (test loop-state key)
	    (let* ((count (get loop-state key))
		   (count-term (get-count-term counter)))
	      (printout (if (zero? (remainder i 5)) ",\n   " ", ")
		($count count count-term)
		(when (overlaps? counter logrates)
		  (let ((rate (/~ count elapsed)))
		    (printout " (" ($showrate rate) " " count-term "/sec)")))))))))
    (when (and (test loop-state 'fillfn)  (test loop-state 'filltime))
      (let ((filltime (get loop-state 'filltime))
	    (clocktime (elapsed-time (get loop-state 'started)))
	    (queued (get loop-state 'queued))
	    (count-term (try (get loop-state 'count-term) "item")))
	(lognotice |Engine/Fill|
	  ($count queued count-term) " have been queued, taking " (secs->string filltime) 
	  " or " (show% filltime clocktime) " of total elapsed time (" (secs->string clocktime) ")")))
    (when loopmax
      (let* ((togo (- loopmax count))
	     (timeleft (/~ togo rate))
	     (finished (timestamp+ (timestamp) timeleft))
	     (timetotal (/~ loopmax rate)))
	(lognotice |Engine/Projection|
	  "At " ($showrate rate count-term) "/sec, "
	  "the loop's " ($count loopmax count-term)
	  " should be finished in " "~" (secs->string timeleft 1)
	  " (~" (get finished 'timestring) 
	  (if (not (equal? (get (timestamp) 'datestring)
			   (get finished 'datestring))) " ")
	  (cond ((equal? (get (timestamp) 'datestring)
			 (get finished 'datestring)))
		((< (difftime finished) (* 24 3600)) "tomorrow")
		((< (difftime finished) (* 24 4 3600))
		 (get finished 'weekday-long))
		(else (get finished 'rfc822date)))
	  ") totalling " (secs->string timetotal 1))))
    (let* ((task (get loop-state 'taskstate))
	   (items (try (get task 'items) #f))
	   (total (try (get task 'total) #f)))
      (when (and items total)
	(let* ((togo (- total items))
	       (timeleft (/~ togo rate))
	       (finished (timestamp+ (timestamp) timeleft))
	       (timetotal (/~ total rate)))
	  (lognotice |Engine/Projection/Total|
	    "At " ($showrate rate count-term) "/sec, "
	    "the task's " ($count total count-term)
	    " should be finished in " "~" (secs->string timeleft 1)
	    " (~" (get finished 'timestring) 
	    (if (not (equal? (get (timestamp) 'datestring)
			     (get finished 'datestring))) " ")
	    (cond ((equal? (get (timestamp) 'datestring)
			   (get finished 'datestring)))
		  ((< (difftime finished) (* 24 3600)) "tomorrow")
		  ((< (difftime finished) (* 24 4 3600))
		   (get finished 'weekday-long))
		  (else (get finished 'rfc822date)))
	    ") totalling " (secs->string timetotal 1)))))))

(define (engine/showrates loop-state)
  (let ((elapsed (elapsed-time (get loop-state 'started)))
	(logrates (get loop-state 'logrates))
	(count-term (getopt loop-state 'count-term "item"))
	(items (get loop-state 'items)))
    (printout
      ($num items) " " count-term " in " (secs->string elapsed) 
      " @ " ($showrate (/ items elapsed) count-term) "/sec, "
      (stringout 
	(do-choices (counter (difference (get loop-state 'counters) 'items) i)
	  (when (test loop-state counter)
	    (let* ((count (get loop-state counter))
		   (rate  (/~ count elapsed)))
	      (printout (if (zero? (remainder i 3)) "\n	  " ", ")
		($num count) " " (downcase counter)
		(when (overlaps? counter logrates)
		  (printout " (" ($showrate rate count-term) "/sec)"))))))))))

(define (engine/logrates batch batchsize coretime time batch-state loop-state state)
  (let ((elapsed (elapsed-time (get loop-state 'started)))
	(items (get loop-state 'items)))
    (lognotice |Engine/Counts| (engine/showrates loop-state))))

(define (engine/showrusage)
  (let* ((usage (rusage))
	 (load (get usage 'loadavg)))
    (printout "cpu=" ($num (get usage 'cpu%) 2) "%; "
      "mem=" ($bytes (memusage)) ", vmem=" ($bytes (vmemusage)) ";\n	"
      "load: " (first load) " · " (second load) " · " (third load) "; "
      "utime=" (compact-interval-string (get usage 'utime)) "; "
      "stime=" (compact-interval-string (get usage 'stime)) "; "
      "elapsed=" (secs->string (get usage 'clock)))))

(define (engine/logrusage batch batchsize coretime time batch-state loop-state state)
  (lognotice |Engine/Resources| (engine/showrusage)))

(define (get-log-counters loop-state)
  (getopt loop-state 'logcounters
	  (choice->vector (difference (get loop-state 'counters) 'items))))

(define (get-log-key counter)
  (if (symbol? counter) counter
      (if (pair? counter) (car counter)
	  (fail))))

(define (get-count-term counter)
  (if (symbol? counter) (downcase counter) 
      (if (pair? counter) (cdr counter)
	  (stringout counter))))

;;; Filling the fifo

(define fill-loglevel #f)
(varconfig! engine:fill:log fill-loglevel config:loglevel)

(define (engine/fill! fifo fillfn loop-state (opts) (fillthresh)
		      (%loglevel (or fill-loglevel %loglevel))
		      (fillstart) (count-term))
  (local added 0)
  (default! opts (try (get loop-state 'opts) #f))
  (default! fillthresh
    (try (get loop-state 'fillthresh)
	 (getopt opts 'fillthresh (quotient (fifo-size fifo) 2))))
  (default! fillstart (getopt opts 'fillstart (elapsed-time)))
  (default! count-term (try (get loop-state 'count-term) "items"))
  (set! %loglevel (try (get loop-state 'loglevel) %loglevel))
  (debug%watch "engine/fill!" fifo fillfn fillthresh "\nloop-state" loop-state)
  (unless (or (test loop-state '{stopped stopping}) (> (fifo/load fifo) fillthresh))
    (if (and (test loop-state 'maxitems) (test loop-state 'queued)
	     (>= (get loop-state 'queued) (get loop-state 'maxitems)))
	(begin (fifo/readonly! fifo #t)
	  (unless (test loop-state 'stopval)
	    (store! loop-state 'stopping (timestamp))
	    (store! loop-state 'stopval (cons 'maxitems (get loop-state 'maxitems)))))
	(let* ((fillsize (get-fill-size loop-state opts fifo))
	       (fillstep (getopt opts 'fillstep fillsize))
	       (need fillsize))
	  (while (and (> need 0) (not (test loop-state '{stopped stopping})))
	    (let ((items (if (= (procedure-arity fillfn) 2)
			     (fillfn (if fillstep (min fillstep need) need) loop-state)
			     (fillfn (if fillstep (min fillstep need) need))))
		  (count 1))
	      (debug%watch "engine/fill!" fifo need "got" (|| items))
	      (when (fail? items)
		(unless (test loop-state '{stopped stopping})
		  (logwarn |Engine/Fill/Failed|
		    "Request for " ($count (if fillsize (min fillsize need)) count-term) " using "
		    ($fn fillfn)))
		(break))
	      (when (and (exists? items) items)
		(cond ((ambiguous? items)
		       (logdetail |EngineFill|
			 "Adding " ($count (|| items) count-term) " to queue " fifo " using " fillfn)
		       (fifo/push/all! fifo (choice->vector items))
		       (set! count (|| items)))
		      ((vector? items)
		       (logdetail |EngineFill|
			 "Adding " ($count (length items) count-term) " to queue " fifo " using " fillfn)
		       (fifo/push/n! fifo items)
		       (set! count (length items)))
		      (else (logdetail |EngineFill|
			      "Adding one item to queue " fifo " using " fillfn)
			    (fifo/push! fifo items [block #f])))
		(set! added (+ added count))
		(set! need (- need count)))))
	  (bump-fillstate! loop-state (elapsed-time fillstart) (- fillsize need))
	  (if (test loop-state '{stopping stopped done})
	      (fifo/readonly! fifo #t)
	      (when (and (test loop-state 'maxitems) (>= (get loop-state 'queued) (get loop-state 'maxitems)))
		(fifo/readonly! fifo #t)
		(unless (test loop-state 'stopval)
		  (store! loop-state 'stopping (timestamp))
		  (store! loop-state 'stopval (cons 'maxitems (get loop-state 'maxitems))))))
	  (logdebug |FinishedFill|
	    "Thread " (threadid) " finished adding " 
	    ($count added (try (get loop-state 'count-term) count-term))
	    " in " (secs->string (elapsed-time fillstart))
	    " to " fifo))))
  (drop! loop-state 'fillthread)
  (store! loop-state 'filldone (elapsed-time)))

;; This is called by the checkpointing thread and avoids having two
;; checkpointing threads at the same time.
(define-init fill/start!
  (slambda (loop-state (%loglevel (or fill-loglevel %loglevel)))
    (if (and (getopt loop-state 'fillthread)
	     (not (test loop-state 'fillthread (threadid))))
	#f
	(begin
	  (store! loop-state 'fillthread (threadid))
	  (store! loop-state 'fillstart (elapsed-time))
	  (logdetail |StartFill| "Thread " (threadid) " is now filling the fifo " (get loop-state 'fifo))
	  #t))))

(define (get-fill-size loop-state opts fifo)
  (smallest {(- (fifo-size fifo) (fifo/load fifo) (* 2 (getopt opts 'fillstep 1)))
	     (getopt opts 'fillsize {})
	     (tryif (test loop-state 'maxitems)
	       (max (- (get loop-state 'maxitems) (try (get loop-state 'queued) 0)) 0))}))

(define-init bump-fillstate!
  (slambda (loop-state secs queued)
    (store! loop-state 'filltime (+ secs (try (get loop-state 'filltime) 0)))
    (store! loop-state 'queued (+ queued (try (get loop-state 'queued) 0)))))

;;; Checkpointing

;;; This is called whenever a batch finishes and returns #t (roughly)
;;; every freq seconds. Note that if there are checktests, they are
;;; actually run (by check/save? below) to determine whether to do a
;;; checkpoint.
(define (docheck? loop-state (fifo) (freq) (space))
  (default! fifo (get loop-state 'fifo))
  (default! freq (try (get loop-state 'checkfreq) #f))
  (and (checkpointing? loop-state)
       (not (test loop-state 'checkthread))
       (with-lock (fifo-condvar fifo)
	 (cond ((not freq) #t)
	       ((not (getopt loop-state 'checking))
		(store! loop-state 'checking (elapsed-time))
		#t)
	       ((> (elapsed-time (getopt loop-state 'checking)) freq)
		(store! loop-state 'checking (elapsed-time))
		#t)
	       (else #f)))))

;;; Tests whether to actually do a checkpoint
(define (check/save? loop-state (fns))
  (default! fns (get loop-state 'checktests))
  (if (fail? fns) #t
      (try (try-choices (fn fns)
	     (tryif (fn loop-state) fn))
	   #f)))

;; This is called by the checkpointing thread and avoids having two
;; checkpointing threads at the same time.
(define-init check/start!
  (slambda (loop-state)
    (if (and (getopt loop-state 'checkthread)
	     (not (test loop-state 'checkthread (threadid))))
	#f
	(begin
	  (store! loop-state 'checkthread (threadid))
	  (store! loop-state 'checkstart (elapsed-time))
	  #t))))

(define (get-check-state loop-state (copy (frame-create #f)))
  (do-choices (slot (choice (get loop-state 'counters) *standard-counters*))
    (when (exists? (get loop-state slot))
      (store! copy slot (get loop-state slot))))
  copy)

;;; Task states:
;;;  'task is the current unsaved state of the task running in the loop
;;;  'task

(define (updated-task-state loop-state)
  (let* ((counters (choice (get loop-state 'counters) *standard-counters*))
	 (start (get loop-state 'task))
	 (task (deep-copy start)))
    (do-choices (counter counters)
      (when (test loop-state counter)
	(if (test loop-state 'absolute counter)
	    (store! task counter (get loop-state counter))
	    (store! task counter (+ (try (get start counter) 0)
				    (get loop-state counter))))))
    (when (test loop-state 'done)
      (store! task 'done (get loop-state 'done)))
    (store! task 'updated (timestamp))
    task))

(define (save-task-state! loop-state)
  (let ((new-task-state (updated-task-state loop-state)))
    (when (test new-task-state 'statefile)
      (statefile/save! new-task-state (get loop-state 'opts)))
    (store! loop-state 'taskstate new-task-state)
    new-task-state))

(define (engine/checkpoint loop-state (fifo) (force #f))
  (default! fifo (get loop-state 'fifo))
  (let ((%loglevel (getopt loop-state 'loglevel %loglevel))
	(opts (get loop-state 'opts))
	(started (elapsed-time))
	(runtime #f)
	(success #f)
	(paused #f))
    (if (or force (check/start! loop-state))
	(unwind-protect 
	    (begin 
	      (logdebug |Engine/Checkpoint| 
		"For " fifo " after loop state=\n  " (void (pprint loop-state)))
	      (when (and fifo (getopt loop-state 'checkpause #t))
		(fifo/pause! fifo 'readwrite)
		(set! paused #t)
		(when (and (not force)
			   (getopt loop-state 'checksync)
			   (not (fifo-pause fifo)))
		  (let ((wait-start (elapsed-time)))
		    (until (fifo/paused? fifo)
		      (condvar/wait (fifo-condvar fifo)))
		    (when (> (elapsed-time wait-start) 1)
		      (lognotice |Engine/Checkpoint| 
			"Waited " (secs->string (elapsed-time wait-start))
			" for FIFO to pause")))))
	      (when (testopt opts 'logchecks 'before)
		(engine-logger (qc) #f 0 (elapsed-time (get loop-state 'started)) 
			       #[] loop-state (get loop-state 'task)))

	      (set! runtime (elapsed-time (try (get loop-state 'checkdone)
					       (get loop-state 'started))))

	      (unless (getopt opts 'dryrun)
		(engine-commit loop-state (get loop-state 'checkpoint)))

	      (save-task-state! loop-state)

	      (store! loop-state 'lastcheck (get-check-state loop-state))

	      (when (getopt opts 'logchecks #f)
		(engine-logger (qc) #f 0 (elapsed-time (get loop-state 'started)) 
			       #[] loop-state (get loop-state 'task)))
	      (when paused (fifo/pause! fifo #f))
	      (set! success #t))
	  (begin (drop! loop-state 'checkthread) ;; lets check/start! work again
	    (store! loop-state 'checkdone (elapsed-time))
	    (if success
		(lognotice |Engine/Checkpoint| 
		  "Saved task state in " (secs->string (elapsed-time started))
		  " for " 
		  (when runtime (printout (secs->string runtime) " of processing on "))
		  fifo)
		(logwarn |Checkpoint/Failed| 
		  "After " (secs->string (elapsed-time started)) " for " fifo))
	    (when fifo (fifo/pause! fifo #f))
	    (when (getconfopt opts 'swapout) (engine-swapout loop-state))))
	(logwarn |BadCheck| 
	  "Declining to checkpoint because check/start! failed: state =\n  "
	  (pprint loop-state)))))

;;; Saving databases

(defambda (engine-commit loop-state dbs (opts))
  (default! opts (getopt loop-state 'opts))
  (let ((modified (knodb/get-modified dbs))
	(%loglevel (getopt loop-state 'loglevel %loglevel))
	(started (elapsed-time))
	(fifo (get loop-state 'fifo)))
    (when (or (test loop-state 'stopped) (exists? modified))
      (lognotice |Checkpoint/Start|
	(if (test loop-state 'stopped) "Final " "Incremental ")
	"checkpoint for " (try (get loop-state 'name) fifo)
	" after " (secs->string (difftime (get loop-state 'started)))
	" and " ($count (- (get loop-state 'items)
			   (try (get (get loop-state 'lastcheck) 'items) 0)))
	" " (get loop-state 'count-term) " committing "
	(choice-size modified) " modified dbs"))
    (when (getopt opts 'precheck)
      (do-choices (precheck (getopt opts 'precheck))
	(cond ((not (applicable? precheck))
	       (logwarn |BadPrecheck| precheck))
	      ((= (procedure-arity precheck) 0) (precheck))
	      (else (precheck loop-state (qc dbs))))))

    (set! started (elapsed-time))

    (knodb/commit dbs (cons [loglevel %loglevel] opts))

    (store! loop-state 'checktime 
      (+ (elapsed-time started)
	 (try (get loop-state 'checktime) 0)))

    (lognotice |Engine/Checkpoint|
      "Committed " (choice-size modified) " dbs "
      (if (fifo-name fifo)
	  (printout "for " (fifo-name fifo)))
      " in " (secs->string (elapsed-time started)))

    (when (testopt opts 'postcheck)
      (do-choices (postcheck (getopt opts 'postcheck {}))
	(let ((started (elapsed-time)))
	  (cond ((not (applicable? postcheck))
		 (logwarn |BadPostcheck| postcheck))
		((= (procedure-arity postcheck) 0)
		 (postcheck)
		 (when (> (elapsed-time started) 0)
		   (lognotice |PostCheckpoint|
		     "Took " (secs->string (elapsed-time started)) " for " postcheck)))
		(else (postcheck loop-state (qc dbs))
		      (when (> (elapsed-time started) 0)
			(lognotice |PostCheckpoint| 
			  "Took " (secs->string (elapsed-time started))
			  " for " postcheck)))))))))

(defambda (engine-swapout loop-state)
  (let ((started (elapsed-time))
	(dbs (getopt loop-state 'caches
		     (getopt (get loop-state 'opts) 'caches)))
	(fifo (get loop-state 'fifo)))
    (cond ((not dbs)
	   (loginfo |EngineSwapout| 
	     "Clearing cached data for " fifo)
	   (swapout)
	   (lognotice |Engine/Swapout| 
	     "Cleared cached data in " (secs->string (elapsed-time started)) "for " fifo))
	  (else (loginfo |Engine/Swapout| 
		  "Clearing cached data from "
		  ($count (choice-size dbs) " databases ")
		  "for " fifo)
		(do-choices (db dbs) (swapout db))
		(lognotice |Engine/Swapout| 
		  "Cleared cached data in " (secs->string (elapsed-time started))  " "
		  "from " ($count (choice-size dbs) " databases ")
		  "for " fifo)))))

(define (inner-commit arg timings start (work #f))
  (cond ((registry? arg) (registry/save! arg))
	((pool? arg) (commit arg))
	((index? arg) (commit arg))
	((and (applicable? arg) (zero? (procedure-min-arity arg))) (arg))
	((and (pair? arg) (applicable? (car arg)))
	 (apply (car arg) (cdr arg)))
	(else (logwarn |Engine/CantSave| "No method for saving " arg) #f))
  (store! timings arg 
    (if work (cons work (elapsed-time start)) (elapsed-time start)))
  arg)

(define (commit-db arg opts timings (start (elapsed-time)))
  (onerror (inner-commit arg timings start)
      (lambda (ex)
	(store! timings arg (- (elapsed-time start)))
	(logwarn |Engine/CommitError| "Error committing " arg ": " ex)
	ex)))

(define (commit-queued fifo opts timings)
  (let ((db (fifo/pop fifo)))
    (while (and (exists? db) db)
      (commit-db db opts timings)
      (set! db (fifo/pop fifo)))))

;;;; Utility functions

(define (engine/fetchoids oids (batch-state #f) (loop-state #f) (state #f))
  (if (vector? oids)
      (prefetch-oids! (elts oids))
      (prefetch-oids! oids)))
(define (engine/fetchkeys keys (batch-state #f) (loop-state #f) (state #f))
  (if (vector? keys)
      (prefetch-keys! (elts keys))
      (prefetch-keys! keys)))
(define (engine/swapout args (batch-state #f) (loop-state #f) (state #f))
  (if (vector? args)
      (swapout (elts args))
      (swapout args)))
(define (engine/lockoids oids (batch-state #f) (loop-state #f) (state #f))
  (cond ((vector? oids) 
	 (let ((oids (elts oids)))
	   (lock-oids! oids)
	   (prefetch-oids! oids)))
	(else 
	 (lock-oids! oids)
	 (prefetch-oids! oids))))

;;;; Utility meta-functions

(define (engine/poolfetch pool)
  (ambda (oids (batch-state #f) (loop-state #f) (task-state #f)) 
    (pool-prefetch! pool oids)))

(define (engine/indexfetch index)
  (ambda (keys (batch-state #f) (loop-state #f) (task-state #f))
    (prefetch-keys! index keys)))

(define (engine/savepool pool)
  (ambda (keys (batch-state #f) (loop-state #f) (task-state #f))
    (commit pool)))

(define (engine/saveindex index)
  (ambda (keys (batch-state #f) (loop-state #f) (task-state #f))
    (commit index)))

(define (engine/savetable table file)
  (ambda (keys (batch-state #f) (loop-state #f) (task-state #f))
    (dtype->file table file)))

(define (engine/interval interval (slot #f) (last (elapsed-time)))
  (defsync (engine/interval/wait (loop-state #f))
    (cond ((and slot (test loop-state slot))
	   (> (elapsed-time (get loop-state slot)) interval))
	  ((> (elapsed-time last) interval)
	   (set! last (elapsed-time))
	   #t)
	  (else #f))))

(define (engine/usage field max)
  (lambda ((loop-state #f)) (> (rusage field) max)))

(define (engine/maxitems max-count)
  (slambda ((loop-state #f))
    (cond ((> (getopt loop-state 'items 0) max-count)
	   #t)
	  (else #f))))

(define (engine/stopfn (opts #f))
  (let ((maxtime (getopt opts 'maxtime #f))
	(maxcount (getopt opts 'maxcount #f))
	(maxbatches (getopt opts 'maxbatches #f))
	(maxmem (getopt opts 'maxmem #f))
	(maxvmem (getopt opts 'maxvmem #f))
	(maxload (getopt opts 'maxload #f))
	(stopfile (getopt opts 'stopfile #f))
	(donefile (getopt opts 'donefile #f))
	(stopconf (getopt opts 'stopconf #f)))
    (lambda (loop-state)
      (or (test loop-state 'stopped)
	  (and (or (and maxcount (> (getopt loop-state 'items 0) maxcount))
		   (and maxbatches (> (get loop-state 'batches) maxbatches))
		   (and maxtime 
			(> (elapsed-time (get loop-state 'started))
			   maxtime))
		   (and maxload
			(if (number? maxload)
			    (> (getload) maxload)
			    (and (vector? maxload) (= (length maxload) 1)
				 (number? (elt maxload 0))
				 (every? (lambda (loadval) (> loadval (elt maxload 0)))
					 (rusage 'loadavg)))))
		   (and maxvmem (> (vmemusage) maxvmem))
		   (and maxmem (> (memusage) maxmem))
		   (and (string? stopfile) (file-exists? stopfile))
		   (and stopconf (config stopconf))
		   (and (test loop-state 'stopfile)
			(string? (get loop-state 'stopfile))
			(file-exists? (get loop-state 'stopfile)))
		   (and (string? donefile) (file-exists? donefile))
		   (and (test loop-state 'donefile)
			(string? (get loop-state 'donefile))
			(file-exists? (get loop-state 'donefile))))
	       (begin (store! loop-state 'stopped (timestamp))
		 #t))))))

(define (engine/callif (opts #f) call)
  (if (not (and (applicable? call) (overlaps? (procedure-arity call) {1 0})))
      (irritant call |NotApplicable| engine/callif)
      (let ((maxtime (getopt opts 'maxtime #f))
	    (maxcount (getopt opts 'maxcount #f))
	    (maxbatches (getopt opts 'maxbatches #f))
	    (maxmem (getopt opts 'maxmem #f))
	    (maxvmem (getopt opts 'maxvmem #f))
	    (filename (getopt opts 'filename #f))
	    (confname (getopt opts 'confname #f))
	    (maxload (getopt opts 'maxload #f))
	    ;; Keep at least *interval* seconds between calls
	    (interval (getopt opts 'interval 1))
	    (last-call #f))
	(let ((clear?
	       (slambda ()
		 (cond ((not interval) #t)
		       ((< (elapsed-time last-call) interval) #f)
		       (else (set! last-call (elapsed-time)) #t)))))
	  (lambda (loop-state)
	    (unless last-call
	      (set! last-call (getopt loop-state 'started (elapsed-time))))
	    (when (or (not interval) 
		      (and (> (elapsed-time last-call) interval) (clear?)))
	      (when (or (and maxcount (> (getopt loop-state 'items 0) maxcount))
			(and maxbatches (> (get loop-state 'batches) maxbatches))
			(and maxtime 
			     (> (elapsed-time (get loop-state 'started))
				maxtime))
			(and maxvmem (> (vmemusage) maxvmem))
			(and maxmem (> (memusage) maxmem))
			(and maxload
			     (if (number? maxload)
				 (> (getload) maxload)
				 (and (vector? maxload) (= (length maxload) 1)
				      (number? (elt maxload 0))
				      (every? (lambda (loadval) (> loadval (elt maxload 0)))
					      (rusage 'loadavg)))))
			(and confname (config confname))
			(and filename (file-exists? filename)))
		(if (zero? (procedure-arity call))
		    (call)
		    (call loop-state)))))))))

(define (BAD-CLAUSE clause)
  (logerr |BadEngineTestClause| clause " (assuming false)")
  #f)

(define (test-clause clause loop-state sysinfo)
  (cond ((applicable? clause) (clause loop-state sysinfo))
	((not (pair? clause)) (BAD-CLAUSE clause))
	((applicable? (car clause))
	 (apply (car clause) loop-state sysinfo (cdr clause)))
	((or (symbol? (car clause)) (applicable? (car clause)))
	 (let* ((field (car clause))
		(current (if (applicable? field)
			     (field loop-state sysinfo)
			     (try (get loop-state field)
				  (get sysinfo field))))
		(args (cdr clause)))
	   (cond ((fail? current) #f)
		 ((pair? args)
		  (let ((arg (car args))
			(rest (cdr args)))
		    (cond ((number? arg) (and (number? current)) (>= current args))
			  ((timestamp? arg) 
			   (and (timestamp? current) (time-later? current args)))
			  ((applicable? arg) (apply arg current rest))
			  ((overlaps? current arg) #t)
			  (else (BAD-CLAUSE clause)))))
		  ((number? args) (and (number? current) (>= current args)))
		  ((timestamp? args) (and (timestamp? current) (time-later? current args)))
		  ((applicable? args) (args current))
		  (else (BAD-CLAUSE clause)))))
	(else (BAD-CLAUSE clause))))

(define (looptest tests loop-state sysinfo)
  (if (null? tests) #f
      (let ((ok (test-clause (car tests) sysinfo loop-state)))
	(if (and (exists? ok) ok) (car tests)
	    (looptest (cdr tests) loop-state sysinfo)))))

(define (fifo-empty? loop-state)
  (zero? (fifo/load (get loop-state 'fifo))))

(defambda (engine/test . spec-args)
  (let ((tests '()) (spec spec-args) (clause #f))
    (while (and (pair? spec) (pair? (cdr spec)))
      (cond ((applicable? (car spec))
	     (unless (fail? (cadr spec))
	       (set! tests (cons (car spec) (cadr spec))))
	     (set! spec (cddr spec)))
	    ((not (symbol? (car spec)))
	     (irritant spec-args |BadTestSpec| engine/looptest))
	    ((fail? (cadr spec))
	     (set! spec (cddr spec)))
	    ((not (applicable? (cadr spec)))
	     (set! tests (cons (cons (car spec) (cadr spec)) tests))
	     (set! spec (cddr spec)))
	    ((= (procedure-arity (cadr spec)) 1)
	     (set! tests (cons (list (car spec) (cadr spec)) tests))
	     (set! spec (cddr spec)))
	    (else
	     (if (< (length spec) 3) (irritant spec-args |BadTestSpec| engine/test))
	     (set! tests (cons (list (car spec) (cadr spec) (caddr spec))
			       tests))
	     (set! spec (cdddr spec)))))
    (unless (null? spec) (irritant spec-args |BadTestSpec| engine/test))
    (if (null? tests)
	(fail)
	(defn (engine/tester (loop-state #[]))
	  (looptest tests loop-state (rusage))))))

(define (engine/delta slot . test)
  (lambda ((loop-state #f))
    (let ((v (get loop-state slot))
	  (past (try (get (get loop-state 'lastcheck) slot) 0)))
      (and (exists? v) (exists? past)
	   (if (applicable? (car test))
	       (apply (car test) (- v past) (cdr test))
	       (if (number? (car test))
		   (> (- v past) (car test))
		   #f))))))

;;;; Stopping engines

(define (engine/stop! loop-state (reason #f) (graceful #t))
  (unless (test loop-state 'stopval)
    (store! loop-state 'stopval reason)
    (store! loop-state 
	(if graceful 'stopping 'stopped)
      (timestamp)))
  (if graceful
      (fifo/readonly! (get loop-state 'fifo))
      (fifo/close! (get loop-state 'fifo))))


