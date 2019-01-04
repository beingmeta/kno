;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2019 beingmeta, inc.  All rights reserved.

(in-module 'engine)

(use-module '{fifo varconfig mttools stringfmts reflection 
	      bugjar bugjar/html logger})
(use-module '{storage/flex storage/registry storage/aggregates})

(define %loglevel %notice%)

(module-export! '{engine/run  
		  engine/checkpoint
		  engine/getopt
		  engine/test
		  batchup})

(module-export! '{engine/showrates engine/showrusage
		  engine/log engine/logrusage engine/logrates})

(module-export! '{engine/fetchoids engine/fetchkeys
		  engine/poolfetch engine/indexfetch engine/swapout
		  engine/savepool engine/saveindex engine/savetable})

(module-export! '{engine/stopfn engine/callif
		  engine/interval engine/maxitems
		  engine/usage
		  engine/delta})

;;;; Configurable

(define engine-bugjar #f)
(varconfig! engine:bugjar engine-bugjar)

(define log-frequency 60)
(varconfig! engine:logfreq log-frequency)

(define check-frequency 15)
(varconfig! engine:checkfreq check-frequency)

(define check-spacing 60)
(varconfig! engine:checkspace check-spacing)

(define-import fifo-condvar 'fifo)

(define ($showrate rate)
  (if (> rate 50)
      ($num (->exact rate 0))
      ($num rate)))

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

1. BEFOREFN is called on the batch, the batch-state, the loop-state, and the
task state)

2. PROCESSFN is the item function itself. It is called on each element
of the batch. If it takes two arguments, it is called on the item and
the batch state. If it takes 4 arguments, it is called on the item,
batch state, loop state, and task state.

3. AFTERFN is called on the batch, the batch-state, the loop-state, and the
task state)

4. The 'loop state' is then 'bumped'. This updates the total
'items' (number of items processed), total 'batches' (number of
batches processed), and 'vtime' (how much realtime this thread spent
on the batch).

5. LOGFN is then called on:
* the items processed;
* the clock time spent on PROCESS
* the clock time spent on BEFORE+PROCESS+AFTER
* the batch state
* the loop state
* the task state

5. The STOPFNs are then called and if any return non-false, the loop
'stops'. This means that the current thread stops processing batches
and all of the other threads stop when they're done with their current
batch.

6. If the loop hasn't stopped, the loop's *monitors* are called. A
monitor is either a function or a pair of functions. In the first
case, the function is simply called on the loop-state; in the second,
the CAR is called on the loop-state and the CDR is called if the CAR
returns #t.

The monitors can stop the loop by storing a value in the 'stopped
slot of the loop state.

|#

;;; These functions divide big sets (choice or vector) of items into smaller batches.
;;; The batches (except for the last one) are all some multiple of
;;; *batchsize* items; the multiple is in the range [1,*batchrange*].
;;; It returns a vector of those batches.

(define (batch-factor (batchrange 1))
  (if (number? batchrange)
      (1+ (random batchrange))
      (if (and (pair? batchrange) 
	       (number? (car batchrange))
	       (number? (cdr batchrange)))
	  (+ (car batchrange) (random (- (cdr batchrange) (car batchrange))))
	  (if (and (pair? batchrange) (pair? (cdr batchrange)) 
		   (number? (car batchrange))
		   (number? (cadr batchrange)))
	      (+ (car batchrange) (random (- (cadr batchrange) (car batchrange))))
	      1))))

(defambda (batchup items batchsize (batchrange 1))
  (let ((n (choice-size items))
	(batchlist '())
	(start 0))
    (while (< start n)
      (let ((step (* (batch-factor batchrange) batchsize)))
	(set! batchlist (cons (qc (pick-n items step start)) batchlist))
	(set! start (+ start step))))
    (reverse (->vector batchlist))))

(define (batchup-vector items batchsize (batchrange 1) (vecbatches #t))
  (let ((n (length items))
	(batchlist '())
	(start 0))
    (while (< start n)
      (let* ((step (* (batch-factor batchrange) batchsize))
	     (limit (min (+ start step) n)))
	(set! batchlist (cons (slice items start limit) batchlist))
	(set! start (+ start step))))
    (if vecbatches
	(reverse (->vector batchlist))
	(map elts (reverse (->vector batchlist))))))

(define (checkpointing? loop-state)
  (getopt (get loop-state 'opts) 'checkpointing
	  (or (and (exists? (get loop-state 'checkpoint)) (get loop-state 'checkpoint))
	      (and (exists? (getopt (get loop-state 'opts) 'statefile))
		   (getopt (get loop-state 'opts) 'statefile)))))

;;; We make this handler unique right now, but it could be engine specific.
(define-init bump-loop-state
  (slambda (batch-state loop-state (count #f) (proc-time #f) (thread-time #f))
    (store! loop-state 'batches (1+ (getopt loop-state 'batches 0)))
    (when count
      (store! loop-state 'items 
	      (+ (getopt loop-state 'items 0) count)))
    (when proc-time
      (store! loop-state 'vproctime 
	      (+ (getopt loop-state 'vproctime 0.0) proc-time)))
    (when thread-time
      (store! loop-state 'vtime 
	      (+ (getopt loop-state 'vtime 0.0) thread-time)))
    (do-choices (counter (get loop-state 'counters))
      (store! loop-state counter
	      (+ (try (get loop-state counter) 0)
		 (try (get batch-state counter) 0))))))

(defambda (engine/getopt loop-state optname (default #f))
  (getopt loop-state optname
	  (getopt (getopt loop-state 'state) optname
		  (getopt loop-state 'opts) optname
		  default)))

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
    (when (exception-irritant? ex)
      (printout "\n" (void (pprint (exception-irritant ex)))))
    (when saved (printout " exception dumped to " (write saved))))
  ;;(dump-bug ex)
  (when (exception-irritant? ex)
    (loginfo |EngineError/irritant| 
      "For " (get loop-state 'fifo) " "
      (exception-condition ex) " @" (exception-caller ex) ":\n  "
      (void (pprint (exception-irritant ex)))))
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
      "Unexpected " condition " in " caller (if details (printout " (" details ")"))
      "\nsaved to " refpath
      (when (error-irritant? ex)
	(printout "\nirritant: " (listdata (error-irritant ex)))))
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

(define (engine-threadfn iterfn fifo opts loop-state state
			 beforefn afterfn fillfn
			 monitors
			 stopfn)
  "This is then loop function for each engine thread."
  (when (and (not (test loop-state 'stopping)) fillfn (getopt opts 'fillmin)) 
    (engine/fill! fifo fillfn loop-state 
		  (getopt opts 'fillmin)
		  (getopt opts 'fillmax (* 2 (getopt opts 'fillmin)))))
  (let* ((batch (fifo/pop fifo))
	 (batch-state `#[loop ,loop-state started ,(elapsed-time) batchno 1])
	 (start (get batch-state 'started))
	 (proc-time #f)
	 (batchno 1))
    (while (and (exists? batch) batch)
      (loginfo |GotBatch| "Processing batch of " ($num (choice-size batch)) " items")
      (do-choices (indexslot (getopt opts 'branchindexes))
	(when (test loop-state indexslot)
	  (store! batch-state indexslot (aggregate/branch (get loop-state indexslot)))))
      (when (and (exists? beforefn) beforefn)
	(beforefn (qc batch) batch-state loop-state state))
      (set! proc-time (elapsed-time))
      (onerror
	  (cond ((singleton? batch)
		 (cond ((= (procedure-arity iterfn) 1) (iterfn batch))
		       ((= (procedure-arity iterfn) 2) (iterfn batch batch-state))
		       ((= (procedure-arity iterfn) 4) (iterfn batch batch-state loop-state state))
		       (else (iterfn batch))))
		((and (getopt opts 'batchcall (non-deterministic? iterfn))
		      (non-deterministic? iterfn))
		 (cond ((= (procedure-arity iterfn) 1) (iterfn batch))
		       ((= (procedure-arity iterfn) 2) (iterfn batch batch-state))
		       ((= (procedure-arity iterfn) 4) (iterfn batch batch-state loop-state state))
		       (else (iterfn batch))))
		((getopt opts 'batchcall (non-deterministic? iterfn))
		 (cond ((= (procedure-arity iterfn) 1) (iterfn (qc batch)))
		       ((= (procedure-arity iterfn) 2) (iterfn (qc batch) batch-state))
		       ((= (procedure-arity iterfn) 4) (iterfn (qc batch) batch-state loop-state state))
		       (else (iterfn (qc batch)))))
		(else
		 (cond ((= (procedure-arity iterfn) 1)
			(do-choices (item batch) (iterfn item)))
		       ((= (procedure-arity iterfn) 2)
			(do-choices (item batch) (iterfn item batch-state)))
		       ((= (procedure-arity iterfn) 4)
			(do-choices (item batch) (iterfn item batch-state loop-state state)))
		       (else (do-choices (item batch) (iterfn item))))))
	  (engine-error-handler batch-state loop-state))
      (set! proc-time (elapsed-time proc-time))
      (do-choices (indexslot (getopt opts 'branchindexes))
	(when (test batch-state indexslot) 
	  (aggregate/merge! (get batch-state indexslot))))
      (unless (test batch-state 'aborted)
	(when (and  (exists? afterfn) afterfn)
	  (afterfn (qc batch) batch-state loop-state state))
	(bump-loop-state batch-state loop-state
			 (choice-size batch)
			 proc-time
			 (elapsed-time start))
	(when (dolog? loop-state fifo)
	  (engine-logger (qc batch) proc-time (elapsed-time start)
			 batch-state loop-state state)))
      ;; Free some stuff up, maybe
      (set! batch {})
      (let ((stopval (or (try (get batch-state 'stopval) #f)
			 (try (get batch-state 'error) #f)
			 (test loop-state 'stopped)
			 (and (exists? stopfn) stopfn (exists stopfn loop-state)))))
	(cond ((and (exists? stopval) stopval 
		    (or (fail? fillfn) (identical? fillfn #f)))
	       (store! loop-state 'stopped (timestamp))
	       (store! loop-state 'stopval stopval))
	      ((and (exists? stopval) stopval)
	       (if (zero? (fifo/load fifo))
		   (store! loop-state 'stopped (timestamp))
		   (store! loop-state 'stopping (timestamp)))
	       (unless (test loop-state 'stopval)
		 (store! loop-state 'stopval stopval))))
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
	(cond ((getopt loop-state 'stopped)
	       (set! batch {}))
	      ((zero? (fifo/load fifo))
	       ;; Don't consider checkpointing unless there is more
	       ;; work to do, expecting that the final checkpoint will
	       ;; handle it.
	       (set! batch {}))
	      (else
	       (unless (getopt loop-state 'stopping)
		 (when (checkpointing? loop-state)
		   (when (or (test loop-state 'checknow) (docheck? loop-state fifo))
		     (when (or (test loop-state 'checknow) (check/save? loop-state))
		       (thread/wait! (thread/call engine/checkpoint loop-state fifo))))))
	       (set! batch (fifo/pop fifo))
	       (set! batchno (1+ batchno))
	       (set! start (elapsed-time))
	       (set! batch-state
		 `#[loop ,loop-state started ,start batchno ,batchno])))))))

(define (pick-spacing opts nthreads)
 (let ((spacing (getopt opts 'spacing 0.1)))
   (and spacing
	(if (number? spacing)
	    spacing
	    (and nthreads 
		 (> nthreads 1)
		 (/~ nthreads (ilog nthreads)))))))

(defambda (pick-batchsize items opts)
  (and (vector? items)
       (let ((n-items (length items)))
	 (if (number? (getopt opts 'nbatches))
	     (1+ (quotient n-items (getopt opts 'nbatches)))
	     (if (number? (getopt opts 'nthreads))
		 (1+ (quotient n-items (* 4 (getopt opts 'nthreads))))
		 (->exact (ceiling (sqrt n-items))))))))
  
(defambda (get-items-vec items opts (max))
  (set! max (getopt opts 'maxitems (config 'maxitems)))
  (if (applicable? items)
      items
      (let ((vec (if (and (singleton? items) (vector? items)) 
		     items
		     (choice->vector items))))
	(if (or (not max) (>= max (length vec)))
	    vec
	    (slice vec 0 max)))))

(defambda (engine/run fcn items-arg (opts #f))
  (let* ((items (get-items-vec items-arg opts))
	 (n-items (and (vector? items) (length items)))
	 (batchsize (getopt opts 'batchsize (pick-batchsize items opts)))
	 (nthreads (mt/threadcount (getopt opts 'nthreads (config 'engine:threads (config 'nthreads #t)))))
	 (spacing (pick-spacing opts nthreads))
	 (batchrange (getopt opts 'batchrange (config 'batchrange 3)))
	 (batches (if (and batchsize (> batchsize 1))
		      (batchup-vector items batchsize batchrange
				      (and (singleton? items-arg)
					   (vector? items-arg)))
		      items))
	 (rthreads (if (and nthreads (> nthreads (length batches)))
		       (length batches)
		       nthreads))
	 (fifo-opts 
	  (frame-create #f
	    'name (getopt opts 'name (or (procedure-name fcn) {}))
	    'fillfn (getopt opts 'fillfifo fifo/exhausted!)))
	 (fifo (if (number? batches)
		   (fifo/make batches fifo-opts)
		   (->fifo batches fifo-opts)))
	 (fill (getopt opts 'fill #f))
	 (before (getopt opts 'before #f))
	 (after (getopt opts 'after #f))
	 (stop (getopt opts 'stopfn #f))
	 (state (getopt opts 'state (init-state opts)))
	 (logfns (getopt opts 'logfns {}))
	 (counters {(getopt state 'counters {}) (getopt opts 'counters {})})
	 (loop-init (getopt opts 'loop))
	 (count-term (getopt opts 'count-term "items"))
	 (loop-state (frame-create #f
		       'fifo fifo
		       'counters counters
		       'started (getopt opts 'started (elapsed-time))
		       'total (tryif (vector? items) n-items)
		       'batchsize batchsize
		       'batchrange batchsize
		       'n-batches (tryif (vector? batches) (length batches))
		       'nthreads rthreads
		       '%loglevel (getopt opts 'loglevel {})
		       'logfreq (getopt opts 'logfreq log-frequency)
		       'checkfreq (getopt opts 'checkfreq check-frequency)
		       'checkspace (getopt opts 'checkspace check-spacing)
		       'checktests (getopt opts 'checktests {})
		       'checkpoint (getopt opts 'checkpoint {})
		       'checkpause (getopt opts 'checkpause {})
		       'checksync (getopt opts 'checksync {})
		       'monitors (getopt opts 'monitors {})
		       'onerror (getopt opts 'onerror 'stopall)
		       'logfns logfns
		       'logcounters (getopt opts 'logcounters {})
		       'logrates (getopt opts 'logrates {})
		       'init (deep-copy state)
		       'state (deep-copy state)
		       'count-term count-term
		       'nthreads nthreads
		       'opts opts
		       'cycles 1))
	 (count 0))
    
    (when (table? loop-init)
      (do-choices (key (getkeys loop-init))
	(unless (test loop-state key)
	  (add! loop-state key (get loop-init key)))))
  
    ;;; Check arguments
    (do-choices fcn
      (unless (and (applicable? fcn) (overlaps? (procedure-arity fcn) {1 2 4}))
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
    (when (and (exists? fill) fill)
      (do-choices fill
	(unless (applicable? fill)
	  (irritant after |ENGINE/InvalidFillFn| engine/run))))

    (when (and (exists? logfns) logfns)
      (do-choices (logfn (difference logfns #t))
	(unless (and (applicable? logfn) (overlaps? (procedure-arity logfn) {1 3 6}))
	  (irritant logfn |ENGINE/InvalidLogfn| engine/run))))

    (if (vector? batches)
	(lognotice |Engine| 
	  "Processing " ($count n-items) " " count-term " "
	  (when (and batchsize (> batchsize 1))
	    (printout "in " ($count (length batches)) " batches "))
	  "using " (or rthreads "no") " threads with "
	  (if (procedure-name fcn)
	      (printout (procedure-name fcn)
		(if (procedure-filename fcn)
		    (printout ":" (procedure-filename fcn))))
	      fcn)))

    (if (and rthreads (> rthreads 1))
	(let ((threads {}))
	  (dotimes (i rthreads)
	    (set+! threads 
	      (thread/call engine-threadfn 
		  fcn fifo opts 
		  loop-state state 
		  (qc before) (qc after) (qc fill)
		  (getopt opts 'monitors)
		  stop))
	    (when spacing (sleep spacing)))
	  (thread/wait threads))
	(engine-threadfn 
	 fcn fifo opts 
	 loop-state state 
	 (qc before) (qc after) (qc fill)
	 (getopt opts 'monitors)
	 stop))
    
    (let* ((elapsed (elapsed-time (get loop-state 'started)))
	   (rate (/ n-items elapsed)))
      (lognotice |Engine| 
	"Finished " ($count n-items) " " count-term " "
	(when (and batchsize (> batchsize 1))
	  (printout "across " ($count (length batches)) " batches "))
	"in " (secs->string elapsed) " "
	"averaging " ($showrate rate) " " count-term "/sec"))

    (unless (test loop-state 'stopped)
      (store! loop-state 'stopped (timestamp))
      (store! loop-state 'stopval 'final))

    (if (getopt opts 'finalcheck #t)
	(begin
	  (when (checkpointing? loop-state)
	    (engine/checkpoint loop-state fifo #t))
	  (when (getopt opts 'finalcommit #f) (commit)))
	(begin
	  (lognotice |Engine| "Skipping final checkpoint for ENGINE/RUN")
	  (do-choices (counter counters)
	    (store! state counter 
		    (+ (try (get state counter) 0)
		       (try (get loop-state counter) 0))))
	  (engine-logger (qc) 0 (elapsed-time (get loop-state 'started))
			 #[] loop-state state)))

    (when (and (exists? (get loop-state 'errors))
	       (overlaps? (get loop-state 'onerror) 'signal))
      (irritant (get loop-state 'errors)
	  |EngineErrors| engine/run
	  (stringout ($num (choice-size (get loop-state 'errors)))
	    " errors occurred running " fifo)))
    loop-state))

(define (init-state opts)
  (let* ((state (or (and (getopt opts 'statefile)
			 (if (file-exists? (getopt opts 'statefile))
			     (file->dtype (getopt opts 'statefile))
			     #[]))
		    #[])))
    (do-choices (counter {(getopt opts 'counters) (get state 'counters)
			  'items 'batches 'cycles})
      (unless (or (not counter) (test state counter))
	(store! state counter 0)))
    (unless (test state 'started) (store! state 'started (timestamp)))
    (when (getopt opts 'statefile)
      (store! state 'statefile (getopt opts 'statefile))
      (store! state 'updated (timestamp))
      (dtype->file state (getopt opts 'statefile)))
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

(define (engine-logger batch proc-time time batch-state loop-state state)
  (let ((logfns (get loop-state 'logfns)))
    (when (or (fail? logfns) (overlaps? #t logfns))
      (engine/log (qc batch) proc-time time
		  batch-state loop-state state))
    (do-choices (logfn (difference logfns #t #f))
      (cond ((not (applicable? logfn))
	     (logwarn |Engine/BadLogFn| 
	       "Couldn't apply the log function " logfn)
	     (drop! loop-state 'logfns logfn))
	    ((= (procedure-arity logfn) 1)
	     (logfn loop-state))
	    ((= (procedure-arity logfn) 3)
	     (logfn batch-state loop-state state))
	    ((= (procedure-arity logfn) 6)
	     (logfn (qc batch) proc-time time batch-state loop-state state))
	    (else
	     (logwarn |Engine/BadLogFn| 
	       "Couldn't use the log function " logfn)
	     (drop! loop-state 'logfns logfn))))))

;;; The default log function
(define (engine/log batch proctime time batch-state loop-state state)
  (let* ((count (getopt loop-state 'items 0))
	 (count-term (try (get loop-state 'count-term) "items"))
	 (logrates (get loop-state 'logrates))
	 (loopmax (getopt loop-state 'total))
	 (total (getopt state 'total))
	 (taskmax (getopt state 'total))
	 (items (try (get loop-state 'items) 0))
	 (elapsed (elapsed-time (get loop-state 'started)))
	 (rate (/~ items elapsed))
	 (%loglevel (getopt loop-state '%loglevel %loglevel))
	 (fifo (getopt loop-state 'fifo)))
    (when (exists? batch)
      (loginfo |Batch/Progress| 
	(when (testopt (fifo-opts fifo) 'static)
	  (printout "(" (show% (fifo/load fifo) (fifo-size fifo) 2)") "))
	"Processed " ($num (choice-size batch)) " " count-term " in " 
	(secs->string (elapsed-time (get batch-state 'started)) 1) " or ~"
	($showrate (/~ (choice-size batch) (elapsed-time (get batch-state 'started))))
	" " count-term "/second for this batch and thread."))
    (debug%watch "ENGINE/LOG" loop-state)
    (lognotice |Engine/Progress|
      "Processed " ($num (getopt loop-state 'items 0)) " " count-term
      (when loopmax
	(printout " (" (show% (getopt loop-state 'items 0) loopmax) " of "
	  ($num loopmax) " " count-term ")"))
      " in " (secs->string (elapsed-time (get loop-state 'started)) 1) 
      ", averaging " ($showrate rate) " " count-term "/second."
      (if (testopt loop-state 'logcounters)
	  (doseq (counter (getopt loop-state 'logcounters) i)
	    (let* ((count (get loop-state counter))
		   (rate (/~ count elapsed)))
	      (printout (if (zero? (remainder i 4)) "\n   " ", ")
		($num count) " " (downcase counter)
		(when (overlaps? counter logrates)
		  (printout " (" ($showrate rate) " " (downcase counter) "/sec)")))))
	  (do-choices (counter (difference (get loop-state 'counters) 'items) i)
	    (let* ((count (get loop-state counter))
		   (rate  (/~ count elapsed)))
	      (printout (if (zero? (remainder i 4)) "\n   " ", ")
		($num count) " " (downcase counter)
		(when (overlaps? counter logrates)
		  (printout " (" ($showrate rate) " " (downcase counter) "/sec)"))))))
      (cond (loopmax
	     (let* ((togo (- loopmax count))
		    (timeleft (/~ togo rate))
		    (finished (timestamp+ (timestamp) timeleft))
		    (timetotal (/~ loopmax rate)))
	       (printout "\nAt " ($showrate rate) " " count-term "/sec, "
		 "the loop's " ($num loopmax) " " count-term " should be finished in "
		 "~" (secs->string timeleft 1) " (~"
		 (get finished 'timestring) 
		 (if (not (equal? (get (timestamp) 'datestring)
				  (get finished 'datestring))) " ")
		 (cond ((equal? (get (timestamp) 'datestring)
				(get finished 'datestring)))
		       ((< (difftime finished) (* 24 3600)) "tomorrow")
		       ((< (difftime finished) (* 24 4 3600))
			(get finished 'weekday-long))
		       (else (get finished 'rfc822date)))
		 ") totalling " (secs->string timetotal 1))))))))

(define (engine/showrates loop-state)
  (let ((elapsed (elapsed-time (get loop-state 'started)))
	(logrates (get loop-state 'logrates))
	(count-term (getopt loop-state 'count-term "items"))
	(items (get loop-state 'items)))
    (printout
      ($num items) " " count-term " in " (secs->string elapsed) 
      " @ " ($showrate (/ items elapsed)) " " count-term "/sec, "
      (stringout 
	(do-choices (counter (difference (get loop-state 'counters) 'items) i)
	  (let* ((count (get loop-state counter))
		 (rate  (/~ count elapsed)))
	    (printout (if (zero? (remainder i 3)) "\n   " ", ")
	      ($num count) " " (downcase counter)
	      (when (overlaps? counter logrates)
		(printout
		  " (" ($showrate rate) " " (downcase counter) "/sec)")))))))))

(define (engine/logrates batch proctime time batch-state loop-state state)
  (let ((elapsed (elapsed-time (get loop-state 'started)))
	(items (get loop-state 'items)))
    (lognotice |Engine/Counts| (engine/showrates loop-state))))

(define (engine/showrusage)
  (let* ((usage (rusage))
	 (load (get usage 'loadavg)))
    (printout "cpu=" ($num (get usage 'cpu%) 2) "%; "
      "mem=" ($bytes (memusage)) ", vmem=" ($bytes (vmemusage)) ";\n    "
      "load: " (first load) " · " (second load) " · " (third load) "; "
      "utime=" (compact-interval-string (get usage 'utime)) "; "
      "stime=" (compact-interval-string (get usage 'stime)) "; "
      "elapsed=" (secs->string (get usage 'clock)))))

(define (engine/logrusage batch proctime time batch-state loop-state state)
  (lognotice |Engine/Resources| (engine/showrusage)))

;;; Filling the fifo

(define (engine/fill! fifo fillfn loop-state fillmin fillmax)
  (unless (or (test loop-state 'stopped)
	      (test loop-state 'stopping)
	      (> (fifo/load fifo) fillmin))
    (dotimes (i (- fillmax (fifo/load fifo)))
      (let ((item (fillfn fifo loop-state)))
	(when (and (exists? item) item)
	  (fifo/push! fifo item))))))

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
  (do-choices (slot (choice (get loop-state 'counters) 
			    '{items batches vproctime}))
    (when (exists? (get loop-state slot))
      (store! copy slot (get loop-state slot))))
  copy)

(define (update-task-state loop-state)
  (let ((state (get loop-state 'state))
	(init (get loop-state 'init))
	(opts (get loop-state 'opts)))
    (do-choices (counter {(getopt opts 'counters) (get state 'counters)
			  'items 'batches 'cycles})
      (when (test loop-state counter)
	(store! state counter (+ (try (get init counter) 0)
				 (get loop-state counter)))))
    (store! state 'updated (timestamp))
    state))

(define (engine/checkpoint loop-state (fifo) (force #f))
  (default! fifo (get loop-state 'fifo))
  (let ((%loglevel (getopt loop-state '%loglevel %loglevel))
	(state (and (test loop-state 'state) (get loop-state 'state)))
	(started (elapsed-time))
	(success #f))
    (if (or force (check/start! loop-state))
	(unwind-protect 
	    (begin 
	      (logdebug |Engine/Checkpoint| 
		"For " fifo " loop state=\n  " (void (pprint loop-state)))
	      (when (and fifo (getopt loop-state 'checkpause #t))
		(fifo/pause! fifo 'readwrite)
		(when (and (not force)
			   (getopt loop-state 'checksync)
			   (not (fifo-pause fifo)))
		  (let ((wait-start (elapsed-time)))
		    (until (fifo/paused? fifo)
		      (condvar-wait (fifo-condvar fifo)))
		    (when (> (elapsed-time wait-start) 1)
		      (lognotice |Engine/Checkpoint| 
			"Waited " (secs->string (elapsed-time wait-start))
			" for FIFO to pause")))))
	      (when (getopt (get loop-state 'opts) 'logchecks #f)
		(engine-logger (qc) 0 (elapsed-time (get loop-state 'started)) 
			       #[] loop-state (get loop-state 'state)))
	      (store! loop-state 'lastcheck (get-check-state loop-state))

	      (engine-commit loop-state (get loop-state 'checkpoint))

	      (when state (update-task-state loop-state))
	      (when (and state (testopt (get loop-state 'opts) 'statefile))
		(dtype->file (get loop-state 'state)
			     (getopt (get loop-state 'opts) 'statefile)))
	      (when (getopt (get loop-state 'opts) 'logchecks #f)
		(engine-logger (qc) 0 (elapsed-time (get loop-state 'started)) 
			       #[] loop-state (get loop-state 'state)))
	      (set! success #t))
	  (begin (drop! loop-state 'checkthread) ;; lets check/start! work again
	    (store! loop-state 'checkdone (elapsed-time))
	    (if success
		(lognotice |Engine/Checkpoint| 
		  "Saved task state in " (secs->string (elapsed-time started)) " for " fifo)
		(logwarn |Checkpoint/Failed| 
		  "After " (secs->string (elapsed-time started)) " for " fifo))
	    (when fifo (fifo/pause! fifo #f))
	    (when (getopt (getopt loop-state 'opts) 'swapout (config 'swapout))
	      (engine-swapout loop-state))))
	(logwarn |BadCheck| 
	  "Declining to checkpoint because check/start! failed: state =\n  "
	  (pprint loop-state)))))

;;; Saving databases

(define (get-modified arg)
  (cond ((registry? arg) (tryif (registry/modified? arg) arg))
	((pool? arg) 
	 (choice (tryif (modified? arg) arg)
		 (get-modified (poolctl arg 'partitions))
		 (tryif (poolctl arg 'adjuncts)
		   (get-modified (getvalues (poolctl arg 'adjuncts))))))
	((index? arg)
	 (choice (tryif (modified? arg) arg)
		 (get-modified (indexctl arg 'partitions))))
	((and (applicable? arg) (zero? (procedure-min-arity arg))) arg)
	((and (pair? arg) (applicable? (car arg))) arg)
	(else {})))

(define commit-threads #t)

(defambda (engine-commit loop-state dbs (opts))
  (default! opts (getopt loop-state 'opts))
  (let ((modified (get-modified dbs))
	(%loglevel (getopt loop-state 'loglevel %loglevel))
	(started (elapsed-time))
	(spec-threads 
	 (mt/threadcount (getopt opts 'threads commit-threads))))
    (when (exists? modified)
      (let ((timings (make-hashtable))
	    (n-threads (and spec-threads
			    (min spec-threads (choice-size modified)))))
	(lognotice |Checkpoint/Start|
	  (if (test loop-state 'stopped) "Final " "Incremental ")
	  "checkpoint of " (choice-size modified) " modified dbs "
	  "using " (or n-threads "no") " threads "
	  "for " (get loop-state 'fifo)
	  (when (>= %loglevel %info%)
	    (do-choices (db modified) (printout "\n\t" db))))
	(cond ((not n-threads)
	       (do-choices (db modified) (commit-db db opts timings)))
	      ((>= n-threads (choice-size modified))
	       (set! n-threads (choice-size modified))
	       (let ((threads (thread/call commit-db modified opts timings)))
		 (thread/wait! threads)))
	      (else
	       (let ((threads {})
		     (commit-queue
		      (fifo/make (choice->vector modified)
				 `#[fillfn ,fifo/exhausted!])))
		 (dotimes (i n-threads)
		   (set+! threads 
		     (thread/call commit-queued commit-queue
		       opts timings)))
		 (thread/wait! threads))))
	(lognotice |Engine/Checkpoint|
	  "Committed " (choice-size (getkeys timings)) " dbs "
	  "in " (secs->string (elapsed-time started)) " "
	  "using " (or n-threads "no") " threads: "
	  (do-choices (db (getkeys timings))
	    (let ((time (get timings db)))
	      (if (>= time 0)
		  (printout "\n\t" ($num time 1) "s \t" db)
		  (printout "\n\tFAILED after " ($num time 1) "s:\t" db)))))))))

(defambda (engine-swapout loop-state)
  (let ((started (elapsed-time))
	(dbs (getopt loop-state 'caches
		     (getopt (get loop-state 'opts) 'caches))))
    (cond ((not dbs)
	   (loginfo |EngineSwapout| 
	     "Clearing cached data for " (get loop-state 'fifo))
	   (swapout)
	   (lognotice |Engine/Swapout| 
	     "Cleared cached data in " (secs->string (elapsed-time started)) " "
	     "for " (get loop-state 'fifo)))
	  (else (loginfo |Engine/Swapout| 
		  "Clearing cached data from "
		  ($count (choice-size dbs) " databases ")
		  "for " (get loop-state 'fifo))
		(do-choices (db dbs) (swapout db))
		(lognotice |Engine/Swapout| 
		  "Cleared cached data in " (secs->string (elapsed-time started))  " "
		  "from " ($count (choice-size dbs) " databases ")
		  "for " (get loop-state 'fifo))))))

(define (inner-commit arg timings start)
  (cond ((registry? arg) (registry/save! arg))
	((pool? arg) (commit arg))
	((index? arg) (commit arg))
	((and (applicable? arg) (zero? (procedure-min-arity arg))) (arg))
	((and (pair? arg) (applicable? (car arg)))
	 (apply (car arg) (cdr arg)))
	(else (logwarn |Engine/CantSave| "No method for saving " arg) #f))
  (store! timings arg (elapsed-time start))
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
  (prefetch-oids! oids))
(define (engine/fetchkeys oids (batch-state #f) (loop-state #f) (state #f))
  (prefetch-keys! oids))
(define (engine/swapout args (batch-state #f) (loop-state #f) (state #f))
  (swapout args))

;;;; Utility meta-functions

(define (engine/poolfetch pool)
  (ambda (oids (state #f)) (pool-prefetch! pool oids)))

(define (engine/indexfetch index)
  (ambda (keys (state #f)) (prefetch-keys! index keys)))

(define (engine/savepool pool)
  (ambda (keys (loop-state #f)) (commit pool)))

(define (engine/saveindex index)
  (ambda (keys (loop-state #f)) (commit index)))

(define (engine/savetable table file)
  (ambda (keys (loop-state #f)) (dtype->file table file)))

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
		   (and stopfile (file-exists? stopfile))
		   (and stopconf (config stopconf)))
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

(define (test-clause sysinfo loop-state clause)
  (cond ((applicable? clause) (clause))
	((not (pair? clause))
	 (logwarn |BadLoopTestClause| "Couldn't interpret " clause)
	 #f)
	((applicable? (car clause))
	 (apply (car clause) (cdr clause)))
	((symbol? (car clause))
	 (let* ((field (car clause))
		(value (try (get sysinfo field) 
			    (get loop-state field)))
		(args (cdr clause))
		(arg (and (pair? args) (car args))))
	   (cond ((fail? value) #t)
		 ((and (number? args) (number? value)) (>= value args))
		 ((and (number? arg) (number? value)) (>= value arg))
		 ((and (timestamp? args) (timestamp? value))
		  (time-later? value args))
		 ((and (timestamp? arg) (timestamp? value))
		  (time-later? value arg))
		 ((and (applicable? arg) (pair? (cdr args)))
		  (apply arg value (cdr args)))
		 ((applicable? arg) (arg value (cdr args)))
		 (else #f))))
	(else #t)))

(define (looptest sysinfo loop-state tests)
  (if (null? tests) #t
      (and (test-clause sysinfo loop-state (car tests))
	   (looptest sysinfo loop-state (cdr tests)))))

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
    (def (engine/tester (loop-state #[]))
	 (looptest (rusage) loop-state tests))))

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


