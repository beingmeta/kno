;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2019 beingmeta, inc.  All rights reserved.

(in-module 'fifo)

;;; Simple FIFO queue gated by a condition variable

(use-module '{ezrecords logger})
(define %used_modules 'ezrecords)

(module-export!
 '{->fifo fifo/make fifo/close!
   fifo/pop fifo/remove!
   fifo/push!
   fifo/push/n!
   fifo/jump! 
   fifo/wait
   fifo/pause!
   fifo/pausing?
   fifo/paused?
   fifo/exhausted!
   fifo/loop
   fifo/queued
   fifo/load
   fifo-name
   fifo-opts})

(module-export!
 '{make-fifo
   fifo-push
   fifo-pop
   fifo-loop
   fifo-jump
   fifo-queued
   close-fifo
   fifo-pause
   fifo-fillfn
   fifo-opts
   fifo-live?
   fifo-size
   fifo-waiting
   fifo-running
   fifo/waiting
   fifo/idle?
   fifo/set-debug!})

(define-init %loglevel %warn%)

;;;; Implementation

(define (fifo->string fifo)
  (if (fifo-name fifo)
      (stringout "#<FIFO "
	(fifo-name fifo) " "
	(- (fifo-end fifo) (fifo-start fifo)) "/" (length (fifo-queue fifo)) 
	"-r" (choice-size (fifo-running fifo)) "-w" (choice-size (fifo-waiting fifo)) " "
	(if (not (fifo-live? fifo)) " (exhausted)")
	(if (fifo-debug fifo) " (debug)")
	">")
      (stringout "#<FIFO "
	(- (fifo-end fifo) (fifo-start fifo)) "/" (length (fifo-queue fifo)) 
	"-r" (choice-size (fifo-running fifo)) "-w" (choice-size (fifo-waiting fifo)) " "
	(glom "0x" (number->string (hashptr fifo) 16)) 
	(if (not (fifo-live? fifo)) " (exhausted)")
	(if (fifo-debug fifo) " (debug)")
	">")))

(defrecord (fifo MUTABLE OPAQUE `(stringfn . fifo->string))
  name    ;; a string
  condvar ;; a condvar
  queue   ;; a vector
  start   ;; the vector index for the next item
  end     ;; the vector index for the last item
  opts    ;; options
  fillfn  ;; a function or (function . more-args) to call when the queue is empty
  items   ;; a hashset of items waiting in the queue, if nodups is set
  (live? #t)  ;; Whether the FIFO is active (callers should wait)
  (pause #f)  ;; Whether the FIFO is paused (value is #f, READ, WRITE, or READWRITE)
  (waiting {}) ;; How many threads are waiting on the FIFO.
  (running {}) ;; How many threads are processing results from the FIFO.
  (debug #f)  ;; Whether we're debugging the FIFO
  )

(define (fifo/make . args)
  "Creates a new FIFO object from its arguments which can be "
  "strings or symbols (for the name), positive fixnums (for the initial size), "
  "vectors (for data), or tables (for options). Options provide defaults "
  "for these other attributes."
  (let ((size #f) (name #f) (data #f) (opts #f) (queue #f) 
	(items #f) (debug #f) (fillfn #f) (live? #t) (init-items 0))
    (doseq (arg args)
      (cond ((string? arg)
	     (if name 
		 (irritant (cons name arg) |FIFO/AmbiguousSpec/Name|)
		 (set! name arg)))
	    ((and (fixnum? arg) (> arg 0))
	     (if size
		 (irritant (cons size arg) |FIFO/AmbiguousSpec/Size|)
		 (set! size arg)))
	    ((number? arg) (irritant arg |FIFO/BadSpec/Size|))
	    ((vector? arg)
	     (if data
		 (irritant (cons data arg) |FIFO/AmbiguousSpec/Data|)
		 (set! data arg)))
	    ((ambiguous? arg)
	     (if data
		 (irritant (cons data arg) |FIFO/AmbiguousSpec/Data|)
		 (set! data (choice->vector arg))))
	    ((table? arg)
	     (if opts
		 (unless (eq? opts arg)
		   (set! opts (cons arg opts)))
		 (set! opts arg)))
	    (else (irritant arg |FIFO/BadSpec|))))
    (unless name (set! name (getopt opts 'name #f)))
    (unless size
      (set! size (getopt opts 'size (if data (length data) 64))))
    (set! debug (getopt opts 'debug #f))
    (set! fillfn (getopt opts 'fillfn fifo/exhausted!))
    (set! live? (getopt opts 'live? live?))
    (if (getopt opts 'nodups #t)
	(set! items (make-hashset))
	(set! items #f))
    (cond ((not data)
	   (set! queue (make-vector (or size 64) #f)))
	  ((not size)
	   (set! queue data)
	   (set! init-items (compact-queue queue))
	   (set! size (length queue)))
	  ((> (length data) size)
	   (set! size (length data))
	   (set! queue data)
	   (set! init-items (compact-queue queue)))
	  (else 
	   (set! queue data)
	   (set! init-items (compact-queue queue))))
    (cons-fifo name (make-condvar) queue 0 init-items
	       opts fillfn items
	       live? #f 0 0 debug)))
(define (make-fifo (size 64)) (fifo/make size))

(defambda (->fifo items (opts #f))
  (fifo/make (if (vector? items) items
		 (choice->vector items))
	     opts))

(define (compact-queue queue)
  "Moves all the non #f items in QUEUE (a vector) towards "
  "the beginning and returns the number of those items."
  (let ((write 0) (len (length queue)))
    (doseq (item queue read)
      (cond ((not item))
	    ((= write read) (set! write (1+ write)))
	    (else (vector-set! queue write item)
		  (set! write (1+ write)))))
    (dotimes (i (- len write))
      (vector-set! queue (+ write i) #f))
    write))

(defambda (check-paused fifo flags)
  "Checks whether a FIFO is paused for any of FLAGS "
  "and blocks until it is un-paused."
  (while (overlaps? (fifo-pause fifo) flags)
    (condvar/wait (fifo-condvar fifo))))

(define (fifo/push! fifo item (broadcast #f))
  "Pushes a new item into the FIFO. If *broadcast* is true, "
  "a broadcast signal is sent to all waiting threads, rather than "
  "just one."
  (unwind-protect
      (begin (if (fifo-debug fifo)
		 (always%watch "FIFO/PUSH!" item broadcast fifo)
		 (debug%watch "FIFO/PUSH!" item broadcast fifo))
	(condvar/lock! (fifo-condvar fifo))
	(check-paused fifo '{write readwrite})
	(if (and (fifo-items fifo)
		 (hashset-get (fifo-items fifo) item))
	    (if (fifo-debug fifo)
		(always%watch "FIFO/PUSH!/REDUNDANT" item fifo)
		(debug%watch "FIFO/PUSH!/REDUNDANT" item fifo))
	    (let ((vec (fifo-queue fifo))
		  (start (fifo-start fifo))
		  (end (fifo-end fifo)))
	      (if (fifo-debug fifo)
		  (always%watch "FIFO/PUSH!/INSERT" item fifo)
		  (debug%watch "FIFO/PUSH!/INSERT" item fifo))
	      (cond ((< end (length vec))
		     (vector-set! vec end item)
		     (set-fifo-end! fifo (1+ end)))
		    ((<= (- end start) (-1+ (length vec)))
		     ;; Move the queue to the start of the vector
		     (dotimes (i (- end start))
		       (vector-set! vec i (elt vec (+ start i))))
		     (let ((zerostart (- end start)))
		       (dotimes (i (- (length vec) zerostart))
			 (vector-set! vec (+ zerostart i) #f)))
		     (set-fifo-start! fifo 0)
		     (vector-set! vec (- end start) item)
		     (set-fifo-end! fifo (1+ (- end start))))
		    (else
		     (let ((newvec (make-vector (* 2 (length vec)))))
		       (if (fifo-debug fifo)
			   (always%watch "FIFO/PUSH!/GROW" fifo)
			   (debug%watch "FIFO/PUSH!/GROW" fifo))
		       (debug%watch "FIFO/PUSH!/GROW" fifo)
		       (dotimes (i (- end start))
			 (vector-set! newvec i (elt vec (+ start i))))
		       (set-fifo-queue! fifo newvec)
		       (set-fifo-start! fifo 0)
		       (vector-set! newvec (- end start) item)
		       (set-fifo-end! fifo (1+ (- end start))))))))
	(condvar-signal (fifo-condvar fifo) broadcast))
    (condvar/unlock! (fifo-condvar fifo))))
(define (fifo-push fifo item (broadcast #f))
  "Pushes a new item into the FIFO. If *broadcast* is true, "
  "a broadcast signal is sent to all waiting threads, rather than "
  "just one."
  (fifo/push! fifo item broadcast))

(define (fifo/push/n! fifo items (broadcast #f) (uselock #t))
  "Pushes multiple items into the FIFO. If *broadcast* is true, "
  "a broadcast signal is sent to all waiting threads, rather than "
  "just one."
  (unwind-protect
      (begin (if (fifo-debug fifo)
		 (always%watch "FIFO/PUSH/N!" "N" (length items) broadcast fifo)
		 (debug%watch "FIFO/PUSH/N!" "N" (length items) broadcast fifo))
	(when uselock (condvar/lock! (fifo-condvar fifo)))
	(check-paused fifo '{write readwrite})
	(let ((vec (fifo-queue fifo))
	      (start (fifo-start fifo))
	      (end (fifo-end fifo))
	      (add (length items)))
	  (cond ((< (+ end add) (length vec))
		 (doseq (item items i)
		   (vector-set! vec (+ end i) item))
		 (set-fifo-end! fifo (+ end add)))
		((< (+ add (- end start)) (length vec))
		 ;; Move the queue to the start of the vector
		 (dotimes (i (- end start))
		   (vector-set! vec i (elt vec (+ start i))))
		 (let ((zerostart (- end start)))
		   (dotimes (i (- (length vec) zerostart))
		     (vector-set! vec (+ zerostart i) #f)))
		 (set-fifo-start! fifo 0)
		 (set! end (- end start))
		 (doseq (item items i)
		   (vector-set! vec (+ end i) item))
		 (set-fifo-end! fifo (+ end add)))
		(else
		 (let* ((needed (+ (- end start) add 1))
			(newlen (getlen needed (length vec)))
			(newvec (make-vector newlen)))
		   (if (fifo-debug fifo)
		       (always%watch "FIFO/PUSH!/GROW" fifo)
		       (debug%watch "FIFO/PUSH!/GROW" fifo))
		   (debug%watch "FIFO/PUSH!/GROW" fifo)
		   (dotimes (i (- end start))
		     (vector-set! newvec i (elt vec (+ start i))))
		   (set-fifo-queue! fifo newvec)
		   (set-fifo-start! fifo 0)
		   (set! end (- end start))
		   (doseq (item items i)
		     (vector-set! vec (+ end i) item))
		   (set-fifo-end! fifo (+ end add))))))
	(condvar-signal (fifo-condvar fifo) broadcast))
    (when uselock (condvar/unlock! (fifo-condvar fifo)))))

(define (getlen needed current)
  (if (< needed current) 
      current
      (let ((trylen (* 2 current)))
	(while (> needed trylen) (set! trylen (* 2 trylen)))
	trylen)))

(define (fifo-waiting! fifo (cvar) (tid))
  "Declare that a thread is waiting on the FIFO"
  (if (fifo-debug fifo)
      (always%watch "FIFO-WAITING!" 
	fifo (threadid) (fifo-waiting fifo) (fifo-running fifo))
      (debug%watch "FIFO-WAITING!" 
	fifo (threadid) (fifo-waiting fifo) (fifo-running fifo)))
  (set! cvar (fifo-condvar fifo))
  (set! tid (threadid))
  (set-fifo-waiting! fifo (choice (fifo-waiting fifo) tid))
  (unless (identical? (thread/get '_fifo) fifo) (thread/set! '_fifo fifo))
  (set-fifo-running! fifo (difference (fifo-running fifo) tid))
  (condvar-signal (fifo-condvar fifo) #t)
  (choice-size (fifo-waiting fifo)))

(define (fifo-running! fifo (cvar) (tid))
  "Declare that a thread is waiting on the FIFO"
  (if (fifo-debug fifo)
      (always%watch "FIFO-WAITING!" 
	fifo (threadid) (fifo-waiting fifo) (fifo-running fifo))
      (debug%watch "FIFO-WAITING!" 
	fifo (threadid) (fifo-waiting fifo) (fifo-running fifo)))
  (set! cvar (fifo-condvar fifo))
  (set! tid (threadid))
  (set-fifo-waiting! fifo (difference (fifo-waiting fifo) tid))
  (set-fifo-running! fifo (choice (fifo-running fifo) tid))
  (condvar-signal (fifo-condvar fifo) #t)
  (choice-size (fifo-running fifo)))

(define (fifo/fill! fifo (fillfn) (condvar)) 
  (set! fillfn (fifo-fillfn fifo))
  (set! condvar (fifo-condvar fifo))
  (cond ((not fillfn))
	((applicable? fillfn) (fillfn fifo))
	((and (pair? fillfn) (applicable? (car fillfn)))
	 (apply (car fillfn) fifo (cdr fillfn)))
	(else (irritant fillfn |FIFO/InvalidFillFn|))))

(define (fifo/pop fifo (maxcount 1) (condvar))
  "Pops some number of items (at least one) from the FIFO."
  "The *maxcount* argument specifies the number of maximum number of items "
  "to be popped."
  (if (fifo-debug fifo)
      (always%watch "FIFO/POP" fifo)
      (debug%watch "FIFO/POP" fifo))
  (set! condvar (fifo-condvar fifo))
  (if (fifo-live? fifo)
      (unwind-protect
	  (begin (condvar/lock! condvar)
	    ;; Assert that we're waiting
	    (fifo-waiting! fifo)
	    ;; Wait if it is paused
	    (check-paused fifo '{read readwrite})
	    ;; Wait for data
	    (while (and (fifo-live? fifo)
			(= (fifo-start fifo) (fifo-end fifo))
			(not (overlaps? (fifo-pause fifo) '{read readwrite})))
	      (when (fifo-fillfn fifo) (fifo/fill! fifo))
	      (when (and (fifo-live? fifo)
			 (= (fifo-start fifo) (fifo-end fifo))
			 (not (overlaps? (fifo-pause fifo) '{read readwrite})))
		(condvar/wait condvar)))
	    (fifo-running! fifo)
	    (check-paused fifo '{read readwrite})
	    ;; If it's still alive, do the pop
	    ;; Wait if it is paused
	    (if (fifo-live? fifo) 
		(let* ((vec (fifo-queue fifo))
		       (start (fifo-start fifo))
		       (end (fifo-end fifo))
		       (count (min maxcount (- end start)))
		       (items (elts vec start (+ start count))))
		  (if (fifo-debug fifo)
		      (always%watch "FIFO/POP/ITEM" start end fifo items)
		      (debug%watch "FIFO/POP/ITEM" start end fifo items))
		  ;; Replace the item with false
		  (dotimes (i count)
		    (vector-set! vec (+ start i) #f))
		  ;; Advance the start pointer
		  (set-fifo-start! fifo (+ start count))
		  (when (= (fifo-start fifo) (fifo-end fifo))
		    ;; If we're empty, move the pointers back
		    (set-fifo-start! fifo 0)
		    (set-fifo-end! fifo 0))
		  (when (fifo-items fifo)
		    (hashset-drop! (fifo-items fifo) items))
		  items)
		(fail)))
	(condvar/unlock! condvar))
      (fail)))
(define (fifo-pop fifo)
  "Pops some number of items (at least one) from the FIFO."
  "The *maxcount* argument specifies the number of maximum number of items "
  "to be popped."
  (fifo/pop fifo))

(define (fifo/remove! fifo item)
  "Removes an item from the FIFO"
  (if (fifo-live? fifo)
      (unwind-protect
	  (begin (condvar/lock! (fifo-condvar fifo))
	    (if (and (fifo-live? fifo)
		     (< (fifo-start fifo) (fifo-end fifo)))
		(let* ((vec (fifo-queue fifo))
		       (start (fifo-start fifo))
		       (end (fifo-end fifo))
		       (pos (position item vec start end)))
		  (if pos
		      (let ((queued (elt vec pos)))
			;; Move everything else down
			(dotimes (i (- end pos))
			  (vector-set! vec (+ pos i) (elt vec (+ pos i 1))))
			(vector-set! vec end #f)
			(set-fifo-end! fifo (-1+ end))
			(when (fifo-items fifo)
			  (hashset-drop! (fifo-items fifo) item))
			queued)
		      (fail)))
		(fail)))
	(condvar/unlock! (fifo-condvar fifo)))
      (fail)))
(define (fifo-jump fifo item)
  (fifo/remove! fifo item))

(define (fifo/loop fifo handler)
  "Repeatedly pops items from *fifo* and calls *handler* on them "
  "until *handler* either fails {} or returns #f"
  (let ((result #t))
    (while (and (exists? result) result)
      (set! result (handler (fifo-pop fifo))))))
(define (fifo-loop fifo handler)
  (fifo/loop fifo handler))

(define (fifo/close! fifo (uselock #t) (result #f) (condvar))
  "Closes a FIFO, returning a vector of the remaining queued items."
  (set! condvar (fifo-condvar fifo))
  (unwind-protect
      (begin
	(when uselock (condvar/lock! condvar))
	(set! result
	  (slice (fifo-queue fifo)
		 (fifo-start fifo) (fifo-end fifo)))
	(set-fifo-live?! fifo #f)
	result)
    (begin
      (condvar-signal condvar #t)
      (when uselock (condvar/unlock! condvar)))))
(define (close-fifo fifo) (fifo/close! fifo))

(define (fifo/exhausted! fifo) (fifo/close! fifo #f))

(define (fifo/queued fifo (result #f))
  "Returns a vector of the queued items in a FIFO, in order"
  (unwind-protect
      (begin (condvar/lock! (fifo-condvar fifo))
	(set! result (subseq (fifo-queue fifo)
			     (fifo-start fifo)
			     (fifo-end fifo))))
    (condvar/unlock! (fifo-condvar fifo)))
  result)
(define (fifo-queued fifo) (fifo/queued fifo))

(define (fifo/load fifo)
  "Returns the number of queued items in *fifo*, locking to compute it"
  (unwind-protect
      (begin (condvar/lock! (fifo-condvar fifo))
	     (- (fifo-end fifo) (fifo-start fifo)))
    (condvar/unlock! (fifo-condvar fifo))))
(define (fifo-load fifo) (- (fifo-end fifo) (fifo-start fifo)))
(define (fifo-size fifo) (length (fifo-queue fifo)))

(define (fifo/wait fifo (state))
  "Waits for something about *fifo* to change."
  (default! state (fifo-condvar fifo))
  (condvar/lock! state)
  (condvar/wait state)
  (condvar/unlock! state))

(define (fifo/waiting fifo (nonzero #t))
  "Returns the number of threads waiting on the fifo.
   If *nonzero* is true, wait (block)  until the number 
   is non-zero."
  (unwind-protect
      (begin (condvar/lock! (fifo-condvar fifo))
	(when nonzero
	  (while (fail? (fifo-waiting fifo))
	    (condvar/wait (fifo-condvar fifo))))
	(choice-size (fifo-waiting fifo)))
    (condvar/unlock! (fifo-condvar fifo))))

(define (fifo/idle? fifo)
  "Returns true when *fifo* is idle, waiting until the queue is empty "
  "and there are still threads waiting on the fifo."
  (unwind-protect
      (begin (condvar/lock! (fifo-condvar fifo))
	(until (and (exists? (fifo-waiting fifo))
		    (= (fifo-start fifo) (fifo-end fifo)))
	  (condvar/wait (fifo-condvar fifo)))
	(and (exists? (fifo-waiting fifo))
	     (= (fifo-start fifo) (fifo-end fifo))))
    (condvar/unlock! (fifo-condvar fifo))))

(define (fifo/pause! fifo rdwr)
  "Pauses operations on *fifo*. *rdwr* can be READ, WRITE, or READWRITE "
  "to pause the corresponding operations. A value of #f resumes all operations."
  (if (not (overlaps? rdwr '{#f read write readwrite}))
      (irritant rdwr |FIFO/BadPauseArg|)
      (unwind-protect 
	  (begin (condvar/lock! (fifo-condvar fifo))
	    (set-fifo-pause! fifo rdwr)
	    (condvar-signal (fifo-condvar fifo) #t))
	(condvar/unlock! (fifo-condvar fifo)))))

(define (fifo/pausing? fifo) (fifo-pause fifo))

(define (fifo/paused? fifo)
  (and (fifo-pause fifo) (fail? (fifo-running fifo))))

(define (fifo/set-debug! fifo (flag #t)) (set-fifo-debug! fifo flag))
