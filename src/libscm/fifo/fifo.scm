;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)

(in-module 'fifo)

;;; Simple FIFO queue gated by a condition variable

(use-module '{ezrecords logger varconfig kno/reflect})
(define %used_modules 'ezrecords)

(module-export!
 '{->fifo fifo/make fifo/close! fifo?
   fifo/pop fifo/popvec fifo/remove!
   fifo/push!
   fifo/push/n!
   fifo/release!
   fifo/finished!
   fifo/jump! 
   fifo/wait
   fifo/pause!
   fifo/pausing?
   fifo/paused?
   fifo/nofill
   fifo/exhausted!
   fifo/loop
   fifo/queued
   fifo/load
   fifo/fill!
   fifo-name
   fifo-condvar
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
   fifo/set-debug!
   fifo/set-fillfn!})

(define-init %loglevel %warn%)

;;;; Implementation

(define default-debug #f)
(varconfig! fifo:debug default-debug config:boolean)

(define (fifo->string fifo)
  (if (fifo-name fifo)
      (stringout "#<FIFO "
	(fifo-name fifo) " "
	(- (fifo-end fifo) (fifo-start fifo)) "/" (length (fifo-queue fifo)) 
	(when (or (fifo-debug fifo) default-debug)
	  (printout " [" (fifo-start fifo) "," (fifo-end fifo) "] "))
	"-r" (choice-size (fifo-running fifo)) "-w" (choice-size (fifo-waiting fifo)) " "
	(if (not (fifo-live? fifo)) " (exhausted)")
	(when (fifo-pause fifo) (printout " paused=" (fifo-pause fifo)))
	(when (or (fifo-debug fifo) default-debug)
	  (printout)" (debug)"
	  " running=" (fifo-running fifo)
	  " waiting=" (fifo-waiting fifo))
	">")
      (stringout "#<FIFO "
	(- (fifo-end fifo) (fifo-start fifo)) "/" (length (fifo-queue fifo)) 
	(when (or (fifo-debug fifo) default-debug)
	  (printout " [" (fifo-start fifo) "," (fifo-end fifo) "] "))
	"-r" (choice-size (fifo-running fifo)) "-w" (choice-size (fifo-waiting fifo)) " "
	(glom "0x" (number->string (hashptr fifo) 16)) 
	(if (not (fifo-live? fifo)) " (exhausted)")
	(when (fifo-pause fifo) (printout " paused=" (fifo-pause fifo)))
	(when (or (fifo-debug fifo) default-debug)
	  (printout)" (debug)"
	  " running=" (fifo-running fifo)
	  " waiting=" (fifo-waiting fifo))
	">")))

(defrecord (fifo MUTABLE OPAQUE `(stringfn . fifo->string))
  name    ;; a string
  condvar ;; a condvar
  queue   ;; a vector
  start   ;; the vector index for the next item
  end     ;; the vector index for the last item
  opts    ;; options
  fillfn  ;; a function or (function . more-args) to call when the queue is empty
  (items #f)   ;; a hashset of items waiting in the queue, if nodups is set
  (maxlen #f)  ;; The max length to which the fifo will grow
  (live? #t)  ;; Whether the FIFO is active (callers should wait)
  (pause #f)  ;; Whether the FIFO is paused (value is #f, READ, WRITE, READWRITE, or CLOSING)
  (waiting {}) ;; The threads waiting on the FIFO.
  (running {}) ;; The threads currently processing results from the FIFO.
  (filling #f) ;; The thread currently filling the FIFO
  (state #[])
  (debug #f)  ;; Whether we're debugging the FIFO
  )

(define (fifo-debug? fifo) (or default-debug (fifo-debug fifo)))

(define (fifo/make . args)
  "Creates a new FIFO object from its arguments which can be "
  "strings or symbols (for the name), positive fixnums (for the initial size), "
  "vectors (for data), or tables (for options). Options provide defaults "
  "for these other attributes."
  (let ((size #f) (name #f) (data #f) (opts #f) (queue #f) 
	(items #f) (debug #f) (fillfn #f) (live? #t) (init-items 0)
	(maxlen #f))
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
    (set! maxlen (getopt opts 'maxlen #f))
    (set! debug (getopt opts 'debug #f))
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
	   (set! queue (make-vector size))
	   (set! init-items (compact-queue data queue))))
    (when (and maxlen (not (fixnum? maxlen)) (> maxlen 0))
      (set! maxlen size))
    (set! fillfn
      (cond ((testopt opts 'fillin #f) fifo/nofill)
	    ((testopt opts 'fillin) (getopt opts 'fillfn #f))
	    ((getopt opts 'filled #f) fifo/nofill)
	    ((getopt opts 'async #f) #f)
	    ((and init-items (> init-items 0)) fifo/nofill)
	    (else #f)))
    (cons-fifo name (make-condvar) queue 0 init-items opts
	       fillfn items maxlen
	       live? #f {} {} #f #[] debug)))
(define (make-fifo (size 64)) (fifo/make size))

(defambda (->fifo items (opts #f))
  (fifo/make (if (vector? items) items
		 (choice->vector items))
	     opts))

(define (compact-queue queue (into))
  "Moves all the non #f items in QUEUE (a vector) towards "
  "the beginning and returns the number of those items."
  (default! into queue)
  (let ((write 0) (len (length queue)))
    (doseq (item queue read)
      (cond ((not item))
	    ((= write read)
	     (unless (eq? queue into) 
	       (vector-set! into write (elt queue read)))
	     (set! write (1+ write)))
	    (else (vector-set! into write item)
		  (set! write (1+ write)))))
    (dotimes (i (- (length into) write))
      (vector-set! into (+ write i) #f))
    write))

(defambda (wait-paused fifo flags)
  "Checks whether a FIFO is paused for any of FLAGS "
  "and blocks until it is un-paused."
  (while (and (fifo-live? fifo) (overlaps? (fifo-pause fifo) flags))
    (condvar/wait (fifo-condvar fifo))))

(define (fifo/push/n! fifo items (opts #f) 
		      (broadcast) (uselock)
		      (block) (grow))
  "Pushes multiple items into the FIFO. If *broadcast* is true, "
  "a broadcast signal is sent to all waiting threads, rather than "
  "just one."
  (locals result #f len (length items))
  (when (eq? opts #t)
    (set! broadcast #t)
    (set! opts #f))
  (default! broadcast (getopt opts 'broadcast #t))
  (default! uselock (getopt opts 'uselock #t))
  (default! block (getopt opts 'block #t))
  (default! grow (getopt opts 'grow #t))
  (unwind-protect
      (begin (if (fifo-debug? fifo)
		 (always%watch "FIFO/PUSH/N!" "N" len broadcast fifo)
		 (debug%watch "FIFO/PUSH/N!" "N" len broadcast fifo))
	(when uselock (condvar/lock! (fifo-condvar fifo)))
	(when (and (overlaps? (fifo-pause fifo) '{write readwrite}) block)
	  (fifo/waiting! fifo)
	  (wait-paused fifo '{write readwrite})
	  (fifo/release! fifo))
	(set! result (fifo-pusher fifo items grow block))
	(condvar/signal (fifo-condvar fifo) broadcast))
    (when uselock (condvar/unlock! (fifo-condvar fifo))))
  (if (and (vector? result) block)
      (fifo/push/n! fifo result broadcast uselock)
      result))
  
(define (fifo-pusher fifo items grow block)
  (cond ((not (fifo-live? fifo)) (cons 'dead items))
	((overlaps? (fifo-pause fifo) '{write readwrite}) (cons 'paused items))
	(else (fifo-push-unlocked fifo items grow block))))

(define (fifo-push-unlocked fifo items grow block)
  (let* ((vec (fifo-queue fifo))
	 (start (fifo-start fifo))
	 (end (fifo-end fifo))
	 (add (length items))
	 (len (length vec))
	 (atend (- len end))
	 (point #f))
    (when (> add atend)
      (set! end (compress-fifo/unlocked fifo vec start end))
      (set! start 0))
    (when (and (> add atend) grow
	       (or (not (fifo-maxlen fifo))
		   (< (length vec) (fifo-maxlen fifo)))
	       (grow-fifo/unlocked fifo))
      (set! vec (fifo-queue fifo))
      (set! start (fifo-start fifo))
      (set! end (fifo-end fifo))
      (set! len (length vec)))
    (cond ((<= add (- len end))
	   (doseq (item items i)
	     (vector-set! vec (+ end i) item))
	   (set-fifo-end! fifo (+ end add))
	   #f)
	  (else
	   (let ((space (- len end))
		 (undone #f))
	     (dotimes (i space)
	       (vector-set! vec (+ end i) (elt items i)))
	     (set-fifo-end! fifo (+ end space))
	     (set! undone (slice items space))
	     (cond ((not block) undone)
		   (else (while (and (= (fifo-space fifo) 0)
				     (not (overlaps? (fifo-pause fifo) '{write readwrite}))
				     (fifo-live? fifo))
			   (condvar/wait fifo))
			 (fifo-pusher fifo undone grow block))))))))

(define (compress-fifo/unlocked fifo vec start end)
  (local point (- end start))
  (dotimes (i point)
    (vector-set! vec i (elt vec (+ start i))))
  (set-fifo-start! fifo 0)
  (set-fifo-end! fifo point)
  (dotimes (i (- (length vec) point))
    (vector-set! vec (+ point i) #f))
  point)
       
(define (grow-fifo/unlocked fifo (add 1))
  (let* ((vec (fifo-queue fifo))
	 (start (fifo-start fifo))
	 (end (fifo-start fifo))
	 (len (length vec)))
    (when (> start 0)
      (set! end (compress-fifo/unlocked fifo vec start end))
      (set! start 0))
    (let* ((maxlen (fifo-maxlen fifo))
	   (needed (+ (- end start) add 1))
	   (newlen (getlen needed len maxlen))
	   (newvec (make-vector newlen)))
      (if (fifo-debug? fifo)
	  (always%watch "FIFO/PUSH!/GROW" fifo add needed maxlen newlen)
	  (info%watch "FIFO/PUSH!/GROW" fifo add needed maxlen newlen))
      (dotimes (i end) (vector-set! newvec i (elt vec i)))
      (set-fifo-queue! fifo newvec))))

(define (getlen needed current maxlen)
  (if (< needed current) 
      current
      (let ((trylen (* 2 current)))
	(while (> needed trylen)
	  (cond ((> trylen maxlen)
		 (set! trylen maxlen)
		 (break))
		(else (set! trylen (* 2 trylen)))))
	trylen)))

(define (fifo/push! fifo item (opts #f) args)
  "Pushes a new item into the FIFO. If *broadcast* is true, "
  "a broadcast signal is sent to all waiting threads, rather than "
  "just one."
  (fifo/push/n! fifo (vector item) opts))
(define (fifo-push fifo item (opts #f))
  "Pushes a new item into the FIFO. If *broadcast* is true, "
  "a broadcast signal is sent to all waiting threads, rather than "
  "just one."
  (fifo/push/n! fifo (vector item) opts))

(define (fifo/waiting! fifo (cvar) (tid))
  "Declare that a thread is waiting on the FIFO"
  (if (fifo-debug? fifo)
      (always%watch "FIFO/WAITING!" 
	fifo (threadid) (fifo-waiting fifo) (fifo-running fifo))
      (debug%watch "FIFO/WAITING!" 
	fifo (threadid) (fifo-waiting fifo) (fifo-running fifo)))
  (set! cvar (fifo-condvar fifo))
  (set! tid (threadid))
  (modify-fifo-waiting! fifo choice tid)
  (unless (identical? (thread/get '_fifo) fifo) (thread/set! '_fifo fifo))
  (modify-fifo-running! fifo difference tid)
  (condvar/signal (fifo-condvar fifo) #t)
  (choice-size (fifo-waiting fifo)))

(define (fifo/running! fifo (cvar) (tid))
  "Declare that a thread is waiting on the FIFO"
  (if (fifo-debug? fifo)
      (always%watch "FIFO/RUNNING!" 
	fifo (threadid) (fifo-waiting fifo) (fifo-running fifo))
      (debug%watch "FIFO/RUNNING!" 
	fifo (threadid) (fifo-waiting fifo) (fifo-running fifo)))
  (set! cvar (fifo-condvar fifo))
  (set! tid (threadid))
  (modify-fifo-waiting! fifo difference tid)
  (modify-fifo-running! fifo choice tid)
  (condvar/signal (fifo-condvar fifo) #t)
  (choice-size (fifo-running fifo)))

(define (fifo/release! fifo (cvar) (tid))
  "Declare that a thread is finished running or waiting on the FIFO"
  (if (fifo-debug? fifo)
      (always%watch "FIFO/RELEASE!" 
	fifo (threadid) (fifo-waiting fifo) (fifo-running fifo))
      (debug%watch "FIFO/RELEASE!" 
	fifo (threadid) (fifo-waiting fifo) (fifo-running fifo)))
  (set! cvar (fifo-condvar fifo))
  (set! tid (threadid))
  (loginfo |FIFO/ThreadFinished| "TID=" tid ", cvar=" cvar ", fifo=" fifo)
  (unless (identical? (thread/get '_fifo) fifo) (thread/set! '_fifo #f))
  (unwind-protect 
      (begin (condvar/lock! cvar)
	(modify-fifo-running! fifo difference tid)
	(modify-fifo-waiting! fifo difference tid))
    (condvar/unlock! cvar))
  (condvar/signal cvar #t)
  (choice-size (fifo-running fifo)))
(define fifo/finished! (fcn/alias fifo/release!))

(define (fill-fifo! fifo (fillfn)) 
  (set! fillfn (fifo-fillfn fifo))
  (cond ((not fillfn))
	((and (applicable? fillfn) (zero? (procedure-arity fillfn)))
	 (fifo/fill! fifo fillfn))
	((applicable? fillfn) (fillfn fifo))
	((and (pair? fillfn) (applicable? (car fillfn)))
	 (apply (car fillfn) fifo (cdr fillfn)))
	(else (irritant fillfn |FIFO/InvalidFillFn|))))

(define (fifo/fill! fifo fillfn)
  "Uses the procedure *fillfn* to fill *fifo*. If the result of "
  "*fillfn* is a choice or a vector, all of its elements are "
  "added to the fifo; otherwise, the returned value is added. "
  "This means that if *fillfn* wants to return a vector as an item, "
  "it needs to be wrapped in another vector. "
  "If the *fifo*'s options specify a `fillmax` property, *fillfn* is called "
  "repeatedly until either *fillmax* elements are generated "
  "or the fifo stops accepting new elements."
  (let* ((fill-max (fifo-size fifo))
	 (filling (fillfn))
	 (fill-count 0))
    (until (fail? filling)
      (if (ambiguous? filling)
	  (fifo/push/n! fifo (choice->vector filling))
	  (if (vector? filling)
	      (fifo/push/n! fifo filling)
	      (fifo/push! fifo filling)))
      (if (and (singleton? filling) (vector? filling))
	  (set! fill-count (+ fill-count (length filling)))
	  (set! fill-count (+ fill-count (|| filling))))
      (when (and fill-max (>= fill-count fill-max)) (break))
      (when (or (not (fifo-live? fifo))
		(overlaps? (fifo-pause fifo) '{write readwrite closing}))
	(break))
      (set! filling (fillfn)))
    fill-count))

(define (stop-waiting? fifo fillthresh fillfn)
  (or (> (- (fifo-end fifo) (fifo-start fifo)) (or fillthresh 0))
      (and fillfn (not (fifo-filling fifo))
	   (not (overlaps? (fifo-pause fifo) '{write readwrite}))
	   (begin
	     (set-fifo-filling! fifo (threadid))
	     (modify-fifo-waiting! fifo difference (threadid))
	     (condvar/unlock! (fifo-condvar fifo))
	     (fill-fifo! fifo)
	     (set-fifo-filling! fifo #f)
	     (condvar/lock! (fifo-condvar fifo))
	     (or (not (fifo-live? fifo))
		 (> (fifo-end fifo) (fifo-start fifo)))))))

(define (fifo/pop fifo (maxcount 1) (fillthresh #f) (condvar) (fillfn))
  "Pops some number of items (at least one) from the FIFO."
  "The *maxcount* argument specifies the number of maximum number of items "
  "to be popped."
  (if (fifo-debug? fifo)
      (always%watch "FIFO/POP" fifo)
      (debug%watch "FIFO/POP" fifo))
  (set! condvar (fifo-condvar fifo))
  (set! fillfn (fifo-fillfn fifo))
  (set! fillthresh
    (if fillfn
	(getopt (fifo-opts fifo) 'fillthresh
		(quotient (fifo-size fifo) 2))
	0))
  (if (fifo-live? fifo)
      (unwind-protect
	  (begin
	    ;; Start critical section
	    (condvar/lock! condvar)
	    ;; Assert that we're waiting
	    (fifo/waiting! fifo)
	    ;; Wait until there's something to do or we're all done
	    (while (and (fifo-live? fifo)
			(or (overlaps? (fifo-pause fifo) '{read readwrite})
			    (not (stop-waiting? fifo fillthresh fillfn))))
	      (condvar/wait condvar))
	    (cond ((not (fifo-live? fifo)) (fail))
		  (else
		   (fifo/running! fifo)
		   (let* ((vec (fifo-queue fifo))
			  (start (fifo-start fifo))
			  (end (fifo-end fifo))
			  (count (min maxcount (- end start)))
			  (items (elts vec start (+ start count))))
		     (when (overlaps? items #f) (dbg fifo))
		     (if (fifo-debug? fifo)
			 (always%watch "FIFO/POP/ITEM" start end fifo items)
			 (debug%watch "FIFO/POP/ITEM" start end fifo items))
		     ;; Replace the popped item with false
		     (dotimes (i count)
		       (vector-set! vec (+ start i) #f))
		     ;; Advance the start pointer
		     (set-fifo-start! fifo (+ start count))
		     (when (= (fifo-start fifo) (fifo-end fifo))
		       ;; If we're empty, move the pointers back to the beginning
		       (set-fifo-start! fifo 0)
		       (set-fifo-end! fifo 0))
		     (when (fifo-items fifo)
		       (hashset-drop! (fifo-items fifo) items))
		     items))))
	(condvar/unlock! condvar))
      (fail)))
(define (fifo-pop fifo)
  "Pops some number of items (at least one) from the FIFO."
  "The *maxcount* argument specifies the number of maximum number of items "
  "to be popped."
  (fifo/pop fifo))

(define (fifo/popvec fifo (maxcount 1) (fillthresh #f) (condvar) (fillfn))
  "Pops some number of items (at least one) from the FIFO."
  "The *maxcount* argument specifies the number of maximum number of items "
  "to be popped."
  (if (fifo-debug? fifo)
      (always%watch "FIFO/POPVEC" fifo maxcount)
      (debug%watch "FIFO/POPVEC" fifo maxcount))
  (set! fillthresh (getopt (fifo-opts fifo) 'fillthresh
			    (quotient (fifo-size fifo) 2)))
  (set! condvar (fifo-condvar fifo))
  (set! fillfn (fifo-fillfn fifo))
  (if (fifo-live? fifo)
      (unwind-protect
	  (begin
	    ;; Start critical section
	    (condvar/lock! condvar)
	    ;; Assert that we're waiting
	    (fifo/waiting! fifo)
	    ;; Wait until there's something to do or we're all done
	    (while (and (fifo-live? fifo)
			(or (overlaps? (fifo-pause fifo) '{read readwrite})
			    (not (stop-waiting? fifo fillthresh fillfn))))
	      (condvar/wait condvar))
	    (cond ((not (fifo-live? fifo)) (fail))
		  (else
		   (fifo/running! fifo)
		   (let* ((vec (fifo-queue fifo))
			  (start (fifo-start fifo))
			  (end (fifo-end fifo))
			  (count (min maxcount (- end start)))
			  (items (slice vec start (+ start count))))
		     (if (fifo-debug? fifo)
			 (always%watch "FIFO/POP/ITEM" start end fifo items)
			 (debug%watch "FIFO/POP/ITEM" start end fifo items))
		     ;; Replace the popped item with false
		     (dotimes (i count)
		       (vector-set! vec (+ start i) #f))
		     ;; Advance the start pointer
		     (set-fifo-start! fifo (+ start count))
		     (when (= (fifo-start fifo) (fifo-end fifo))
		       ;; If we're empty, move the pointers back to the beginning
		       (set-fifo-start! fifo 0)
		       (set-fifo-end! fifo 0))
		     (when (fifo-items fifo)
		       (hashset-drop! (fifo-items fifo) (elts items)))
		     items))))
	(condvar/unlock! condvar))
      (fail)))

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
  (when (fifo-debug? fifo) (always%watch "FIFO/CLOSE!" fifo condvar))
  (unwind-protect
      (begin
	(when uselock (condvar/lock! condvar))
	(let ((queue (fifo-queue fifo)))
	  (set! result (slice queue (fifo-start fifo) (fifo-end fifo)))
	  (dotimes (i (length queue)) (vector-set! queue i #f))
	  (set-fifo-start! fifo 0)
	  (set-fifo-end! fifo 0)
	  (set-fifo-live?! fifo #f)
	  result))
    (begin
      (condvar/signal condvar #t)
      (when uselock (condvar/unlock! condvar)))))
(define (close-fifo fifo) (fifo/close! fifo))

(define (fifo/nofill fifo) 
  (when (zero? (fifo-load fifo))
    (when (fifo-debug? fifo) (always%watch "FIFO/EXHAUSTED!" fifo))
    (fifo/close! fifo #f)))
(define fifo/exhausted! fifo/nofill)

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
(define (fifo-space fifo) 
  (- (length (fifo-queue fifo))(- (fifo-end fifo) (fifo-start fifo))))

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
  (if (not (overlaps? rdwr '{#f read write readwrite closing}))
      (irritant rdwr |FIFO/BadPauseArg|)
      (unwind-protect 
	  (begin (condvar/lock! (fifo-condvar fifo))
	    (set-fifo-pause! fifo rdwr)
	    (condvar/signal (fifo-condvar fifo) #t))
	(condvar/unlock! (fifo-condvar fifo)))))

(define (fifo/pausing? fifo) (fifo-pause fifo))

(define (fifo/paused? fifo)
  (and (fifo-pause fifo) (fail? (fifo-running fifo))))

(define (fifo/set-debug! fifo (flag #t)) (set-fifo-debug! fifo flag))
(define (fifo/set-fillfn! fifo (fillfn #t)) (set-fifo-fillfn! fifo fillfn))
