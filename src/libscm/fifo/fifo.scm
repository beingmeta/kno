;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)

(in-module 'fifo)

;;; Simple FIFO queue gated by a condition variable

(use-module '{ezrecords logger varconfig reflection})
(define %used_modules 'ezrecords)

(module-export!
 '{->fifo fifo/make fifo/close! fifo?
   fifo/pop fifo/popvec fifo/remove!
   fifo/push/n!
   fifo/push!
   fifo/push/all!
   fifo/release!
   fifo/finished!
   fifo/jump! 
   fifo/wait
   fifo/pause!
   fifo/pausing?
   fifo/paused?
   fifo/exhausted?
   fifo/loop
   fifo/queued
   fifo/load
   fifo/fill!
   fifo-name
   fifo-notes
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
   fifo-readonly?
   fifo-opts
   fifo-live?
   fifo-size
   fifo-waiting
   fifo-running
   fifo/waiting
   fifo/idle?
   fifo/set-debug!
   fifo/readonly!})

(define-init %loglevel %warn%)

;;;; Implementation

(define default-debug #f)
(varconfig! fifo:debug default-debug config:boolean)

(define (fifo/exhausted? fifo)
  (and (fifo-readonly? fifo) (zero? (- (fifo-end fifo) (fifo-start fifo)))))

(define (fifo->string fifo)
  (if (fifo-name fifo)
      (stringout "#<FIFO "
	(fifo-name fifo) " "
	(- (fifo-end fifo) (fifo-start fifo)) "/" (length (fifo-queue fifo)) 
	(when (or (fifo-debug fifo) default-debug)
	  (printout " [" (fifo-start fifo) "," (fifo-end fifo) "] "))
	"-r" (choice-size (fifo-running fifo)) "-w" (choice-size (fifo-waiting fifo)) " "
	(if (not (fifo-live? fifo)) " (exhausted)")
	(if (fifo-readonly? fifo) " (readonly)")
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
  name           ;; a string
  condvar        ;; a condvar
  queue          ;; a vector
  start          ;; the vector index for the next item
  end            ;; the vector index for the last item
  opts           ;; options
  (items #f)     ;; a hashset of items waiting in the queue, if nodups is set
  (grow #f)
  (maxlen #f)    ;; The max length to which the fifo will grow
  (readonly? #f) ;; whether the FIFO is readonly
  (live? #t)     ;; Whether the FIFO is active (callers should wait)
  (pause #f)     ;; Whether the FIFO is paused (value is #f, READ, WRITE, READWRITE, or CLOSING)
  (waiting {})   ;; The threads waiting on the FIFO.
  (running {})   ;; The threads currently processing results from the FIFO.
  (filling #f)   ;; The thread currently filling the FIFO
  (notes #[])    ;; Slotmap for generic notes
  (debug #f)     ;; Whether we're debugging the FIFO
  )

(define (fifo-debug? fifo) (or default-debug (fifo-debug fifo)))

(define (fifo/make . args)
  "Creates a new FIFO object from its arguments which can be "
  "strings or symbols (for the name), positive fixnums (for the initial size), "
  "vectors (for data), or tables (for options). Options provide defaults "
  "for these other attributes."
  (let ((size #f) (name #f) (data #f) (opts #f) (queue #f) 
	(items #f) (debug #f) (live? #t) (init-items 0)
	(grow #f) (maxlen #f))
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
    (set! grow (if maxlen 
		   (and (< size maxlen) (getopt opts 'grow #f))
		   (getopt opts 'grow #f)))
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
    (cons-fifo name (make-condvar) queue 0 init-items opts items
	       grow maxlen (getopt opts 'readonly (> init-items 0))
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

;;; Wrappers for fifo/push/n!

(define (fifo/push/all! fifo items (opts #f))
  "Pushes all of *items* into the FIFO *fifo*. "
  "See fifo/push/n! for information on *opts*."
  (set! opts (opt+ 'block #f opts (fifo-opts opts)))
  (while (and items (> (length items) 0))
    (set! items (fifo/push/n! fifo items opts))))

(define (fifo/push! fifo item (opts #f))
  "Pushes *item* into the FIFO *fifo*. "
  "See fifo/push/n! for information on *opts*."
  (fifo/push/n! fifo (vector item) opts))

(define (fifo-push fifo item (opts #f))
  "Pushes *item* into the FIFO *fifo*. "
  "See fifo/push/n! for information on *opts*."
  (fifo/push/n! fifo (vector item) opts))

;;; The core procedure

(define (fifo/push/n! fifo items (opts #f))
  "Pushes multiple items into the FIFO, possibly returning any "
  "unpushed items as a vector. Options include: "
  "* GROW (grows the FIFO if needed) and "
  "* BLOCK (wait for at least some available space). "
  "If #f is returned, all the items were pushed."
  (when (fifo-readonly? fifo) (irritant fifo |ReadOnlyFIFO| "Pushing " items))
  (cond ((not items) items)
	((not (vector? items)) (irritant items |NotAVector| fifo/push/n!))
	((= (length items) 1) (fifo/push/some! fifo items opts))
	((zero? (length items)) #f)
	(else (let ((topush items))
		(while (vector? topush)
		  (set! topush (fifo/push/some! fifo topush opts)))
		topush))))

(define (fifo/push/some! fifo items (opts #f))
  "Pushes multiple items into the FIFO and returns any "
  "unpushed items as a vector. Options include: "
  "* GROW (grows the FIFO if needed and allowed) and "
  "* BLOCK (wait for at least some available space). "
  "If #f is returned, all the items were pushed. If a pair "
  "is returned it indicates that the fifo is blocked and "
  "returns the reason for the blockage and the remaining "
  "items to be added."
  (when (fifo-readonly? fifo) (irritant fifo |ReadOnlyFIFO|))
  (set! opts (opt+ opts (fifo-opts fifo)))
  (locals len (length items)
	  condvar (fifo-condvar fifo)
	  block (getopt opts 'block #t)
	  grow (fifo-grow fifo)
	  locked #f)
  (unwind-protect
      (begin (if (fifo-debug? fifo)
		 (always%watch "FIFO/PUSH/N!/prelock" "N" len fifo "\n" opts)
		 (debug%watch "FIFO/PUSH/N!/prelock" "N" len fifo "\n" opts))
	(condvar/lock! (fifo-condvar fifo))
	(set! locked #t)
	(when block
	  (while (and (fifo-live? fifo)
		      (or (overlaps? (fifo-pause fifo) '{write readwrite})
			  (and (not grow)
			       (= (fifo-size fifo) (- (fifo-end fifo) (fifo-start fifo))))))
	    (debug%watch "FIFO/BLOCKING/unlocking" fifo grow (fifo-size fifo) (fifo-load fifo))
	    (set! locked #f)
	    (condvar/wait condvar)
	    (set! locked #t)
	    (set! grow (fifo-grow fifo)))
	  (debug%watch "FIFO/BLOCKING/run" fifo grow (fifo-size fifo) (fifo-load fifo)))
	(cond ((not (fifo-live? fifo)) (cons 'dead items))
	      ((overlaps? (fifo-pause fifo) '{write readwrite}) (cons 'paused items))
	      ((= (fifo-size fifo) (- (fifo-end fifo) (fifo-start fifo)))
	       items)
	      (else (prog1 (fifo-push-unlocked fifo items grow)
		      (condvar/signal condvar #t)))))
    (when locked (condvar/unlock! (fifo-condvar fifo)))))
  
(define (fifo-push-unlocked fifo items (grow #f))
  (locals addn (length items))
  (if (and (> addn (fifo-load fifo)) grow)
      (grow-fifo/unlocked fifo addn)
      (if (> addn (fifo-tail fifo)) (compress-fifo/unlocked fifo)))
  (let ((pushn (min (length items) (fifo-tail fifo)))
	(queue (fifo-queue fifo))
	(end (fifo-end fifo)))
    (dotimes (i pushn)
      (vector-set! queue (+ end i) (elt items i)))
    (set-fifo-end! fifo (+ (fifo-end fifo) pushn))
    (and (> (length items) pushn)
	 (slice items pushn))))

(define (compress-fifo/unlocked fifo (vec) (start) (end))
  (default! vec (fifo-queue fifo))
  (default! start (fifo-start fifo))
  (default! end (fifo-end fifo))
  (local load (- end start))
  (dotimes (i load)
    (vector-set! vec i (elt vec (+ start i))))
  (set-fifo-start! fifo 0)
  (set-fifo-end! fifo load)
  (dotimes (i (- (length vec) load))
    (vector-set! vec (+ load i) #f))
  ;; Return the load, also the new fifo-end
  load)
       
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
      (set-fifo-queue! fifo newvec)
      (when (= newlen maxlen) (set-fifo-grow! fifo #f)))))

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

;;;; FIFO control flow functions

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

(define (fifo/pop fifo (maxcount 1) (opts #f))
  "Pops some number of items (at least one) from the FIFO."
  "The *maxcount* argument specifies the number of maximum number of items "
  "to be popped."
  (elts (fifo/popvec fifo maxcount opts)))

(define (fifo-pop fifo)
  "Pops some number of items (at least one) from the FIFO."
  "The *maxcount* argument specifies the number of maximum number of items "
  "to be popped."
  (fifo/pop fifo))

(define (fifo/popvec fifo (maxcount 1) (opts #f))
  "Pops some number of items (at least one) from the FIFO."
  "The *maxcount* argument specifies the number of maximum number of items "
  "to be popped."
  (locals condvar (fifo-condvar fifo)
	  block (getopt opts 'block (not (fifo-readonly? fifo))))
  (if (fifo-debug? fifo)
      (always%watch "FIFO/POPVEC" fifo maxcount)
      (debug%watch "FIFO/POPVEC" fifo maxcount))
  (if (fifo-live? fifo)
      (unwind-protect
	  (begin
	    ;; Start critical section
	    (condvar/lock! condvar)
	    ;; Assert that we're waiting
	    (fifo/waiting! fifo)
	    ;; Wait until there's something to do or we're all done
	    (while (and block
			(fifo-live? fifo)
			(or (overlaps? (fifo-pause fifo) '{read readwrite}) 
			    (and (not (fifo-readonly? fifo))
				 (zero? (- (fifo-end fifo) (fifo-start fifo))))))
	      (condvar/wait condvar))
	    (cond ((not (fifo-live? fifo)) (fail))
		  ((and (= (fifo-end fifo) (fifo-start fifo)) (fifo-readonly? fifo))
		   (fifo/close! fifo #f)
		   (fail))
		  ((= (fifo-end fifo) (fifo-start fifo)) (fail))
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

(define (fifo/close! fifo (needlock #t))
  "Closes a FIFO, returning a vector of the remaining queued items."
  (locals condvar (fifo-condvar fifo) result #f)
  (when (fifo-debug? fifo) (always%watch "FIFO/CLOSE!" fifo condvar))
  (unwind-protect
      (begin
	(when needlock (condvar/lock! condvar))
	(let ((queue (fifo-queue fifo)))
	  (set! result (slice queue (fifo-start fifo) (fifo-end fifo)))
	  (dotimes (i (length queue)) (vector-set! queue i #f))
	  (set-fifo-start! fifo 0)
	  (set-fifo-end! fifo 0)
	  (set-fifo-live?! fifo #f)
	  (set-fifo-readonly?! fifo 'closed)
	  result))
    (begin
      (condvar/signal condvar #t)
      (when needlock (condvar/unlock! condvar)))))
(define (close-fifo fifo) (fifo/close! fifo))

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
(define (fifo-tail fifo) (- (length (fifo-queue fifo)) (fifo-end fifo)))
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

(define (fifo/readonly! fifo (flag #t))
  (set-fifo-readonly?! fifo flag)
  (condvar/signal (fifo-condvar fifo) #t))
