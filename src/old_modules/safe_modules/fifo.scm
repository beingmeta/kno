;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

(in-module 'fifo)

;;; Simple FIFO queue gated by a condition variable

(use-module '{ezrecords logger})
(define %used_modules 'ezrecords)

(module-export!
 '{fifo/make fifo/close
   fifo/pop fifo/remove! fifo/push! fifo/jump!  
   fifo/loop fifo/queued fifo/load})

(module-export!
 '{make-fifo
   fifo-push fifo-pop fifo-jump fifo-loop fifo-queued close-fifo
   fifo-load fifo-live? fifo-waiting fifo/waiting
   fifo/idle? fifo/set-debug!})

(define-init %loglevel %warn%)

;;;; Implementation

(define (fifo->string fifo)
  (stringout "#<FIFO "
    (- (fifo-end fifo) (fifo-start fifo)) "/" (length (fifo-queue fifo)) " "
    (or (fifo-name fifo) (glom "0x" (number->string (hashptr fifo) 16))) 
    (if (not (fifo-live? fifo)) " (dead)")
    (if (fifo-debug fifo) " (debug)")
    ">"))

(defrecord (fifo MUTABLE OPAQUE `(stringfn . fifo->string))
  name state queue items start end live? (waiting 0) (debug #f))

(define (fifo/make (name #f) (size #f))
  (cond ((and (number? name) (not size))
	 (set! size name) (set! name #f))
	((not (number? size)) (set! size 64)))
  (cons-fifo name (make-condvar) (make-vector size) (make-hashset) 0 0 #t))
(define (make-fifo (size 64)) (fifo/make size))

(define (fifo/push! fifo item (broadcast #f))
  (unwind-protect
      (begin (if (fifo-debug fifo)
		 (always%watch "FIFO/PUSH!" item broadcast fifo)
		 (debug%watch "FIFO/PUSH!" item broadcast fifo))
	(condvar-lock (fifo-state fifo))
	(if (hashset-get (fifo-items fifo) item)
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
	(condvar-signal (fifo-state fifo) broadcast))
    (condvar-unlock (fifo-state fifo))))
(define (fifo-push fifo item (broadcast #f)) (fifo/push! fifo item broadcast))

(define (fifo-waiting! fifo flag)
 (if (fifo-debug fifo)
     (always%watch "FIFO-WAITING!" flag (fifo-waiting fifo) fifo)
     (debug%watch "FIFO-WAITING!" flag (fifo-waiting fifo) fifo))
 (set-fifo-waiting! fifo (+ (if flag 1 -1) (fifo-waiting fifo)))
 (condvar-signal (fifo-state fifo) #t)
 (fifo-waiting fifo))

(define (fifo/pop fifo)
  (if (fifo-debug fifo)
      (always%watch "FIFO/POP" fifo)
      (debug%watch "FIFO/POP" fifo))
  (if (fifo-live? fifo)
      (unwind-protect
	  (begin (condvar-lock (fifo-state fifo))
	    ;; Wait for something to pop
	    (fifo-waiting! fifo #t)
	    (while (and (fifo-live? fifo)
			(= (fifo-start fifo) (fifo-end fifo)))
	      (condvar-wait (fifo-state fifo)))
	    (fifo-waiting! fifo #f)
	    ;; If it's still alive, do the pop
	    (if (fifo-live? fifo) 
		(let* ((vec (fifo-queue fifo))
		       (start (fifo-start fifo))
		       (end (fifo-end fifo))
		       (item (elt vec start)))
		  (if (fifo-debug fifo)
		      (always%watch "FIFO/POP/ITEM" start end item fifo)
		      (debug%watch "FIFO/POP/ITEM" start end item fifo))
		  ;; Replace the item with false
		  (vector-set! vec start #f)
		  ;; Advance the start pointer
		  (set-fifo-start! fifo (1+ start))
		  (when (= (fifo-start fifo) (fifo-end fifo))
		    ;; If we're empty, move the pointers back
		    (set-fifo-start! fifo 0)
		    (set-fifo-end! fifo 0))
		  (hashset-drop! (fifo-items fifo) item)
		  item)
		(fail)))
	(condvar-unlock (fifo-state fifo)))
      (fail)))
(define (fifo-pop fifo) (fifo/pop fifo))

(define (fifo/remove! fifo item)
  (if (fifo-live? fifo)
      (unwind-protect
	  (begin (condvar-lock (fifo-state fifo))
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
			     (hashset-drop! (fifo-items fifo) item)
			     queued)
			   (fail)))
		     (fail)))
	(condvar-unlock (fifo-state fifo)))
      (fail)))
(define (fifo-jump fifo item) (fifo/remove! fifo item))

(define (fifo/loop fifo handler)
  (let ((result #t))
    (while (and (exists? result) result)
      (set! result (handler (fifo-pop fifo))))))
(define (fifo-loop fifo handler) (fifo/loop fifo handler))

(define (fifo/close fifo (broadcast #t) (result #f))
  (unwind-protect
      (begin
	(condvar-lock (fifo-state fifo))
	(set! result
	      (slice (fifo-queue fifo)
		     (fifo-start fifo) (fifo-end fifo)))
	(set-fifo-live?! fifo #f)
	result)
    (begin
      (unless broadcast (fifo-state fifo))
      (when broadcast
	(condvar-signal (fifo-state fifo) #t)
	(condvar-unlock (fifo-state fifo))))))
(define (close-fifo fifo (broadcast #t))
  (fifo/close fifo broadcast))

(define (fifo/queued fifo (result #f))
  (unwind-protect
      (begin (condvar-lock (fifo-state fifo))
	(set! result (subseq (fifo-queue fifo)
			     (fifo-start fifo)
			     (fifo-end fifo))))
    (condvar-unlock (fifo-state fifo))))
(define (fifo-queued fifo) (fifo/queued fifo))

(define (fifo/load fifo)
  (unwind-protect
      (begin (condvar-lock (fifo-state fifo))
	     (- (fifo-end fifo) (fifo-start fifo)))
    (condvar-unlock (fifo-state fifo))))
(define (fifo-load fifo) (- (fifo-end fifo) (fifo-start fifo)))

(define (fifo/waiting fifo (nonzero #t))
  (unwind-protect
      (begin (condvar-lock (fifo-state fifo))
	(when nonzero
	  (while (zero? (fifo-waiting fifo))
	    (condvar-wait (fifo-state fifo))))
	(fifo-waiting fifo))
    (condvar-unlock (fifo-state fifo))))

(define (fifo/idle? fifo)
  (unwind-protect
      (begin (condvar-lock (fifo-state fifo))
	(until (and (not (zero? (fifo-waiting fifo)))
		    (= (fifo-start fifo) (fifo-end fifo)))
	  (condvar-wait (fifo-state fifo)))
	(and (not (zero? (fifo-waiting fifo)))
	     (= (fifo-start fifo) (fifo-end fifo))))
    (condvar-unlock (fifo-state fifo))))

(define (fifo/set-debug! fifo (flag #t)) (set-fifo-debug! fifo flag))
