;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc.  All rights reserved.

(in-module 'fifo)

;;; Simple FIFO queue gated by a condition variable
(define version "$Id$")
(define revision "$Revision:$")

(use-module 'ezrecords)

(module-export!
 '{fifo/make fifo/close
   fifo/pop fifo/push! fifo/jump!  
   fifo/loop fifo/queued fifo/load})

(module-export!
 '{make-fifo
   fifo-push fifo-pop fifo-jump fifo-loop fifo-queued close-fifo
   fifo-load})

;;;; Implementation

(defrecord (fifo MUTABLE OPAQUE) condvar queue items start end live?)

(define (fifo/make (size 64))
  (cons-fifo (make-condvar) (make-vector size) (make-hashset) 0 0 #t))
(define (make-fifo (size 64)) (fifo/make size))

(define (fifo/push! fifo item (broadcast #f))
  (unwind-protect
      (begin (condvar-lock (fifo-condvar fifo))
	(unless (hashset-get (fifo-items fifo) item)
	  (let ((vec (fifo-queue fifo))
		(start (fifo-start fifo))
		(end (fifo-end fifo)))
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
		     (dotimes (i (- end start))
		       (vector-set! newvec i (elt vec (+ start i))))
		     (set-fifo-queue! fifo newvec)
		     (set-fifo-start! fifo 0)
		     (vector-set! newvec (- end start) item)
		     (set-fifo-end! fifo (1+ (- end start))))))))
	(condvar-signal (fifo-condvar fifo) broadcast))
    (condvar-unlock (fifo-condvar fifo))))
(define (fifo-push fifo item (broadcast #f)) (fifo/push! fifo item broadcast))

(define (fifo/pop fifo)
  (if (fifo-live? fifo)
      (unwind-protect
	  (begin (condvar-lock (fifo-condvar fifo))
		 ;; Wait for something to pop
		 (while (and (fifo-live? fifo)
			     (= (fifo-start fifo) (fifo-end fifo)))
		   (condvar-wait (fifo-condvar fifo)))
		 ;; If it's still alive, do the pop
		 (if (fifo-live? fifo) 
		     (let* ((vec (fifo-queue fifo))
			    (start (fifo-start fifo))
			    (end (fifo-end fifo))
			    (item (elt vec start)))
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
	(condvar-unlock (fifo-condvar fifo)))
      (fail)))
(define (fifo-pop fifo) (fifo/pop fifo))

(define (fifo/remove! fifo item)
  (if (fifo-live? fifo)
      (unwind-protect
	  (begin (condvar-lock (fifo-condvar fifo))
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
	(condvar-unlock (fifo-condvar fifo)))
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
	(condvar-lock (fifo-condvar fifo))
	(set! result
	      (slice (fifo-queue fifo)
		     (fifo-start fifo) (fifo-end fifo)))
	(fifo-set-live? fifo #f)
	result)
    (begin
      (unless broadcast (fifo-condvar fifo))
      (when broadcast
	(condvar-signal (fifo-condvar fifo) #t)
	(condvar-unlock (fifo-condvar fifo))))))
(define (close-fifo fifo (broadcast #t))
  (fifo/close fifo broadcast))

(define (fifo/queued fifo (result #f))
  (unwind-protect
      (begin (condvar-lock (fifo-condvar fifo))
	(set! result (subseq (fifo-queue fifo)
			     (fifo-start fifo)
			     (fifo-end fifo))))
    (condvar-unlock (fifo-condvar fifo))))
(define (fifo-queued fifo) (fifo/queued fifo))

(define (fifo/load fifo)
  (unwind-protect
      (begin (condvar-lock (fifo-condvar fifo))
	     (- (fifo-end fifo) (fifo-start fifo)))
    (condvar-unlock (fifo-condvar fifo))))
(define (fifo-load fifo) (fifo/load fifo))


