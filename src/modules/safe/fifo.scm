;;; -*- Mode: Scheme; Character-Encoding: utf-8; -*-

(in-module 'fifo)

;;; Simple FIFO queue gated by a condition variable

(define version "$Id$")

(use-module 'ezrecords)

(module-export!
 '{make-fifo fifo-push fifo-pop fifo-loop fifo-queued close-fifo})

;;;; Implementation

(defrecord (fifo MUTABLE) condvar queue start end live?)

(define (make-fifo (size 64))
  (cons-fifo (make-condvar) (make-vector size) 0 0 #t))

(define (fifo-push fifo item (broadcast #f))
  (unwind-protect
      (begin (condvar-lock (fifo-condvar fifo))
	     (let ((vec (fifo-queue fifo))
		   (start (fifo-start fifo))
		   (end (fifo-end fifo)))
	       (cond ((< end (length vec))
		      (vector-set! vec end item)
		      (set-fifo-end! fifo (1+ end)))
		     ((< (- end start) (-1+ (length vec)))
		      ;; Move the queue to the start of the vector
		      (dotimes (i (- end start))
			(vector-set! vec i (elt vec (+ start i))))
		      (dotimes (i (- (length vec) end))
			(vector-set! vec (+ end i) #f))
		      (set-fifo-start! fifo 0)
		      (set-fifo-end! fifo (- end start))
		      (vector-set! vec (- end start) item))
		     (else
		      (let ((newvec (make-vector (* 2 (length vec)))))
			(dotimes (i (- end start))
			  (vector-set! newvec i (elt vec (+ start i))))
			(set-fifo-queue! fifo newvec)
			(set-fifo-start! fifo 0)
			(set-fifo-end! fifo (- end start))
			(vector-set! newvec (- end start) item))))
	       (condvar-signal (fifo-condvar fifo) broadcast)))
    (condvar-unlock (fifo-condvar fifo))))

(define (fifo-pop fifo)
  (if (fifo-live? fifo)
      (unwind-protect
	  (begin (condvar-lock (fifo-condvar fifo))
		 (while (and (fifo-live? fifo)
			     (= (fifo-start fifo) (fifo-end fifo)))
		   (condvar-wait (fifo-condvar fifo)))
		 (if (fifo-live? fifo) 
		     (let* ((vec (fifo-queue fifo))
			    (start (fifo-start fifo))
			    (end (fifo-end fifo))
			    (item (elt vec start)))
		       ;; Replace the item with false
		       (vector-set! vec start #f)
		       ;; Advance the start pointer
		       (set-fifo-start! fifo (1+ start))
		       item)
		     (fail)))
	(condvar-unlock (fifo-condvar fifo)))
      (fail)))

(define (fifo-loop fifo handler)
  (let ((result #t))
    (while (and (exists? result) result)
      (set! result (handler (fifo-pop fifo))))))

(define (close-fifo fifo (broadcast #t))
  (unwind-protect
      (begin
	(condvar-lock (fifo-condvar fifo))
	(vector-set! fifo 4 #f))
    (begin
      (unless broadcast (fifo-condvar fifo))
      (when broadcast
	(condvar-signal (fifo-condvar fifo) #t)
	(condvar-unlock (fifo-condvar fifo))))))

(define (fifo-queued fifo)
  (let ((result #f))
    (unwind-protect
	(begin (condvar-lock (fifo-condvar fifo))
	       (set! result (subseq (fifo-queue fifo)
				    (fifo-start fifo)
				    (fifo-end fifo))))
      (condvar-unlock (fifo-condvar fifo)))))



