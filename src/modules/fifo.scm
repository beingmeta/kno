(in-module 'fifo)

;;; Simple FIFO queue gated by a condition variable

(module-export! '{make-fifo fifo-push fifo-pop fifo-loop})

(define (make-fifo (size 64))
  (vector (make-condvar) (make-vector size) 0 0))

(define (fifo-push fifo item (broadcast #f))
  (unwind-protect
      (begin (condvar-lock (first fifo))
	     (let ((vec (elt fifo 1))
		   (start (elt fifo 2))
		   (end (elt fifo 3)))
	       (cond ((< end (length vec))
		      (vector-set! vec end item)
		      (vector-set! fifo 3 (1+ end)))
		     ((< (- end start) (-1+ (length vec)))
		      (dotimes (i (- end start))
			(vector-set! vec i (elt vec (+ start i))))
		      (dotimes (i (- (length vec) end))
			(vector-set! vec (+ end i) #f))
		      (vector-set! fifo 2 0)
		      (vector-set! fifo 3 (- end start))
		      (vector-set! vec (- end start) item))
		     (let ((newvec (make-vector (* 2 (length vec)))))
		       (dotimes (i (- end start))
			 (vector-set! newvec i (elt vec (+ start i))))
		       (vector-set! fifo 1 newvec)
		       (vector-set! fifo 2 0)
		       (vector-set! fifo 3 (- end start))
		       (vector-set! newvec (- end start) item))))
	     (condvar-signal (first fifo) broadcast))
    (condvar-unlock (first fifo))))

(define (fifo-pop fifo)
  (unwind-protect
      (begin (condvar-lock (first fifo))
	     (while (= (elt fifo 2) (elt fifo 3))
	       (condvar-wait (first fifo)))
	     (let* ((vec (elt fifo 1))
		    (start (elt fifo 2))
		    (end (elt fifo 3))
		    (item (elt vec start)))
	       ;; Replace the item with false
	       (vector-set! vec start #f)
	       ;; Advance the start pointer
	       (vector-set! fifo 2 (1+ start))
	       item))
    (condvar-unlock (first fifo))))

(define (fifo-loop fifo handler)
  (let ((result #t))
    (while result
      (set! result (handler (fifo-pop fifo))))))

