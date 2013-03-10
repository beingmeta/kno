;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc.  All rights reserved.

(in-module 'workqueue)

(use-module '{ezrecords logger varconfig})

(module-export! '{cons-workqueue
		  queue/get queue/probe queue/push! queue/close!
		  queue/tester})

(define-init %loglevel %info!)
;;(define %loglevel %debug!)

(defrecord (workqueue MUTABLE OPAQUE)
  (queue '()) (lock (make-condvar)))

(define (queue/push! queue value)
  (unwind-protect
      (let ((items (begin (condvar-lock (workqueue-lock queue))
		     (workqueue-queue queue))))
	(if (not items) (error "Pushing onto a closed queue")
	    (begin (set-workqueue-queue! queue (cons value items))
	      (condvar-signal (workqueue-lock queue) #t))))
    (condvar-unlock (workqueue-lock queue))))

(define (queue/get queue)
  (unwind-protect
      (let* ((items (begin (condvar-lock (workqueue-lock queue))
		      (workqueue-queue queue))))
	(if (not items) items
	    (begin
	      (while (null? (workqueue-queue queue)) (condvar-wait (workqueue-lock queue)))
	      (let* ((items (workqueue-queue queue)) (v (car items)))
		(set-workqueue-queue! queue (cdr items))
		v))))
    (condvar-unlock (workqueue-lock queue))))

(define (queue/probe queue value)
  (unwind-protect
      (begin (condvar-lock (workqueue-lock queue))
	(if (not (workqueue-queue queue))
	    (error "Queue is closed")
	    (if (null? ) #f (length (workqueue-queue queue)))))
    (condvar-unlock (workqueue-lock queue))))

(define (queue/tester q wait)
  (let ((item (queue/get q)))
    (while item
      (let ((wait (+ (/ wait 2) (random (/ wait 2)))))
	(lognotice "QUEUE/TESTER" "Got item " item ", waiting " wait)
	(sleep (floor wait))
	(lognotice "Finished with " item))
      (set! item (queue/get q)))))
