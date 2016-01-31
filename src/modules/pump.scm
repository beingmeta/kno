;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

(in-module 'pump)

(use-module '{ezrecords logger varconfig fifo})

(module-export! '{pump/make
		  pump/push! pump/cancel!
		  pump/pending pump/shutdown
		  pump-name pump-input})

(define-init %loglevel %notice%)

(define default-pump-queue 64)
(define named-pumps 64)

(defrecord (pump OPAQUE)
  name dofn donefn options
  (input (fifo/make default-pump-queue))
  (output (fifo/make default-pump-queue))
  (live? #t) (threads (make-hashtable)) (items (make-hashset))
  (running (make-hashtable)))

(define default-opts #[])

;;; The default pump function

(define (pumpfn pump threadid)
  (and (pump-live? pump)
       (let ((item (fifo/pop (pump-input pump)))
	     (running (pump-running pump))
	     (liveitems (pump-items pump))
	     (dofn (pump-dofn pump))
	     (donefn (pump-donefn pump)))
	 (while (and (exists? item) (pump-live? pump))
	   (unwind-protect
	       (begin (store! running item (cons threadid (gmtimestamp)))
		 (store! running threadid item)
		 (dofn item))
	     (hashset-drop! liveitems item)
	     (when donefn (donefn item pump threadid))
	     (drop! running item (cons threadid (gmtimestamp)))
	     (drop! running threadid))
	   (set! item (fifo/pop (pump-input pump))))
	 #f)))

;;; Making new pumps

(define (pump/make dofn (donefn #f) (name #f) (opts #f))
  (if (not opts)
      (set! opts default-opts)
      (if (not (pair? opts))
	  (set! opts (cons opts default-opts))))
  (if (and name (test named-pumps name))
      (error DUPLICATENAME pump/make "A pump named " name " already exists")
      (freshpump dofn donefn name opts)))

(defslambda (freshpump dofn donefn name opts)
  (let* ((input (fifo/make default-pump-queue))
	 (pump (cons-pump (or name (getuuid)) dofn donefn input))
	 (minthreads (getopt opts 'minthreads 4)))
    (dotimes (i minthreads)
      (let* ((threadid (threadcall pumpfn pump))
	     (thread (threadcall pumpfn pump threadid)))
	(store! (pump-threads pump) threadid thread)
	(set+! threads thread)))
    (when name (store! named-pumps name pump))
    pump))

(define (pump/push! pump item)
  (unless (get (pump-items pump) item)
    (fifo/push! (pump-input pump) item)))

(define (pump/cancel! pump item)
  (when (get (pump-items pump) item)
    (fifo/remove! (pump-input pump) item)))

(define (pump/pending pump)
  (fifo/queued (pump-input pump)))

(define (pump/shutdown pump)
  (fifo/close (pump-input pump)))




