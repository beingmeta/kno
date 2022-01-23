;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)

(in-module 'fifo/call)

(use-module '{fifo logger varconfig reflection kno/threads})

(module-export! '{fifo/call})

;;; Simple FIFO queue gated by a condition variable

(define (dcall-threadfn fifo fcn results)
  (local args (fifo/pop fifo))
  (while (exists? args)
    (cond ((not results) (apply fcn args))
	  ((hashset? results)
	   (hashset-add! results (apply fcn args)))
	  ((applicable? results)
	   (results (apply fcn args)))
	  (else (apply fcn args)))
    (set! args (fifo/pop fifo))))

(defambda (fifo/call opts fcn . args)
  (let* ((choices (apply list args))
	 (n-threads (threadcount (if (opts? opts) (getopt opts 'threads #t) opts)))
	 (opts (if (opts? opts) opts #[]))
	 (results (getopt opts 'results (make-hashset)))
	 (fifo (->fifo choices opts))
	 (threads {}))
    (cond ((not n-threads) (set! n-threads 1))
	  ((> n-threads (|| choices)) (set! n-threads (|| choices)))
	  ((not (number? n-threads)) (set! n-threads (threadcount #t))))
    (dotimes (i n-threads)
      (set+! threads (thread/call dcall-threadfn fifo fcn results)))
    (store! (fifo-notes fifo) 'threadids (thread-id threads))
    (cons fifo (qc threads))))
