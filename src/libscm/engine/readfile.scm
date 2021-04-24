;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)

(in-module 'engine/readfile)

(use-module '{fifo varconfig kno/threads text/stringfmts binio logger})
(use-module '{engine})

(module-export '{engine/readfile/fillfn})

(define %loglevel %notice%)

(define (engine/readfile/fillfn count loop-state)
  (unless (test loop-state 'instream) (init-instream! loop-state))
  (let* ((opts (get loop-state 'opts))
	 (task (get loop-state 'taskstate))
	 (readfn (getopt opts 'readfn #f))
	 (instream (get loop-state 'instream))
	 (result (make-vector count))
	 (written 0))
    (dotimes (i count)
      (let ((item (readfn instream)))
	(when (and (exists? item) item)
	  (vector-set! result written item)
	  (set! written (1+ written)))))
    (store! loop-state 'bytes (getpos stream))
    (if (= written count) result
	(slice result 0 written))))

(define (init-instream! loop-state)
  (let* ((opts (get loop-state 'opts))
	 (task (get loop-state 'taskstate))
	 (infile (getopt opts 'infile #f))
	 (openfn (getopt opts 'openfn open-input-file))
	 (bytes (try (get task 'bytes) 0))
	 (instream (openfn infile opts))
	 (maxbytes (try (get task 'maxbytes) (file-size infile))))
    (setpos! stream bytes)
    (store! loop-state 'instream instream)
    (store! loop-state 'bytes bytes)
    (store! task 'bytes bytes)
    (store! task 'maxbytes maxbytes)
    (add! loop-state 'counters 'bytes)
    instream))

