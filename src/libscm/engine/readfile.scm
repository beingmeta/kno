;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)

(in-module 'engine/readfile)

(use-module '{fifo varconfig kno/threads text/stringfmts binio logger})
(use-module '{engine})

(module-export! '{engine/readfile/fillfn engine/readfile/log})

(define %loglevel %notice%)

(define (engine/readfile/fillfn count loop-state)
  (unless (test loop-state 'instream) (init-instream! loop-state))
  (let* ((opts (get loop-state 'opts))
	 (task (get loop-state 'task))
	 (readfn (getopt opts 'readfn getline))
	 (instream (get loop-state 'instream))
	 (result (make-vector count))
	 (written 0)
	 (item #f))
    (dotimes (i count)
      (set! item (readfn instream))
      (cond ((eof-object? item)
	     (unless (test loop-state 'done)
	       (engine/stop! loop-state "InputEndOfFile")
	       (store! loop-state 'done (timestamp)))
	     (break))
	    ((fail? item)
	     (engine/stop! loop-state "FillFailed")
	     (break))
	    (item
	     (vector-set! result written item)
	     (set! written (1+ written)))
	    (else)))
    (store! loop-state 'bytes (getpos instream))
    (if (= written count) result
	(slice result 0 written))))

(define (init-instream! loop-state)
  (let* ((opts (get loop-state 'opts))
	 (task (get loop-state 'task))
	 (infile (getopt opts 'infile #f))
	 (openfn (getopt opts 'openfn open-input-file))
	 (instream (openfn infile opts))
	 (bytes (try (get task 'bytes) (getpos instream)))
	 (maxbytes (try (get task 'maxbytes) (file-size infile))))
    (setpos! instream bytes)
    (store! loop-state 'instream instream)
    (add! loop-state 'absolute '{bytes maxbytes})
    (store! loop-state 'bytes bytes)
    (store! loop-state 'maxbytes maxbytes)
    (add! loop-state 'counters 'bytes)
    instream))

(define (engine/readfile/log loop-state)
  (when (and (test loop-state 'instream) (test loop-state 'bytes))
    (let* ((elapsed (elapsed-time (get loop-state 'started)))
	   (task (get loop-state 'task))
	   (total-read (get loop-state 'bytes))
	   (delta-read (- total-read (try (get task 'bytes) 0)))
	   (bytes-total (try (get task 'maxbytes) (get loop-state 'maxbytes)))
	   (%loglevel (getopt loop-state 'loglevel %loglevel)))
      (lognotice |Engine/Readfile/Progress|
	"Read " ($count delta-read "byte") " in " (secs->string elapsed) 
	", averaging " ($rate delta-read elapsed) " bytes/sec")
      (when (exists? bytes-total)
	(let* ((total-items (+ (try (get task 'items) 0) (get loop-state 'items)))
	       (clocktime (try (+ (try (get task 'clocktime) 0) elapsed)
			       (difftime (get task 'started))))
	       (count-term (try (get loop-state 'count-term) "item")))
	  (lognotice |Engine/Readfile/Overall|
	    "Read " ($count total-read "byte") 
	    " (" (show% total-read bytes-total) ") "
	    "comprising "
	    ($count total-items count-term) 
	    " in " (secs->string clocktime) 
	    " since "  (get (get task 'started) 'string)
	    " effectively " ($rate total-read clocktime) " bytes/second")
	  (let* ((current-rate (/~ delta-read elapsed))
		 (bytes-to-go (- bytes-total total-read))
		 (secs-to-go (/ bytes-to-go current-rate)))
	    (lognotice |Engine/Readfile/Projection|
	      "At the current rate of " ($rate delta-read elapsed) " bytes/sec, "
	      "reading the remaining " ($count bytes-to-go "byte")
	      " should take " (secs->string secs-to-go) " finishing " 
	      "around " (get (timestamp+ secs-to-go) 'string)))
	  (let* ((overall-rate (/~ total-read clocktime))
		 (bytes-to-go (- bytes-total total-read))
		 (secs-to-go (/ bytes-to-go overall-rate)))
	    (lognotice |Engine/Readfile/Projection|
	      "At the overall rate of " ($rate total-read clocktime) " bytes/sec, "
	      "reading the remaining " ($count bytes-to-go "byte")
	      " should take " (secs->string secs-to-go) " finishing " 
	      "around " (get (timestamp+ secs-to-go) 'string))))))))
