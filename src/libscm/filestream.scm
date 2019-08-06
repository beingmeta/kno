;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2018-2019 beingmeta, inc.  All rights reserved.

(in-module 'filestream)

(use-module '{fifo varconfig ezrecords stringfmts reflection 
	      libarchive bugjar bugjar/html logger})
(use-module '{flexdb flexdb/registry flexdb/branches})

(define %loglevel %notice%)

(module-export! '{filestream/open filestream/read filestream/state 
		  filestream/save! filestream/log!})

(module-export! '{filestream-filename filestream-itemcount})

(define filestream-inbufsize (* 3 #mib))
(varconfig! filestream:inbufsize filestream-inbufsize config:bytes)

(defrecord (filestream mutable)
  (port) (readfn) (lock) (filename) (archive) (statefile) (state)
  (started) (itemcount) (filepos) (state) 
  ;; not yet used
  (fifo (fifo/make)))

(defrecord (filestream/batch) itemcount start end)

(define (batch-runtime batch)
  (difftime (filestream/batch-end batch) (filestream/batch-start batch)))
(define (batch-count batch) (filestream/batch-itemcount batch))

(define (filestream/open filename (opts #f))
  (let* ((statefile (glom filename ".state"))
	 (archive (and (has-suffix filename {".bz" ".gz" ".bz2" ".Z" ".tar"})
		       (archive/open filename)))
	 (state (if (file-exists? statefile)
		    (read (open-input-file statefile))
		    (frame-create #f
		      'filename filename 'statefile statefile
		      'started (gmtimestamp)
		      'filetime (file-modtime filename)
		      'filesize (file-size filename)
		      'filepos (tryif (not archive) 0)
		      'total (getopt opts 'total {})
		      'itemcount 0)))
	 (port (if archive
		   (archive/open archive 0)
		   (open-input-file filename)))
	 (readfn (getopt opts 'readfn getline)))
    (setbuf! port (getopt opts 'inbufsize (getopt opts 'bufsize filestream-inbufsize)))
    (let ((stream (cons-filestream port readfn (make-mutex)
				   filename archive statefile state
				   (gmtimestamp) 0 (and (not archive) (getpos port))
				   #f)))
      (if (> (getopt state 'filepos 0) 0)
	  (setpos! port (getopt state 'filepos))
	  (begin
	    (when (getopt opts 'startfn) ((getopt opts 'startfn) port))
	    (unless archive (store! state 'filepos (getpos port)))
	    (when archive
	      ;; When you can't set file positions, skip over all the
	      ;;  items which have been read in the past
	      (dotimes (i (getopt state 'itemcount 0)) (readfn port)))))
      (store! state 'updated (timestamp))
      (fileout statefile (pprint state))
      stream)))

(define (filestream/read stream)
  (with-lock (filestream-lock stream)
    (let* ((port (filestream-port stream))
	   (item ((filestream-readfn stream) port)))
      (set-filestream-itemcount! stream (1+ (filestream-itemcount stream)))
      (set-filestream-filepos! stream (getpos port))
      item)))

(define (filestream/state stream)
  (with-lock (filestream-lock stream)
    (let ((state (deep-copy (filestream-state stream)))
	  (now (gmtimestamp)))
      (store! state 'updated now)
      (store! state 'itemcount (+ (get state 'itemcount) (filestream-itemcount stream)))
      (when (filestream-filepos stream)
	(store! state 'filepos (filestream-filepos stream)))
      (add! state 'batches
	(cons-filestream/batch (filestream-itemcount stream)
			       (filestream-started stream)
			       now))
      state)))

(define (filestream/save! stream)
  (with-lock (filestream-lock stream)
    (let* ((state (deep-copy (filestream-state stream)))
	   (count (filestream-itemcount stream))
	   (started (filestream-started stream))
	   (now (gmtimestamp))
	   (batch (cons-filestream/batch count started now)))
      (store! state 'updated now)
      (store! state 'itemcount (+ (get state 'itemcount) count))
      (when (filestream-filepos stream)
	(store! state 'filepos (filestream-filepos stream)))
      (add! state 'batches batch)
      (fileout (filestream-statefile stream)
	(pprint state)))))

(define (filestream/log! stream (opts #f) (%loglevel %notice%))
  (let ((count (filestream-itemcount stream))
	(filepos (filestream-filepos stream))
	(started (filestream-started stream))
	(state (filestream/state stream)))
    (when (zero? count)
      (lognotice |FileStream| 
	"Starting at item #" (get state 'itemcount) 
	(when (and (test state 'total) (> (get state 'itemcount) 0))
	  (printout " (" (show% (get state 'itemcount) (get state 'total))))
	(when (> (get state 'itemcount) 0)
	  (printout " after " ($bytes filepos) 
	    " (" (show% filepos (get state 'filesize))
	    " of " ($bytes (get state 'filesize))))))
    (when (and (getopt opts 'batch #t) (not (zero? count)))
      (lognotice |FileStream| 
	"Processed " ($count count "item") " in " 
	(secs->string (time-since started)) 
	" (" ($rate count (time-since started)) " items/sec)"))
    (when (getopt opts 'overall #f)
      (let ((runtime (+ (reduce-choice + (get state 'batches) 0 batch-runtime)
			(time-since started)))
	    (full-count (+ (reduce-choice + (get state 'batches) 0 batch-count)
			   (filestream-itemcount stream))))
	(when (> full-count 0)
	  (lognotice |FileStream| 
	    "Overall, processed " ($count (+ count (get state 'itemcount)) "item") 
	    (when filepos (printout " and " ($bytes filepos) " bytes"))
	    " in " (secs->string (time-since (get state 'started))) 
	    " over " ($count (1+ (||  (get state 'batches))) "batch" "batches")
	    " running " (secs->string runtime))
	  (lognotice |FileStream|
	    "Overall, processing " ($rate count (time-since started)) " items/sec"
	    (when filepos (printout " and " ($bytes/sec filepos (time-since started)))))
	  (cond (filepos
		 (let ((rate (/~ filepos runtime))
		       (togo (- (get state 'filesize) filepos)))
		   (lognotice |FileStream|
		     "Inshallah, everything should be done in "
		     (secs->string (/~ togo rate)))))
		((test state 'total)
		 (let ((rate (/~ full-count runtime))
		       (togo (- (get state 'total) full-count)))
		   (lognotice |FileStream|
		     "Inshallah, everything should be done in "
		     (secs->string (/~ togo rate)))))
		(else)))))))
