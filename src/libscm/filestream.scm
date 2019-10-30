;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2018-2019 beingmeta, inc.  All rights reserved.

(in-module 'filestream)

(use-module '{fifo varconfig ezrecords stringfmts reflection 
	      libarchive bugjar bugjar/html logger})
(use-module '{flexdb flexdb/registry flexdb/branches})

(define %loglevel %notice%)
(set! %loglevel %debug%)

(module-export! '{filestream/open filestream/read filestream/state 
		  filestream/save! filestream/log!
		  filestream/done?})

(module-export! '{filestream-filename filestream-itemcount})

(define filestream-inbufsize (* 3 #mib))
(varconfig! filestream:inbufsize filestream-inbufsize config:bytes)

(defrecord (filestream mutable)
  (port) (readfn) (lock) (filename) (archive) (statefile) (state)
  (opened) (itemcount) (filepos) (state) 
  (startpos 0)
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
				   #f
				   (and (not archive) (try (get state 'filepos) 0)))))
      (cond ((> (getopt state 'filepos 0) 0)
	     (setpos! port (getopt state 'filepos))
	     (set-filestream-filepos! stream (getopt state 'filepos 0)))
	    (else
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
			       (filestream-opened stream)
			       now))
      state)))

(define (filestream/save! stream)
  (with-lock (filestream-lock stream)
    (let* ((state (deep-copy (filestream-state stream)))
	   (count (filestream-itemcount stream))
	   (opened (filestream-opened stream))
	   (now (gmtimestamp))
	   (batch (cons-filestream/batch count opened now)))
      (store! state 'updated now)
      (store! state 'itemcount (+ (get state 'itemcount) count))
      (when (filestream-filepos stream)
	(store! state 'filepos (filestream-filepos stream)))
      (add! state 'batches batch)
      (fileout (filestream-statefile stream)
	(pprint state)))))

(define (filestream/log! stream (opts #f) (%loglevel %notice%))
  (let* ((count (filestream-itemcount stream))
	 (filepos (filestream-filepos stream))
	 (opened (filestream-opened stream))
	 (state (filestream/state stream))
	 (bytes (and filepos (- filepos (filestream-startpos stream))))
	 (delta-time (time-since opened)))
    (when (zero? count)
      (lognotice |FileStream| 
	"Starting at item #" ($num (get state 'itemcount)) 
	(when (and (test state 'total) (> (get state 'itemcount) 0))
	  (printout " (" (show% (get state 'itemcount) (get state 'total))))
	(when (> (get state 'itemcount) 0)
	  (printout " after " ($bytes filepos) 
	    " (" (show% filepos (get state 'filesize))
	    " of " ($bytes (get state 'filesize)) ")"))))
    (when (and (getopt opts 'batch #t) (not (zero? count)))
      (lognotice |FileStream| 
	"Processed " ($count count "item")
	(when filepos (printout" and " ($bytes bytes)))
	" in " (secs->string delta-time) " (" ($rate count delta-time) " items/sec)"
	(when filepos (printout" (" ($bytes/sec bytes delta-time) ")"))))
    (when (getopt opts 'overall #f)
      (let* ((clock-time (time-since opened))
	     (overall-time (time-since (try (get state 'started) opened)))
	     (run-time (+ (difftime opened)
			  (reduce-choice + (get state 'batches) 0 batch-runtime)))
	     (delta-count (reduce-choice + (get state 'batches) 0 batch-count))
	     (full-count (+ delta-count (filestream-itemcount stream))))
	(debug%watch "FILESTREAM/LOG"
	  filepos opened clock-time overall-time run-time 
	  count full-count
	  "\nSTATE" state)
	(when (> full-count 0)
	  (lognotice |FileStream| 
	    "Overall, processed " ($count (+ count (get state 'itemcount)) "item") 
	    (when (test state 'total)
	      (printout " (" (show% (+ count (get state 'itemcount)) (get state 'total)) ")"))
	    (when filepos (printout " and " ($bytes filepos) " bytes"))
	    (when (and filepos (test state 'filesize))
	      (printout " (" (show% filepos (get state 'filesize)) ")"))
	    " after " (secs->string overall-time) 
	    " over " ($count (1+ (|| (get state 'batches))) "batch" "batches")
	    " taking " (secs->string run-time) " together.")
	  (lognotice |FileStream|
	    "Overall, processing " ($rate full-count overall-time) " items/sec"
	    (when filepos (printout " and " ($bytes/sec filepos overall-time))))
	  (cond ((and filepos (test state 'filesize))
		 (let ((rate (/~ bytes delta-time))
		       (togo (- (get state 'filesize) filepos)))
		   (debug%watch bytes delta-time run-time rate 
				($numstring togo) ($numstring filepos)
				(/~ togo rate))
		   (lognotice |FileStream|
		     "At the current rate, everything should be done in "
		     (secs->string (/~ togo rate)) " at "
		     ($bytes/sec rate) " for the remaining "
		     ($bytes togo))))
		((test state 'total)
		 (let ((rate (/~ delta-count delta-time))
		       (togo (- (get state 'total) full-count)))
		   (lognotice |FileStream|
		     "At the current rate, everything should be done in "
		     (secs->string (/~ togo rate)))))
		(else)))))))

(define (filestream/done? in)
  (if (filestream-filepos in)
      (= (filestream-filepos in)
	 (get (filestream-state in) 'filesize))
      (and (get (filestream-state in) 'total)
	   (= (filestream-filepos in)
	      (get (filestream-state in) 'total)))))
