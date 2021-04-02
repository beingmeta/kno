;;; -*- Mode: Scheme -*-

(in-module 'knodb/actions/flexsplit)

(module-export! '{flexsplit main})

(use-module '{varconfig logger texttools text/stringfmts binio optimize})
(use-module '{knodb knodb/indexes knodb/flexindex})

(define %loglevel (config 'loglevel %notice%))
(define %optimize '{knodb/actions/splitindex
		    knodb/indexes
		    knodb/hashindexes
		    ezrecords
		    fifo
		    engine})

(when (config 'showsource) (logwarn |Loading| (get-component)))

(define (->slotid arg)
  (if (not (string? arg))
      arg 
      (if (has-prefix arg {"@" ":@"})
	  (parse-arg arg)
	  (string->symbol (downcase arg)))))

(define (overwriting file)
  (when (file-exists? (glom file ".part"))
    (remove-file (glom file ".part")))
  (when (file-exists? file)
    (cond ((config 'unsafe)
	   (remove-file! file)
	   (logwarn |FileExists| "Removed existing file " (write file)))
	  (else
	   (onerror
	       (move-file! file (glom file ".bak"))
	       (lambda (ex)
		 (logwarn |FileExists|
		   "Couldn't back up " file " to " (glom file ".bak"))
		 (exit)))
	   (logwarn |FileExists|
	     "Moved existing file " file " " "to " (glom file ".bak"))))))

(define (get-new-type old opts)
  (getopt opts 'type
	  (config 'NEWTYPE 
		  (config 'TYPE 
			  (or (indexctl old 'metadata 'type)
			      'kindex)))))

(define (no-slotids? input (slotids))
  (set! slotids (dbctl input 'slotids))
  (or (not slotids) (and (vector? slotids) (zero? (length slotids)))))

(define (do-splitindex in head-file tail-file tailcount)
  (let* ((restart (config 'restart #f config:boolean))
	 (input (open-index in #[register #f]))
	 (keyslot (indexctl input 'keyslot))
	 (input-keycount (table-size input)))
    (when (and head-file (file-exists? (glom head-file ".part")))
      (cond (restart 
	     (logwarn |Restarting| "Removing partial file " (write (glom head-file ".part")))
	     (remove-file (glom head-file ".part")))
	    (else (logerr |PartialFile|
		    "The temporary output file " (write (glom head-file ".part")) " exists.\n  "
		    "Specify RESTART=yes to remove.")
		  (exit))))
    (let* ((head (index/ref head-file (frame-create #f
					'maxload (config 'HEADLOAD (config 'MAXLOAD 0.5))
					'addkeys input-keycount
					'create #t)))
	   (tail (and tail-file
		      (index/ref tail-file (frame-create #f
					     'maxload (config 'TAILLOAD (CONFIG 'MAXLOAD 0.6))
					     'maxsize (config 'TAILMAXSIZE (CONFIG 'MAXSIZE #2gib))
					     'addkeys input-keycount
					     'create #t))))
	   (opts (frame-create #f
		   'tail (or tail {})
		   'tailcount (or tailcount {})
		   'maxcount (config 'maxcount {})
		   'mincount (config 'mincount {}))))
      (config! 'appid (glom "splitindex(" (basename in) ")"))
      (unless (index/copy! in head opts)
	(error "Pack index failed")))))

(define (do-flexsplit flexindex (tailcount 1))
  (let* ((flex-opts (read-xtype flexindex))
	 (prefix (try (get flex-opts 'prefix) (basename flexindex ".flexindex")))
	 (rootdir (dirname (abspath flexindex)))
	 (root-prefix (mkpath rootdir prefix))
	 (headfile (mkpath (dirname flexindex) (glom prefix ".index")))
	 (tailfile (mkpath (dirname flexindex) (glom prefix "_tail.flexindex")))
	 (keyslot (get flex-opts 'keyslot)))
    (let* ((pattern `#(,rootdir ,prefix "." (isdigit+) ".index"))
	   (partitions (sorted (pick (getfiles rootdir) string-matches? pattern)))
	   (sizes (map get-index-keycount partitions))
	   (max-size (largest (elts sizes)))
	   (headsize (* max-size (1+ (ilog (length partitions)))))
	   (tailsize (* max-size 3)))
      (let ((headindex (index/ref headfile
				  [type (config 'headtype 'kindex)
				   capacity headsize
				   keyslot (try keyslot #f)
				   maxload 0.8
				   create #t]))
	    (tailindex (flex/open-index tailfile
					[type (config 'headtype 'kindex)
					 capacity tailsize
					 keyslot (try keyslot #f)
					 maxload 0.8
					 create #t])))
	;;(doseq)
	partitions))))

(define (get-index-keycount file)
  (let ((index (open-index file #[register #f cachelevel 1])))
    (indexctl index 'metadata 'keys)))

(define (main (in #f) (head)
	      (tail (config 'tailfile #f)) 
	      (tailcount (config 'TAILCOUNT 1)))
  (default! head in)
  (when (overlaps? head '{"inplace" "-"}) (set! head in))
  (default-configs)
  (if (and (string? in) (file-exists? in))
      (do-flexsplit in tailcount)
      (usage)))

(define configs-done #f)

(define (default-configs)
  (unless configs-done
    (config! 'cachelevel 2)
    (config! 'optlevel 4)
    (config! 'logprocinfo #t)
    (config! 'logthreadinfo #t)
    (config-default! 'logelapsed #t)
    (config! 'thread:logexit #f)
    (set! configs-done #t)))

(define (usage)
  (lineout "Usage: split-index <input> <head> [tail]\n"
    ($indented 4
	       "Repacks the file index stored in <from> either in place or into [to]."
	       "Common options include (first value is default) : \n"
	       ($indented 4
			  "INDEXTYPE=keep|knopool|filepool\n"
			  "OVERWRITE=no|yes\n")
	       "If specified, [to] must not exist unless OVERWRITE=yes")))
