;;; -*- Mode: Scheme -*-

(in-module 'knodb/actions/flexpack)

(module-export! '{flexpack main flexpack-tasks})

(use-module '{varconfig logger texttools text/stringfmts binio optimize})
(use-module '{knodb knodb/filenames knodb/indexes knodb/flexindex})

(define %loglevel (config 'loglevel %warn%))
(define %optmods '{knodb/actions/copykeys
		   knodb/indexes
		   knodb/hashindexes
		   ezrecords
		   engine
		   fifo})

(when (config 'showsource) (logwarn |Loading| (get-component)))

(define (->slotid arg)
  (if (not (string? arg))
      arg 
      (if (has-prefix arg {"@" ":@"})
	  (parse-arg arg)
	  (string->symbol (downcase arg)))))

(define (get-new-type old opts)
  (getopt opts 'type
	  (config 'NEWTYPE 
		  (config 'TYPE 
			  (or (indexctl old 'metadata 'type)
			      'kindex)))))

(define (no-slotids? input (slotids))
  (set! slotids (dbctl input 'slotids))
  (or (not slotids) (and (vector? slotids) (zero? (length slotids)))))

(define (get-index-file-stats filename)
  (dbctl (open-index filename #[register #f shared #f]) 'stats))

(define (do-flexpack flexfile (opts #f) (headfile #f) (tailfile #f) (tailcount #f))
  (local flex-opts (read-xtype flexfile))
  (local absroot (dirname (abspath flexfile)))
  (unless headfile
    (set! headfile
      (getopt opts 'headfile
	      (getopt flex-opts 'headfile
		      (mkpath absroot (glom (basename flexfile #t) ".index"))))))
  (unless tailfile
    (set! tailfile (getopt opts 'tailfile (getopt flex-opts 'tailfile #f))))
  (unless tailcount
    (set! tailcount (getopt opts 'tailcount (getopt flex-opts 'tailcount (and tailfile 1)))))
  (let* ((prefix (try (get flex-opts 'prefix) (basename flexfile ".flexindex")))
	 (partdir (mkpath absroot (getopt flex-opts 'partdir "")))
	 (tailfile (cond ((not tailfile) #f)
			 ((overlaps? (downcase tailfile) '{"none" "skip" "no" "merge"}) #f)
			 ((overlaps? (downcase tailfile) '{"tail" "auto" "default" "split"})
			  (mkpath absroot (glom prefix "_tail.index")))
			 (else tailfile)))
	 (part-prefix (mkpath partdir prefix))
	 (keyslot (get flex-opts 'keyslot))
	 (pack-start (elapsed-time)))
    (info%watch "do-flexpack"
      flexfile headfile tailfile tailcount
      "\nopts" flex-opts)
    (let* ((pattern `#(,part-prefix "." (isdigit+) ".index"))
	   (partitions (sorted (pick (getfiles partdir) string-matches? pattern) file-size))
	   (sizes (map get-index-keycount partitions))
	   (max-size (largest (elts sizes)))
	   (headsize (* max-size (1+ (ilog (length partitions)))))
	   (tailsize (and tailfile (* max-size 3)))
	   (backup (getopt opts 'backup (config 'backup #f)))
	   (maxsecs (getopt opts 'maxsecs (config 'maxsecs #f)))
	   (msgsecs (getopt opts 'msgsecs (config 'msgsecs 120))))
      (let* ((headtype (getopt opts 'headtype 'kindex))
	     (tailtype (getopt opts 'tailtype 'kindex))
	     (headindex (index/ref headfile
				   [type headtype
				    flexindex (getopt opts 'flexhead #f)
				    capacity headsize
				    keyslot (try keyslot #f)
				    maxload (getopt opts 'headload
						    (getopt flex-opts 'headload
							    (getopt opts 'maxload
								    (getopt flex-opts 'maxload 0.8))))
				    register #f
				    shared #f
				    create #t]))
	     (tailindex (index/ref (or tailfile {})
				   [type tailtype
				    flexindex (getopt opts 'flextail #f)
				    capacity tailsize
				    keyslot (try keyslot #f)
				    maxload
				    (getopt opts 'tailload
					    (getopt flex-opts 'tailload
						    (getopt opts 'maxload
							    (getopt flex-opts 'maxload 0.8))))
				    register #f
				    shared #f
				    create #t]))
	     (logfile (config 'LOGFILE 
			(glom "flexpack_" (basename flexfile ".flexindex")
			  "_" (number->string (config 'pid))
			  ".log")))
	     (n-partitions (length partitions))
	     (head-stats (get-index-file-stats headfile))
	     (tail-stats (and tailfile (get-index-file-stats tailfile)))
	     (total-keys 0)
	     (total-size 0)
	     (size-to-date 0))
	;; Close the indexes so that copykeys can lock them
	;;  (Note that this doesn't matter for kindexes but it does for rocksdb/etc)
	(when headindex (close-index headindex))
	(when tailindex (close-index tailindex))
	(lognotice |FlexPack|
	  (if tailfile "Merging and splitting " "Merging ")
	  ($count (length partitions) "partitions")
	  " for " flexfile)
	(doseq (partition partitions)
	  (set! total-size (+ total-size (file-size partition))))
	(doseq (partition partitions i)
	  (let ((started (elapsed-time))
		(stats (dbctl (open-index partition [register #f shared #f background #f]) 'stats))
		(proc (proc/run "knodb"
				[lookup #t stdout logfile stderr 'temp
				 id (glom "copykeys_" (basename partition #t))
				 ;; configs (frame-create #f
				 ;; 	   'headtype (if headtype headtype {})
				 ;; 	   'tailtype (if tailtype tailtype {})
				 ;; 	   'backup (getopt opts 'backup #f))
				 ]
				"copykeys"
				partition headfile
				tailfile tailcount))
		(lastmsg (elapsed-time)))
	    (lognotice |Launched|
	      "copykeys " (write partition) " for " flexfile " "
	      (when (test stats 'nkeys) (printout ($count (get stats 'nkeys) "keys"))))
	    (while (and (proc/live? proc)
			(or (not maxsecs) (< (elapsed-time started) maxsecs)))
	      (when (and msgsecs (> (elapsed-time lastmsg) msgsecs))
		(logwarn |Waiting| "Waited " (secs->string (elapsed-time started)) 
			 " for " ($pid proc) " to process " (write partition))
		(set! lastmsg (elapsed-time)))
	      (sleep 1))
	    (cond ((test (proc/status proc) 'exited 0)
		   (set! size-to-date  (+ size-to-date (file-size partition)))
		   (knodb/backup! partition backup)
		   ;; (let ((new-head-buckets (get-index-buckets headfile))
		   ;; 	 (new-tail-buckets (and tailfile (get-index-buckets tailfile))))
		   ;;   (lognotice |Finished|
		   ;;     "(" (1+ i) "/" n-partitions ") " partition
		   ;;     " (~" (show% size-to-date total-size 1) ")"
		   ;;     " in " (secs->string (elapsed-time started))
		   ;;     (unless (equal? new-head-buckets head-buckets)
		   ;; 	 (if (exists? tail)
		   ;; 	     (printout " (w/head resize)")
		   ;; 	     (printout " (w/resize)"))
		   ;; 	 (set! head-buckets new-head-buckets))
		   ;;     (when (exists? tail)
		   ;; 	 (unless (equal? new-tail-buckets tail-buckets)
		   ;; 	   (printout " (tail resize)")
		   ;; 	   (set! tail-buckets new-tail-buckets)))))
		   (lognotice |Finished|
		     "(" (1+ i) "/" n-partitions ") " partition
		     " (~" (show% size-to-date total-size 1) ")"
		     " in " (secs->string (elapsed-time started))))
		  (else (logerr |Error|
			  "Processing " partition ", see " (write (proc-stdout proc))
			  "for details.")
			(exit 1)))))
	(lognotice |Finished| "Packed " flexfile " in " (secs->string (elapsed-time pack-start)))
	
	;; (when (and headfile (overlaps? (dbctl headindex 'type) 'kindex))
	;;   (fork/cmd/wait "knodb" "packindex" headfile))
	;; (when (and tailfile tailindex (overlaps? (dbctl tailindex 'type) 'kindex))
	;;   (fork/cmd/wait "knodb" "packindex" tailfile))
	)
      (knodb/backup! flexfile backup))))

(define (flexpack-tasks flexfile (opts #f) (headfile #f) (tailfile #f) (tailcount #f))
  (local flex-opts (read-xtype flexfile))
  (local absroot (dirname (abspath flexfile)))
  (unless headfile
    (set! headfile
      (getopt opts 'headfile
	      (getopt flex-opts 'headfile
		      (mkpath absroot (glom (basename flexfile #t) ".index"))))))
  (unless tailfile
    (set! tailfile (getopt opts 'tailfile (getopt flex-opts 'tailfile #f))))
  (unless tailcount
    (set! tailcount (getopt opts 'tailcount (getopt flex-opts 'tailcount (and tailfile 1)))))
  (let* ((prefix (try (get flex-opts 'prefix) (basename flexfile ".flexindex")))
	 (partdir (mkpath absroot (getopt flex-opts 'partdir "")))
	 (part-prefix (mkpath partdir prefix))
	 (tailfile (cond ((not tailfile) #f)
			 ((overlaps? (downcase tailfile) '{"none" "skip" "no" "merge"}) #f)
			 ((overlaps? (downcase tailfile) '{"tail" "auto" "default" "split"})
			  (mkpath absroot (glom prefix "_tail.index")))
			 (else tailfile)))
	 (keyslot (get flex-opts 'keyslot))
	 (pack-start (elapsed-time)))
    (info%watch "do-flexpack"
      flexfile headfile tailfile tailcount
      "\nopts" flex-opts)
    (let* ((pattern `#(,part-prefix "." (isdigit+) ".index"))
	   (partitions (sorted (pick (getfiles partdir) string-matches? pattern)))
	   (sizes (map get-index-keycount partitions))
	   (max-size (largest (elts sizes)))
	   (headsize (* max-size (1+ (ilog (length partitions)))))
	   (tailsize (and tailfile (* max-size 3)))
	   (backup (getopt opts 'backup (config 'backup #f)))
	   (maxsecs (getopt opts 'maxsecs (config 'maxsecs #f)))
	   (msgsecs (getopt opts 'msgsecs (config 'msgsecs 120))))
      (let* ((headtype (getopt opts 'headtype 'kindex))
	     (tailtype (getopt opts 'tailtype 'kindex))
	     (headindex (index/ref headfile
				   [type headtype
				    flexindex (getopt opts 'flexhead #f)
				    capacity headsize
				    keyslot (try keyslot #f)
				    maxload (getopt opts 'headload
						    (getopt flex-opts 'headload
							    (getopt opts 'maxload
								    (getopt flex-opts 'maxload 0.8))))
				    register #f
				    shared #f
				    create #t]))
	     (tailindex (index/ref (or tailfile {})
				   [type tailtype
				    flexindex (getopt opts 'flextail #f)
				    capacity tailsize
				    keyslot (try keyslot #f)
				    maxload
				    (getopt opts 'tailload
					    (getopt flex-opts 'tailload
						    (getopt opts 'maxload
							    (getopt flex-opts 'maxload 0.8))))
				    register #f
				    shared #f
				    create #t])))
	;; Close the indexes so that copykeys can lock them
	;;  (Note that this doesn't matter for kindexes but it does for rocksdb/etc)
	(when headindex (close-index headindex))
	(when tailindex (close-index tailindex))
	(for-choices (partition (elts partitions))
	  `(merge ,partition ,headfile ,tailfile ,tailcount))
	
	;; (when (and headfile (overlaps? (dbctl headindex 'type) 'kindex))
	;;   (fork/cmd/wait "knodb" "packindex" headfile))
	;; (when (and tailfile tailindex (overlaps? (dbctl tailindex 'type) 'kindex))
	;;   (fork/cmd/wait "knodb" "packindex" tailfile))
	))))

(define (get-index-keycount file)
  (let ((index (open-index file #[register #f shared #f cachelevel 1])))
    (indexctl index 'metadata 'keycount)))

(define (flexpack (in #f) (head #f) (tail #f) (tailcount (config 'tailcount #f)))
  (default-configs)
  (debug%watch "flexpack" in head tail)
  (if (and (string? in) (file-exists? in))
      (let ((opts (frame-create #f
		    'tailcount  (or tailcount (config 'TAILCOUNT 1))
		    'headtype (config 'headtype 'kindex)
		    'tailtype (config 'tailtype 'kindex)
		    'flexhead (config 'flexhead {})
		    'flextail (config 'flextail {})
		    'headload (config 'headload {})
		    'tailload (config 'tailload {})
		    'backup   (config 'backup {})
		    'maxload (config 'maxload {}))))
	(do-flexpack in opts head tail))
      (usage)))
(define main flexpack)

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
  (lineout "Usage: flexpack <input.flexindex> <headfile> [tailfile] [tailcount]\n"
    ($indented 4
      "Merges the partitions of *input.flexindex* into the index files *headfile* and "
      "*tailfile*, saving keys with fewer than *tailcount* values (in a given partition) "
      "into *tailfile* (if specified).")))
