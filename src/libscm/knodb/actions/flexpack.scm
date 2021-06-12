;;; -*- Mode: Scheme -*-

(in-module 'knodb/actions/flexpack)

(module-export! '{flexpack main})

(use-module '{varconfig logger texttools text/stringfmts binio optimize})
(use-module '{knodb knodb/indexes knodb/flexindex})

(define %loglevel (config 'loglevel %notice%))
(define %optmods '{knodb/actions/splitindex
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

(define (do-flexpack flexindex (headfile #f) (tailfile #f) (tailcount 1))
  (let* ((flex-opts (read-xtype flexindex))
	 (prefix (try (get flex-opts 'prefix) (basename flexindex ".flexindex")))
	 (partdir (try (get flex-opts 'partdir) #f))
	 (absroot (dirname (abspath flexindex)))
	 (rootdir (mkpath (if partdir (mkpath absroot partdir) absroot) ""))
	 (root-prefix (mkpath rootdir prefix))
	 (headfile (or headfile (mkpath rootdir (glom prefix ".index"))))
	 (tailfile (cond ((not tailfile) #f)
			 ((overlaps? (downcase tailfile) '{"none" "skip" "no" "merge"}) #f)
			 ((overlaps? (downcase tailfile) '{"tail" "auto" "default" "split"})
			  (mkpath rootdir (glom prefix "_tail.flexindex")))
			 (else tailfile)))
	 (keyslot (get flex-opts 'keyslot)))
    (let* ((pattern `#(,rootdir ,prefix "." (isdigit+) ".index"))
	   (partitions (sorted (pick (getfiles rootdir) string-matches? pattern)))
	   (sizes (map get-index-keycount partitions))
	   (max-size (largest (elts sizes)))
	   (headsize (* max-size (1+ (ilog (length partitions)))))
	   (tailsize (and tailfile (* max-size 3)))
	   (maxsecs (config 'maxsecs #f))
	   (msgsecs (config 'msgsecs 120)))
      (let ((headindex (index/ref headfile
				  [type (config 'headtype 'kindex)
				   flexindex (config 'flexhead #f)
				   capacity headsize
				   keyslot (try keyslot #f)
				   maxload (config 'headload (config 'maxload 0.8))
				   create #t]))
	    (tailindex (and tailfile
			    (index/ref tailfile
				       [type (config 'headtype 'kindex)
					flexindex (config 'flextail #f)
					capacity tailsize
					keyslot (try keyslot #f)
					maxload (config 'tailload (config 'maxload 0.8))
					create #t]))))
	(lognotice |FlexPack|
	  (if tailfile "Merging and splitting " "Merging ")
	  ($count (length partitions) "partition"))
	(doseq (partition partitions)
	  (let ((started (elapsed-time))
		(proc (proc/open "knodb" #[lookup #t isknox #t stdout temp stderr temp]
				 "splitindex" partition headfile tailfile tailcount))
		(lastmsg (elapsed-time)))
	    (logwarn |Launched| 
	      "splitindex " (write partition) " stdout=" (proc-stdout proc))
	    (while (and (proc/live? proc)
			(or (not maxsecs) (< (elapsed-time started) maxsecs)))
	      (when (and msgsecs (> (elapsed-time lastmsg) msgsecs))
		(set! msgsecs (elapsed-time))
		(logwarn |Waiting| "Waited " (secs->string (elapsed-time started)) 
			 " for " (proc-pid proc) " to process " (write partition)))
	      (sleep 1))
	    (cond ((test (proc/status proc) 'exited 0)
		   (move-file partition (glom partition ".bak"))
		   (logwarn |Finished| partition " in " (secs->string (elapsed-time started))))
		  (else (logwarn |Error|
			  "Processing " partition ", see " (write (proc-stdout proc))
			  "for details.")
			(break)))))
	(when (and headfile (overlaps? (dbctl headindex 'type) 'kindex))
	  (fork/cmd/wait "knodb" "packindex" headfile))
	(when (and tailfile tailindex (overlaps? (dbctl tailindex 'type) 'kindex))
	  (fork/cmd/wait "knodb" "packindex" tailfile))))))

(define (get-index-keycount file)
  (let ((index (open-index file #[register #f cachelevel 1])))
    (indexctl index 'metadata 'keys)))

(define (main (in #f) (head #f)
	      (tail (config 'tailfile #f)))
  (default! head in)
  (when (overlaps? head '{"inplace" "-"}) (set! head in))
  (default-configs)
  (if (and (string? in) (file-exists? in))
      (do-flexpack in head tail (config 'TAILCOUNT 1))
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
  (lineout "Usage: split-index <input> <head> [tailfile] [tailcount]\n"
    ($indented 4
      "Repacks the file index stored in <from> either in place or into [to]."
      "Common options include (first value is default) : \n"
      ($indented 4
	"INDEXTYPE=keep|knopool|filepool\n"
	"OVERWRITE=no|yes\n")
      "If specified, [to] must not exist unless OVERWRITE=yes")))
