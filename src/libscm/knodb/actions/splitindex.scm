;;; -*- Mode: Scheme -*-

(in-module 'knodb/actions/splitindex)

(module-export! '{splitindex main})

(use-module '{varconfig logger text/stringfmts optimize knodb})
(use-module '{knodb/indexes knodb/filenames})

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
	 (input (open-index in #[register #f shared #f]))
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
    (let* ((head (index/ref head-file
			    (frame-create #f
			      'maxload (config 'HEADLOAD (config 'MAXLOAD 0.8))
			      'indextype (config 'headtype (config 'indextype (config 'type {})))
			      'keyslot (tryif keyslot keyslot)
			      'addkeys input-keycount
			      'create #t)))
	   (tail (index/ref (or tail-file {}) 
			    (frame-create #f
			      'maxload (config 'TAILLOAD (CONFIG 'MAXLOAD 0.8))
			      'indextype (config 'tailtype (config 'indextype (config 'type {})))
			      ;; 'maxsize (config 'TAILMAXSIZE (CONFIG 'MAXSIZE #2gib))
			      'keyslot (tryif keyslot keyslot)
			      'addkeys input-keycount
			      'create #t)))
	   (opts (frame-create #f
		   'tail tail
		   'checksync (config 'checksync (config 'checklimit 30))
		   'tailcount (or tailcount {})
		   'maxcount (config 'maxcount {})
		   'mincount (config 'mincount {}))))
      (config! 'appid (glom "splitindex(" (basename in) ")"))
      (unless (index/copy! in head opts)
	(error "Pack index failed"))
      (close-index head)
      (when tail (close-index tail)))))

(define (splitindex (in #f) (head) 
		    (tail (config 'tailfile #f)) 
		    (tailcount (config 'TAILCOUNT #f)))
  (default! head in)
  (when (overlaps? head '{"inplace" "-"}) (set! head in))
  (default-configs)
  (when (equal? tail "none") (set! tail #f))
  (if (and (string? in) (file-exists? in))
      (do-splitindex in head tail tailcount)
      (usage)))

(define (main (in #f) (head)
	      (tail (config 'tailfile #f)) 
	      (tailcount (config 'TAILCOUNT #f)))
  (default! head in)
  (when (overlaps? head '{"inplace" "-"}) (set! head in))
  (default-configs)
  (when (equal? tail "none") (set! tail #f))
  (config! 'dbloglevel (-1+ %loglevel))
  (if (and (string? in) (file-exists? in))
      (do-splitindex in head tail tailcount)
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
			  "INDEXTYPE=keep|kindex|fileindex\n"
			  "OVERWRITE=no|yes\n")
	       "If specified, [to] must not exist unless OVERWRITE=yes")))
