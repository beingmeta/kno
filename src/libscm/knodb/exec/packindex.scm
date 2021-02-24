;;; -*- Mode: Scheme -*-

(in-module 'knodb/exec/packindex)

(module-export! '{packindex main})

(use-module '{optimize varconfig logger text/stringfmts kno/mttools})
(use-module '{knodb/indexes})

(define %loglevel (config 'loglevel %notice%))

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

(define (do-packindex in out)
  (let* ((overwrite (config 'overwrite #f))
	 (input (open-index in #[register #f]))
	 (newtype (get-new-type input #f))
	 (keyslot (->slotid (or (config 'keyslot)
				(indexctl input 'keyslot))))
	 (opts (frame-create #f
		 'type newtype
		 'newsize (config 'newsize {})
		 'keyslot (tryif keyslot keyslot)
		 'mincount (config 'mincount {})
		 'maxcount (config 'maxcount {})
		 'slotids 
		 (tryif (and newtype 
			     (equal? (downcase newtype) "hashindex")
			     (not (config 'slotids)))
		   (tryif (config 'slotcodes #t config:boolean)
		     #(type)))
		 'rarefile (or (config 'rare) {})
		 'repair (config 'repair #f)
		 'overwrite overwrite)))
    (info%watch "pack-index/main" 
      in out input newtype keyslot overwrite
      "\nOPTS" opts)
    (when (and out (file-exists? out) (not overwrite))
      (logwarn |FileExists|
	"The output file " (write out) " already exists.\n  "
	"Specify OVERWRITE=yes to remove.")
      (exit))
    (when (and overwrite out (file-exists? (glom out ".part")))
      (remove-file (glom out ".part")))
    (when (and overwrite (not out) (file-exists? (glom in ".part")))
      (remove-file (glom in ".part")))
    (when (and overwrite (getopt opts 'rarefile))
      (overwriting (getopt opts 'rarefile)))
    (config! 'appid (glom "pack(" (basename in) ")"))
    (unless (index/pack! in out opts)
      (error "Pack index failed"))))

(define (packindex (in #f) (out))
  (default! out in)
  (when (overlaps? out '{"inplace" "-"}) (set! out in))
  (default-configs)
  (if (and (string? in) (file-exists? in))
      (do-packindex in out)
      (usage)))

(define (main (in #f) (out #f))
  (default! out in)
  (when (overlaps? out '{"inplace" "-"}) (set! out in))
  (packindex in out))

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
  (lineout "Usage: pack-index <from> [to]\n"
    ($indented 4
	       "Repacks the file index stored in <from> either in place or into [to]."
	       "Common options include (first value is default) : \n"
	       ($indented 4
			  "INDEXTYPE=keep|knopool|filepool\n"
			  "OVERWRITE=no|yes\n")
	       "If specified, [to] must not exist unless OVERWRITE=yes")))

(when (config 'optimize #t)
  (optimize! '{knodb/indexes knodb/hashindexes ezrecords fifo engine})
  (optimize!))


