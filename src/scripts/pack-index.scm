;;; -*- Mode: Scheme -*-

(config! 'cachelevel 2)
(config! 'logthreadinfo #t)
(config! 'logelapsed #t)
(use-module '{optimize varconfig logger mttools})
(use-module '{knodb/indexes})

(onerror (use-module 'rocksdb))
(onerror (use-module 'leveldb))

(define (->slotid arg)
  (if (not (string? arg))
      arg 
      (if (has-prefix arg {"@" ":@"})
	  (parse-arg arg)
	  (string->symbol (downcase arg)))))

(define (overwriting file)
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
			      'hashindex)))))

(define (no-slotids? input (slotids))
  (set! slotids (dbctl input 'slotids))
  (or (not slotids) (and (vector? slotids) (zero? (length slotids)))))

(define (main in (out #f))
  (let* ((overwrite (config 'overwrite #f))
	 (input (open-index in #[register #f]))
	 (newtype (get-new-type input #f))
	 (keyslot (->slotid (or (config 'keyslot)
				(indexctl input 'keyslot))))
	 (opts (frame-create #f
		 'type newtype
		 'newsize (or (config 'newsize) {})
		 'keyslot keyslot
		 'mincount (or (config 'mincount) {})
		 'maxcount (or (config 'maxcount) {})
		 'slotids 
		 (tryif (and newtype 
			     (equal? (downcase newtype) "hashindex")
			     (not (config 'slotids)))
		   (tryif (config 'slotcodes #t config:boolean)
		     #(type)))
		 'rarefile (or (config 'rare) {})
		 'uniquefile (or (config 'unique) {})
		 'repair (config 'repair #f)
		 'overwrite overwrite)))
    (when (and out (file-exists? out) (not overwrite))
      (logwarn |FileExists|
	"The output file " (write out) " already exists.\n  "
	"Specify OVERWRITE=yes to remove.")
      (exit))
    (when (and overwrite (getopt opts 'unique)
	       (file-exists? (getopt opts 'unique)))
      (overwriting (getopt opts 'unique)))
    (when (and overwrite (getopt opts 'rare)
	       (file-exists? (getopt opts 'rare)))
      (overwriting (getopt opts 'rare)))
    (config! 'appid (glom "pack(" (basename in) ")"))
    (index/pack! in out opts)))

(when (config 'optimize #t)
  (optimize! '{knodb/indexes knodb/hashindexes ezrecords fifo engine})
  (optimize!))


