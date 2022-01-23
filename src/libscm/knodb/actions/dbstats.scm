;;; -*- Mode: Scheme -*-

(in-module 'knodb/actions/dbstats)

(module-export! '{dbstats main})

(use-module '{varconfig logger texttools text/stringfmts binio optimize})
(use-module '{knodb knodb/filenames knodb/indexes knodb/flexindex})

(define %loglevel (config 'loglevel %warn%))

(define (dbstats file)
  (unless (config 'SKIPDEFAULTS) (default-configs))
  (cond ((or (not file) (not (file-exists? file)))
	 (usage))
	((has-suffix file ".index")
	 (let* ((index (open-index file #[register #f shared #f]))
		(stats (dbctl index 'stats))
		(nkeys (try (dbctl index 'keycount) (get stats 'nkeys) #f))
		(buckets (try (dbctl index 'buckets)
			      #f))
		(nbuckets (cond ((and stats (test stats 'nbuckets)) (get stats 'nbuckets))
				((or (fail? buckets) (not buckets)) #f)
				((number? buckets) buckets)
				((ambiguous? buckets) (|| buckets))
				((sequence? buckets) (length buckets))
				(else #f)))
		(xrefs (try (dbctl index 'xrefs) #f))
		(nxrefs (try (get stats 'nxrefs) (and xrefs (length xrefs))))
		(load (try (get stats 'load) (and nkeys nbuckets (/~ nkeys nbuckets)) #f)))
	   (lineout ";; " file " (" (dbctl index 'type) ") " 
	     (when nkeys (printout ($count nkeys "key" "keys")))
	     (when buckets (printout " across " ($count nbuckets "bucket")))
	     (when load (printout " (load=" load ")"))
	     (when xrefs (printout ", " ($count nxrefs "xref")))
	     "\n;; " ($fileinfo file) "\n"
	     (listdata stats))))
	((has-suffix file ".pool")
	 (let* ((pool (open-pool file #[register #f shared #f adjunct #t]))
		(xrefs (try (dbctl pool 'xrefs) #f))
		(nxrefs (and xrefs (length xrefs)))
		(label (pool-label pool)))
	   (lineout ";; " file " (" (dbctl pool 'type) ") " 
	     (when label (printout "(" label ") "))
	     "at base " (pool-base pool) ", load=" (number->string (pool-load pool) 10 #t)
	     ", cap=" (number->string (pool-capacity pool) 10 #t)
	     ", xrefs=" (number->string nxrefs 10 #t)
	     "\n;; " ($fileinfo file))))
	(else (lineout ";; " file ($fileinfo file)))))

(define main dbstats)

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
  (lineout "Usage: knodb stats <dbfile>"))

