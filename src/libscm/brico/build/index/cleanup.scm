#!/usr/bin/fdexec
;;; -*- Mode: Scheme; -*-

(config! 'cachelevel 2)
(use-module '{optimize mttools morph morph/en})
(load "brico.scm")
(use-module 'audit)

;;; Initial scanning

(define (scan-pool pool deleted invrefs)
  ;; HASHSET contains deleted frames, invrefs contains frames
  ;;  which have inverse /sumterms /refterms pointers which are
  ;;  left over from previous inversions and need to be reset
  (notify "Fixing words, while getting deleted frames and invrefs from " pool)
  (let ((local-deleted (make-hashset 2000000))
	(local-invrefs (make-hashset 2000000)))
    (do-choices-mt
	(f (pool-elts pool) (config 'nthreads 4) mt/lockoids
	   (config 'blocksize 100000)
	   (mt/custom-progress
	    (if (pool-label pool)
		(string-append "Scanning " (pool-label pool))
		"Scanning")))
      (if (fail? (getslots f))
	  (hashset-add! local-deleted f)
	  (begin
	    (when (%test f '{@1/2ab55{/SUMTERMS} @1/2ab5a{/REFTERMS}})
	      (hashset-add! local-invrefs f))
	    (when (%test f '%words) (minimize-words! f))))
      (swapout f))
    (notify "Done with scan, merging results")
    (hashset-add! deleted (hashset-elts local-deleted))
    (hashset-add! invrefs (hashset-elts local-invrefs)))
  (notify "Done fixing words and getting deleted frames and invrefs from "
	  pool))

;;; Fixing OID refs

(define (fix-oidrefs f deleted)
  ;; (notify "Fixing oidrefs for " f)
  (do-choices (slotid (getkeys f))
    (let ((povalues
	   (tryif (and (oid? slotid) (not (test slotid 'type 'oddslot)))
		  (filter-choices (p (pick (%get f slotid) pair?))
		    (and (null? (cdr p)) (oid? (car p)))))))
      (when (exists? povalues)
	(message "Fixing pair/oid values for " slotid " of " f ": " povalues)
	(drop! f slotid povalues)
	(add! f slotid (car povalues))))
    (let ((dropvalues  (pick (%get f slotid) deleted)))
      (when (exists? dropvalues)
	(message "Dropping " (choice-size dropvalues) " value(s) "
		 "from " slotid " of " f ": " dropvalues)
	(drop! f slotid dropvalues)))))

;;; Isolating pools

(defambda (lockfetch oids)
  (lock-oids! oids)
  (prefetch-oids! oids)
  oids)

(define (isolate-prefetcher frames done)
  (when done (commit) (clearcaches))
  (unless done
    (lockfetch frames)
    (lockfetch
     (for-choices (f frames)
       (reject (pick (%get f (getkeys f)) oid?) brico-pool)))))

(define (isolate-frame f)
  (do-choices (slotid (getkeys f))
    (let* ((values (%get f slotid))
	   (xvalues (reject (pick values oid?) brico-pool)))
      (when (exists? xvalues)
	(drop! f slotid xvalues)
	(if (and (oid? slotid) (test slotid 'inverse))
	    (begin (message "Inverting " slotid " of " f "=" xvalues)
		   (add! (pick xvalues valid-oid?) (get slotid 'inverse) f))
	    (message "Losing " slotid " of " f "=" xvalues))))))

(define (isolate-pool pool)
  (do-choices-mt (f (pool-elts pool) 4 isolate-prefetcher 50000
		    (mt/custom-progress "Isolating BRICO"))
    (isolate-frame f)))

;;; The main fix pool loop

(define (fix-pool pool deleted)
  ;; (notify "Fixing " pool)
  (do-choices-mt (f (difference (pool-elts pool) deleted)
		    4 mt/lockoids
		    (config 'blocksize 200000)
		    (mt/custom-progress
		     (if (pool-label pool)
			 (string-append "Fixing " (pool-label pool))
			 "Fixing frames")))
    (fix-oidrefs f deleted)))

;;; Fixing words

(define morph-langs
  '{@/brico/2c1c7"English" @/brico/2c1fc"Spanish" @/brico/2c122"French"})

(define (stdcase x)
  (if (uppercase? x) x
      (if (somecap? x) (capitalize x)
	  (downcase x))))

(define (english-noun-root x)
  (choice (get-noun-root x @/brico/2c1c7"English")
	  (get-noun-root (stdcase x) @/brico/2c1c7"English")))
(define (english-verb-root x)
  (choice (get-verb-root x @/brico/2c1c7"English")
	  (get-verb-root (stdcase x) @/brico/2c1c7"English")))
(define (french-noun-root x)
  (choice (get-noun-root x @/brico/2c122"French")
	  (get-noun-root (stdcase x) @/brico/2c122"French")))
(define (french-verb-root x)
  (choice (get-verb-root x @/brico/2c122"French")
	  (get-verb-root (stdcase x) @/brico/2c122"French")))
(define (spanish-noun-root x)
  (choice (get-noun-root x @/brico/2c1fc"Spanish")
	  (get-noun-root (stdcase x) @/brico/2c1fc"Spanish")))
(define (spanish-verb-root x)
  (choice (get-verb-root x @/brico/2c1fc"Spanish")
	  (get-verb-root (stdcase x) @/brico/2c1fc"Spanish")))

(define (get-rootfn lang pos)
  (cond ((and (eq? lang @/brico/2c1c7"English") (eq? pos 'noun))
	 english-noun-root)
	((and (eq? lang @/brico/2c1c7"English") (eq? pos 'verb))
	 english-verb-root)
	((and (eq? lang @/brico/2c1fc"Spanish") (eq? pos 'noun))
	 spanish-noun-root)
	((and (eq? lang @/brico/2c1fc"Spanish") (eq? pos 'verb))
	 spanish-verb-root)
	((and (eq? lang @/brico/2c122"French") (eq? pos 'noun))
	 french-noun-root)
	((and (eq? lang @/brico/2c122"French") (eq? pos 'verb))
	 french-verb-root)
	(else (fail))))

(defambda (compute-morphmap words lang pos)
  (if (and (overlaps? lang morph-langs) (overlaps? pos '{noun verb}))
      (let ((table (make-hashtable))
	    (rootfn (get-rootfn lang pos)))
	(do-choices (word words) (add! table word (stdcase word)))
	(when (exists? rootfn)
	  (do-choices (word words) (add! table word (rootfn word))))
	table)
      (let ((table (make-hashtable)))
	(do-choices (word words) (add! table word (stdcase word)))
	table)))

(define (minimize-words! f (lang))
  (default! lang
    (choice @/brico/2c1c7"English"
	    (get language-map (car (get f '%words)))))
  (do-choices lang
    (when (eq? lang english)
      ;; Move english terms that probably aren't
      (let ((odd (filter-choices (wd (get (get f '%words) 'en))
		   (not (latin1? (basestring wd))))))
	(when (exists? odd)
	  (message "Dropping 'English' from " f " :" odd)
	  (drop! f '%words (cons 'en odd))
	  (add! f '%indices (cons @/brico/2c1c7"English" odd)))))
    (let* ((base (choice
		  (tryif (eq? lang @/brico/2c1c7"English") (get f 'words))
		  (get (get f '%norm) lang)))
	   (langkey (get lang 'key))
	   (words (difference (get (get f '%words) langkey) base))
	   (morphmap
	    (compute-morphmap words lang
			      (intersection (get f 'type) '{noun verb})))
	   (stdbase (stdcase base))
	   (basedrop (pick words morphmap stdbase))
	   (basekeep (reject words morphmap stdbase))
	   (ascii (pick basekeep ascii?))
	   (notascii (difference basekeep ascii))
	   (asciidrop (intersection ascii (basestring notascii)))
	   (remaining (difference words basedrop asciidrop))
	   (stdwords (intersection (get morphmap remaining) remaining))
	   (otherdrop (filter-choices (wd (difference remaining stdwords))
			(overlaps? (get morphmap wd) stdwords))))
      (when (or (exists? basedrop) (exists? asciidrop)
		(exists? otherdrop))
	(message "Dropping " lang " for " f ":"
		 (do-choices (d basedrop) (printout " " (write d)))
		 (do-choices (d asciidrop) (printout " " (write d)))
		 (do-choices (d otherdrop) (printout " " (write d))))
	(drop! f '%words (cons langkey basedrop))
	(drop! f '%words (cons langkey asciidrop))
	(drop! f '%words (cons langkey otherdrop))))))

;;; The main loop

(define (main)
  (let ((deleted (make-hashset 4000000))
	(invrefs (make-hashset 1000000)))
    (notify "Scanning for deleted frames, inverse refs")
    (parallel (scan-pool brico-pool deleted invrefs)
	      (scan-pool xbrico-pool deleted invrefs)
	      (scan-pool names-pool deleted invrefs)
	      (scan-pool places-pool deleted invrefs))
    (notify "Fixing " (table-size invrefs) " inverse refs")
    (do-choices-mt (f (hashset-elts invrefs)
		      (config 'nthreads 4)  mt/lockoids
		      (config 'blocksize 100000))
      (store! f @1/2ab55{/SUMTERMS} (audit-get f @1/2ab55{/SUMTERMS}))
      (store! f @1/2ab5a{/REFTERMS} (audit-get f @1/2ab5a{/REFTERMS})))
    (notify "Fixing references")
    (parallel (fix-pool brico-pool deleted)
	      (fix-pool xbrico-pool deleted)
	      (fix-pool names-pool deleted)
	      (fix-pool places-pool deleted))
    (notify "Isolating brico")
    (isolate-pool brico-pool)))

;; (define (main) (isolate-pool brico-pool))

(optimize! 'mttools 'brico 'brico/indexing 'audit)
(optimize! 'morph 'morph/en 'morph/es 'morph/fr)
(optimize!)

