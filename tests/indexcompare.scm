#!./kno
;;; -*- Mode: Scheme; text-encoding: utf-8 -*-

(use-module '{logger stringfmts optimize varconfig})

(define %loglevel %notice%)
(varconfig! verbose %loglevel config:loglevel #f)

(defambda (get-key-summary xfile xkeys yfile ykeys)
  (cond ((empty? xkeys)
	 (stringout xfile "has no keys and " yfile " has " (choice-size ykeys)))
	((empty? ykeys)
	 (stringout yfile "has no keys and " xfile " has " (choice-size xkeys)))
	((empty? (difference xkeys ykeys))
	 (stringout yfile " has a proper subset of the keys in " xfile
	   "(" (choice-size ykeys) " .vs. " (choice-size xkeys) ")"))
	((empty? (difference ykeys xkeys))
	 (stringout xfile " has a proper subset of the keys in " yfile
	   "(" (choice-size xkeys) " .vs. " (choice-size ykeys) ")"))
	(else (stringout
		xfile "(" (choice-size xkeys) " keys)  and "
		yfile  "(" (choice-size ykeys) " keys), "
		"sharing " (choice-size (intersection xkeys ykeys)) " keys"))))

(define (compare-indexes x y (nkeys #f))
  (if (not (number? nkeys)) (set! nkeys #f))
  (let ((xkeys (getkeys x))
	(ykeys (getkeys y)))
    (unless (identical? xkeys ykeys)
      (let ((x-y (difference xkeys ykeys))
	    (y-x (difference ykeys xkeys)))
	(when (exists? x-y)
	  (loginfo |IndexesDiffer/Keys|
	    "Only in " (index-source x) ": \n"
	    (do-choices (key x-y) (printout "  " key))))
	(when (exists? y-x)
	  (loginfo |IndexesDiffer/Keys|
	    "Only in " (index-source y) ": \n"
	    (do-choices (key y-x) (printout "  " key)))))
      (lognotice |IndexesDiffer| (get-key-summary x xkeys y ykeys))
      (lineout "Indexes differ")
      (exit 42))
    (let ((compare-keys (if (and (number? nkeys) (>= nkeys 0))
			    (pick-n xkeys nkeys)
			    xkeys)))
      (logdebug |ComparingIndexes|
	"Over " (choice-size compare-keys) " keys")
      (do-choices (key compare-keys)
	(logdetail |ComparingIndexes| "Comparing values for " key)
	(let ((xv (get x key))
	      (yv (get x key)))
	  (unless (identical? xv yv)
	    (lognotice |IndexesDiffer/value| "Values for " key " differ")
	    (loginfo |IndexesDiffer/value|
	      (index-source x) ": " xv)
	    (loginfo |IndexesDiffer/value|
	      (index-source y) ": " yv)
	    (lineout "Indexes differ")
	    (exit 42))))
      (lognotice |IndexesIdentical|
	(index-source x) " and " (index-source y)
	" have the same keys and match on "
	(if (= (choice-size compare-keys) (choice-size xkeys))
	    "all values"
	    (printout ($num (choice-size compare-keys)) " values")))
      (lineout "Indexes identical"
	(if (not (= (choice-size compare-keys) (choice-size xkeys)))
	    (printout " (" (choice-size compare-keys) "/" (choice-size xkeys) ")"))))))

(define (main xfile yfile)
  (compare-indexes
   (open-index xfile #[readonly #t cachelevel 2])
   (open-index yfile #[readonly #t cachelevel 2])
   (config 'NKEYS 512 config:boolean+parse)))

(when (config 'optimize #t) (optimize!))
