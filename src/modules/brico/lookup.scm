;;; -*- Mode: Scheme; Character-Encoding: utf-8; -*-

(in-module 'brico/lookup)

(define version "$Id$")

;;; Looking up terms in BRICO

;;; This module provides utilities and end-user functions for mapping
;;;  natural language terms into BRICO concepts.

(use-module 'brico)
(use-module '{texttools reflection})
(use-module 'rulesets)

;;; EXPORTS

;; Looking up words
(module-export! '{brico/lookup
		  lookup-word lookup-combo vary-word
		  word-override? lookup-word-prefetch
		  lookup-term brico/resolve})

;;; LOOKING up words

;;; There are two ways of customizing lookup: overrides and overlays.
;;;   overrides replace normal lookup with specific meanings
;;;   overlays augment normal lookup with additional meanings
;;;  An override value of false #f causes lookup to fail (return {}).

(define word-overrides #f)
(define word-overlays #f)

(define (override-get word language)
  (custom-get word language word-overrides))
(define (word-override? word language)
  (exists? (override-get word language)))
(define (overlay-get word language)
  (custom-get word language word-overlays))

(config-def! 'WORDOVERRIDE
	     (ruleset-configfn word-overrides conform-maprule))
(config-def! 'WORDOVERLAY
	     (ruleset-configfn word-overlays conform-maprule))

;;; Morphing rules

(define morphrules {})
(config-def! 'MORPHRULES
	     (ruleset-configfn morphrules conform-maprule))

(define (getbaselang s)
  (if (overlaps? s all-languages) s
      (try (get norm-map s)
	   (get index-map s))))

(define (vary-word word language)
  (for-choices (rule (pick morphrules custom-map-language
			   (choice #f language)))
    (if (custom-map-language rule)
	(if (applicable? (custom-map-handler rule))
	    ((custom-map-handler rule) word)
	    (get (custom-map-handler rule) word))
	(if (applicable? (custom-map-handler rule))
	    ((custom-map-handler rule) word language)
	    (get (custom-map-handler rule) (cons language word))))))

;;; Looking up words

;;; This code looks up words in BRICO, potentially applying
;;;  some entirely textual cleverness.

;;; It also allows applications to provide OVERRIDES and OVERLAYS
;;;  for word lookup.  OVERRIDES keep searches from going to the
;;;  background index; OVERLAYS are just combined with the background
;;;  index.

;;; The TRYHARD argument indicates how hard to try when doing a lookup.
;;; The values are roughly interpreted as follows:
;;;  #f don't try anything but basestring normalization (Malmö ==> Malmo)
;;;  #t or 1 strips out internal punctuation (e.g. set-up ==> setup)
;;;  >1 looks for misspellings (using metaphone) and also combines
;;;     porter-stemming with metaphone.  Note that these are probably
;;;     not really portable between languages, but that's a TODO.
;;;  >2 look for compounds with overlapping words and picks the
;;;      highest ranking concepts that have an overlap greater 3,
;;;      where a correctly spelled word gets 2 points and a possible
;;;      variant gets 1.
;;;  >4 just like above but accepts any overlap grather than 2
;;;  Warning: this compound finding algorithm has a potential issue
;;;   in that BRICO's fragment indexing doesn't distinguish fragments
;;;   from the same compound, so a concept with terms "George Bush" and
;;;   "Scourge of Washington" would match a fragment-based search for
;;;   "George Washington".

(define (lookup-word word (language default-language) (tryhard #f))
  (if (has-prefix word "~")
      (lookup-word (subseq word 1) language #t)
      (if word-overrides
	  (let ((override (override-get word language)))
	    (if (exists? override) (or override (fail))
		(lookup-word-core word language tryhard)))
	  (lookup-word-core word language tryhard))))

(define (lookup-word-core word language tryhard)
  (try (choice (?? language word)
	       (tryif word-overlays (overlay-get word language))
	       (for-choices (variant (vary-word word (getbaselang language)))
		 (if (string? variant)
		     (?? language variant)
		     (if (pair? variant)
			 (?? language (first variant)
			     (second variant) (third variant))))))
       (tryif (not (ascii? word))
	      (let ((sbword (basestring word)))
		(choice (?? language sbword)
			(tryif word-overlays (overlay-get sbword language)))))
       (tryif tryhard (lookup-simple-variants word language tryhard))
       ;; Find misspellings, etc
       ;; This is probably somewhat language-specific
       (tryif (and (number? tryhard) (> tryhard 1))
 	      (?? language
 		  (choice (metaphone word #t)
 			  (metaphone (porter-stem word) #t))))
       ;; Find concepts which have some overlapping words
       (tryif (and (number? tryhard) (> tryhard 2))
	      (lookup-overlapping-words word language tryhard))))

(define (lookup-simple-variants word language tryhard)
  (choice 
   (tryif (or (position #\- word) (position #\Space word) (position #\_ word))
	  (?? language (depunct word)))
   (tryif (and (compound? word) (textsearch '(isupper) word))
	  (let* ((wordv (words->vector word))
		 (len (length wordv)))
	    (tryif (or (and (number? tryhard) (> tryhard 3))
		       (> len 2))
		   ;; Try trimming the first word from the compound
		   ;;  if either there are more than 2 words or
		   ;;  tryhard > 3.
		   (try (choice
			 (lookup-word (seq->phrase wordv 1) language #f)
			 (lookup-word (seq->phrase wordv 0 -1) language #f))
			(tryif (> len 3)
			 (lookup-word (seq->phrase wordv 2) language #f))
			(tryif (> len 4)
			 (lookup-word (seq->phrase wordv 3) language #f))))))))

(define (lookup-overlapping-words word language tryhard)
  (let* ((table (make-hashtable))
	 (wordv (words->vector word))
	 (words (elts wordv))
	 (altwords (choice (metaphone words #t)
			   (metaphone (porter-stem words) #t)))
	 (minscore (max 1 (- (length wordv) (- tryhard 3)))))
    (prefetch-keys! (list language (choice words altwords)))
    (do-choices (word words)
      (let* ((alt (tryif (> tryhard 4)
			 (choice (metaphone word #t)
				 (metaphone (porter-stem word) #t))))
	     (word+alt (choice word alt)))
	(hashtable-increment! table
	    (choice (?? language (list word+alt))
		    (overlay-get (list word+alt) language)
		    (overlay-get (list word+alt) language)))))
    (tryif (and (exists? (table-maxval table))
		(> (table-maxval table) minscore))
	   (table-max table))))

(defambda (lookup-word-prefetch words (language default-language) (tryhard #f))
  (let* ((stds (stdspace words))
	 (bases (basestring stds))
	 (words (choice stds bases))
	 (extra (choice (tryif tryhard (depunct words))
			(tryif (and tryhard (number? tryhard) (> tryhard 1))
			       (choice (metaphone words #t)
				       (metaphone (porter-stem words) #t)))))
	 (shortfrags (tryif tryhard
			    (let ((compounds (words->vector (pick words compound?))))
			      (choice (subseq compounds 1) (subseq compounds 0 -1)
				      (subseq (pick compounds length > 2) 2)))))
	 (frags (tryif (and tryhard (number? tryhard) (> tryhard 2))
		       (let* ((frags (elts (words->vector words)))
			      (altfrags (choice (metaphone frags #t)
						(metaphone (porter-stem frags) #t))))
			 (list frags)))))
    (prefetch-keys! (cons language (choice words extra shortfrags frags)))))

;;; Looking up combos

(define default-combo-slotids (choice partof* genls* implies*))

(define (lookup-combo base cxt (slotid #f) (language default-language))
  ;; (message "Looking up combo of " base " and " cxt " linked by " slotid " in " language)
  (let ((sym (pick cxt symbol?)))
    (if (empty? sym)
	(choice
	 (intersection (lookup-word base language)
		       (lookup-word (pick cxt string?) language))
	 (intersection (lookup-word base language)
		       (?? (or slotid default-combo-slotids)
			   (choice (lookup-word (pick cxt string?) language)
				   (pick cxt oid?)))))
	(choice
	 (intersection (lookup-word base language)
		       (lookup-word (pick cxt string?) language)
		       (?? '{type sensecat} sym))
	 (intersection (lookup-word base language)
		       (?? (or slotid default-combo-slotids)
			   (choice (lookup-word (pick cxt string?) language)
				   (pick cxt oid?)))
		       (?? '{type sensecat} sym))))))

;;; Smart utility lookup function
;;;  This figures out the function from the arguments.
;;;  If you're trying to run fast, don't bother with this.

(define (brico/lookup base . args)
  (if (not (string? base))
      (error 'type "Word is not an string")
      (let* ((args (elts args))
	     (strings (pick args string?))
	     (languages (try (pick (pickoids args) 'type 'language) default-language))
	     (slotids (pick (pickoids args) 'type 'slot))
	     (symbols (pick args symbol?))
	     (others (difference args (choice strings languages slotids))))
	(if (exists? symbols)
	    (intersection
	     (if (or (exists? slotids) (exists? strings))
		 (lookup-combo base (qc strings) (qc (try slotids default-combo-slotids))
			       (qc languages))
		 (if (exists? strings)
		     (lookup-word (qc base strings) languages)
		     (lookup-word base languages)))
	     (?? '{sensecat type} symbols))
	    (if (or (exists? slotids) (exists? strings))
		(lookup-combo base (qc strings) (qc (try slotids default-combo-slotids))
			      (qc languages))
		(if (exists? strings)
		    (lookup-word (qc base strings) languages)
		    (lookup-word base languages)))))))

;;; Term rules
;;;  Terms are compounds which don't neccessarily have lexical entries.
;;;  Often, they combine other forms to help disambiguate, either in
;;;  conventional ways (e.g. "Portland, Oregon") or in peculiar but
;;;  useful ways (e.g. "fish:animal").

(define termrules
  (choice #((label implies (capword)) (spaces) (label word (rest)))
	  #((label implies #((capword) (spaces) (capword))) (spaces) (label word (rest)))	  
	  #((label word (not> ",")) "," (spaces) (label partof (rest)))
	  #((label word (not> ":")) ":" (label genls (rest)))
	  #((label word (not> "(")) "(" (label implies (rest)) ")")
	  #((label word (not> "(")) "(" (label partof (rest)) ")")))

(config-def! 'TERMRULES (ruleset-configfn termrules))

(define (try-termrules word language tryhard)
  (let ((cxthard (and (number? tryhard) (if (<= tryhard 1) #f (-1+ tryhard)))))
    (for-choices (match (text->frame termrules word))
      (let ((word (get match 'word))
	    (implies (get match 'implies))
	    (partof (get match 'partof))
	    (genls (get match 'genls))
	    (norm (get norm-map language)))
	(cons (get match 'word)
	      (qcx (intersection (lookup-word word language tryhard)
				 (try (?? @?implies (try (lookup-word implies norm cxthard)
							 (lookup-word implies language cxthard))
					  @?partof* (try (lookup-word partof norm cxthard)
							 (lookup-word partof language)))
				      (?? @?implies (try (lookup-word implies norm cxthard)
							 (lookup-word implies language)))
				      (?? @?genls* (try (lookup-word genls norm cxthard)
							(lookup-word genls language)))
				      (?? @?partof* (try (lookup-word partof norm cxthard)
							 (lookup-word partof language)))))))))))

(define (lookup-term term (language default-language) (tryhard 1))
  (try (cons term (lookup-word term language tryhard))
       (try-termrules term language tryhard)))

(define (brico/resolve term (language default-language))
  (cdr (lookup-term term language)))

