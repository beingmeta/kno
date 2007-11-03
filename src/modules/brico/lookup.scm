;;; -*- Mode: Scheme; Character-Encoding: utf-8; -*-

(in-module 'brico/lookup)

(define version "$Id$")

;;; Looking up terms in BRICO

;;; This module provides utilities and end-user functions for mapping
;;;  natural language terms into BRICO concepts.

(use-module 'brico)
(use-module '{texttools reflection})
(use-module 'rulesets)

(define logger comment)

(define metaphone-max 16)

;;; EXPORTS

;; Looking up words
(module-export! '{brico/lookup
		  lookup-word lookup-combo vary-word
		  word-override? lookup-word-prefetch
		  lookup-term brico/resolve brico/resolveone})

(define %nosubst
  '{word-overrides word-overlays morphrules termrules})

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
  (pickoids (custom-get word language word-overlays)))

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
      (lookup-word (subseq word 1) language 2)
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
       ;; Try looking it up with normalized capitalzation
       (tryif (somecap? word)
	      (let ((variant (capitalize word)))
		(choice (?? language variant)
			(tryif word-overlays (overlay-get variant language)))))
       ;; Try looking it up without diacritics
       (tryif (not (ascii? word))
	      (let ((variant (basestring word)))
		(choice (?? language variant)
			(tryif word-overlays (overlay-get variant language))
			(for-choices (variant (vary-word variant (getbaselang language)))
			  (if (string? variant)
			      (?? language variant)
			      (if (pair? variant)
				  (?? language (first variant)
				      (second variant) (third variant)))))))
	      (tryif (somecap? word)
		     (let ((variant (capitalize (basestring word))))
		       (choice (?? language variant)
			       (tryif word-overlays (overlay-get variant language))))))
       (lookup-simple-variants word language tryhard)
       (lookup-simple-variants (basestring word) language tryhard)
       (tryif tryhard (lookup-subphrase word language tryhard))
       ;; Find misspellings, etc
       ;; This is really language-specific and the implementation
       ;;  doesnt currently reflect that.
       (tryif (and (number? tryhard) (> tryhard 1)
		   (not (uppercase? word))
		   (> (length word) 4))
	      (choice
	       (choice-max
		(?? language
		    (if (somecap? word)
			(string->packet (capitalize (metaphone word)))
			(choice (metaphone word #t)
				(metaphone (porter-stem word) #t))))
		metaphone-max)
	       (tryif (and (number? tryhard) (> tryhard 2))
		      (if (capitalized? word)
			  (?? language (downcase word))
			  (?? language (capitalize word)))
		      (if (capitalized? word)
			  (?? language (downcase (basestring word)))
			  (?? language (capitalize (basestring word)))))))))

(define (lookup-simple-variants word language tryhard)
  (choice 
   (tryif (or (position #\- word) (position #\Space word) (position #\_ word))
	  (?? language (depunct word)))
   (tryif (position #\- word)
	  (?? language (string-subst word "-" " ")))
   (tryif (position #\_ word)
	  (?? language (string-subst word "_" " ")))
   (tryif (uppercase? word) (?? language (capitalize word)))))

(define (lookup-subphrase word language tryhard)
  ;; This method identifies compounds by stripping off the initial or
  ;; final words and seeing if the resulting phrase has an
  ;; unambiguous meaning.
  (tryif (and (compound? word) (textsearch '(isupper) word))
	 (let* ((wordv (words->vector word))
		(len (length wordv)))
	   (tryif (or (and (number? tryhard) (> tryhard 3))
		      (> len 2))
		  ;; Try trimming the first word from the compound
		  ;;  if either there are more than 2 words or
		  ;;  tryhard > 3.
		  (try (choice
			(singleton (lookup-word (seq->phrase wordv 1) language #f))
			(singleton (lookup-word (seq->phrase wordv 0 -1) language #f)))
		       (tryif (> len 3)
			      (singleton (lookup-word (seq->phrase wordv 2) language #f)))
		       (tryif (> len 4)
			      (singleton (lookup-word (seq->phrase wordv 3) language #f))))))))

(define (lookup-overlapping-words word language tryhard)
  (let* ((table (make-hashtable))
	 (wordv (words->vector word))
	 (words (elts wordv))
	 (altwords (choice (metaphone words #t)
			   (metaphone (porter-stem words) #t)))
	 (minscore (max 2 (- (length wordv) (- tryhard 3)))))
    (prefetch-keys! (list language (choice words altwords)))
    (do-choices (word words)
      (let* ((alt (tryif (and (number? tryhard) (> tryhard 4))
			 (choice (metaphone word #t)
				 (metaphone (porter-stem word) #t))))
	     (word+alt (choice word alt)))
	(hashtable-increment! table
	    (choice (?? language (list word+alt))
		    (overlay-get (list word+alt) language)
		    (overlay-get (list word+alt) language)))))
    (tryif (and (exists? (table-maxval table))
		(>= (table-maxval table) minscore))
	   (table-max table))))

(defambda (lookup-word-prefetch words (language default-language) (tryhard #f))
  (let* ((words (stdspace words))
	 (bases (basestring words))
	 (words+bases (choice words bases))
	 (simple-vary
	  (choice (depunct (pick words+bases string-contains? {"-" " " "_"}))
		  (string-subst words+bases "-" " ")
		  (string-subst words+bases "_" " ")
		  (capitalize (pick words+bases uppercase?))))
	 (rich-vary (vary-word words+bases (getbaselang language)))
	 (variations (choice (pick rich-vary string?)
			     (car (pick rich-vary pair?))))
	 (vary-constraints (for-choices (entry (pick rich-vary pair?))
			     (cons (second entry) (third entry)))))
    (if (or (not tryhard) (eq? tryhard 0))
	(prefetch-keys!
	 (choice (cons @?en (choice words+bases simple-vary variations))
		 vary-constraints))
	(let* ((caps (pick words+bases capitalized?))
	       (lower (capitalize (reject words+bases capitalized?)))
	       (metaphones
		(tryif (and tryhard (number? tryhard) (> tryhard 1))
		       (choice (metaphone bases #t)
			       (metaphone (porter-stem bases) #t)
			       (string->packet (capitalize (metaphone caps))))))
	       (capvary (tryif (and (number? tryhard) (> tryhard 2))
			       (choice (downcase (reject caps uppercase?)) (capitalize lower))))
	       (shortfrags
		(tryif tryhard
		       (let ((compounds
			      (words->vector
			       (pick words+bases compound?))))
			 (choice (subseq compounds 1) (subseq compounds 0 -1)
				 (subseq (pick compounds length > 2) 2)))))
	       (frags (tryif
		       #f 
		       ;; (and tryhard (number? tryhard) (> tryhard 2))
		       (let* ((frags (elts (words->vector words)))
			      (altfrags
			       (choice (metaphone frags #t)
				       (metaphone (porter-stem frags) #t))))
			 (list frags)))))
	  (prefetch-keys!
	   (choice (cons @?en (choice words+bases simple-vary variations
				      metaphones shortfrags capvary frags))
		   vary-constraints))))))

(define (track-lookup-word word (language default-language) (tryhard 1))
  (clearcaches)
  (lookup-word-prefetch word language tryhard)
  ((within-module 'trackrefs trackrefs)
   (lambda () (choice-size (lookup-word word language tryhard)))))
(module-export! 'track-lookup-word)

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
	     (languages (try (pick (pickoids args) 'type 'language)
			     default-language))
	     (slotids (reject (pick (pickoids args) 'type 'slot)
			      'type 'language))
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
  (choice #((label word (not> ",")) "," (spaces) (label partof (rest)))
	  #((label word (not> ",")) "," (spaces)
	    (label partof (not> "("))
	    "(" (label implies (not> ")")) ")")
	  #((label word (not> ":")) ":" (label genls (rest)))
	  #((label word (not> "(")) "(" (label implies (not> ")"))  ")")
	  #((label word (not> "(")) "(" (label partof (not> ")")) ")")
	  #((label word (not> "(")) "(e.g." (spaces) (label eg (rest)) ")")
	  #((label word (not> "(")) "(*" (label defterm (not> ")")) ")")))

(config-def! 'TERMRULES (ruleset-configfn termrules))

(define (lookup-context match language (tryhard #f))
  (logger "Looking up context " match " in " language ", tryhard=" tryhard)
  (let ((implies-cxt (lookup-word (get match 'implies) language tryhard))
	(partof-cxt (lookup-word (get match 'partof) language tryhard))
	(genls-cxt (lookup-word (get match 'genls) language tryhard))
	(eg-cxt (lookup-word (get match 'eg) language tryhard))
	(defterm-cxt (lookup-word (get match 'defterm) language tryhard)))
    (if (exists? partof-cxt)
	(try (intersection (?? partof* partof-cxt) (?? implies implies-cxt))
	     (intersection (?? partof* partof-cxt) (?? defterms defterm-cxt))
	     (?? partof* partof-cxt))
	(if (exists? implies-cxt)
	    (if (fail? (choice genls-cxt eg-cxt defterm-cxt))
		(?? implies implies-cxt)
		(intersection (?? implies implies-cxt)
			      (choice (?? specls* eg-cxt)
				      (?? specls* (get eg-cxt implies))
				      (?? defterms defterm-cxt)
				      (?? genls* genls-cxt))))
	    (choice (?? specls* eg-cxt) (?? specls* (get eg-cxt implies))
		    (?? defterms defterm-cxt) (?? genls* genls-cxt))))))

(define (try-termrules word language tryhard)
  (let ((cxthard (and (number? tryhard) (if (<= tryhard 1) #f (-1+ tryhard)))))
    (for-choices (match (text->frame termrules word))
      (let* ((word (stdspace (get match 'word)))
	     (norm (get norm-map language))
	     (normcxt (lookup-context match norm tryhard)))
	(cons word
	      (qcx (try (intersection (lookup-word word norm tryhard) normcxt)
			(intersection (lookup-word word language tryhard) normcxt)
			(intersection
			 (lookup-word word language tryhard)
			 (lookup-context match language tryhard)))))))))

(define (lookup-term term (language default-language) (tryhard 1))
  (if (has-prefix term "~")
      (if (has-prefix term "~~")
	  (lookup-term (subseq term 2) language 4)
	  (lookup-term (subseq term 1) language 2))
      (try (cons term (singleton (lookup-word term language #f)))
	   (if (or (position #\, term) (position #\: term) (position #\( term))
	       (try (try-termrules term language tryhard)
		    (tryif (uppercase? term)
			   (try-termrules (capitalize term) language tryhard)))
	       (try (tryif (uppercase? term)
			   (cons term (qcx (lookup-word (capitalize term) language tryhard))))
		    (cons term (qcx (lookup-word term language tryhard))))))))

(define (brico/resolve term (language default-language) (tryhard 2))
  (cdr (lookup-term term language tryhard)))

(define (absfreq c) (choice-size (?? @?refterms c)))

(define (brico/resolveone term (language default-language) (tryhard 2))
  (let ((possible (cdr (lookup-term term language tryhard))))
    (if (fail? possible) {}
	(try (singleton possible)
	     (singleton (difference possible (?? @?defterms possible)))
	     (pick-one (largest possible absfreq))))))



