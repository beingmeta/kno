;;; -*- Mode: Scheme; Character-Encoding: utf-8; -*-

(in-module 'brico/lookup)

(define version "$Id$")

;;; Looking up terms in BRICO

;;; This module provides utilities and end-user functions for mapping
;;;  natural language terms into BRICO concepts.

(use-module 'brico)
(use-module '{texttools reflection})
(use-module '{morph morph/en})
(use-module '{rulesets})

(define logger %logger)

(define metaphone-max 16)

;;; EXPORTS

;; Looking up words
(module-export! '{brico/lookup
		  lookup-word lookup-term lookup-combo vary-word word-override?
		  lookup-word/prefetch lookup-term/prefetch
		  brico/resolve brico/resolveone brico/ref})

(define %nosubst
  '{word-overrides word-overlays morphrules termrules})

(define remote-lookup-term #f)

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


(define (apply-variation rule word language)
  (if (custom-map-language rule)
      (tryif (eq? (custom-map-language rule) language)
	     (if (applicable? (custom-map-handler rule))
		 ((custom-map-handler rule) word)
		 (get (custom-map-handler rule) word)))
      (if (applicable? (custom-map-handler rule))
	  ((custom-map-handler rule) word language)
	  (get (custom-map-handler rule) (cons language word)))))

(define (vary-word word language (tryhard #f) (juststrings #f))
  "This generates 'normal' variants "
  (choice
   (tryif (somecap? word) (difference (capitalize word) word))
   (tryif (not (ascii? word)) (difference (basestring word) word))
   (for-choices (rule (pick morphrules custom-map-language
			    (choice #f language)))
     (let ((variations (apply-variation rule word language)))
       (choice (difference (pickstrings variations) word)
	       (if juststrings
		   (difference (car (pick variations pair?)) word)
<<<<<<< .mine
		   (reject (pick variations pair?) car word)))))))
=======
		   (difference (reject variations pair?) word)))))))
>>>>>>> .r2386


(define (vary-more word)
  (difference
   (choice
    (tryif (or (compound? word)
	       (position #\- word)
	       (position #\_ word))
	   (choice (depunct word)
		   (string-subst word "-" " ")
		   (string-subst word "_" " ")))
    (capitalize word) (downcase word)
    (tryif (lowercase? word) (upcase word)))
   word))

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
;;;      and capitalization normalization
;;;  #t or 1 strips out internal punctuation (e.g. set-up ==> setup)
;;;  >1 looks for misspellings (using metaphone) and also combines
;;;     porter-stemming with metaphone.  Note that these are probably
;;;     not really portable between languages, but that's a TODO.
;;;  >2 look for compounds with overlapping words and picks the
;;;      highest ranking concepts that have an overlap greater 3,
;;;      where a correctly spelled word gets 2 points and a possible
;;;      variant gets 1.
;;;  >4 just like above but accepts any overlap greater than 2
;;;  Warning: this compound finding algorithm has a potential issue
;;;   in that BRICO's fragment indexing doesn't distinguish fragments
;;;   from the same compound, so a concept with terms "George Bush" and
;;;   "Scourge of Washington" would match a fragment-based search for
;;;   "George Washington".

(define tryhard-default 1)

(define (lookup-word word (language default-language)
		     (tryhard tryhard-default))
  "Looks up a word in a language with a given level of effort"
  (if (not tryhard) (set! tryhard 0)
      (if (eq? tryhard #t) (set! tryhard 1)))
  (if (not (number? tryhard)) (error "Tryhard argument should be number"))
  ;; Syntactic prefix tricks:
  ;; ~ means try harder, xx$ means try in language xx, $ means try in English
  (if (has-prefix word "~")
      (lookup-word (subseq word 1) language (+ tryhard 1))
      (if (has-prefix word "$")
	  (lookup-word (subseq word 1) english tryhard)
	  (if (exists? (textmatcher #((isalpha) (isalpha) "$") word))
	      (lookup-word (subseq word 3)
			   (?? 'iso639/1 (subseq word 0 2))
			   tryhard)
	      (if word-overrides
		  ;; Word overrides cut short the lookup process
		  (let ((override (override-get word language)))
		    (if (exists? override) (or override (fail))
			(lookup-word-core word language tryhard)))
		  (lookup-word-core word language tryhard))))))

(defambda (lookup-variants variants language tryhard)
  (for-choices (variant variants)
    (if (string? variant)
	(lookup-word variant language tryhard)
	(if (pair? variant)
	    (intersection
	     (lookup-word (first variant) language tryhard)
	     (?? (second variant) (third variant)))
	    (fail)))))

(define (lookup-word-core word language tryhard)
  ;; (message "lookup-word-core " (write word) " " language " " tryhard)
  (try (choice (?? language word)
	       (tryif word-overlays (overlay-get word language))
	       (let* ((baselang (getbaselang language))
		      (variants (vary-word word baselang tryhard)))
		 (lookup-variants (difference variants word) language tryhard)))
       (tryif (and tryhard (not (= tryhard 0)))
	      (choice
	       (lookup-subphrase word language tryhard)
	       (lookup-variants (difference (vary-more word) word) language #f)))
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
   (tryif (uppercase? word)
	  (?? language (capitalize word)))
   (tryif (and tryhard (number? tryhard) (> tryhard 1) (lowercase? word))
	  (?? language (capitalize word)))))

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

;;; Prefetching

(defambda (lookup-word/prefetch words (language default-language) (tryhard #f))
  (let* ((words (stdspace words))
	 (baselang (getbaselang language))
	 (vwords (vary-word words baselang))
	 (vvwords (vary-word vwords baselang))
	 (all-vary (choice words vwords vvwords))
	 (vary-words (choice (pickstrings all-vary)
			     (car (pick all-vary pair?))))
	 (vary-constraints (for-choices (entry (pick all-vary pair?))
			     (cons (second entry) (third entry)))))
    (prefetch-keys! (choice (cons @?en vary-words) vary-constraints))))

#|
(define (track-lookup-word word (language default-language) (tryhard 1))
  (clearcaches)
  (lookup-word/prefetch word language tryhard)
  ((within-module 'trackrefs trackrefs)
   (lambda () (choice-size (lookup-word word language tryhard)))))
|#
(module-export! 'track-lookup-word)

;;; Fragmentary lookup

(define (score-fragments word language tryhard)
  (let* ((table (make-hashtable))
	 (wordv (words->vector word))
	 (words (elts wordv))
	 (altwords (choice (metaphone words #t)
			   (metaphone (porter-stem words) #t)))
	 (firstword (list #f (elt wordv 0)))
	 (lastword (list (elt wordv -1) #f))
	 (fragslot (get frag-map language)))
    (prefetch-keys! (list language (choice words altwords)))
    (do-choices (word words)
      (let* ((alt (tryif (and (number? tryhard) (> tryhard 2))
			 (choice (metaphone word #t)
				 (metaphone (porter-stem word) #t))))
	     (word+alt (choice word alt)))
	(hashtable-increment! table
	    (choice (?? fragslot (list word+alt))
		    (overlay-get (list word+alt) language)))))
    
    (hashtable-increment! table
	(choice (?? fragslot firstword) (overlay-get firstword language)))
    (hashtable-increment! table
	(choice (?? fragslot lastword) (overlay-get lastword language)))
    table))

(define (lookup-fragments word (language default-language) (tryhard #f))
  (let ((table (score-fragments word language tryhard))
	(minscore (max 2 (- (length (words->vector word)) 2))))
    (tryif (and (exists? (table-maxval table))
		(>= (table-maxval table) minscore))
	   (table-max table))))

(module-export! '{score-fragments lookup-fragments})

;;; Looking up combos

(define default-combo-slotids (choice partof* genls* implies*))

(define (lookup-combo base cxt (slotid #f) (language default-language))
  ;; (message "Looking up combo of " base " and " cxt " linked by " slotid " in " language)
  (let ((sym (pick cxt symbol?)))
    (if (empty? sym)
	(choice
	 (intersection (lookup-word base language)
		       (lookup-word (pickstrings cxt) language))
	 (intersection (lookup-word base language)
		       (?? (or slotid default-combo-slotids)
			   (choice (lookup-word (pickstrings cxt) language)
				   (pickoids cxt)))))
	(choice
	 (intersection (lookup-word base language)
		       (lookup-word (pickstrings cxt) language)
		       (?? '{type sensecat} sym))
	 (intersection (lookup-word base language)
		       (?? (or slotid default-combo-slotids)
			   (choice (lookup-word (pickstrings cxt) language)
				   (pickoids cxt)))
		       (?? '{type sensecat} sym))))))

;;; Smart utility lookup function
;;;  This figures out the function from the arguments.
;;;  If you're trying to run fast, don't bother with this.

; (define (brico/lookup base . args)
;   (if (not (string? base))
;       (error 'type "Word is not an string")
;       (let* ((args (elts args))
; 	     (strings (pick args string?))
; 	     (languages (try (pick (pickoids args) 'type 'language)
; 			     default-language))
; 	     (slotids (reject (pick (pickoids args) 'type 'slot)
; 			      'type 'language))
; 	     (symbols (pick args symbol?))
; 	     (others (difference args (choice strings languages slotids))))
; 	(if (exists? symbols)
; 	    (intersection
; 	     (if (or (exists? slotids) (exists? strings))
; 		 (lookup-combo base (qc strings) (qc (try slotids default-combo-slotids))
; 			       (qc languages))
; 		 (if (exists? strings)
; 		     (lookup-word (qc base strings) languages)
; 		     (lookup-word base languages)))
; 	     (?? '{sensecat type} symbols))
; 	    (if (or (exists? slotids) (exists? strings))
; 		(lookup-combo base (qc strings) (qc (try slotids default-combo-slotids))
; 			      (qc languages))
; 		(if (exists? strings)
; 		    (lookup-word (qc base strings) languages)
; 		    (lookup-word base languages)))))))

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
	  #((label word (not> "=")) "=" (label synonyms (rest)))
	  #((label word (not> "(")) "(" (label implies (not> ")"))  ")")
	  #((label word (not> "(")) "(" (label partof (not> ")")) ")")
	  #((label word (not> "(")) "(e.g." (spaces) (label eg (not> ")")) ")")
	  #((label word (not> "(")) "(*" (label defterm (not> ")")) ")")))

(config-def! 'TERMRULES (ruleset-configfn termrules))

(define (try-termrules term language tryhard)
  (let* ((cxthard (and (number? tryhard) (if (<= tryhard 1) #f (-1+ tryhard))))
	 (matches (text->frame termrules term))
	 (word (get matches 'word))
	 (norm (get norm-map language))
	 (syn-cxt (get matches 'synonyms))
	 (implies-cxt (get matches 'implies))
	 (partof-cxt (get matches 'partof))
	 (genls-cxt (get matches 'genls))
	 (eg-cxt (get matches 'eg))
	 (defterm-cxt (get matches 'defterm))
	 (meanings (lookup-word word language tryhard)))
    ;; (%watch matches word syn-cxt implies-cxt partof-cxt genls-cxt eg-cxt)
    (cons word
	  (try (intersection meanings (lookup-word syn-cxt language))
	       (tryif (and (exists? partof-cxt) (exists? implies-cxt))
		      (intersection meanings (?? @?partof* (lookup-word partof-cxt language))
				    (?? @?implies (lookup-word implies-cxt))))
	       (tryif (exists? partof-cxt)
		      (intersection meanings (?? @?partof* (lookup-word partof-cxt language))))
	       (tryif (exists? genls-cxt)
		      (intersection meanings (?? @?genls* (lookup-word genls-cxt language))))
	       (tryif (exists? defterm-cxt)
		      (intersection meanings (?? @?defterms (lookup-word defterm-cxt language))))
	       (tryif (exists? eg-cxt)
		      (let ((cxt (lookup-word eg-cxt language)))
			(try (intersection meanings (?? @?specls* cxt))
			     (filter-choices (meaning meanings)
			       (overlaps? cxt (?? @?implies meaning))))))))))

(define (lookup-term term (language default-language) (tryhard 1))
  (if (has-prefix term "~")
      (lookup-term (subseq term 1) language (1+ tryhard))
      (if (has-prefix term "$")
	  (lookup-term (subseq term 1) english)
	  (if (exists? (textmatcher #((isalpha) (isalpha) "$") term))
	      (lookup-term (subseq term 3) (?? 'iso639/1 (subseq term 0 2)) tryhard)
	      (try (cons term (singleton (lookup-word term language #f)))
		   (if (or (position #\, term) (position #\: term)
			   (position #\= term) (position #\( term))
		       (try (try-termrules term language tryhard)
			    (tryif (uppercase? term)
				   (try-termrules (capitalize term) language tryhard)))
		       (try (tryif (uppercase? term)
				   (cons term (qcx (lookup-word (capitalize term) language tryhard))))
			    (cons term (qcx (lookup-word term language tryhard))))))))))

(defambda (try-termrules/prefetch term language tryhard)
  (lookup-word/prefetch term language #f)
  (let* ((cxthard (and (number? tryhard) (if (<= tryhard 1) #f (-1+ tryhard))))
	 (matches (text->frame termrules term))
	 (word (get matches 'word))
	 (norm (get norm-map language))
	 (syn-cxt (get matches 'synonyms))
	 (implies-cxt (get matches 'implies))
	 (partof-cxt (get matches 'partof))
	 (genls-cxt (get matches 'genls))
	 (eg-cxt (get matches 'eg))
	 (defterm-cxt (get matches 'defterm))
	 (meanings (lookup-word word language tryhard)))
    (lookup-word/prefetch
     (choice word syn-cxt implies-cxt partof-cxt genls-cxt eg-cxt defterm-cxt)
     language tryhard)
    (prefetch-keys!
     (choice (cons @?partof* (lookup-word partof-cxt language))
	     (cons @?genls* (lookup-word genls-cxt language))
	     (cons @?defterms (lookup-word defterm-cxt language))
	     (cons @?specls* (lookup-word eg-cxt language))))))

(define (lookup-term/prefetch term (language default-language) (tryhard 1))
  (if (has-prefix term "~")
      (lookup-term/prefetch (subseq term 1) language (1+ tryhard))
      (if (has-prefix term "$")
	  (lookup-term/prefetch (subseq term 1) english)
	  (if (exists? (textmatcher #((isalpha) (isalpha) "$") term))
	      (lookup-term/prefetch (subseq term 3) (?? 'iso639/1 (subseq term 0 2)) tryhard)
	      (try (cons term (singleton (lookup-word term language #f)))
		   (if (or (position #\, term) (position #\: term)
			   (position #\= term) (position #\( term))
		       (try-termrules/prefetch
			(choice term (tryif (uppercase? term) (capitalize term)))
			language tryhard)
		       (lookup-word/prefetch (choice term (tryif (uppercase? term) (capitalize term)))
					     language tryhard)))))))

;;; Top level wrappers

(define (brico/resolve term (language default-language) (tryhard 2))
  (cdr ((or remote-lookup-term lookup-term) term language tryhard)))

(define (absfreq c) (choice-size (?? @?refterms c)))

(define (brico/resolveone term (language default-language) (tryhard 2))
  (let ((possible
	 (cdr ((or remote-lookup-term lookup-term) term language tryhard))))
    (if (fail? possible) {}
	(try (singleton possible)
	     ;; This biases towards terms which aren't defined
	     ;;  in terms of other terms.
	     (singleton (difference possible (?? @?defterms possible)))
	     (pick-one (largest possible absfreq))))))

(define brico/ref brico/resolveone)

