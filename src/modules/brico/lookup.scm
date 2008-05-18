;;; -*- Mode: Scheme; Character-Encoding: utf-8; -*-

(in-module 'brico/lookup)

(define version "$Id$")

;;; Looking up terms in BRICO

;;; This module provides utilities and end-user functions for mapping
;;;  natural language terms into BRICO concepts.

(use-module 'brico)
(use-module '{texttools reflection})
(use-module '{morph morph/en})
(use-module '{brico/maprules rulesets})

(define logger %logger)

(define metaphone-max 16)

;;; EXPORTS

;; Looking up words
(module-export! '{brico/ref
		  lookup-word lookup-term parse-term
		  lookup-combo vary-word word-override?
		  lookup-word/prefetch lookup-term/prefetch
		  brico/resolve})

(define %nosubst '{morphrules termrules})

(define remote-lookup-term #f)

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
		   (reject (pick variations pair?) car word)))))))


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

(define (lookup-word word (language default-language) (tryhard tryhard-default))
  "Looks up a word in a language with a given level of effort"
  (if (not tryhard) (set! tryhard 0)
      (if (not (number? tryhard)) (set! tryhard tryhard-default)))
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
	      (lookup-word-core (stdspace word) language tryhard)))))

(define (lookup-word-core word language tryhard)
  ;; (message "lookup-word-core " (write word) " " language " " tryhard)
  (choice (?? language word)
	  (tryif (> tryhard 0)
		 (let* ((baselang (getbaselang language))
			(variants (vary-word word baselang tryhard)))
		   (lookup-variants variants language tryhard)))
	  (tryif (and (> tryhard 1) (not (uppercase? word)) (> (length word) 4))
		 (choice-max
		  (?? language (metaphone (choice word (porter-stem word))
					  #t))
		  metaphone-max))
	  (tryif (> tryhard 1)
		 (if (capitalized? word)
		     (?? language (downcase (choice word (basestring word))))
		     (?? language (capitalize (choice word (basestring word))))))))

(defambda (lookup-variants variants language tryhard)
  (for-choices (variant variants)
    (if (string? variant)
	(lookup-word variant language #f)
	(if (pair? variant)
	    (intersection
	     (lookup-word (first variant) language #f)
	     (?? (second variant) (third variant)))
	    (fail)))))

;;; Prefetching

(defambda (lookup-word/prefetch words (language default-language) (tryhard #f))
  (let* ((words (stdspace words))
	 (baselang (getbaselang language))
	 (vwords (vary-word words baselang tryhard))
	 (vvwords (vary-word (vary-word words baselang tryhard #t) baselang))
	 (all-vary (choice words vwords vvwords))
	 (vary-words (choice (pickstrings all-vary)
			     (car (pick all-vary pair?))))
	 (vary-constraints (for-choices (entry (pick all-vary pair?))
			     (cons (second entry) (third entry)))))
    (prefetch-keys! (choice (cons english vary-words)
			    vary-constraints))))

#|
(define (track-lookup-word word (language default-language) (tryhard 1))
  (clearcaches)
  (lookup-word/prefetch word language tryhard)
  ((within-module 'trackrefs trackrefs)
   (lambda () (choice-size (lookup-word word language tryhard)))))
|#
(module-export! 'track-lookup-word)

;;; Fragmentary lookup

(define (score-fragment! table language frag (weight 1))
  (let* ((fragslot (get frag-map language))
	 (results (?? fragslot frag)))
    (when (exists? results)
      (hashtable-increment! table
	(?? fragslot frag)
	(if (inexact? weight)
	    (/ weight (ilog (choice-size results)))
	    weight)))))

(define (score-fragments word language tryhard (weight 1))
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
      (score-fragment! table language (list word) 2.0)
      (let* ((alt (tryif (and (number? tryhard) (> tryhard 2))
			 (choice (metaphone word #t)
				 (metaphone (porter-stem word) #t)))))
	(hashtable-increment! table (?? fragslot (list alt))
	  (/~ 1.0 (ilog (choice-size (?? fragslot (list word))))))))
    (hashtable-increment! table
      (?? fragslot firstword)
      (/~ 1.0 (ilog (choice-size (?? fragslot firstword)))))
    (hashtable-increment! table
      (?? fragslot lastword)
      (/~ 1.0 (ilog (choice-size (?? fragslot firstword)))))
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
	  ;; #((label word (not> "=")) "=" (label synonyms (rest)))
	  #((label word (not> "(")) "(-" (label never (not> ")"))  ")")
	  #((label word (not> "(")) "(" (label implies (not> ")"))  ")")
	  #((label word (not> "(")) "(" (label partof (not> ")")) ")")
	  #((label word (not> "(")) "(e.g." (spaces) (label eg (not> ")")) ")")
	  #((label word (not> "(")) "(:" (label defterm (not> ")")) ")")))

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
	 (never-cxt (get matches 'never))
	 (eg-cxt (get matches 'eg))
	 (defterm-cxt (get matches 'defterm))
	 (meanings (lookup-word word language tryhard)))
    ;;(%watch matches word language syn-cxt implies-cxt partof-cxt genls-cxt eg-cxt meanings)
    (cons word
	  (try (intersection meanings (lookup-word syn-cxt language))
	       (tryif (and (exists? partof-cxt) (exists? implies-cxt))
		      (intersection meanings (?? partof* (lookup-word partof-cxt language))
				    (?? implies (list (lookup-word implies-cxt))))
		      (intersection meanings (?? partof* (lookup-word partof-cxt language))
				    (?? implies (lookup-word implies-cxt))))
	       (tryif (exists? partof-cxt)
		      (intersection meanings (?? partof* (lookup-word partof-cxt language))))
	       (tryif (and (exists? genls-cxt) (string? genls-cxt) (> (length genls-cxt) 0))
		      (if (eqv? (elt genls-cxt 0) #\-)
			  (intersection meanings (?? never (lookup-word (subseq genls-cxt 1) language)))
			  (intersection meanings (?? always (lookup-word genls-cxt language)))))
	       (tryif (exists? implies-cxt)
		      (try (intersection meanings (?? (choice sometimes always)
						      (list (lookup-word implies-cxt language))))
			   (intersection meanings (?? (choice sometimes always)
						      (lookup-word implies-cxt language)))))
	       (tryif (exists? never-cxt)
		      (intersection meanings (?? never (lookup-word never-cxt language))))
	       (tryif (exists? defterm-cxt)
		      (intersection meanings (?? defterms (lookup-word defterm-cxt language))))
	       (tryif (exists? eg-cxt)
		      (let ((cxt (lookup-word eg-cxt language)))
			(try (intersection meanings (?? sometimes cxt))
			     (filter-choices (meaning meanings)
			       (overlaps? cxt (?? implies meaning))))))))))

(define (parse-term term (language default-language) (tryhard 1) (solenorms #t))
  (if (not tryhard) (set! tryhard 0)
      (if (not (number? tryhard)) (set! tryhard 1)))
  (if (has-prefix term "~")
      (lookup-term (subseq term 1) language (1+ tryhard))
      (if (has-prefix term "$")
	  (lookup-term (subseq term 1) english)
	  (if (exists? (textmatcher #((isalpha) (isalpha) "$") term))
	      (lookup-term (subseq term 3) (?? 'iso639/1 (subseq term 0 2))
			   tryhard)
	      (try (tryif (and solenorms
			       (not (or (position #\, term) (position #\: term)
				   (position #\= term) (position #\( term))))
			  (cons term (singleton (?? (get norm-map language) term))))
		   (cons term (singleton (lookup-word term language #f)))
		   (if (or (position #\, term) (position #\: term)
			   (position #\= term) (position #\( term))
		       (try-termrules term language tryhard)
		       (try (cons term (lookup-word term language tryhard))
			    (cons (capitalize term)
				  (qcx (lookup-word (capitalize term)
					       language tryhard))))))))))

(define (lookup-term term (language default-language) (tryhard 1) (solenorm #t))
  (if (not tryhard) (set! tryhard 0)
      (if (not (number? tryhard)) (set! tryhard 1)))
  (cdr (parse-term term language tryhard solenorm)))

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
     (choice (cons partof* (lookup-word partof-cxt language))
	     (cons implies (lookup-word implies-cxt language))
	     (cons implies (list (lookup-word implies-cxt language)))
	     (cons (choice genls* implies)
		   (lookup-word genls-cxt language))
	     (cons defterms (lookup-word defterm-cxt language))
	     (cons specls* (lookup-word eg-cxt language))))))

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

(define (absfreq c) (choice-size (?? refterms c)))

(define (brico/ref term (language default-language) (tryhard 2))
  (let ((possible ((or remote-lookup-term lookup-term) term language tryhard)))
    (if (fail? possible) {}
	(try (singleton possible)
	     ;; This biases towards terms which aren't defined
	     ;;  in terms of other terms.
	     (singleton (difference possible (?? defterms possible)))
	     (pick-one (largest possible absfreq))))))


