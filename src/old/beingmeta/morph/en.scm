;; -*- coding: latin-1 -*-

;; English Morphology
;;  Copyright (C) 2001 Kenneth Haase, All Rights Reserved
;;  Copyright (C) 2001-2013 beingmeta, inc.

(in-module 'morph/en)

(use-module 'texttools)

(module-export!
 '{noun-root verb-root noun-roots verb-roots
	     gerund stop-words wordfreqs
	     prepositions aux-words glue-words pronouns
	     determiners conjunctions
	     adjectives adjectiveset
	     adverbs adverbset})

(define irregular-nouns
  (file->dtype (get-component "data/en-noun-roots.dtype"))
  ;; (use-index (get-component "data/en-noun-roots.index"))
  )
(define noun-roots
  (choice->hashset
   (file->dtypes (get-component "data/en-noun.dtype"))))

(define plural-rules
  '(#({"a" "e" "i" "o" "u"} (subst "ies" "y")) 
    ("xes" . "x") ("zes" . "z") ("ses" . "s")
    ("shes" . "sh") ("ches" . "ch") ("s" . "")))
(define (noun-root x (nountest #f))
  (try (get irregular-nouns x)
       (tryif (position #\Space x)
	      (list->phrase (compound-noun-root (segment x " ") nountest)))
       (morphrule x plural-rules (or nountest noun-roots))))

(define (compound-noun-root segmented nountest)
  (if (null? (cdr segmented))
      (list (noun-root (car segmented) nountest))
      (cons (car segmented) (compound-noun-root (cdr segmented) nountest))))

(define irregular-verbs
  (file->dtype (get-component "data/en-verb-roots.dtype"))
  ;; (use-index (get-component "data/en-verb-roots.index"))
  )
(define verb-roots
  (choice->hashset
   (file->dtypes (get-component "data/en-verb.dtype"))))
(define ing-forms
  (let ((table (make-hashtable)))
    (do-choices (key (getkeys irregular-verbs))
      (when (has-suffix key "ing")
	(add! table (get irregular-verbs key) key)))
    table))

(define gerund-rules
  '(#((+ (isnotvowel)) (isvowel)
      {(SUBST "bbing" "b")
       (SUBST "dding" "d")
       (SUBST "gging" "g")
       (SUBST "nning" "n")
       (SUBST "pping" "p")
       (SUBST "rring" "r")
       (SUBST "tting" "t")})
      ("ing" . "e")
      ("ing" . "")))
(define past-rules
  '(#({"a" "e" "i" "o" "u"} (subst "ied" "y"))
    ("ed" . "ee")
    ("ed" . "e")
    ("ed" . "")))
(define s-rules
  '(#({"a" "e" "i" "o" "u"} (subst "ies" "y"))
    ("xes" . "x") ("zes" . "z") ("ses" . "s") 
    ("shes" . "sh") ("ches" . "ch") ("s" . "")))

(define (verb-root x (verbtest #f))
  (if (capitalized? x)
      (let ((d (downcase x)))
	(if (equal? d x) (fail) (verb-root d #f)))
      (try (get irregular-verbs x)
	   (tryif (position #\Space x)
		  (compound-verb-root x verbtest))
	   (morphrule x gerund-rules (or verbtest verb-roots))
	   (morphrule x past-rules (or verbtest verb-roots))
	   (morphrule x s-rules (or verbtest verb-roots)))))

(define (compound-verb-root x verbtest)
  (let ((pos (position #\Space x)))
    (append (verb-root (subseq x 0 pos) verbtest)
	    (subseq x pos))))

(define (base-gerund x)
  (try (get ing-forms x)
       (morphrule x
		  '(("ee" . "eeing")
		    ("e" . "ing")
		    #((+ (isnotvowel)) (isvowel)
		      {(SUBST "b" "bbing")
		       (SUBST "d" "dding")
		       (SUBST "g" "gging")
		       (SUBST "n" "nning")
		       (SUBST "p" "pping")
		       (SUBST "r" "rring")
		       (SUBST "t" "tting")})
		    ("" . "ing"))
		  #t)))

(define (gerund x)
  (if (position #\Space x)
      (let ((pos (position #\Space x)))
	(stringout (base-gerund (subseq x 0 pos)) (subseq x pos)))
      (base-gerund x)))

;;;; Stop words

(define stop-words
  (let ((data (file->dtypes (get-component "data/en-stop-words.dtype"))))
    (if (and (singleton? data) (hashset? data))
	data
	(choice->hashset data))))

;;;; Other categories

(define glue-words
  (file->dtypes (get-component "data/en-glue-words.dtype")))

(define pronouns
  (file->dtypes (get-component "data/en-pronouns.dtype")))

(define aux-words
  (file->dtypes (get-component "data/en-aux-words.dtype")))

(define prepositions
  (file->dtypes (get-component "data/en-prepositions.dtype")))

(define determiners
  (file->dtypes (get-component "data/en-determiners.dtype")))

(define conjunctions
  (file->dtypes (get-component "data/en-conjunctions.dtype")))

(define adverbs
  (file->dtypes (get-component "data/en-adverbs.dtype")))
(define adverbset (choice->hashset adverbs))

(define adjectives
  (file->dtypes (get-component "data/en-adjectives.dtype")))
(define adjectiveset (choice->hashset adjectives))

(hashset-add! stop-words
	      (choice glue-words pronouns aux-words prepositions determiners
		      conjunctions))

;;; Word frequencies

(define wordfreqs
  (file->dtypes (get-component "data/en-freqs.dtype")))

;;; Interfacing with BRICO

;;; This interfaces with brico's lookup-word.to use English morphology
;;;  Note that through the magic of CONFIG, we don't have to have
;;;   BRICO loaded or working to specify this, though we do need to
;;;   use absolute OID references to specify the language.

(defambda (english-morphology word)
  (choice (list (noun-root word) 'type 'noun)
	  (list (verb-root word) 'type 'verb)))

(config! 'MORPHRULES
	 (vector 'ENGLISH-MORPHOLOGY @1/2c1c7"English" english-morphology))

