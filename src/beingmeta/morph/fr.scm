;;; -*- coding: latin-1 -*-

;; French Morphology
;;  Copyright (C) 2001 Kenneth Haase, All Rights Reserved
;;  Copyright (C) 2001-2013 beingmeta, inc,. All Rights Reserved

(in-module 'morph/fr)
(use-module 'morph)
(use-module 'texttools)

(define known-names (make-hashtable))

(define irregulars
  (file->dtypes (get-component "data/fr-exceptions.dtype")))
(define (irregulars-table tag)
  (let ((map (make-hashtable)))
    (do-choices (irregular irregulars)
      (if (eq? (cadr irregular) tag)
	  (store! map (third irregular) (car irregular))))
    map))
(define verb-roots
  (choice->hashset
   (file->dtypes (get-component "data/fr-verb.dtype"))))

(define present-indicative
  '(({"e" "es" "ons" "ez" "ent"} . "er")
    ({"is" "it" "issons" "issez" "issent"} . "ir")
    ({"s" "" "ons" "ez" "ent"} . "re")))
(define imperfect-indicative
  '(({"ais" "ait" "ions" "iez" "aient"} . "ons")))

(define simple-past
  '(({"ai" "as" "a" "âmes" "âtes" "èrent"} . "er")
    ({"is" "it" "îmes" "îtes" "irent"} . {"ir" "re"})))


(define future
  '(("" . {"ai" "as" "a" "ons" "ez" "ont"})))
(define conditional
  '(("" . {"ais" "ait" "ions" "iez" "aient"})))

(define past-participle
  '(("é" . "er")
    ("i" . "ir")
    ("u" . "re")))
;; This isn't right!
(define present-participle
  '(("é" . "er")
    ("i" . "ir")
    ("u" . "re")))

(define imperfect-subjunctive
  '(({"asse" "asses" "ât" "assions"
				 "assiez" "assent"}
				. "er")
    ({"isse" "isses" "ît" "issions"
      "issiez" "issent"}
     . "ir")
    ({"usse" "usses" "ût" "ussions" "ussiez" "ussent"}
     . "re")))

(define (present-indicative-rule word)
  (morphrule word present-indicative verb-roots))
(define (present-participlep word)
  (morphrule word present-participle present-indicative-rule))

(define (verb-root verb (lex #f))
  (unless lex (set! lex verb-roots))
  (try (morphrule verb '() verb-roots)
       (morphrule verb imperfect-subjunctive verb-roots)
       (morphrule verb past-participle verb-roots)
       (morphrule verb conditional verb-roots)
       (morphrule verb future verb-roots)
       (morphrule verb simple-past verb-roots)
       (morphrule verb imperfect-indicative verb-roots)
       (morphrule verb present-indicative verb-roots)
       (present-indicative-rule verb)
       (present-indicative-rule verb)
       (morphrule verb '(({"e" "es" "ions" "iez" "ent"} . "ant"))
		  present-participlep)))

(define noun-roots
  (choice->hashset
   (file->dtypes (get-component "data/fr-noun.dtype"))))

(define feminine-rules
  '(("e" . "")
    ("anne" . "an")
    ("enne" . "en")
    ("ère" . "er")
    ("euse" . "eur")
    ("trice" . "teur")))
(define plural-rules
  '(("x" . "x")
    ("s" . "s")
    ("z" . "z")
    ("es" . "")
    ("s" . "")
    ("aux" . {"ail" "al"})
    ("eaux" . "eau")
    ("eux" . "eu")
    ("oux" . "ou")))

(define (noun-root noun (lex #f))
  (try (morphrule noun '() (or lex noun-roots))
       (morphrule noun feminine-rules (or lex noun-roots))
       (morphrule noun plural-rules (or lex noun-roots))))

(define adjective-roots
  (choice->hashset
   (file->dtypes (get-component "data/fr-adjective.dtype"))))

(define feminine-adjective-rules
  '(("e" . "")
    ("euse" . {"eux" "eur"})
    ("ve" . "f")
    ("ère" . "er")
    ("ille" . "il")
    ("elle" . "el")
    ("ulle" . "ul")
    ("enne" . "en")
    ("onne" . "on")
    ("ète" . "et")))
(define plural-adjective-rules
  '(("es" . "")
    ("s" . "")
    ("x" . "x")
    ("s" . "s")
    ("aux" . {"eau" "al"})))

(define (adjective-root word (lex #f))
  (try (morphrule word '() (or lex adjective-roots))
       (morphrule word feminine-adjective-rules (or lex adjective-roots))
       (morphrule word plural-adjective-rules (or lex adjective-roots))))

(define adverb-roots
  (choice->hashset
   (file->dtypes (get-component "data/fr-adverb.dtype"))))

(define feminine-adverb-rules
  '(("e" . "")
    ("euse" . {"eux" "eur"})
    ("ve" . "f")
    ("ère" . "er")
    ("ille" . "il")
    ("elle" . "el")
    ("ulle" . "ul")
    ("enne" . "en")
    ("onne" . "on")
    ("ète" . "et")))

(define (adverb-root word (lex #f))
  (try (morphrule word '() (or lex adverb-roots))
       (morphrule word feminine-adverb-rules (or lex adverb-roots))))

(module-export! '{noun-root verb-root adjective-root adverb-root known-names})

;;; Interfacing with BRICO

;;; This interfaces with brico's lookup-word.to use English morphology
;;;  Note that through the magic of CONFIG, we don't have to have
;;;   BRICO loaded or working to specify this, though we do need to
;;;   use absolute OID references to specify the language.

(defambda (french-morphology word)
  (choice (list (noun-root word) 'type 'noun)
	  (list (verb-root word) 'type 'verb)))

(config! 'MORPHRULES
	 (vector 'FRENCH-MORPHOLOGY @1/2c122"French" french-morphology))

