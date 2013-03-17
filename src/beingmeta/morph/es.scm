;;; -*- coding: latin-1 -*-

;; Spanish Morphology
;;  Copyright (C) 2001 Kenneth Haase. All Rights Reserved
;;  Copyright (C) 2001-2013 beingmeta, inc. All Rights Reserved

(in-module 'morph/es)

(use-module '{morph texttools})

(module-export! '{noun-root verb-root adjective-root adverb-root known-names})

(define known-names (make-hashtable))

(define verb-roots
  (choice->hashset
   (file->dtypes (get-component "data/es-verb.dtype"))))
(define pseudo-verb-roots
  (file->dtypes (get-component "data/es-changed-stems.dtype")))
(define irregular-verbs
  (file->dtypes (get-component "data/es-verb-roots.dtype")))

(define past-participle
  '(("ado" . "ar")
    ("ido" . "er")
    ("ido" . "ir")))
(define present-participle
  '(("ando" . "ar")
    ("iendo" . "er")
    ("iendo" . "ir")))
(define present-indicative
  '(({"o" "as" "a" "amos" "áis" "an"} . "ar")
    ({"o" "es" "e" "emos" "éis" "en"} . "er")
    ({"o" "es" "e" "emos" "éis" "en"} . "ir")))
(define imperfect-indicative
  '(({"aba" "abas"  "ábamos" "abais" "aban"} . "ar")
    ({"ía" "ías"  "íamos" "íais" "ían"} . "er")
    ({"ía" "ías"  "íamos" "íais" "ían"} . "ir")))
(define preterit
  '(({"é" "aste"  "ó" "amos" "asteis" "aron"} . "ar")
    ({"í" "iste"  "ió" "imos" "isteis" "ieron"} . "er")
    ({"í" "iste"  "ió" "imos" "isteis" "ieron"} . "ir")))
(define future
  '(({"é" "ás" "á" "emos" "éis" "án"} . "")))
(define conditional
  '(({"ía" "iás" "íamos" "íais" "ían"} . "")))

(define present-subjunctive
  '(({"e" "es" "emos" "éis" "en"} . "ar")
    ({"a" "as" "amos" "áis" "an"} . "er")
    ({"a" "as" "amos" "áis" "an"} . "ir")))
(define imperfect-subjunctive
  '(({"ra" "ras" "ramos" "rais" "rain"
      "se" "ses" "semos" "ses" "sen"} . "ron")))

(define (present-indicative-rule word lex)
  (morphrule word present-indicative lex))
(define (present-participlep word lex)
  (morphrule word present-participle lex))

(define (verb-root verb (lex #f))
  (unless lex (set! lex verb-roots))
  (try (morphrule verb '() lex)
       (morphrule verb imperfect-subjunctive lex)
       (morphrule verb past-participle lex)
       (morphrule verb conditional lex)
       (morphrule verb future lex)
       ;; (morphrule verb simple-past lex)
       (morphrule verb imperfect-indicative lex)
       (morphrule verb present-indicative lex)
       (present-indicative-rule verb lex)
       (present-indicative-rule verb lex)))

(define noun-roots
  (choice->hashset
   (file->dtypes (get-component "data/es-noun.dtype"))))
(define plural-rules
  `(("ces" . "z")
    (,(string-append {"a" "e" "i" "o" "u"} "s") . "")
    ("es" . "")
    ("s" . "")))

(define (noun-root noun (lex #f))
  (unless lex (set! lex noun-roots))
  (try (morphrule noun '() lex)
       (morphrule noun plural-rules lex)))

(define adjective-roots 
  (choice->hashset (file->dtypes (get-component "data/es-adjective.dtype"))))

(define feminine-adjective
  '(("a" . "o")))
(define plural-adjective
  '(("ces" . "z")
    ({"a" "e" "i" "o" "u"} . "s")
    ("es" . "")))

(define (adjective-root adj (lex #f))
  (unless lex (set! lex adjective-roots))
  (try (morphrule adj '() lex)
       (morphrule adj plural-adjective lex)
       (morphrule adj feminine-adjective lex)))

(define adverb-roots
  (choice->hashset
   (file->dtypes (get-component "data/es-adverb.dtype"))))
(define (adverb-root adv (lex #f))
  (unless lex (set! lex adjective-roots))
  (morphrule adv '() lex))

;;; Other word classes

(define prepositions
  (elts (segment "a de sin con en hasta para por")))
(define negatives
  (elts (segment "no nada nadie nunca jamás ninguno ni tampoco")))
(define pronouns
  (choice
   ;; direct object
   (elts (segment "yo tú él ella usted nosotros vosotros ellos ellas ustedes"))
   ;; object and indirect object
   (elts (segment "me te le lo la nos os los las les mí ti"))
   ;; possessive
   (elts (segment "mi mis tu tus su sus nuestro nuestra nuestros nuestras vuestro vuestra vuestros vuestros"))
   ;; demonstrative
   (elts (segment "éste ésta ése ésa aquél aquélla éstos éstas ésos ésas aquéllos aquéllas"))
   ;; relative pronouns
   (elts (segment "que quien quienes"))
   ;; Interrogatives
   (elts (segment "qué cuándo dónde adónde cuánto quién quiénes cómo"))
   (elts (segment "cual cuales cuál"))))
(define determiners
  (elts (segment "el la los las al del un una unos unas")))
(define conjunctions
  (elts (segment "y u o ho")))
;;; More parsing stuff

(define stop-words
  (choice->hashset
   (choice prepositions negatives pronouns determiners conjunctions)))

(define glue-words
  (choice->hashset (choice (elts (segment "que quien quienes"))
			    (elts (segment "qué cuándo dónde adónde cuánto quién quiénes cómo"))
			    (elts (segment "cual cuales cuál")))))
(define aux-words
  (choice->hashset negatives))

;(ing-verb "ing" verb-stem)
;(ed-verb "ed" verb-stem)
;(s-verb "ed" verb-root)
;(verb-stem (drop "e") verb) ; #((isalpha+) (subst "e" ""))
;(verb-stem (add "p") verb) ; #((isalpha+) "p" (subst "" "p"))


;;; Interfacing with BRICO

;;; This interfaces with brico's lookup-word.to use English morphology
;;;  Note that through the magic of CONFIG, we don't have to have
;;;   BRICO loaded or working to specify this, though we do need to
;;;   use absolute OID references to specify the language.

(defambda (spanish-morphology word)
  (choice (list (noun-root word) 'type 'noun)
	  (list (verb-root word) 'type 'verb)))

(config! 'MORPHRULES
	 (vector 'SPANISH-MORPHOLOGY @1/2c1fc"Spanish" spanish-morphology))

