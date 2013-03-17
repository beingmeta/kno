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
  '(({"o" "as" "a" "amos" "�is" "an"} . "ar")
    ({"o" "es" "e" "emos" "�is" "en"} . "er")
    ({"o" "es" "e" "emos" "�is" "en"} . "ir")))
(define imperfect-indicative
  '(({"aba" "abas"  "�bamos" "abais" "aban"} . "ar")
    ({"�a" "�as"  "�amos" "�ais" "�an"} . "er")
    ({"�a" "�as"  "�amos" "�ais" "�an"} . "ir")))
(define preterit
  '(({"�" "aste"  "�" "amos" "asteis" "aron"} . "ar")
    ({"�" "iste"  "i�" "imos" "isteis" "ieron"} . "er")
    ({"�" "iste"  "i�" "imos" "isteis" "ieron"} . "ir")))
(define future
  '(({"�" "�s" "�" "emos" "�is" "�n"} . "")))
(define conditional
  '(({"�a" "i�s" "�amos" "�ais" "�an"} . "")))

(define present-subjunctive
  '(({"e" "es" "emos" "�is" "en"} . "ar")
    ({"a" "as" "amos" "�is" "an"} . "er")
    ({"a" "as" "amos" "�is" "an"} . "ir")))
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
  (elts (segment "no nada nadie nunca jam�s ninguno ni tampoco")))
(define pronouns
  (choice
   ;; direct object
   (elts (segment "yo t� �l ella usted nosotros vosotros ellos ellas ustedes"))
   ;; object and indirect object
   (elts (segment "me te le lo la nos os los las les m� ti"))
   ;; possessive
   (elts (segment "mi mis tu tus su sus nuestro nuestra nuestros nuestras vuestro vuestra vuestros vuestros"))
   ;; demonstrative
   (elts (segment "�ste �sta �se �sa aqu�l aqu�lla �stos �stas �sos �sas aqu�llos aqu�llas"))
   ;; relative pronouns
   (elts (segment "que quien quienes"))
   ;; Interrogatives
   (elts (segment "qu� cu�ndo d�nde ad�nde cu�nto qui�n qui�nes c�mo"))
   (elts (segment "cual cuales cu�l"))))
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
			    (elts (segment "qu� cu�ndo d�nde ad�nde cu�nto qui�n qui�nes c�mo"))
			    (elts (segment "cual cuales cu�l")))))
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

