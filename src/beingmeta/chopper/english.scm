;; -*- Mode: Scheme; Character-encoding: utf-8;  -*-

(load-component "chopper.scm")
(load-component "chunker.scm")

(use-module '{logger logctl stringfmts})
(define-init %loglevel %notice%)

(define english-verb-root (get (get-module 'morph/en) 'verb-root))
(define english-noun-root (get (get-module 'morph/en) 'noun-root))
(define basic-verb-roots (get (get-module 'morph/en) 'irregular-verbs))
(define basic-noun-roots (get (get-module 'morph/en) 'irregular-nouns))

(set! dictionary
      (if (file-exists? (get-component "english/dictionary.table"))
	  (file->dtype (get-component "english/dictionary.table"))
	  (make-hashtable)))
(set! fragments
      (if (file-exists? (get-component "english/fragments.hashset"))
	  (choice->hashset
	   (file->dtypes (get-component "english/fragments.hashset")))
	  (make-hashset)))

(define noun-roots {})
(define verb-roots {})

(define (check-noun-phrase word)
  (exists? (get (get dictionary word) '{noun-phrase noun})))
(define (noun-root word)
  (if (capitalized? word)
      (noun-root (difference (downcase word) word))
      (let ((root (english-noun-root word)))
	(if (position #\Space root)
	    (if (check-noun-phrase root) root (fail))
	    root))))

(define (check-verb-phrase word)
  (exists? (get (get dictionary word) 'verb)))
(define (verb-root word (lex #f))
  (if (capitalized? word)
      (verb-root (difference (downcase word) word) lex)
      (let ((root (english-verb-root word lex)))
	(if (position #\Space root)
	    (if (check-verb-phrase root) root (fail))
	    root))))

; (define basic-noun-roots
;   (file->dtype (get-component "english/noun-roots.table")))
; (define basic-verb-roots
;   (file->dtype (get-component "english/verb-roots.table")))
; (define basic-adjective-roots
;   (file->dtype (get-component "english/adj-roots.table")))
; (define basic-adverb-roots
;   (file->dtype (get-component "english/adv-roots.table")))

; (define (glue-words wordlist)
;   (stringout (doseq (word wordlist i) (printout (if (> i 0) " ") word))))
; (define plural-rules
;   '(#({"a" "e" "i" "o" "u"} (subst "ies" "y")) 
;     ("xes" . "x") ("zes" . "z") ("ses" . "s")
;     ("shes" . "sh") ("ches" . "ch") ("s" . "")))
; (define (noun-root x)
;   (try (get basic-noun-roots x)
;        (tryif (position #\Space x)
; 	      (let ((root (glue-words (compound-noun-root (segment x " ")))))
; 		(tryif (check-noun-phrase root) root)))
;        (morphrule x plural-rules (cons dictionary 'noun))))
; (define (compound-noun-root segmented)
;   (if (null? (cdr segmented))
;       (list (noun-root (car segmented)))
;       (cons (car segmented) (compound-noun-root (cdr segmented)))))

; (define gerund-rules
;   '(("bbing" . "b")
;     ("dding" . "d")
;     ("gging" . "g")
;     ("nning" . "n")
;     ("pping" . "p")
;     ("rring" . "r")
;     ("tting" . "t")
;     ("ing" . "e")
;     ("ing" . "")))
; (define past-rules
;   '(#({"a" "e" "i" "o" "u"} (subst "ied" "y"))
;     ("ed" . "ee")
;     ("ed" . "e")
;     ("ed" . "")))
; (define s-rules
;   '(#({"a" "e" "i" "o" "u"} (subst "ies" "y"))
;     ("xes" . "x") ("zes" . "z") ("ses" . "s") 
;     ("shes" . "sh") ("ches" . "ch") ("s" . "")))

; (define (compound-verb-root x lex)
;   (let ((pos (position #\Space x)))
;     (append (verb-root (subseq x 0 pos) lex)
; 	    (subseq x pos))))

; (define (verb-root x (lex #f))
;   (if (capitalized? x) (verb-root (downcase x) #f)
;       (try (get basic-verb-roots x)
; 	   (tryif (position #\Space x)
; 		  (let ((root (compound-verb-root x lex)))
; 		    (if (has-part-of-speech root 'verb) root (fail))))
; 	   (morphrule x gerund-rules (or lex (cons dictionary 'verb)))
; 	   (morphrule x past-rules (cons dictionary 'verb))
; 	   (morphrule x s-rules (or lex (cons dictionary 'verb))))))

(define adj-comp-rules
  '(("bber" . "b")
    ("dder" . "d")
    ("gger" . "g")
    ("nner" . "n")
    ("pper" . "p")
    ("rrer" . "r")
    ("tter" . "t")
    ("er" . "e")
    ("er" . "")))
(define adj-super-rules
  '(("bbest" . "b")
    ("ddest" . "d")
    ("ggest" . "g")
    ("nnest" . "n")
    ("ppest" . "p")
    ("rrest" . "r")
    ("ttest" . "t")
    ("est" . "e")
    ("est" . "")))

(define (adjective-root x)
  (try (get basic-adjective-roots x)
       (morphrule x adj-comp-rules (cons dictionary 'adjective))
       (morphrule x adj-super-rules (cons dictionary 'adjective))))

(define (adverb-root x (lex #f))
  (get basic-adjective-roots x))

(define (word-root x)
  (choice (noun-root x) (verb-root x)))

(define (weight-plus weight delta)
  (if weight
      (+ weight delta)
      weight))


;;;; Arc type definitions

(define *closed-tags*
  '{preposition
    glue conjunction punctuation sentence-end
    subject-pronoun object-pronoun pronoun
    definite-article indefinite-article quantifier
    demonstrative-pronoun
    singular-determiner plural-determiner determiner})

(define (NUMBER x)
  (or (has-part-of-speech x 'number)
      (and (> (length x) 0) (char-numeric? (elt x 0)))
      (and (> (length x) 1) (char-numeric? (elt x 1)))))
(define (COUNT-ADJECTIVE x)
  (or (has-part-of-speech x 'count-adjective)
      (and (> (length x) 0) (char-numeric? (elt x 0)))
      (and (> (length x) 1) (char-numeric? (elt x 1)))))
(define (QUANTIFIER x)
  (or (has-part-of-speech x 'count-adjective)
      (has-part-of-speech x 'quantifier)
      (and (> (length x) 0) (char-numeric? (elt x 0)))
      (and (> (length x) 1) (char-numeric? (elt x 1)))))
(define (ANYTHING x) 20)
(define (PUNCTUATION x)
  (or (has-part-of-speech x 'punctuation)
      (and (search x "\"'`") 1)
      (and (not (char-alphanumeric? (elt x 0)))
	   (not (char-alphanumeric? (elts x))) 0)))
(define (QUOTE-MARK x)
  (or (and (search x "\"''``") 0)
      (and (has-part-of-speech x 'punctuation)
	   (or (position #\' x) (position #\" x) (position #\` x)))))
(define (SENTENCE-END x)
  (and (overlaps? x '{"|" "." "!" ";" "?" ":"}) 0))
(define (PREPOSITION x)
  (has-part-of-speech x 'preposition))
(define (TIME-PREPOSITION x)
  (has-part-of-speech x 'time-prep))
(define (DANGLING-PREPOSITION x)
  (and (has-part-of-speech x 'preposition) 5))
(define-word-arc-type (DETERMINER x)
  (has-part-of-speech x 'determiner)
  (has-part-of-speech x 'definite-article)
  (has-part-of-speech x 'indefinite-article))
(define-word-arc-type (SINGULAR-DETERMINER x)
  (has-part-of-speech x 'singular-determiner)
  (has-part-of-speech x 'quantifier 2))
(define-word-arc-type (PLURAL-DETERMINER x)
  (has-part-of-speech x 'plural-determiner)
  (has-part-of-speech x 'quantifier))

(define-word-arc-type (POSSESSIVE x)
  (has-part-of-speech x 'possessive)
  (and (capitalized? x)
       (has-suffix x "'s") 2)
  (and (> (length x) 2)
       (or (has-suffix x "'s")  (has-suffix x "s'")
	   (has-suffix x "\u2019s")  (has-suffix x "s\u2019"))
       (or (plural-noun (subseq x 0 -2))
	   (noun (subseq x 0 -2)))
       0))
(define-word-arc-type (PROPER-POSSESSIVE x)
  (has-part-of-speech x 'proper-possessive)
  (and (capitalized? x)
       (or (has-suffix x "'s")  (has-suffix x "s'")
	   (has-suffix x "\u2019s")  (has-suffix x "s\u2019"))
       0))
(define-word-arc-type (POSSESSIVE-PRONOUN x)
  (has-part-of-speech x 'possessive-pronoun))

(define-word-arc-type (TIME-WORD x)
  (has-part-of-speech x 'time-word))

(define-word-arc-type (GLUE x)
  (has-part-of-speech x 'glue)
  (has-part-of-speech x 'complementizer))

(define-word-arc-type (CONJUNCTION x)
  (has-part-of-speech x 'conjunction))

(define (compound-first-word x)
  (let ((pos (position #\Space x)))
    (if pos (subseq x 0 pos) (fail))))

(define-word-arc-type (ADJECTIVE x)
  (and (has-suffix x "ing")
       (has-part-of-speech x 'adjective 1))
  (has-part-of-speech x 'adjective)
  (and (quantifier x) 1)
  (and (not (known-word? x))
       (position #\- x)
       (if (plural-noun x) 4 2))
  (exists? (morphrule x '(("est" . "") ("er" . ""))
		      (cons dictionary 'adjective)))
  (and (not (or (known-word? x) (punctuation x)
		(plural-noun x))) 4))
(define-word-arc-type (COMPLEMENT-ADJECTIVE x)
  (has-part-of-speech x 'COMPLEMENT-ADJECTIVE)
  (if (position #\Space x)
      (let ((word (compound-first-word x)))
	(and (not (or (zero-arc 'VERB word)
		      (satisfied? (zero-arc 'VERB (verb-root word)))))
	     (has-part-of-speech word 'ADJECTIVE)
	     1))
      (and (not (or (zero-arc 'VERB x)
		    (satisfied? (zero-arc 'VERB (verb-root x)))))
	   (weight-plus (has-part-of-speech x 'ADJECTIVE) 1)))
  (and (not (known-word? x)) (position #\- x) 2))
(define-word-arc-type (DANGLING-ADJECTIVE x)
  (weight-plus (has-part-of-speech x 'ADJECTIVE) 1))

(define-word-arc-type (DANGLING-ADVERB x)
  (and (capitalized? x)
       (or (has-part-of-speech (downcase x) 'DANGLING-ADVERB)
	   (has-part-of-speech (downcase x) 'ADVERB)))
  (has-part-of-speech x 'DANGLING-ADVERB)
  (has-part-of-speech x 'ADVERB)
  (and (not (known-word? x)) (has-suffix x "ly")))
(define-word-arc-type (ADVERB x)
  (has-part-of-speech x 'ADVERB)
  (and (not (known-word? x)) (has-suffix x "ly") 2))
(define-word-arc-type (TRAILING-ADVERB x)
    (has-part-of-speech x 'TRAILING-ADVERB)
    (has-part-of-speech x 'adverb 1)
    (and (not (known-word? x)) (has-suffix x "ly") 2))
;; Adverb modifying preposition
(define-word-arc-type (PADVERB x) 
  (has-part-of-speech x 'PADVERB)
  (has-part-of-speech x 'adverb 1))

(define-word-arc-type (MODAL-AUX x)
  (has-part-of-speech x 'modal-aux))
(define-word-arc-type (HAVE-AUX x)
  (has-part-of-speech x 'have-aux)
  (has-part-of-speech x 'have-contraction))
(define-word-arc-type (NEGATOR x)
  (has-part-of-speech x 'negator))
(define-word-arc-type (BE-AUX x)
  (has-part-of-speech x 'be-aux)
  (has-part-of-speech x 'be-contraction)
  (has-part-of-speech x 'be-verb))
(define-word-arc-type (AUX x)
  (has-part-of-speech x 'aux)
  (has-part-of-speech x 'negator)
  (has-part-of-speech x 'be-aux)
  (has-part-of-speech x 'have-aux)
  (has-part-of-speech x 'modal-aux)
  ;; We do a kludge that makes contractions into aux forms
  (has-part-of-speech x 'be-contraction)
  (has-part-of-speech x 'have-contraction)
  (has-part-of-speech x 'will-contraction)
  (has-part-of-speech x 'would-contraction))

(define-word-arc-type (PRONOUN x)
    (has-part-of-speech x 'PRONOUN)
  (has-part-of-speech x 'SUBJECT-PRONOUN)
  (has-part-of-speech x 'OBJECT-PRONOUN)
  (has-part-of-speech x 'RELATIVE-PRONOUN)
  (has-part-of-speech x 'REFLEXIVE-PRONOUN)
  (has-part-of-speech x 'DEMONSTRATIVE-PRONOUN)
  (and (has-part-of-speech x 'QUANTIFIER) 1))
(define-word-arc-type (KNOWN-NOUN x)
  (has-part-of-speech x 'NOUN)
  (has-part-of-speech x 'NOUN-PHRASE))
(define-word-arc-type (NOUN x)
  (and (fail? (noun-root x))
       (has-part-of-speech x 'NOUN))
  (has-part-of-speech x 'NOUN-PHRASE)
  (has-part-of-speech
   (morphrule x '(("ization" . "ize")) (cons dictionary 'verb))
   'verb)
  (and (ing-verb x) 1)
  (and (not (known-word? x))
       (not (punctuation x))
       (not (position #\Space x))
       (cond ((has-suffix x "ed") 8)
	     ((plural-noun x) 4)
	     (else 2))))
(define-word-arc-type (SOLITARY-NOUN x)
  (has-part-of-speech x 'solitary-noun)
;   (and (char-upper-case? (elt x 0))
;        (has-part-of-speech x 'noun))
  (has-part-of-speech
   (morphrule x '(("ization" . "ize")) (cons dictionary 'verb))
   'verb)
  (and (has-part-of-speech x 'noun)
       (satisfied? (has-suffix x {"ship" "dom" "ion" "hood" "ness"}))
       2)
  (and (has-part-of-speech x '#{noun noun-phrase})
       (noun x) (+ (noun x) 2))
  (and (ing-verb x) 1))
(define-word-arc-type (NOMINALIZATION x)
  (has-part-of-speech x 'nominalization))

(define (closed-class-word? x)
  (or (preposition x) (conjunction x) (determiner x) (pronoun x)
      (glue x)))

(define-word-arc-type (PLURAL-NOUN x)
  (has-part-of-speech x 'PLURAL-NOUN)
  (and (has-part-of-speech x 'QUANTIFIER) 3)
  (and (closed-class-word? x) 10)
  (and (not (known-word? x))
       (exists? (noun-root x))
       (exists? (verb-root x))
       (known-noun (noun-root x))
       (1+ (pick (known-noun (noun-root x)) number?)))
  (and (not (known-word? x))
       (exists? (noun-root x))
       (known-noun (noun-root x)))
  (and (exists? (noun-root x)) (known-noun (noun-root x)))
  (10 (has-suffix x "'s"))
  (and (has-suffix x "ations") 0)
  (and (has-suffix x "s")
       (has-part-of-speech
	(if (has-suffix x "es")
	    (subseq x 0 -2)
	    (subseq x 0 -1))
	'noun))
  (and (has-suffix x "s")
       (not (known-word? x))
       (not (exists? (verb-root x)))
       (if (capitalized? x) 3 2)))

(define abbrevs-init
  "Corp.Calif.Mass.Ariz.Wash.Mich.Kans.Colo.Neva.Penn.Okla.Sept.Gov.Sen.Rep.Dr.Lt.Col.Gen.Mr.Mrs.Miss.Ms.Co.Inc.Ltd.Jan.Feb.Mar.Apr.Jun.Jul.Aug.Sep.Sept.Oct.Nov.Dec.Rev.Fr.Sis.")
(define *abbreviations* (downcase (elts (segment abbrevs-init "."))))
(define-word-arc-type (ABBREV x)
  (overlaps? (downcase x) *abbreviations*)
  (and (string? x) (capitalized? x)
       (< (length x) 5) 3))

(define (has-apostrophe x)
  (if (string? x)
      (position #\' x)
      (some? (lambda (s) (position #\' s)) x)))
(define-word-arc-type (PROPER-NAME x)
  (has-part-of-speech x 'proper-name)
  (and (string? x)
       (char-upper-case? (elt x 0))
       (has-part-of-speech x 'time-word)
       1)
  (and (string? x)
       (char-upper-case? (elt x 0))
       (not (has-apostrophe x))
       (let* ((lowered (downcase x))
	      (tags (car (get dictionary lowered))))
	 (if (and (fail? tags)
		  (fail? (noun-root (downcase x)))
		  (fail? (verb-root (downcase x))))
	     (if (has-suffix x ".")
		 (if (< (position #\. x) (1- (length x))) 0 1)
		 0)
	     (if (closed-class-word? lowered) 4
		 (if (or (verb lowered) (adverb lowered)) 1 0))))))
(define-word-arc-type (PLURAL-NAME x)
  (and (has-suffix x "s") (proper-name x)))
(define-word-arc-type (PROPER-MODIFIER x)
  (has-part-of-speech x 'proper-modifier)
  (has-part-of-speech x 'proper-name)
  (and (string? x)
       (char-upper-case? (elt x 0))
       (not (has-apostrophe x))
       (let* ((lowered (downcase x))
	      (tags (car (get dictionary lowered))))
	 (if (fail? tags)
	     (if (has-suffix x ".")
		 (if (< (position #\. x) (1- (length x))) 0 1)
		 0)
	     (if (closed-class-word? lowered) 3
		 (if (verb lowered) 1 0))))))
(define-word-arc-type (NAME-SUFFIX x)
  (or (and (capitalized? x) (has-suffix x ".") 0)
      (overlaps? (downcase x) {"inc" "co" "corp" "phd" "msw"})))
(define-word-arc-type (NOUN-MODIFIER x)
  (has-part-of-speech x 'noun-modifier)
  (and (capitalized? x) 2)
  (and (quantifier x) 4)
  (and (ing-verb x)
       (has-part-of-speech x 'noun)
       (has-part-of-speech x 'adjective)
       1)
  (and (has-part-of-speech x 'adjective)
       (has-part-of-speech x 'noun))
  (and (has-part-of-speech x 'verb)
       (has-part-of-speech x 'noun 1))
  (has-part-of-speech x 'noun-phrase)
  (and (closed-class-word? x) 10)
  (and (exists? (noun-root x)) 4)
  (has-part-of-speech x 'noun 1)
  (and (ing-verb x) 2)
  (and (not (known-word? x))
       (not (punctuation x))
       (not (possessive x))
       (cond ((has-suffix x "ed") 8)
	     (else 2))))

(define-word-arc-type (INFINITIVAL-VERB x)
  (has-part-of-speech x 'infinitival-verb)
  (has-part-of-speech x 'verb))
(define-word-arc-type (VERB x)
  (and (be-verb x) 10)
  (and (exists? (verb-root x))
       (has-part-of-speech (verb-root x) 'verb))
  (has-part-of-speech x 'verb)
  (and (not (known-word? x))
       (exists? (verb-root x #t))
       4))
(define-word-arc-type (INFLECTED-VERB x)
  (and (exists? (verb-root x))
       (verb x)))
(define-word-arc-type (BE-VERB x)
  (has-part-of-speech x 'be-verb)
  (has-part-of-speech x 'be-contraction))
(define complement-auxes
  {"become" "be" "made" "go" "get" "start" "stop" "see" "set" "try" "begin" "quit" "plan"
   "look" "seem"
   "ain't" "am" "are" "aren't" "be" "been" "is" "isn't" "was" "wasn't" "were" "weren't"})
(define-word-arc-type (COMPLEMENT-AUX x)
  (overlaps? (verb-root x) complement-auxes)
  (and (overlaps? x complement-auxes) 1)
  (has-part-of-speech x 'be-contraction))
(define-word-arc-type (ING-VERB x)
  (and (word-suffix? "ing" x)
       (if (exists? (verb-root x)) 0 4))
  (and (search "ing " x)
       (if (exists? (verb-root x)) 0 4)))
(define-word-arc-type (VERB-AS-ADJECTIVE x)
  (and (or (has-suffix x "ed") (has-suffix x "ing"))
       (exists? (verb-root x)) 0))

(define ing-taking-verbs
  {"go" "rue" "see" "try" "bear" "busy" "deny" "find" "hate" "hear" "help" "keep"
   "like" "live" "love" "mean" "mind" "miss" "plan" "push" "quit" "risk" "send"
   "stop" "abide" "admit" "adore" "avoid" "begin" "catch" "cease" "delay"
  "dread" "enjoy" "evade" "fancy" "merit" "scorn" "shirk" "smell" "spend"
  "start" "advise" "chance" "choose" "defend" "deride" "detest" "employ" "endure"
  "excuse" "exempt" "finish" "forbid" "forget" "lament" "loathe" "notice" "permit"
  "prefer" "punish" "rebuke" "recall" "reduce" "relish" "report" "resent" "resist"
  "resume" "reward" "attempt" "conceal" "condone" "conduce" "confess" "deserve" "dislike"
  "forbear" "forgive" "imagine" "include" "inhibit" "involve" "justify" "neglect" "observe"
  "preface" "prevent" "propose" "provoke" "reprove" "require" "suggest" "suspect" "warrant"
  "advocate" "approach" "begrudge" "commence" "consider" "continue" "envisage" "foretell"
  "forswear" "overhear" "perceive" "postpone" "practice" "preclude" "prohibit" "remember"
  "galvanize" "introduce" "legislate" "recollect" "recommend" "represent" "visualize"
  "anticipate" "recommence" "acknowledge" "contemplate" "discontinue" "reintroduce"})
(define-word-arc-type (take-ing x)
  (or (overlaps? x ing-taking-verbs)
      (overlaps? (verb-root x) ing-taking-verbs)))



;;;;.State machines.

(define-state-machine (*start-state* terminal chunk)
  ((epsilon *rest-state*)
   (dangling-adverb *rest-state*)))

(define-state-machine (*rest-state* terminal chunk)
  ((conjunction *conj-state* 1)
   (punctuation *rest-state* 1)
   (sentence-end *rest-state*)
   ("," *rest-state*)
   (time-word *rest-state*)
   (sentence-end *rest-state*)
   (glue *rest-state*)
   (adverb *glue* 1)
   (adverb *timeref*)
   ;; Sometimes these dangle
   (dangling-adverb *rest-state* 1)
   ;; Sometimes these dangle
   (dangling-preposition *rest-state*)
   (anything *rest-state*)
   (epsilon *object*)
   (epsilon *determined-object* 2)
   (epsilon *nominalization*)
   (epsilon *predicate*)
   (epsilon *attribute*)))

(define-state-machine (*conj-state*)
  ((conjunction *rest-state* 4)
   ;; Sometimes these dangle
   (dangling-adverb *rest-state* 2)
   ;; Sometimes these dangle
   (dangling-preposition *rest-state*)
   (anything *rest-state*)
   (epsilon *object*)
   (epsilon *determined-object* 2)
   (epsilon *nominalization*)
   (epsilon *predicate*)
   (epsilon *attribute*)))

(define-state-machine (*timeref*)
  ((adverb *timeref*)
   (time-word *rest-state*)
   (time-word conj-timeref)
   (quantifier *timeref*))
  (conj-timeref
   ("and" *timeref*)
   ("or" *timeref*)
   ("to" *timeref*)
   (time-word *rest-state*)))

(define-state-machine (*glue* chunk)
  ((glue *rest-state* 2)))

(define-state-machine (*object* chunk)
  ((singular-determiner *singular-object*)
   (singular-determiner *determined-name*)
   (plural-determiner *plural-object*)
   (quantifier *determined-object*)
   (pronoun *after-object*)
   (pronoun conjoined-pronoun)
   (number *rest-state*)
   (possessive *determined-object* 1)
   (possessive-pronoun *determined-object*)
   (noun-modifier head)
   (proper-possessive *determined-object*)
   (proper-possessive *determined-name*)
   (proper-modifier *determined-name*)
   (proper-modifier *determined-object* 1)
   (proper-modifier head)
   (proper-modifier proper-modifiers)
   (proper-modifier embedded-proper-possessive)
   (plural-noun *after-object*)
   (solitary-noun *after-object*)
   (proper-name *after-object*)
   (adjective modified-phrase)
   (adjective head)
   (adverb waiting-for-adjective)
   (noun *after-object* 5)
   ;; (noun *proper-name* 2)
   )
  (conjoined-pronoun
   ("and" head)
   ("or" head))
  (modified-phrase
   (adverb waiting-for-adjective)
   (conjunction waiting-for-adjective)
   ("," waiting-for-adjective)
   (adjective modified-phrase)
   (epsilon head)
   (epsilon proper-modifiers)
   (noun-modifier head))
  (waiting-for-adjective
   (adverb waiting-for-adjective)
   (adjective modified-phrase)
   ;; (verb-as-adjective head)
   (adjective head))
  (proper-modifiers
   (proper-modifier head)
   (proper-modifier proper-modifiers)
   (proper-modifier embedded-proper-possessive))
  (embedded-possessive
   (noun-modifier embedded-possessive)
   (possessive head))
  (embedded-proper-possessive
   (proper-modifier embedded-possessive)
   (possessive head)
   (proper-possessive *determined-object*))
  (noun-sequence 
   ("," head)
   ("and" head)
   ("or" head)
   ("&" head))
  (head
   (noun-modifier head 1)
   (noun *after-object* 3)
   (plural-noun *after-object*)
   (solitary-noun *after-object*)
   (plural-noun noun-sequence)
   (solitary-noun noun-sequence)))

(define-state-machine (*determined-object* chunk)
  ((epsilon *singular-object*)
   (epsilon *plural-object*)))

(define-state-machine (*plural-object* chunk)
  ((noun-modifier head)
   (possessive *plural-object* 2)
   (adverb waiting-for-adjective)
   (quantifier modified-phrase)
   (adjective modified-phrase)
   (quantifier head)
   (adjective head)
   (quantifier more-modifiers)
   (noun-modifier head)
   (noun-modifier more-modifiers)
   (noun-modifier embedded-possessive)
   (proper-modifier proper-modifiers)
   (proper-modifier head)
   (proper-modifier embedded-proper-possessive)
   (epsilon head))
  (modified-phrase
   (adverb waiting-for-adjective)
   (conjunction waiting-for-adjective)
   ("," waiting-for-adjective)
   (adjective modified-phrase)
   (epsilon proper-modifiers)
   (noun-modifier head)
   (epsilon head))
  (waiting-for-adjective
   (adverb waiting-for-adjective)
   (adjective modified-phrase)
   ;; (verb-as-adjective head)
   (adjective head))
  (proper-modifiers
   (proper-modifier head)
   (proper-modifier proper-modifiers)
   (proper-modifier embedded-proper-possessive))
  (embedded-possessive
   (noun-modifier embedded-possessive)
   (possessive head))
  (embedded-proper-possessive
   (proper-modifier embedded-possessive)
   (possessive head)
   (proper-possessive *determined-object*))
  (noun-sequence 
   ("," head)
   ("and" head)
   ("or" head)
   ("&" head))
  (head
   (noun-modifier head 1)
   (plural-noun *after-object*)
   (plural-noun noun-sequence)))

(define-state-machine (*singular-object* chunk)
  ((noun-modifier head)
   (adverb waiting-for-adjective)
   (quantifier modified-phrase)
   (adjective modified-phrase)
   (quantifier head)
   (adjective head)
   (possessive *singular-object*)
   (quantifier more-modifiers)
   (noun-modifier head)
   (noun-modifier more-modifiers)
   (noun-modifier embedded-possessive)
   (proper-modifier head)
   (proper-modifier proper-modifiers)
   (proper-modifier embedded-proper-possessive)
   (proper-name *after-object*)
   (solitary-noun *after-object*)
   (epsilon head))
  (modified-phrase
   (adverb waiting-for-adjective)
   (conjunction waiting-for-adjective)
   ("," waiting-for-adjective)
   (adjective modified-phrase)
   (epsilon proper-modifiers)
   (noun-modifier head)
   (epsilon head))
  (waiting-for-adjective
   (adverb waiting-for-adjective)
   (adjective modified-phrase)
   ;; (verb-as-adjective head)
   (adjective head))
  (proper-modifiers
   (proper-modifier head)
   (proper-modifier proper-modifiers)
   (proper-modifier embedded-proper-possessive))
  (embedded-possessive
   (noun-modifier embedded-possessive)
   (possessive head))
  (embedded-proper-possessive
   (proper-modifier embedded-possessive)
   (possessive head)
   (proper-possessive *determined-object*))
  (noun-sequence 
   ("," head)
   ("and" head)
   ("or" head)
   ("&" head))
  (head
   (noun-modifier head 1)
   (noun *after-object*)
   (noun noun-sequence)))

(define-state-machine (*determined-name* chunk)
  ((proper-modifier head)
   (quote-mark embedded-quote)
   (proper-modifier embedded-proper-possessive)
   (proper-name *after-object*)
   (proper-name head-suffix))
  (embedded-quote
   (proper-modifier embedded-quote)
   (quote-mark head))
  (embedded-proper-possessive
   (proper-modifier embedded-possessive)
   (proper-possessive *determined-object*))
  (head
   (proper-name head-suffix)
   (quote-mark embedded-quote)
   (proper-modifier head)
   (proper-name *after-object*))
  (head-suffix
   ("," take-name-suffix))
  (take-name-suffix
   (name-suffix *after-object*)
   (name-suffix final-comma)
   (proper-modifier final-dotcomma 0)
   (number *after-object*)
   (number final-comma))
  (final-comma
   ("," *after-object*))
  (final-dotcomma
   (".," *after-object*)))

(define-state-machine (*attribute*)
  ((adverb wait-for-preposition 2)
   (padverb wait-for-preposition)
   (time-preposition *timeref*)
   (preposition conjoined-preposition)
   (preposition *object*)
   (preposition *determined-object* 2))
  (wait-for-preposition
   (conjunction another-adverb)
   (preposition *object*)
   (preposition *determined-object*)
   ;; preposition cues noun	
   (preposition (*determined-object* object) 4))
  (another-adverb (adverb wait-for-preposition))
  (conjoined-preposition
   (conjunction *attribute*)))

(define-state-machine (*after-object* terminal)
  ((epsilon *rest-state* 3)
   (sentence-end *rest-state*)
   (dangling-adjective *rest-state* 1)
   (dangling-adverb *rest-state* 1)
   (pronoun *rest-state* 1)
   ("," *rest-state*)
   (quantifier *object* 1)
   (determiner *object* 1)
   (punctuation *after-object*)
   (conjunction *rest-state*)
   (punctuation *rest-state*)
   ("&" *rest-state*)
   ("and" *rest-state*)
   ("or" *rest-state*)
   (glue *rest-state*)
   (adverb *timeref*)
   (time-word *rest-state*)
   (epsilon *nominalization*)
   (epsilon *attribute*)
   (epsilon *predicate*)
   (epsilon *glue*)))

(define-state-machine (*predicate*)
  ((epsilon head)
   (epsilon comp-adj 3)
   (modal-aux post-modal)
   (have-aux post-have)
   (negator post-not)
   (aux *predicate* 1)
   (adverb *predicate* 1)
   (ing-verb post-head)
   (inflected-verb post-head)
   (inflected-verb gerund)
   (inflected-verb *nominalization*)
   (ing-verb comp-adj 1)
   (be-aux comp-adj)
   (be-aux negated-comp-adj)
   (be-verb post-be)
   (glue head))
  (head
   (complement-aux comp-adj)
   (be-aux passive)
   (adverb head)
   (inflected-verb post-head)
   (verb post-head 1))
  (passive (inflected-verb post-head) (adverb passive))
  (post-modal
   (epsilon head)
   (epsilon post-have)
   (have-aux post-have))
  (post-have
   (epsilon head)
   (negator post-not))
  (post-not
   (epsilon head)
   (epsilon gerund)
   (epsilon comp-adj))
  (gerund
   (adverb gerund)
   (ing-verb post-head))
  (conj-gerund
   (conjunction gerund))
  (negated-comp-adj
   (negator comp-adj))
  (comp-adj
   (adverb comp-adj)
   (inflected-verb post-head)
   (inflected-verb conj-comp-adj)
   (complement-adjective post-head)
   (complement-adjective conj-comp-adj))
  (conj-comp-adj
   (conjunction comp-adj))
  (post-head
   (epsilon *after-predicate*)
   (trailing-adverb post-head)
   (time-word post-head))
  (post-be
   (negator post-be)
   (trailing-adverb post-be)
   (time-word post-be 2)
   (epsilon *object*)
   (epsilon *rest-state* 3)
   (epsilon *nominalization*)))

(define-state-machine (*comp-predicate*)
  ((epsilon head)
   (modal-aux post-modal)
   (have-aux post-have)
   (negator post-not)
   (aux *comp-predicate* 1)
   (adverb *comp-predicate* 1)
   (ing-verb post-head)
   (verb post-head)
   (verb gerund)
   (verb *nominalization*)
   (ing-verb comp-adj 1)
   (be-aux comp-adj)
   (be-aux negated-comp-adj)
   (be-verb post-be))
  (head
   (complement-aux comp-adj)
   (be-aux passive)
   (adverb head)
   (verb post-head))
  (passive (inflected-verb post-head) (adverb passive))
  (post-modal
   (epsilon head)
   (epsilon post-have)
   (have-aux post-have))
  (post-have
   (epsilon head)
   (negator post-not))
  (post-not
   (epsilon head)
   (epsilon gerund)
   (epsilon comp-adj))
  (gerund
   (adverb gerund)
   (ing-verb post-head))
  (conj-gerund
   (conjunction gerund))
  (negated-comp-adj
   (negator comp-adj))
  (comp-adj
   (adverb comp-adj)
   (inflected-verb post-head)
   (inflected-verb conj-comp-adj)
   (complement-adjective post-head)
   (complement-adjective conj-comp-adj))
  (conj-comp-adj
   (conjunction comp-adj))
  (post-head
   (epsilon *after-predicate*)
   (trailing-adverb post-head)
   (time-word post-head))
  (post-be
   (negator post-be)
   (trailing-adverb post-be)
   (time-word post-be 2)
   (epsilon *object*)
   (epsilon *rest-state* 3)
   (epsilon *nominalization*)))

;; This state is for verbs which are acting as nouns in infinitives
;; or other constructions
(define-state-machine (*nominalization*)
  ((adverb wait-for-preposition)
   ("to" embedded-gerund)
   ("to" infinitive)
   ("to" *predicate*)
   (preposition gerund 1)
   (ing-verb post-head 1))
  (embedded-gerund
   (complement-aux gerund))
  (wait-for-preposition
   (adverb wait-for-preposition)
   (preposition gerund 1))
  (infinitive
   (adverb infinitive)
   (infinitival-verb post-head))
  (gerund
   (adverb gerund)
   (ing-verb post-head))
  (post-head
   (trailing-adverb post-head)
   (epsilon *after-predicate*)))

(define-state-machine (*after-predicate* terminal)
  ((epsilon *rest-state* 3)
   (sentence-end *rest-state*)
   ("," *object*)
   ("," *attribute*)
   ("," *rest-state* 1)
   ;; (punctuation *rest-state*)
   (punctuation *after-predicate*)
   ("&" *rest-state*)
   ("and" *rest-state*)
   ("or" *rest-state*)
   (glue *rest-state*)
   (time-word *rest-state*)
   (epsilon *object*)
   (epsilon *nominalization*)
   (epsilon *attribute*)
   (epsilon *glue*)))


;;;; Chunker categories

(define *names* '{PROPER-NAME PROPER-MODIFIER PROPER-POSSESSIVE})
(define *things*
  (union *names* '{PRONOUN SOLITARY-NOUN NOUN PLURAL-NOUN}))
(define *predicates*
  '{INFINITIVAL-VERB VERB ING-VERB INFLECTED-VERB
    BE-VERB COMPLEMENT-ADJECTIVE})
(define *danglers*
  '{ANYTHING DANGLING-TIME-WORD DANGLING-ADVERB})
(define *heads* (union *predicates* *things*))

(define *nouns*
  '{SOLITARY-NOUN NOUN PLURAL-NOUN})
(define *verbs*
  '{INFINITIVAL-VERB INFLECTED-VERB BE-AUX
    VERB ING-VERB PASSIVE-VERB
    BE-VERB VERB-AS-ADJECTIVE})
(define *adjectives*
  '{COMPLEMENT-ADJECTIVE QUANTIFIER COUNT-ADJECTIVE ADJECTIVE})
(define *adverbs*
  '{ADVERB PADVERB DANGLING-ADVERB})
(define *glob-heads*
  '{
    SOLITARY-NOUN NOUN PLURAL-NOUN POSSESSIVE
    PROPER-NAME PROPER-POSSESSIVE PROPER-MODIFIER})
(define *glob-mods* '{NOUN-MODIFIER PROPER-MODIFIER QUOTE-MARK})

(define *preps* '{GLUE PREPOSITION "to"})
(define *prefixes*
  (union *preps* 
	 '{AUX BE-VERB BE-AUX DETERMINER QUANTIFIER "or" "and" ","
	       PADVERB ADVERB POSSESSIVE NUMBER
	       VERB-AS-ADJECTIVE NOUN-MODIFIER GLUE
	       PROPER-MODIFIER ADJECTIVE CONJUNCTION DANGLING-ADVERB}))
(define *suffixes*
  '{TIME-WORD SENTENCE-END "or" "and" "," GLUE
	      ADVERB DANGLING-ADVERB CONJUNCTION})
(define *modifiers* (union *prefixes* *suffixes* *danglers*))


;;; Parser

(define (add-root entry)
  (if (overlaps? (second entry) *names*)
      (append entry (list (first entry)))
      (let ((lower (downcase (first entry)))
	    (tag (second entry)))
	(cond ((overlaps? tag *nouns*)
	       (append entry (list (try (noun-root lower) lower))))
	      ((overlaps? (second entry) *verbs*)
	       (append entry (list (qc (try (verb-root lower) lower)))))
	      ((overlaps? (second entry) *adjectives*)
	       (append entry (list (qc (try (adjective-root lower) lower)))))
	      ((overlaps? (second entry) *adverbs*)
	       (append entry (list (qc (try (adverb-root lower) lower)))))
	      (else (append entry (list lower)))))))

(define (tag-english string)
  (map add-root (tagit string)))
(define (phrase-english string)
  (extract-phrases (tag-english string)))


;;;; Writing tables for C parser

(define inverted-noun-roots
  (let ((table (make-hashtable)))
    (do-choices (inflected (getkeys basic-noun-roots))
      (add! table (get basic-noun-roots inflected) inflected))
    table))

(define (valid-plural word)
  (if (exists? (noun-root word)) word (fail)))

(define (expand-noun word)
  (cond ((exists? (noun-root word)) word)
	((exists? (get inverted-noun-roots word))
	 (get inverted-noun-roots word))
	((position #\Space word)
	 (let* ((words (words->vector word))
		(last (1- (length words))))
	   (tryif (>= last 0)
		  (stringout
		    (doseq (word words i)
		      (if (= i last)
			  (printout (pick-one (expand-noun word)))
			  (printout word " ")))))))
	((and (position #\- word) (> (length word) 4))
	 (let* ((words (segment word "-"))
		(lim (1- (length words))))
	   (stringout
	       (doseq (word words i)
		 (if (= i lim)
		     (printout (pick-one (expand-noun word)))
		     (printout word "-"))))))
	(else
	 (valid-plural
	  (morphrule word '(#((not {"a" "e" "i" "o" "u"})
			      (SUBST "y" "ies"))
			    ("s" . "ses")
			    ("x" . "xes")
			    ("zz" . "zzes")
			    ("" . "s"))
		     #t)))))

(define inverted-verb-roots
  (let ((table (make-hashtable)))
    (do-choices (inflected (getkeys basic-verb-roots))
      (add! table (get basic-verb-roots inflected) inflected))
    table))

(define (valid-inflection word)
  (if (exists? (verb-root word)) word (fail)))

(define (inflect-verb word)
  (cond ((position #\Space word)
	 (let ((pt (position #\Space word)))
	   (append (inflect-verb (subseq word 0 pt))(subseq word pt))))
	((exists? (verb-root word)) (fail))
	((exists? (get inverted-verb-roots word))
	 (get inverted-verb-roots word))
	(else
	 (valid-inflection
	  (morphrule word
		     '{(#({"a" "e" "i" "o" "u"}
			  (SUBST "y" "ies"))
			("s" . "ses")
			("x" . "xes")
			("zz" . "zzes")
			("" . "s"))
		       (#({"a" "e" "i" "o" "u"}
			  (SUBST "y" "ied"))
			("ee" . "ed")
			("e" . "ed")
			("" . "ed"))
		       (("e" . "ing")
			#((+ (isnotvowel)) (+ (isvowel))
			  {(SUBST "b" "bbing")
			   (SUBST "d" "dding")
			   (SUBST "g" "gging")
			   (SUBST "n" "nning")
			   (SUBST "p" "pping")
			   (SUBST "r" "rring")
			   (SUBST "t" "tting")})
			("" . "ing"))}
		     #t)))))

(define (expand-verb verb)
  (choice (inflect-verb verb)
	  (morphrule verb '(("ize" . "ization")) #t)
	  (morphrule verb '(("ize" . "izations")) #t)))

(define (expand-adjective adj)
  (append adj {"er" "est"}))

(define (write-lexicon-entries root lex-table noun-roots verb-roots)
  ;; (lineout "Writing lexicon entry for " root)
  (write-lexicon-entry root lex-table)
  (when (or (exists? (get (get dictionary root) 'noun))
	    (exists? (get (get dictionary root) 'noun-phrase)))
    (let ((expansion (expand-noun root)))
      (add! noun-roots expansion root)
      (write-lexicon-entry expansion lex-table)))
  (when (exists? (get (get dictionary root) 'verb))
    (let ((expansion (expand-verb root)))
      (add! verb-roots expansion root)
      (write-lexicon-entry expansion lex-table)))
  (when (exists? (get (get dictionary root) 'adjective))
    (let ((expansion (expand-adjective root)))
      (write-lexicon-entry expansion lex-table))))

(define (capword s)
  (append (upcase (subseq s 0 1)) (subseq s 1)))

(define (generate-lexicon lex noun-roots verb-roots)
  (lognotice |WriteLexicon| "Generating lexicon for fast tagger")
  (let* ((allkeys (getkeys dictionary))
	 (root-forms (pick allkeys string?))
	 (n-root-forms (choice-size root-forms)) 
	 (closed-terms (pick root-forms closed-class-word?))
	 (cap-closed-terms (capword closed-terms))
	 (lex-table (open-index lex))
	 (i 0))
    (lognotice |WriteLexicon|
      "Writing case variants for " (choice-size closed-terms)
      " closed class words")
    (prefetch-keys! lex-table (choice closed-terms cap-closed-terms))
    (do-choices (closed-term (capword closed-terms))
      (unless (exists? (get lex-table closed-term))
	(write-lexicon-entry closed-term lex)))
    (commit lex) (swapout lex)
    (lognotice |WriteLexicon| 
      "Writing base and variants for " (choice-size root-forms) " roots")
    (do-choices (root root-forms)
      (unless (fail? (get dictionary root))
	(set! i (+ i 1))
	(when (zero? (random 1000))
	  (lognotice |WriteLexicon|
	    "---- " i "/" n-root-forms " " (show% i n-root-forms)
	    " ---- " root " (root)"))
	(write-lexicon-entries root lex-table noun-roots verb-roots)
	(when (zero? (remainder i 10000)) (commit) (swapout))))))

(define (write-special-entries index)
  (add! index "," (lexicon-entry ","))
  (add! index "\"" (lexicon-entry "\""))
  (add! index "'" (lexicon-entry "'"))
  (add! index "''" (lexicon-entry "''"))
  (add! index "`" (lexicon-entry "`"))
  (add! index "``" (lexicon-entry "``"))
  (add! index '%punctuation (lexicon-entry "("))
  (add! index '%proper-name (lexicon-entry "Cthulu"))
  (add! index '%xproper-name 
	(map (lambda (x) (if (eq? x 0) 2 x)) (lexicon-entry "Cthulu")))
  (add! index '%time-word (lexicon-entry "Monday"))
  (add! index '%proper-possessive (lexicon-entry "Cthulu's"))
  (let ((pos-weights (deep-copy (lexicon-entry "Cthulu's"))))
    (vector-set! pos-weights (+ 2 (position 'possessive all-arcs)) 0)
    (vector-set! pos-weights (+ 2 (position 'proper-possessive all-arcs)) 1)
    (add! index '%xproper-possessive pos-weights))
  (add! index '%possessive (lexicon-entry "trees's"))
  (add! index '%number (lexicon-entry "666"))
  (add! index '%dollars	(lexicon-entry "$666"))
  (add! index '%dashed-word
	(slotmap->lexentry
	 #[adjective 2 noun-modifier 2 solitary-noun 2 noun 2 verb 1]))
  (add! index '%dashed-sword
	(slotmap->lexentry
	 #[adjective 2 noun-modifier 2 noun 0 plural-noun 0 verb 0]))
  (add! index '%strange-word (lexicon-entry "nebob"))
  (add! index '%ing-word (lexicon-entry "hopping"))
  (add! index '%ed-word (lexicon-entry "hopped"))
  (add! index '%ly-word (lexicon-entry "hopefully"))
  (add! index '%contraction
	      (make-lexicon-entry
	       'aux 0 'verb 3 'preposition 5 'noun 5))
  (add! index "an'" (lexicon-entry "and"))
  (add! index "t'" (lexicon-entry "to")))

(define (get-arcs-from-grammar file)
  (if (file-exists? file) (file->dtype file)
      (fail)))

(define (check-arc-compatability lexicon)
  (let* ((old-arcs-vector (first (get lexicon '%grammar)))
	 (old-arcs (elts old-arcs-vector))
	 (new-arcs (choice 'epsilon 'fragment (elts all-arcs))))
    (when (exists? old-arcs)
      (if (exists? (choice (difference old-arcs new-arcs)
			   (difference new-arcs old-arcs)))
	  #f
	  (begin (message "Arcs are compatible")
		 (set! all-arcs (cdr (cdr (->list old-arcs-vector))))
		 #t)))))

(define (new-index filename size)
  (if (file-exists? filename) (remove-file filename))
  (make-hash-index filename size)
  (open-index filename))

(define (write-lexdata directory (with-lexicon #f))
  (unless with-lexicon
    (unless (and (file-exists? (mkpath directory "lexicon"))
		 (check-arc-compatability
		  (open-index (mkpath directory "lexicon"))))
      (lineout "Can't use existing lexicon, regenerating...")
      (system "rm " (mkpath directory "lexicon"))
      (system "rm " (mkpath directory "noun-roots.index"))
      (system "rm " (mkpath directory "verb-roots.index"))
      (set! with-lexicon #t)))
  (write-state-machine (mkpath directory "grammar") '*start-state*)
  (lognotice |WriteLexdata|
    "Wrote state machine into " (mkpath directory "grammar"))
  (write-hookup-table (mkpath directory "hookup"))
  (lognotice |WriteLexdata|
    "Wrote hookup data into " (mkpath directory "hookup"))
  (if with-lexicon
      (let ((lexicon (new-index (mkpath directory "lexicon") 1000000))
	    (noun-roots
	     (new-index (mkpath directory "noun-roots.index") 1000000))
	    (verb-roots
	     (new-index (mkpath directory "verb-roots.index") 1000000)))
	(lognotice |WriteLexicon| "Writing special entries")
	(write-special-entries lexicon)
	(generate-lexicon lexicon noun-roots verb-roots)
	(lognotice |WriteLexicon| "Writing compound prefixes")
	(do-choices (prefix (pick (getkeys dictionary) pair?))
	  (add! lexicon prefix (get dictionary prefix)))
	;; (write-lexer-data lexicon)
; 	(system "cp verb-links.index "
; 		(stringout directory "verb-links.index"))
	(lognotice |WriteLexicon| "Embedding grammar into lexicon")
	(store! lexicon '%grammar
		(->vector (dump-state-machine '*start-state*)))
	(store! lexicon '%heads *glob-heads*)
	(store! lexicon '%mods *glob-mods*)
	(store! lexicon '%names *names*)
	(store! lexicon '%nouns *nouns*)
	(store! lexicon '%verbs *verbs*))
      (let ((lexicon (open-index (mkpath directory "lexicon"))))
	(logwarn |LexiconUpdate| 
	  "Only updating grammar and hookup rules (by request)")
	(store! lexicon '%grammar
		(->vector (dump-state-machine '*start-state*)))
	(store! lexicon '%heads *glob-heads*)
	(store! lexicon '%mods *glob-mods*)
	(store! lexicon '%names *names*)
	(store! lexicon '%nouns *nouns*)
	(store! lexicon '%verbs *verbs*))))

(define (optimize-compilation)
  (optimize-chopper)
  (optimize! write-special-entries generate-lexicon ;; write-lexer-data
	     capword expand-adjective expand-verb inflect-verb valid-inflection
	     expand-noun valid-plural))

