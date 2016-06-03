;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc.  All rights reserved.

(in-module 'knodules/plaintext)

;;; Parser for the plaintext knodule encoding

(use-module '{texttools fdweb ezrecords logger varconfig})
(use-module '{knodules knodules/drules})

(define-init %loglevel %warn%)

(module-export!
 '{kno/read-plaintext
   kno/write-plaintext
   kno/plaintext kno/plaintext/escape
   kno/plaintext/entries
   knodule->string
   knodule->file file->knodule
   string->knodule})

;;; Text processing

(define (escaped-segment string sep)
  (if (character? sep) (set! sep (->string sep)))
  (let* ((split (->vector (textslice string sep 'sep)))
	 (i 0) (lim (length split))
	 (seg #f) (sep #f) (merging #f)
	 (results '()))
    (while (< i lim)
      (set! seg (elt split i))
      (set! sep (elt split (1+ i)))
      (set! i (+ i 2))
      (if (has-suffix seg "\\")
	  (set! merging (glom merging (slice seg 0 -1) sep))
	  (begin (set! results (cons (glom merging seg) results))
	    (set! merging #f))))
    (reverse results)))
(module-export! 'escaped-segment)

(define (unescape-string string)
  (strinnnnneg-subst* (decode-entities string) "\\;" ";" "\\|" "|" "\\\\" "\\"))

(define escaped-segment splitsep)
(define escaped-find findsep)
(define unescape-string unslashify)

;;; Handling clauses

(define (handle-lang-term subject slotid lang value)
  (tryif (and (string? value) (not (equal? value "")))
    (if (and (eqv? (elt value 0) #\$)
	     (string-starts-with? value #("$" (isalpha) (isalpha) "$")))
	(let ((langid (string->lisp (subseq value 1 3))))
	  (if (eq? langid lang)
	      (list subject slotid (subseq value 4))
	      (list subject slotid
		    (cons langid (subseq value 4)))))
	(list subject slotid value))))

;; A DRULE is a set of clauses separated by ~ characters.
;;  + is an indicator, at least one of which must be present
;;  - indicates a contraindicator, none of which must be present
;;  += and -= indicates dterms which count as indicators
;;  +/ and -/ indicates a regex to match indicators or contraindicators
;;  = indicates a dterm, previously resolved, which counts
;;     as an contributor
;;  anything else is a term which counts as a contributor
;;  #N is a threshold, indicating that N terms must be matched
(define (plaintext->drule string subject knodule language)
  (let* ((dclauses (map trim-spaces (escaped-segment string #\~)))
	 (dtermtable (knodule-dterms knodule))
	 (cues {}) (context+ {}) (context- {})
	 (threshold 1))
    (doseq (dclause (remove "" dclauses) i)
      (cond ((eqv? (first dclause) #\+)
	     (set+! cues
		    (try (get dtermtable (stdcap (subseq dclause 1)))
			 (subseq dclause 1))))
	    ((eqv? (first dclause) #\-)
	     (set+! context-
		    (if (eqv? (second dclause) #\=)
			(kno/dref (subseq dclause 2) knodule)
			(try (get dtermtable (stdcap (subseq dclause 1)))
			     (subseq dclause 1)))))
	    ((eqv? (first dclause) #\=)
	     (set+! context+ (kno/dref (subseq dclause 1) knodule)))
	    ((eqv? (first dclause) #\#)
	     (if (equal? dclause "#*")
		 (set! threshold #f)
		 (set! threshold (string->lisp (subseq dclause 1)))))
	    (else (set+! context+
			 (try (get dtermtable (stdcap (subseq dclause 1)))
			      (subseq dclause 1))))))
    (kno/drule subject language cues context+ context- threshold
	       knodule)))

(define (clause->triples op mod value subject knodule)
  (cond ((or (not op) (eq? op #\\))
	 (list subject (knodule-language knodule) value))
	((eq? op #\$)
	 ;; Term in another language
	 (let ((lang (knodule-language knodule))
	       (langid (string->lisp (subseq value 0 2))))
	   (if (eq? langid lang)
	       (list subject lang value)
	       (list subject langid (subseq value 3)))))
	((eq? op #\^)
	 ;; Either a statement of a generalization or of a
	 ;; generalization with an argument
	 (let* ((open (escaped-find value #\openparen))
		(close (and open (escaped-find value #\closeparen open)))
		(context
		 (and open close
		      (kno/dref (subseq value (1+ open) close)
				knodule)))
		(value (if (and open close) (subseq value 0 open)
			   value))
		(genl (kno/dref value knodule)))
	   (choice
	    (list subject
		  (try (get #[#f genls #\* commonly #\~ sometimes] mod)
		       'genls)
		  genl)
	    (tryif context (list subject 'roles (cons genl context)))
	    (tryif context (list context genl subject)))))
	((eq? op #\_)
	 ;; Declaration of specializations
	 (let ((object (kno/dref value knodule)))
	   (choice (list subject 'specls object)
		   (list object 'genls subject)
		   (list object
			 (get #[#\* typical #\~ atypical] mod)
			 subject))))
	((eq? op #\-)
	 ;; Declaration of complements
	 (let ((object (kno/dref value knodule)))
	   (choice (list subject 'never object)
		   (list object 'never subject)
		   (list object
			 (get #[#\* rarely #\~ somenot] mod)
			 subject))))
	((eq? op #\.)
	 (let ((eqpos (position #\= value)))
	   (if eqpos
	       (let ((role (kno/dref (subseq value 0 eqpos) knodule))
		     (filler (kno/dref (subseq value (1+ eqpos)) knodule)))
		 (choice (list subject role filler)
			 (list filler (get role 'mirror) subject)))
	       (error "Bad dot clause" clause)))
	 (tryif (position #\= value)
	   (list subject
		 (string->lisp (slice value 0 (position #\= value)))
		 (parse-arg (slice value (1+ (position #\= value)))))))
	((eq? op #\:)
	 ;; Declaration of a named relationship
	 (let ((eqpos (position #\= value)))
	   (if eqpos
	       (let ((role (kno/dref (subseq value 1 eqpos) knodule))
		     (value (parse-arg (subseq value (1+ eqpos)))))
		 (list subject role value))
	       (error "Bad colon clause" clause))))
	((eq? op #\=)
	 ;; Declaration of underlying identity
	 (let ((atpos (position #\@ value)))
	   (if (and atpos (zero? atpos))
	       (list subject 'oid (string->lisp value))
	       (let ((dterm
		      (if atpos
			  (kno/dref (subseq value 0 atpos)
				    (knodule/ref (subseq value (1+ atpos))))
			  (kno/dref value knodule))))
		 (choice
		  (list subject
			(get #[#\* equiv #f identical #\~ somenot] mod)
			dterm)
		  (tryif (eq? mod #\*) (list dterm 'equiv subject)))))))
	((eq? op #\amp)
	 (list subject
	       (get #[#f assocs #\* defs #\~ refs] mod)
	       (kno/dref value knodule)))
	((eq? op #\@)
	 ;; Reference to an external definition/name
	 (handle-lang-term subject
			   (get #[#f xref #\* xdef #\~ xuri] mod)
			   (knodule-language knodule) value))
	((eq? op #\*)
	 ;; Declaration of a natural language normative term
	 (handle-lang-term subject 'norms (knodule-language knodule)
			   value))
	((eq? op #\~)
	 ;; Declaration of a hook or DRULE
	 (if (position #\~ value)
	     (list subject 'drules
		   (plaintext->drule (string-append "+" value)
				     subject knodule
				     (knodule-language knodule)))
	     (handle-lang-term subject 'hooks (knodule-language knodule)
			       value)))
	((eq? op #\") ;; Deprecated
	 (logwarn "Use of \" for glosses is deprecated, use + instead!")
	 (when (has-suffix value "\"")
	   (set! value (subseq value 0 -1)))
	 (handle-lang-term subject
			   (get #[#f gloss #\* explanation #\~ aside] mod)
			   (knodule-language knodule) value))
	((eq? op #\+)
	 (handle-lang-term subject
			   (get #[#f explanation #\* gloss #\~ aside] mod)
			   (knodule-language knodule) value))
	((eq? op #\%)
	 (let ((eqpos (position #\= value)))
	   (if eqpos
	       (list subject (string->lisp (subseq value 1 eqpos))
		     (kno/dterm (subseq value (1+ eqpos))))
	       (list subject 'meta (string->lisp (subseq value 1))))))
	(else (error "Bad clause" op mod value))))

(define (handle-clause clause subject knodule)
  (let* ((op (and (char-punctuation? (first clause))
		  (overlaps? (first clause)
			     {#\^ #\= #\_ #\amp #\\ #\$ #\-
			      #\. #\: #\@ #\* #\~ #\+ #\%})
		  (first clause)))
	 (modifier (and op (> (length clause) 1)
			(overlaps? (second clause) {#\* #\~})
			(second clause)))
	 (rest (unescape-string
		(if op (subseq clause (if modifier 2 1)) clause)))
	 (triples (clause->triples op modifier rest subject knodule)))
    (logdebug "Applying clause " (write clause) " to " subject " yielding "
	      (choice-size triples) " triple"
	      (if (not (singleton? triples)) "s") ":"
	      (do-choices (triple triples)
		(printout "\n\t" (write (first triple))
		  "\t" (write (second triple)) "\t" (write (third triple)))))
    (do-choices (triple triples) (apply kno/add! triple))
    subject))

(define (handle-subject-entry entry knodule)
  (let* ((clauses (remove "" (map trim-spaces (escaped-segment entry #\|))))
	 (dterm (kno/dterm (first clauses) knodule)))
    (loginfo "Applying " (length clauses) " plaintext clauses to " dterm
	     "\n\t in " knodule)
    (doseq (clause (cdr clauses))
      (handle-clause clause dterm knodule))
    (add! dterm 'clauses clauses)
    (add! dterm 'defs entry)
    dterm))

(define (handle-entry entry knodule)
  (if (not (char-punctuation? (first entry)))
      (handle-subject-entry entry knodule)
      (cond ((eq? (first entry) #\*)
	     (kno/add! (handle-subject-entry (subseq entry 1) knodule)
		       'type 'primary))
	    (else (error "Invalid knodule entry" entry)))))

(define (kno/plaintext/entries string)
  (let* ((sliced (textslice string '(GREEDY {"\n" ";" ";\n"}) #t))
	 (entry (car sliced))
	 (scan (cdr sliced))
	 (merged '()))
    (until (null? scan)
      (cond ((has-suffix entry "\\;")
	     (set! entry (string-append entry (car scan)))
	     (set! scan (cdr scan)))
	    ((has-suffix entry "\\\n")
	     (set! entry (string-append (slice entry -2) (elt entry -1) (car scan)))
	     (set! scan (cdr scan)))
	    ((has-suffix entry ";\n")
	     (set! merged (cons (slice entry 0 -2) merged))
	     (set! entry (car scan))
	     (set! scan (cdr scan)))
	    ((and (has-suffix entry ";")
		  (string-ends-with?
		   entry #("&" {#((isalpha) (isalnum+)) #("#" (isdigit+)) #("#x" (isxdigit+))}
			   ";" (eos))))
	     (set! entry (string-append entry (car scan)))
	     (set! scan (cdr scan)))
	    ((empty-string? entry)
	     (set! entry (car scan))
	     (set! scan (cdr scan)))
	    (else
	     (set! merged (cons (slice entry 0 -1) merged))
	     (set! entry (car scan))
	     (set! scan (cdr scan)))))
    (reverse (cons entry merged))))

(define (kno/read-plaintext text (knodule default-knodule))
  (map (lambda (x)
	 (if (eq? (first x) #\*)
	     (let ((e (handle-subject-entry (subseq x 1) knodule)))
	       (hashset-add! (knodule-prime knodule) e)
	       e)
	     (if (char-punctuation? (first x))
		 x
		 (handle-subject-entry x knodule))))
       (remove "" (map trim-spaces (kno/plaintext/entries text)))))
(define (kno/plaintext text (knodule default-knodule))
  "Parses a single plaintext subject entry and returns the subject"
  (handle-subject-entry (trim-spaces text) knodule))

;;; Generating the plaintext representation

(define slot-codes-init
  '(("^" genls) ("^*" commonly) ("^~" sometimes)
    ("_" examples) ("_*" typical) ("_~" atypical)
    ("-" never) ("^*" rarely) ("^~" somenot)
    ("&" assocs) ("&*" defs) ("&~" refs)
    ("=" identical) ("=*" equiv) ("=~" sorta)
    ("@" xref) ("@*" xdef) ("@~" xuri)))
(define slot-codes
  (let ((table (make-hashtable)))
    (dolist (sci slot-codes-init)
      (add! table (second sci) (first sci)))
    table))

(define (escape-string string)
  (string-subst (string-subst (stdspace string) ";" "\\;") "|" "\\|"))
(define (kno/plaintext/escape string (clause #f))
  (if (and clause (char-punctuation? (first string)))
      (stringout  "\\" (subseq string 0 1)
		  (escape-string (subseq string 1)))
      (escape-string string)))

(define (output-value value (knodule default-knodule))
  (if (oid? value)
      (if (test value 'knodule (knodule-oid knodule))
	  (printout (escape-string (get value 'dterm)))
	  (printout (escape-string (get value 'dterm))
		    "@" (escape-string (knodule-name knodule))))
      (if (and (pair? value) (overlaps? (car value) langids))
	  (printout "$" (car value) "$" (escape-string (cdr value)))
	  (printout (escape-string value)))))

(define (dterm->plaintext dterm (knodule default-knodule) (settings #[]))
  (let ((languages (choice (knodule-language knodule) 'en
			   (get settings 'languages)
			   (tryif (test settings 'languages 'all)
				  langids)))
	(prime (knodule-prime knodule)))
    (printout
      (if (get prime dterm) "*")
      (get dterm 'dterm)
      (do-choices (slotid (difference (getkeys dterm) (get settings 'exclude)))
	(do-choices (value (get dterm slotid))
	  (let ((code (get slot-codes slotid)))
	    (cond ((exists? code)
		   (printout "|" code (output-value value knodule)))
		  ((oid? slotid)
		   (printout "|." (get slotid 'dterm)
			     "=" (output-value value knodule)))
		  ((eq? slotid (knodule-language knodule))
		   (when (string? value)
		     (if (char-punctuation? (elt value 0))
			 (printout "|\\" value)
			 (printout "|" value))))
		  ((eq? slotid 'gloss)
		   (printout "|\"" (output-value value) "\""))
		  ((overlaps? slotid langids)
		   (if (overlaps? slotid languages)
		       (printout "|$" slotid "$" value)))
		  ((eq? slotid 'norms)
		   (if (string? value)
		       (printout "|*" value)
		       (if (overlaps? (car value) languages)
			   (printout "|*$" (car value) "$" (cdr value)))))
		  ((eq? slotid 'hooks)
		   (if (string? value)
		       (printout "|~" value)
		       (if (overlaps? (car value) languages)
			   (printout "|~$" (car value) "$" (cdr value)))))
		  ((eq? slotid 'roles)
		   (printout "|" (cond ((test dterm 'genls (car value)) "^")
				       ((test dterm 'always (car value)) "*")
				       ((test dterm 'sometimes (car value)) "~")
				       (else "^"))
			     (output-value (car value) knodule)
			     "(" (output-value (cdr value) knodule) ")" ))
		  ((eq? slotid 'mirror)
		   (printout "|%MIRROR=" (get value 'dterm)))
		  ((eq? slotid 'drules)
		   (printout "|")
		   (if (eq? (knodule-language knodule)
			    (drule-language value))
		       (do-choices (cue (drule-cues value) i)
			 (printout (if (> i 0) "&") "+"
				   (if (string? cue) cue (get cue 'dterm))))
		       (do-choices (cue (drule-cues value) i)
			 (if (= i 0)
			     (printout "+$" (knodule-language value) "$")
			     (printout "&+"))
			 (printout (if (string? cue) cue (get cue 'dterm)))))
		   (do-choices (cue (drule-context- value) i)
		     (printout "&-" (if (string? cue) cue (get cue 'dterm))))
		   (do-choices (cue (drule-context+ value) i)
		     (printout "&" (if (string? cue) cue (get cue 'dterm))))
		   (unless (eq? (drule-threshold value) 1)
		     (printout "&#" (drule-threshold value)))
		   (when (not (drule-threshold value))
		     (printout "&#*" (drule-threshold value))))
		  ((eq? slotid 'oid)
		   (if (test value 'sensecat)
		       (printout
			 "|=@" (number->string (oid-hi value) 16) "/"
			 (number->string (oid-lo value) 16))
		       (if (test value 'dterm)
			   ;; Need to add relative knodules
			   (printout "|=" (get value 'dterm)))))
		  (else ))))))))

(defambda (kno/write-plaintext dterms (settings #[]) (kl)  (sep))
  (when (knodule? dterms)
    (set! kl dterms)
    (set! dterms (hashset-elts (knodule-alldterms kl))))
  (default! sep (try (getopt settings 'sep) ";\n"))
  (default! kl
    (knodule/ref (try (getopt settings 'knodule)
		      (pick-one (get dterms 'knodule)))))
  (if (sequence? dterms)
      (doseq (dterm dterms i)
	(if (> i 0) (printout sep))
	(dterm->plaintext dterm kl settings))
      (do-choices (dterm dterms i)
	(if (> i 0) (printout sep))
	(dterm->plaintext dterm kl settings)))
  (lineout))

(define (knodule->file knodule (file #f) (settings #[]))
  (if (and (string? knodule) (not file))
      (knodule->file default-knodule knodule settings)
      (fileout file (kno/write-plaintext knodule settings))))

(define (knodule->string knodule (settings #[]))
  (stringout (kno/write-plaintext knodule settings)))

(define (file->knodule file (knodule default-knodule))
  (lognotice "Loading file " file " into " knodule)
  (kno/read-plaintext (filestring file) knodule))

(define (string->knodule string (knodule default-knodule) (cxt #f))
  (lognotice "Loading " (length string) " bytes into " knodule
	     (when cxt (printout " from " cxt)))
  (kno/read-plaintext string knodule))

