;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2009 beingmeta, inc.  All rights reserved.

(in-module 'knowlets/plaintext)

;;; Parser for the plaintext knowlet encoding
(define id "$Id$")
(define revision "$Revision$")

(use-module '{texttools fdweb ezrecords varconfig})
(use-module '{knowlets knowlets/drules})

(module-export!
 '{kno/read-plaintext
   kno/write-plaintext
   kno/plaintext kno/plaintext/escape
   knowlet->file file->knowlet})

(module-export! '{escaped-segment escaped-find})

;;; Text processing

;;; These were original definitions which are now in C
(define (escaped-segment string sep)
  (let ((result '()) (start 0) (pos (position sep string)))
    (while pos
      (if (and (> pos 0) (eqv? (elt string (- pos 1)) #\\)
	       (or (= pos 1)
		   (not (eqv? (elt string (- pos 2)) #\\))))
	  (set! pos (position sep string (1+ pos)))
	  (begin
	    (unless (= pos start)
	      (set! result (cons (subseq string start pos) result)))
	    (set! start (1+ pos))
	    (set! pos (position sep string start)))))
    (unless (= start (length string))
      (set! result (cons (subseq string start) result)))
    (reverse result)))

(define (escaped-find string sep)
  (let ((found #f) (start 0) (pos (position sep string)))
    (while (and pos (not found))
      (if (and (> pos 0)
	       (eqv? (elt string (- pos 1)) #\\)
	       (or (= pos 1)
		   (eqv? (elt string (- pos 2)) #\\)))
	  (set! pos (position sep string (1+ pos)))
	  (set! found #t)))
    pos))

(define (unescape-string string)
  (string-subst* string "\\;" ";" "\\|" "|" "\\\\" "\\"))

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

(define (plaintext->drule string subject knowlet language)
  (let* ((dclauses (map trim-spaces (escaped-segment string #\&)))
	 (cues {}) (context+ {}) (context- {})
	 (threshold 1))
    (doseq (dclause (remove "" dclauses) i)
      (cond ((eqv? (first dclause) "+")
	     (set+! cues
		    (try (get knowlet-dterms (subseq dclause 1))
			 (subseq dclause 1))))
	    ((eqv? (first dclause) "-")
	     (set+! context-
		    (try (get knowlet-dterms (subseq dclause 1))
			 (subseq dclause 1))))
	    ((eqv? (first dclause) "#")
	     (if (equal? dclause "#*")
		 (set! threshold #f)
		 (set! threshold (string->lisp (subseq dclause 1)))))
	    (else (set+! context+
			 (try (get knowlet-dterms (subseq dclause 1))
			      (subseq dclause 1))))))
    (kno/drule subject language cues context+ context- threshold
	       knowlet)))

(define (clause->triples op mod value subject knowlet)
  (cond ((or (not op) (eq? op #\\))
	 (list subject (knowlet-language knowlet) value))
	((eq? op #\$)
	 (let ((lang (knowlet-language knowlet))
	       (langid (string->lisp (subseq value 0 2))))
	   (if (eq? langid lang)
	       (list subject lang value)
	       (list subject langid (subseq value 3)))))
	((eq? op #\^)
	 (let* ((open (escaped-find value #\())
		(close (and open (escaped-find value #\) open)))
		(context
		 (and open close
		      (kno/dref (subseq value (1+ open) close)
				knowlet)))
		(value (if (and open close) (subseq value 0 open)
			   value))
		(genl (kno/dref value knowlet)))
	   (choice
	    (list subject
		  (try (get #[#f genls #\* commonly #\~ sometimes] mod)
		       'genls)
		  genl)
	    (tryif context (list subject 'roles (cons genl context)))
	    (tryif context (list context genl subject)))))
	((eq? op #\_)
	 (let ((object (kno/dref value knowlet)))
	   (choice (list subject 'specls object)
		   (list object 'genls subject)
		   (list object
			 (get #[#\* typical #\~ atypical] mod)
			 subject))))
	((eq? op #\-)
	 (let ((object (kno/dref value knowlet)))
	   (choice (list subject 'never object)
		   (list object 'never subject)
		   (list object
			 (get #[#\* rarely #\~ somenot] mod)
			 subject))))
	((eq? op #\.)
	 (let ((eqpos (position #\= value)))
	   (if eqpos
	       (let ((role (kno/dref (subseq value 0 eqpos) knowlet))
		     (filler (kno/dref (subseq value (1+ eqpos)) knowlet)))
		 (choice (list subject role filler)
			 (list filler (get role 'mirror) subject)))
	       (error "Bad dot clause" clause))))
	((eq? op #\=)
	 (let ((atpos (position #\@ value)))
	   (if (and atpos (zero? atpos))
	       (list subject 'oid (string->lisp value))
	       (let ((dterm
		      (if atpos
			  (kno/dref (subseq value 0 atpos)
				    (knowlet (subseq value (1+ atpos))))
			  (kno/dref value knowlet))))
		 (choice
		  (list subject
			(get #[#\* equiv #f identical #\~ somenot] mod)
			dterm)
		  (tryif (eq? mod #\*) (list dterm 'equiv subject)))))))
	((eq? op #\&)
	 (list subject
	       (get #[#f assocs #\* defs #\~ refs] mod)
	       (kno/dref value knowlet)))
	((eq? op #\@)
	 (handle-lang-term subject
			   (get #[#f xref #\* xdef #\~ xuri] mod)
			   (knowlet-language knowlet) value))
	((eq? op #\*)
	 (handle-lang-term subject 'norms (knowlet-language knowlet)
			   value))
	((eq? op #\~)
	 (handle-lang-term subject 'hooks (knowlet-language knowlet)
			   value))
	((eq? op #\")
	 (when (has-suffix value "\"")
	   (set! value (subseq value 0 -1)))
	 (handle-lang-term subject
			   (get #[#f explanation #\* gloss #\~ aside] mod)
			   (knowlet-language knowlet) value))
	((eq? op #\+)
	 (list subject 'drules
	       (plaintext->drule (string-append "+" value)
				 subject knowlet
				 (knowlet-language knowlet))))
	((eq? op #\%)
	 (let ((eqpos (position #\= value)))
	   (if eqpos
	       (list subject (string->lisp (subseq value 1 eqpos))
		     (kno/dterm (subseq value (1+ eqpos))))
	       (list subject 'meta (string->lisp (subseq value 1))))))
	(else (error "Bad clause" op mod value))))

(define (handle-clause clause subject knowlet)
  (let* ((op (and (char-punctuation? (first clause))
		  (first clause)))
	 (modifier (and op (> (length clause) 1)
			(overlaps? (second clause) {#\* #\~})
			(second clause)))
	 (rest (unescape-string
		(if op (subseq clause (if modifier 2 1)) clause)))
	 (triples (clause->triples op modifier rest subject knowlet)))
    (do-choices (triple triples) (apply kno/add! triple))
    subject))

(define (handle-subject-entry entry knowlet)
  (let* ((clauses (remove "" (map trim-spaces (escaped-segment entry #\|))))
	 (dterm (kno/dterm (first clauses) knowlet)))
    (when (overlaps? kno/logging '{defterm defs})
      (%watch "PLAINTEXT" dterm (length clauses)))
    (doseq (clause (cdr clauses))
      (handle-clause clause dterm knowlet))
    dterm))

(define (handle-entry entry knowlet)
  (if (not (char-punctuation? (first entry)))
      (handle-subject-entry entry knowlet)
      (cond ((eq? (first entry) #\*)
	     (kno/add! (handle-subject-entry (subseq entry 1) knowlet)
		       'type 'primary))
	    (else (error "Invalid knowlet entry" entry)))))

(define (kno/read-plaintext text (knowlet default-knowlet))
  (map (lambda (x)
	 (if (char-punctuation? (first x))
	     x
	     (handle-subject-entry x knowlet)))
       (remove "" (map trim-spaces (escaped-segment text #\;)))))
(define (kno/plaintext text (knowlet default-knowlet))
  "Parses a single plaintext subject entry and returns the subject"
  (handle-subject-entry (trim-spaces text) knowlet))

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

(define (output-value value (knowlet default-knowlet))
  (if (oid? value)
      (if (test value 'knowlet (knowlet-oid knowlet))
	  (printout (escape-string (get value 'dterm)))
	  (printout (escape-string (get value 'dterm))
		    "@" (escape-string (knowlet-name knowlet))))
      (if (and (pair? value) (overlaps? (car value) langids))
	  (printout "$" (car value) "$" (escape-string (cdr value)))
	  (printout (escape-string value)))))

(define (dterm->plaintext dterm (knowlet default-knowlet) (settings #[]))
  (let ((languages (choice (knowlet-language knowlet) 'en
			   (get settings 'languages)
			   (tryif (test settings 'languages 'all)
				  langids))))
    (printout
      (get dterm 'dterm)
      (do-choices (slotid (difference (getkeys dterm) (get settings 'exclude)))
	(do-choices (value (get dterm slotid))
	  (let ((code (get slot-codes slotid)))
	    (cond ((exists? code)
		   (printout "|" code (output-value value knowlet)))
		  ((oid? slotid)
		   (printout "|." (get slotid 'dterm)
			     "=" (output-value value knowlet)))
		  ((eq? slotid (knowlet-language knowlet))
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
			     (output-value (car value) knowlet)
			     "(" (output-value (cdr value) knowlet) ")" ))
		  ((eq? slotid 'mirror)
		   (printout "|%MIRROR=" (get value 'dterm)))
		  ((eq? slotid 'drules)
		   (printout "|")
		   (if (eq? (knowlet-language knowlet)
			    (drule-language value))
		       (do-choices (cue (drule-cues value) i)
			 (printout (if (> i 0) "&") "+"
				   (if (string? cue) cue (get cue 'dterm))))
		       (do-choices (cue (drule-cues value) i)
			 (if (= i 0)
			     (printout "+$" (knowlet-language value) "$")
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
			   ;; Need to add relative knowlets
			   (printout "|=" (get value 'dterm)))))
		  (else ))))))))

(defambda (kno/write-plaintext dterms (settings #[]) (kl)  (sep))
  (when (knowlet? dterms)
    (set! kl dterms)
    (set! dterms (hashset-elts (knowlet-alldterms kl))))
  (default! sep (try (get settings 'sep) ";\n"))
  (default! kl (knowlet (try (get settings 'knowlet)
			     (pick-one (get dterms 'knowlet)))))
  (if (sequence? dterms)
      (doseq (dterm dterms i)
	(if (> i 0) (printout sep))
	(dterm->plaintext dterm kl settings))
      (do-choices (dterm dterms i)
	(if (> i 0) (printout sep))
	(dterm->plaintext dterm kl settings)))
  (lineout))

(define (knowlet->file knowlet (file #f) (settings #[]))
  (if (and (string? knowlet) (not file))
      (knowlet->file default-knowlet knowlet settings)
      (fileout file (kno/write-plaintext knowlet settings))))

(define (file->knowlet file (knowlet default-knowlet))
  (kno/read-plaintext (filestring file) knowlet))

