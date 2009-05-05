(in-module 'knowlets/plaintext)

(use-module '{texttools fdweb ezrecords varconfig})
(use-module '{knowlets knowlets/drules})

(module-export! '{kno/read-plaintext kno/write-plaintext})

(module-export! '{escaped-segment escaped-find})

;;; Text processing

(define (escaped-segment string sep)
  (let ((result '()) (start 0) (pos (position sep string)))
    (while pos
      (unless (and (> pos 0)
		   (eqv? (elt string (- pos 1)) #\\))
	(unless (= pos start)
	  (set! result (cons (subseq string start pos) result)))
	(set! start (1+ pos))
	(set! pos (position sep string start))))
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

;;; Handling clauses

(define (handle-lang-term subject slotid lang value)
  (if (exists? (textmatcher #("$" (isalpha) (isalpha) "$") value))
      (let ((langid (string->lisp (subseq value 1 3))))
	(if (eq? langid lang)
	    (list subject slotid (subseq value 4))
	    (list subject slotid
		  (cons langid (subseq value 4)))))
      (list subject slotid value)))

(define (plaintext->drule string subject knowlet language)
  (let* ((dclauses (map trim-spaces (segment-string string #\&)))
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
				    (kno/knowlet (subseq value (1+ atpos))))
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
	((eq? op #\%)
	 (if (eq? mod #\*)
	     (let ((meta (kno/dterm value knowlet)))
	       (list subject 'meta meta))
	     (if (eq? mod #\~)
		 (list subject 'meta value)
		 (let ((mirror (kno/dterm value knowlet)))
		   (list subject 'mirror mirror)))))
	((eq? op #\+)
	 (list subject 'drules
	       (plaintext->drule (string-append "+" value)
				 subject knowlet
				 (knowlet-language knowlet))))
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
    (doseq (clause (cdr clauses))
      (handle-clause clause dterm knowlet))
    dterm))

(define (handle-entry entry knowlet)
  (if (not (char-punctuation? (first entry)))
      (handle-subject-entry entry knowlet)
      (cond ((eq? (first entry) #\*)
	     (kno/add! (handle-subject-entry (subseq entry 1))
		       'type 'primary))
	    (else (error "Invalid knowlet entry" entry)))))

(define (kno/read-plaintext text (knowlet default-knowlet))
  (map (lambda (x)
	 (if (char-punctuation? (first x))
	     x
	     (handle-subject-entry x knowlet)))
       (remove "" (map trim-spaces (escaped-segment text #\;)))))

;;; Generating the plaintext representation

(define slot-codes-init
  '(("^" genls) ("^*" commonly) ("^~" sometimes)
    ("_" examples) ("_*" typical) ("_~" atypical)
    ("-" never) ("^*" rarely) ("^~" somenot)
    ("&" assocs) ("&*" defs) ("&~" refs)
    ("=" identical) ("=*" equiv) ("=~" sorta)
    ("@" xref) ("@*" xdef) ("@~" xuri) ))
(define slot-codes
  (let ((table (make-hashtable)))
    (dolist (sci slot-codes-init)
      (add! table (second sci) (first sci)))
    table))

(define (output-value value (knowlet default-knowlet))
  (if (oid? value)
      (if (test value 'knowlet (knowlet-oid knowlet))
	  (printout (get value 'dterm))
	  (printout (get value 'dterm) "@" (knowlet-name knowlet)))
      (if (and (pair? value) (overlaps? (car value) langids))
	  (printout "$" (car value) "$" (cdr value))
	  (printout value))))

(define (dterm->plaintext dterm (knowlet default-knowlet))
  (printout
    (get dterm 'dterm)
    (do-choices (slotid (getkeys dterm))
      (do-choices (value (get dterm slotid))
	(let ((code (get slot-codes slotid)))
	  (cond ((exists? code)
		 (printout "|" code (output-value value knowlet)))
		((oid? slotid)
		 (printout "|." (get slotid 'dterm)
			   "=" (output-value value knowlet)))
		((eq? slotid (knowlet-language knowlet))
		 (printout "|" value))
		((overlaps? slotid langids)
		 (printout "|$" slotid "$" value))
		((eq? slotid 'norms)
		 (if (string? value)
		     (printout "|*" value)
		     (printout "|*$" (car value) "$" (cdr value))))
		((eq? slotid 'hooks)
		 (if (string? value)
		     (printout "|~" value)
		     (printout "|~$" (car value) "$" (cdr value))))
		((eq? slotid 'roles)
		 (printout "|" (cond ((test dterm 'genls (car value)) "^")
				     ((test dterm 'always (car value)) "*")
				     ((test dterm 'sometimes (car value)) "~")
				     (else "^"))
			   (output-value (car value) knowlet)
			   "(" (output-value (cdr value) knowlet) ")" ))
		((eq? slotid 'mirror)
		 (printout "|%" (get value 'dterm)))
		((eq? slotid 'meta)
		 (if (oid? value)
		     (printout "|%*" (get value 'dterm))
		     (printout "|%~" value)))
		((eq? slotid 'drules)
		 (printout "|")
		 (if (eq? (knowlet-language knowlet)
			  (drule-language value))
		     (do-choices (cue (drule-cues value) i)
		       (printout (if (> i 0) "&") "+"
				 (if (string? cue) cue (get cue 'dterm))))
		     (do-choices (cue (drule-cues value) i)
		       (if (= i 0)
			   (printout "+$" (knowlet-langauge value) "$")
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
		(else )))))))

(defambda (kno/write-plaintext dterms (kl) (sep ";\n"))
  (default! kl (knowlet (pick-one (get dterms 'knowlet))))
  (if (sequence? dterms)
      (doseq (dterm dterms i)
	(if (> i 0) (printout sep))
	(dterm->plaintext dterm kl))
      (do-choices (dterm dterms i)
	(if (> i 0) (printout sep))
	(dterm->plaintext dterm kl))))

