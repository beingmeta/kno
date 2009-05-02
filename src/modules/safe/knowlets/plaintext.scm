(in-module 'knowlets/plaintext)

(use-module '{texttools fdweb ezrecords varconfig})
(use-module '{knowlets})

(module-export! '{kno/plaintext})

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
	    (list subject slotid value)
	    (list subject slotid
		  (cons langid (subseq value 4)))))
      (list subject slotid value)))

(define (clause->triples op mod value subject knowlet)
  (%watch "CLAUSE->TRIPLES" op mod value)
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
		 (kno/dref
		  (and open close
		       (kno/dref (subseq value (1+ open) close)))))
		(value (if (and open close) (subseq value 0 open)
			   value))
		(genl (kno/dref value)))
	   (choice
	    (list subject
		  (try (get #[#f genls #\* commonly #\~ sometimes] mod)
		       'genls)
		  genl)
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
	       (let ((role (kno/dref (subseq value 0 eqpos)))
		     (filler (kno/dref (subseq value (1+ eqpos)))))
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
	       (kno/dref value)))
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
  (let* ((clauses (escaped-segment entry #\|))
	 (dterm (kno/dterm (first clauses))))
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

(define (kno/plaintext text (knowlet default-knowlet))
  (map (lambda (x)
	 (if (char-punctuation? (first x))
	     x
	     (handle-subject-entry x knowlet)))
       (escaped-segment text #\;)))

