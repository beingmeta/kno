;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-

(in-module 'gnosys/nlp)

;;; This provides various NLP algorithms for GNOSYS

(define version "$Id$")

(use-module '{texttools tagger morph gnosys parsetime})

(module-export! '{oddcaps? reduce-names
			   unique-name-map
			   is-abbreviation?})
(module-export! '{parsetext  join-commas})
(module-export! '{nlp/analyze nlp/unanalyze})

;;;; Tracing support

(define tracedetails #f)
(define traceresults #f)
(define tracetime #t)

(define logger comment)
;;(define logger logif)
(define %volatile '{logger traceresults tracedetails tracetime})

;;;; Helpful stuff

(define *timekeys*
  '{year season dmy dm monthid dowid timeofday})

(define *nlp-slotids*
  {@?gn/proper-names
   @?gn/phrases @?gn/words @?gn/nouns @?gn/verbs @?gn/adjectives
   @?gn/timerefs @?gn/stemkeys
   @?gn/text-bites @?gn/quotes @?gn/mailids @?gn/phone-numbers})

(define (iadd! index f slotid values)
  (assert! f slotid values)
  (when (and index (exists? values))
    (index-frame index f slotid values)))

;;; Calling the parser

(define (oddcaps? string)
  (let* ((words (elts (words->vector string)))
	 (caps (pick words capitalized?)))
    (> (* (choice-size caps) 2) (choice-size words))))

(define (parsetext text)
  (tagger text (if (oddcaps? text) '(glom oddcaps) 'glom)))

;;; Reducing names

(define (probe-abbrev letters words i j ilim jlim)
  (or (>= i ilim) 
      (and (< j jlim)
	   (if (eq? (elt letters i) (first (elt words j)))
	       (probe-abbrev letters words (1+ i) (1+ j) ilim jlim)
	       (and (or (not (capitalized? (first (elt words j))))
			(< (length (elt words j)) 4))
		    (probe-abbrev letters words i (1+ j) ilim jlim))))))

(define (is-abbreviation? word name)
  (let ((wordv (words->vector name)))
    (and (<= (length word) (length wordv))
	 (eqv? (first word) (first (first wordv)))
	 (probe-abbrev word wordv 1 1 (length word) (length wordv)))))

(defambda (compute-name-maps-from names)
  (for-choices (name1 names)
    (for-choices (name2 names)
      (tryif (not (eq? name1 name2))
	     ;; We can use EQ? because items in the choice are unique
	     (if (or (has-word-suffix? name2 name1)
		     (has-word-prefix? name2 name1)
		     (and (uppercase? name1)
			  (compound? name2)
			  (not (compound? name1))
			  (< (length name1) 8)
			  (is-abbreviation? name1 name2)))
		 (cons name1 name2)
		 (fail))))))

(defambda (compute-name-maps from)
  (let ((prefix-map (make-hashtable))
	(suffix-map (make-hashtable)))
    (do-choices (name from)
      (if (compound? name)
	  (begin
	    (add! prefix-map
		  (subseq name 0 (position #\Space name))
		  name)
	    (unless (position #\, name)
	      (add! suffix-map
		    (subseq name (1+ (rposition #\Space name)))
		    name)))
	  (begin (add! prefix-map name name)
		 (add! suffix-map name name))))
    (choice
     (for-choices (prefix (getkeys prefix-map))
       (let ((v (get prefix-map prefix)))
	 (tryif (not (singleton? v)) (compute-name-maps-from v))))
     (for-choices (suffix (getkeys suffix-map))
       (let ((v (get suffix-map suffix)))
	 (tryif (not (singleton? v)) (compute-name-maps-from v)))))))

(defambda (reduce-names names)
  (let ((name-maps (compute-name-maps names)))
    (choice (difference names (car name-maps))
	    ;; Keep uppercase terms as full names
	    (pick names uppercase?)
	    name-maps)))

(defambda (unique-name-map map)
  (for-choices (key (car map))
    (cons key (singleton (get map key)))))

;;; Text utility functions

(define phone-number-pattern
  #((label country-prefix (opt #("+" (isdigit+) (opt "-"))))
    (label region-code
	   {#("-" (isdigit+) "-") #((isdigit+) "-") #("(" (isdigit+) ")") ""})
    (label number
	   #((+ #((isdigit) (isdigit+) {"." "-"}))
	     (isdigit) (isdigit+)))
    (opt #((spaces*) (label extension #("x" (isdigit+)))))))

(define (make-quote-pattern start (end #f))
  (let ((end (or end start)))
    (vector '{(bol) (isspace)} start
	    `(label content (not> #({(ispunct) ""} ,end)))
	    '{(ispunct) ""} end)))

(define quote-pattern
  (choice (make-quote-pattern {"\"" "'" "&quot;"})
	  (make-quote-pattern "&ldquot;" "&rdquot;")
	  (make-quote-pattern "``" "''")
	  (make-quote-pattern "`" "'")))

(define (make-text-bite-pattern tag)
  `#("<" (ic ,tag) (char-not ">") ">" (label content (not> #("</" (ic ,tag))))
     "</" (ic ,tag)))

(define (short-string? x) (< (length x) 50))
(define (capitalized-phrase? s)
  (and (capitalized? s)
       (< (length s) 40)
       (< (length (getwords s)) 5)
       (or (textmatch #((capword) (* #((isspace) (capword)))) s)
	   (textmatch #((capword)
			(* #((isspace) (capword)))
			#((isspace) (lword))
			(* #((isspace) (capword))))
		      s))))

(define text-bite-pattern
  (make-text-bite-pattern {"b" "i" "strong" "em" "quote" "a"}))

;;; Natural language analysis

(define *noun-tags*
  '{NOUN PLURAL-NOUN SOLITARY-NOUN POSSESSIVE})
(define *name-tags*
  '{PROPER-NAME PROPER-POSSESSIVE PROPER-MODIFIER})
(define *name-modifier-tags*
  'PROPER-MODIFIER)
(define *verb-tags*
  '{VERB INFINITIVAL-VERB INFLECTED-VERB ING-VERB PASSIVE-VERB})
(define *adjective-tags*
  '{ADJECTIVE COMPLEMENT-ADJECTIVE NOUN-MODIFIER DANGLING-ADJECTIVE PROPER-MODIFIER})
(define *content-tags*
  '{SOLITARY-NOUN NOUN-MODIFIER
    NOUN PLURAL-NOUN
    POSSESSIVE
    VERB INFINITIVAL-VERB INFLECTED-VERB
    ING-VERB PASSIVE-VERB
    ADJECTIVE COMPLEMENT-ADJECTIVE NOUN-MODIFIER
    ADVERB DANGLING-ADVERB DANGLING-ADJECTIVE})

;;; Joining commas

(defambda (join-commas-loop vec i lim results)
  (if (>= i lim) results
      (if (and (equal? (elt (elt vec (1+ i)) 0) ",")
	       (equal? (elt (elt vec i) 1) 'proper-name)
	       (equal? (elt (elt vec (+ i 2)) 1) 'proper-name))
	  (join-commas-loop vec (+ i 2) lim
			    (choice (string-append (phrase-string (elt vec i))
						   ", "
						   (phrase-string (elt vec (+ i 2))))
				    results))
	  (join-commas-loop vec (+ i 1) lim results))))

(define (join-commas parse)
  (for-choices (sentence (tryif (pair? parse) (if (vector? (car parse)) parse (elts parse))))
    (let* ((vec (->vector sentence)) (len (length vec)))
      (tryif (> len 2)
	     (join-commas-loop vec 0 (- len 2) {})))))

;;; NLP analysis

(define (justfail ex) (fail))

(defambda (nlp/analyze passage (textslotid 'text) (index #f))
  
  (for-choices passage
    (let* ((start-time (gmtimestamp))
	   (clock-start (elapsed-time))
	   (result (frame-create #f))
	   (text (if (string? passage) passage
		     (if (string? textslotid) textslotid
			 (get passage textslotid))))
	   (frame (pickoids passage))
	   (index (if (string? passage) #f index))
	   (plaintext (strip-markup text))
	   (language (try (get frame @?gn/language)
			  default-language)))
      (let* ((stemkeys (porter-stem (elts (getwords plaintext))))
	     ;; Get some simple patterns
	     (mailids (gather '(mailid) text))
	     (phone-numbers (gather phone-number-pattern plaintext))
	     ;; Get quotes and quotes inside quotes
	     (quotes-base (get (text->frames quote-pattern plaintext) 'content))
	     (quotes+ (choice quotes-base
			      (get (text->frames quote-pattern quotes-base)
				   'content)))
	     (quotes (filter-choices (quote quotes+) (< (length quote) 40)))
	     ;; Get fragments wrapped in markup
	     (text-bites (pick (get (text->frames text-bite-pattern plaintext)
				    'content)
			       short-string?))
	     (timerefs (gather (qc time-patterns) text))
	     (timestamps (for-choices (timeref timerefs)
			   (onerror (parsetime timeref) justfail)))
	     ;; Do natural language analysis
	     ;; (tagged (tagpassage passage textslotid))
	     (parse-start (elapsed-time))
	     (parse (parsetext text))
	     (parse-done (elapsed-time)))
	;; Extract particular terms
	(let* ((content-terms (gettags parse *content-tags*))
	       (name-terms
		(choice (phrase-string (getroots parse *name-tags*))
			(pick (phrase-string (getroots parse *name-modifier-tags*))
			      compound?)
			(join-commas parse)))
	       (nouns (phrase-string (getroots parse *noun-tags*)))
	       (verbs (phrase-string (getroots parse *verb-tags*)))
	       (adjectives (phrase-string (getroots parse *adjective-tags*)))
	       (content-words (choice nouns verbs adjectives quotes))
	       (extract-done (elapsed-time))
	       (result (frame-create #f)))
	  
	  (add! result 'parse parse)
	  
	  (add! frame @?gn/timerefs (choice timerefs timestamps))
	  (add! result @?gn/timerefs (choice timerefs timestamps))
	  (when index
	    (index-frame index frame @?gn/timerefs (get timestamps *timekeys*)))
	  
	  (iadd! index frame @?gn/text-bites text-bites)
	  (add! result @?gn/text-bites text-bites)
	  
	  (iadd! index frame @?gn/quotes quotes)
	  (add! result @?gn/quotes quotes)
	  
	  (iadd! index frame @?gn/phrases quotes)
	  (add! result @?gn/quotes quotes)
	  
	  (iadd! index frame @?gn/phrases (pick content-words compound?))
	  (add! result @?gn/phrases (pick content-words compound?))
	  
	  (iadd! index frame @?gn/phrases text-bites)
	  (add! result @?gn/phrases text-bites)
	  
	  (iadd! index frame @?gn/proper-names
		 (pick text-bites capitalized-phrase?))
	  (add! result @?gn/proper-names
		(pick text-bites capitalized-phrase?))
	  
	  (iadd! index frame @?gn/mailids mailids)
	  (add! result @?gn/mailids mailids)
	  
	  (iadd! index frame @?gn/phone-numbers phone-numbers)
	  (add! result @?gn/phone-numbers phone-numbers)
	  
	  ;; Handle the proper-names
	  (iadd! index frame @?gn/proper-names name-terms)
	  (add! result @?gn/proper-names name-terms)
	  
	  ;; Handle the content words
	  (iadd! index frame @?gn/words content-words)
	  (iadd! index frame @?gn/nouns nouns)
	  (iadd! index frame @?gn/verbs verbs)
	  (iadd! index frame @?gn/adjectives adjectives)
	  
	  ;; Handle the content words
	  (add! result @?gn/words content-words)
	  (iadd! index frame @?gn/nouns nouns)
	  (iadd! index frame @?gn/verbs verbs)
	  (iadd! index frame @?gn/adjectives adjectives)
	  
	  ;; Handle the content words
	  (add! result @?gn/words content-words)
	  (add! result @?gn/nouns nouns)
	  (add! result @?gn/verbs verbs)
	  (add! result @?gn/adjectives adjectives)
	  
	  ;; Handle stemmed keys
	  (add! result @?gn/stemkeys stemkeys)
	  (when (not index) (assert! frame @?gn/stemkeys stemkeys))
	  (if index (index-frame index frame @?gn/stemkeys stemkeys))
	  
	  ;; Drop any history from past nlp analysis
	  (drop! frame '%history
		 (pick (get frame '%history)
		       '{analyze/nlp nlp lexhacks parse extract}))
	  
	  ;; Add current analysis stats to the history
	  (add! (choice result frame)
		'%history (choice (cons 'analyze/nlp start-time)
				  (cons 'nlp (- extract-done clock-start))
				  (cons 'lexhacks (- parse-start clock-start))
				  (cons 'parse (- parse-done parse-start))
				  (cons 'extract (- extract-done parse-done))))
	  
	  (let ((now (elapsed-time)))
	    (logger tracetime
	      "Analyzed " passage " "
	      (if (or (symbol? textslotid)  (oid? textslotid)) textslotid "text")
	      " in " (- extract-done clock-start) "="
	      (- parse-start clock-start) "(l)+"
	      (- parse-done parse-start) "(p)+"
	      (- extract-done parse-done) "(x)")
	    (logger traceresults
	      (do-choices (key (getkeys result))
		(lineout traceresults "\t" key "\t" (get result key)))))
	  
	  result)))))

(define (nlp/unanalyze passage)
  "Removes the traces left by nlp/analyze"
  (retract! passage *nlp-slotids*)
  (drop! passage '%history
	 (pick (get passage '%history)
	       '{analyze/nlp nlp lexhacks parse extract})))

