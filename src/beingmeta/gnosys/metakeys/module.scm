;;; -*- Mode: Scheme; -*-

;;; Copyright (C) 2005-2007 beingmeta, inc.
;;; This program code is proprietary to and a valuable trade secret of 
;;;   beingmeta, inc.  Disclosure or distribution without explicit
;;;   approval is forbidden.

(in-module 'gnosys/metakeys)

;;;; METAKEYS
;;; This is the core metakeys implementation for eFramerD.  The
;;; functions below implement the core functionality for dealing with
;;; natural language queries and concepts.  A few data definitions:
;;;  A 'dterm' is a string which designates a concept or (sometimes) a
;;;   range of concepts.  Two special cases of dterms are 'combo terms' 
;;;   and 'place names'. 
;;;  A 'keytext' is a string comprised of natural language keywords and
;;;   dterms entered by the user or resulting from interaction with 
;;;   with the interface
;;;  A keylist is the result of converting a natural language keytext
;;;   into concepts.  It consists of a the original keytext and
;;;   a list of keyentries describing each component.
;;;  A key entry is a vector consisting of a term from the original query,
;;;  a 'base' (a natural language term) when that term is a dterm,
;;;   a set of possible meanings, and a set of actual meanings.

(use-module 'reflection)
(use-module '{brico gnosys gnosys/nlp morph texttools tagger})
;(use-module '{gnosys/utils/glomming})

;;; Lookup term functions

(load-component "lookupterm.scm")

;;; DTERM functions

(load-component "dterms.scm")

;;; Utility functions for looking at scores

(define (%showscores arg (opt #f))
  (if (table? arg)
      (doseq (key (rsorted (getkeys arg) arg))
	(lineout (get arg key) "\t" key))
      (if (and (vector? arg) (equal? opt (keyentry-base arg)))
	  (%showscores (keyentry-scores arg)))))

(module-export! '%showscores)

;;; A priori scoring

(define (morph-frequency concept language term)
  (try (largest
	(+ (concept-frequency concept language term)
	   (try (cond ((test concept 'type 'noun)
		       (concept-frequency
			concept language (get-noun-root term language)))
		      ((test concept 'type 'verb)
		       (concept-frequency
			concept language (get-verb-root term language)))
		      (else 1))
		1)))
       0))

(define get-apriori-scores
  (ambda (concepts language term)
    (if (singleton? concepts)
	(frame-create #f concepts 1.0)
	(let ((scores (if (> (choice-size concepts) 7)
			  (make-hashtable)
			  (frame-create #f)))
	      (sum 0))
	  (do-choices (c concepts)
	    (let ((cfreq (morph-frequency c language term)))
	      (set! sum (+ sum cfreq))
	      (table-increment! scores c cfreq)))
	  (let ((sum (* 1.0 sum)))
	    (do-choices (c concepts)
	      (store! scores c (/ (get scores c) sum))))
	  scores))))
(module-export! 'get-apriori-scores)

(define normalize-scores
  (ambda (scores (candidates #f) (bias #f))
    (if bias
	(let ((newscores (make-hashtable)))
	  (do-choices (key (or candidates (getkeys scores)))
	    (store! newscores key (* (get scores key) (try (get bias key) 1))))
	  (normalize-scores newscores))
	(let ((sum (reduce-choice + (or candidates (getkeys scores)) 0 scores))
	      (newscores (make-hashtable)))
	  (do-choices (candidate candidates)
	    (store! newscores candidate (/~ (get scores candidate) sum)))
	  newscores))))

;;; Keyentries
;;;  Should use records when they're ready

(define keyentry-term first)
(define keyentry-base second)
(define keyentry-language third)
(define keyentry-possible fourth)
(define keyentry-actual fifth)
(define keyentry-scores sixth)

(define (keyentry-candidates entry)
  (try (keyentry-actual entry) (keyentry-possible entry)))

(define (make-keyentry term base language possible actual (scores #f))
  ;; (lineout "make-keyentry: " term " " base " " language " " possible " " actual " " scores)
  (vector term base language (qc possible) (qc actual)
	  (if (table? scores) scores
	      (and scores (get-apriori-scores possible language base)))))

(define (term->keyentry x (language default-language) (tryhard #f))
  (cond ((< (length x) 2) (fail))
	((char-numeric? (elt x 0)) (fail))
	(else (let ((possible (lookup-term x language tryhard)))
		;; (message "Making keyentry from " (write x) " for " possible)
		(make-keyentry x x language
			       (qc possible)
			       (if (singleton? possible)
				   possible (qc)))))))

(define (variants->keyentry x variants (language default-language) (altcase #f) (scores #f))
  (let ((possible (lookup-term variants language)))
    ;; (message "Making keyentry from " (write x) " and variants " variants " for "possible)
    (make-keyentry x x language
		   (qc possible)
		   (if (singleton? possible)
		       possible (qc))
		   scores)))

(define (concept->keyentry concept language (keys #f))
  (let* ((term (try (tryif (and (exists? keys) keys)
			   (intersection
			    keys
			    (get concept (choice language 'names))))
		    (get-term concept language)))
	 (dterm (get-dterm concept language term)))
    (make-keyentry dterm term language (parse-dterm term language) concept)))

;;; Keyentry operations

(define (keyentry/scored entry (rescore #f))
  (if (and (keyentry-scores entry) (not rescore)) entry
      (make-keyentry
       (keyentry-term entry) (keyentry-base entry)
       (keyentry-language entry)
       (qc (keyentry-possible entry))
       (qc (keyentry-actual entry))
       (if (table? rescore)
	   (normalize-scores rescore (keyentry-candidates entry))
	   (get-apriori-scores
	    (keyentry-candidates entry)
	    (keyentry-language entry)
	    (keyentry-base entry))))))

(define (keyentry/bias entry bias)
  (make-keyentry
   (keyentry-term entry) (keyentry-base entry)
   (keyentry-language entry)
   (qc (keyentry-possible entry))
   (qc (keyentry-actual entry))
   (normalize-scores (keyentry-scores entry) 
		     (keyentry-candidates entry)
		     bias)))

(define (keyentry/disambig entry (disambig #t) (rescore #t))
  (let* ((entry (keyentry/scored entry rescore))
	 (scores (keyentry-scores entry)))
    (make-keyentry
     (keyentry-term entry) (keyentry-base entry)
     (keyentry-language entry)
     (qc (keyentry-possible entry))
     (if disambig
	 (qc (try (keyentry-actual entry)
		  (singleton (keyentry-possible entry))
		  (tryif (not (number? disambig)) (table-max scores))
		  (tryif (inexact? disambig)
			 (if (> disambig 0) (table-skim scores disambig)
			     (tryif (> (table-maxval scores) (- disambig))
				    (table-max scores))))))
	 (qc (keyentry-actual entry)))
     (keyentry-scores entry))))

(defambda (keyentry/useknown entry (known #f) (unique #t))
  "This disambiguates an entry based on overlap with known concepts. \
   If any possible meanings for entry overlap with known, they are
   selected.  If UNIQUE is true, it is required that the overlap
   be unique."
  (if (fail? known) entry
      (if known
	  (for-choices entry
	    (make-keyentry
	     (keyentry-term entry) (keyentry-base entry)
	     (keyentry-language entry)
	     (qc (keyentry-possible entry))
	     (if unique
		 (qc (singleton (intersection (keyentry-possible entry) known)))
		 (qc (intersection (keyentry-possible entry) known)))
	     (keyentry-scores entry)))
	  (keyentry/useknown entry (keyentry-actual entry) unique))))

(defambda (keyentry/reduce entry known (unique #t))
  "This reduces the possible meanings for an entry based on known set of concepts.  \
   The possible concepts are intersected with KNOWN.  This may disambiguate
   the entry if it reduces it to a singleton. "
  (for-choices entry
    (let ((reduced (intersection (keyentry-possible entry) known)))
      (make-keyentry
       (keyentry-term entry) (keyentry-base entry)
       (keyentry-language entry)
       (qc reduced)
       (qc (singleton reduced))
       (keyentry-scores entry)))))

;;; This does the knowledge base prefetch for apriori scoring. 
(defambda (keyentry/apriori-prefetch entries)
  (concept-frequency-prefetch
   (keyentry-possible entries)
   (keyentry-language entries)
   (keyentry-base entries)))

;;; These are kind of legacy operations.

(define (keyentry/apriori entry (scores #t))
  (if (keyentry-scores entry) entry
      (make-keyentry
       (keyentry-term entry) (keyentry-base entry)
       (keyentry-language entry)
       (qc (keyentry-possible entry))
       (qc (keyentry-actual entry))
       scores)))
(define (keyentry/norm entry)
  (if (exists? (keyentry-actual entry)) entry
      (make-keyentry
       (keyentry-term entry) (keyentry-base entry)
       (keyentry-language entry)
       (qc (keyentry-possible entry))
       (qc (singleton (?? (get norm-map (keyentry-language entry))
			  (keyentry-base entry))))
       (keyentry-scores entry))))
(define (keyentry/assign entry concepts (scores #f))
  (if (exists? (keyentry-actual entry)) entry
      (make-keyentry
       (keyentry-term entry) (keyentry-base entry)
       (keyentry-language entry)
       (qc (keyentry-possible entry))
       (qc (intersection (keyentry-possible entry) concepts))
       (or scores (keyentry-scores entry)))))
(define (keyentry/unique entry concepts (scores #f))
  (if (exists? (keyentry-actual entry)) entry
      (make-keyentry
       (keyentry-term entry) (keyentry-base entry)
       (keyentry-language entry)
       (qc (keyentry-possible entry))
       (qc (singleton (intersection (keyentry-possible entry) concepts)))
       (or scores (keyentry-scores entry)))))
(define (keyentry/default entry concepts)
  (if (exists? (keyentry-actual entry)) entry
      (make-keyentry
       (keyentry-term entry) (keyentry-base entry)
       (keyentry-language entry)
       (qc (keyentry-possible entry))
       (qc (intersection (keyentry-possible entry)
			 (?? (keyentry-language entry) (keyentry-base entry))))
       (keyentry-scores entry))))


;;; Keylists

(define keylist-text first)
(define keylist-language second)
(define keylist-entries third)

(define (strings->keylist string language (maximize #t))
  (let ((stringlist (rsorted (difference string "") length)))
    (vector (stringout (doseq (s stringlist) (printout s ";")))
	    language
	    (map (lambda (x) (term->keyentry x language))
		 (->list stringlist)))))

(define (fragments->keylist string language)
  (let ((stringlist (rsorted (difference string "") length))
	(exclude {}) (strings '()) (entries '()))
    (doseq (s stringlist)
      (let ((std (stdstring s)))
	(unless (overlaps? s exclude)
	  (cond ((overlaps? std exclude))
		((position #\Space s)
		 (let ((s (if (exists? (parse-dterm (capitalize s) language))
			      (capitalize s)
			      s)))
		   (when (exists? (parse-dterm s language))
		     (set+! exclude (elts (getwords std)))
		     (set! strings (cons s strings))
		     (set! entries (cons (term->keyentry s language) entries)))))
		((exists? (parse-dterm s language))
		 (set! strings (cons s strings))
		 (set! entries (cons (term->keyentry s language) entries)))
		((exists? (parse-dterm (capitalize s) language))
		 (set! strings (cons (capitalize s) strings))
		 (set! entries (cons (term->keyentry (capitalize s) language) entries)))))))
    (vector (stringout (doseq (s (reverse strings)) (printout s ";")))
	    language
	    (reverse entries))))

(define (concepts->keylist concepts language (keywords #f))
  (let ((entries (concept->keyentry concepts language (qc keywords))))
    (if (fail? entries)
	(vector (qc) language '())
	(vector (stringout (do-choices (entry entries i)
			     (printout (if (> i 0) ";") (keyentry-base entry))))
		language
		(choice->list entries)
		#f))))

(define (string->keylist string language (sepchar #f))
  (if (ambiguous? string)
      (let ((stringlist (reverse (sorted (difference string "") length))))
	(vector (stringout (doseq (s stringlist) (printout s ";")))
		language
		(map (lambda (x) (term->keyentry x language))
		     (->list stringlist))))
      (let ((stopwords (get-stop-words language)))
	(vector string language
		(map (lambda (x) (term->keyentry x language))
		     (remove ""
			     (if sepchar
				 (segment string sepchar)
				 (removeif (get-stop-words language)
					   (keystring-segment string language)))))))))

;;; Breaking up strings, identifying compounds
;;;  This is also probably where spelling correction would go.

(define entrypat '{(not> (isspace+)) #("\"" (char-not "\"") "\"")})

(define (getsegments string (i 0))
  (if (< i (length string))
      (let ((start (textsearch (qc entrypat) string i)))
	(if (and start (< start (length string)))
	    (let ((end (largest (textmatcher (qc entrypat) string start))))
	      (cons (subseq string start end)
		    (getsegments string end)))
	    '()))
      '()))

(define (getgloms language base rest)
  (if (null? rest) (fail)
      (if (and (string? (car rest))
	       (> (length (car rest)) 0)
	       (not (eq? (elt (car rest) 0) #\")))
	  (let ((compound (append base " " (car rest)))
		(compound2 (append base (car rest))))
	    (choice (tryif (exists? (parse-dterm compound language))
			   compound)
		    (tryif (exists? (parse-dterm compound2 language))
			   compound2)
		    (getgloms language compound (cdr rest))))
	  (fail))))

(define (strip-comma string)
  (if (has-suffix string ",")
      (subseq string 0 -1)
      string))

(define (probe-glom segments language)
  (let ((found (car segments)) (remainder (cdr segments)))
    (dotimes (i (- (length segments) 1))
      (let ((glom (stringout (doseq (elt (subseq segments 0 (+ 2 i)) j)
			       (printout (if (> j 0) " ") elt)))))
	(when (exists? (parse-dterm glom language))
	  (set! found glom) (set! remainder (subseq segments (+ 2 i))))))
    (cons found remainder)))

(define (keystring-glom segments language)
  (cond ((null? segments) '())
	;; Skip illegal items
	((or (not (string? (car segments)))
	     (= (length (car segments)) 0))
	 (keystring-glom (cdr segments) language))
	;; Strip quotes from compounds but continue
	((eq? (elt (car segments) 0) #\")
	 (cons (subseq (car segments) 1 -1)
	       (keystring-glom (cdr segments) language)))
	;; In other cases
	(else (let ((glom (probe-glom segments language)))
		(cons (car glom) (keystring-glom (cdr glom) language))))))

(define (keystring-segment string language)
  (keystring-glom (getsegments string) language))

;;;; sentence->keyentries

;;; Using NLP to generate keylists constrained by part of speech information and
;;;  language informed glomming.

(define pos2type (make-hashtable))
(define *noun-tags*
  '{SOLITARY-NOUN NOUN-MODIFIER
    NOUN PLURAL-NOUN
    POSSESSIVE})
(define *verb-tags*
  '{VERB INFINITIVAL-VERB INFLECTED-VERB
	 ING-VERB PASSIVE-VERB})
(define *name-tags*
  '{PROPER-NAME PROPER-MODIFIER PROPER-POSSESSIVE})
(define *adjective-tags*
  '{ADJECTIVE COMPLEMENT-ADJECTIVE NOUN-MODIFIER})

(define (parse->keyentries parse language)
  (let* ((posinfo (frame-create #f))
	 (nametags (gettags parse *name-tags*))
	 ;; (names (choice (phrase-root nametags) (probe-compounds nametags)))
	 (names (phrase-root nametags))
	 (entries {}))
    (do-choices (word (phrase-string (getroots parse *noun-tags*)))
      (add! posinfo word 'noun))
    (do-choices (word (getroots parse *verb-tags*))
      (add! posinfo word 'verb))
    (do-choices (word (getroots parse *adjective-tags*))
      (add! posinfo word 'adjective))
    (prefetch-keys! (choice (cons language (getkeys posinfo))
			    (cons english names)))
    (do-choices (phrase (getphrase parse
				   '({adjective noun-modifier}
				     {noun plural-noun noun-modifier solitary-noun})))
      (let ((meanings (?? language phrase 'type 'noun)))
	(when (exists? meanings)
	  (set+! entries
		 (vector phrase phrase language
			 (qc meanings)
			 (qc (singleton meanings))
			 #f)))))
    (do-choices (word (getkeys posinfo))
      (let ((meanings (?? language word 'type (get posinfo word))))
	(set+! entries
	       (vector word word language
		       (qc meanings)
		       (qc (singleton meanings))
		       #f))))
    (do-choices (name names)
      (let ((meanings (parse-dterm (phrase-root name) language)))
	(set+! entries
	       (vector (phrase-string name) (phrase-root name) language
		       (qc meanings) (qc (singleton meanings))
		       #f))))
    entries))

(define (sentence->keyentries string language (options #f))
  (let* ((start (elapsed-time))
	 (parse (tagger string
			(if options (choice options 'glom) 'glom)))
	 (parsedone (elapsed-time)))
    (prog1 (parse->keyentries parse language)
	   (let ((alldone (elapsed-time)))
	     (logit "SENTENCE->KEYENTRIES took "
		    (- alldone start) "="
		    (- parsedone start) "+"
		    (- alldone parsedone) " secs for "
		    string)))))
(define (sentence->keylist string language (options #f))
  (vector string language
	  (choice->list
	   (parse->keyentries (tagger string (choice options 'glom))
			      language))))

;;;; Combining NL parsing and simple term/phrase recognition.

(define (text->keylist text language (trylower #f))
  ;; Could use compounds explictly here, using the nlp.scm stuff
  (sentence->keylist text language))

(define (texts->keylist texts language)
  (if (empty? texts)
      (vector (qc) language '())
      (let* ((ordered (sorted texts))
	     (keylist (text->keylist (first ordered) language)))
	(dotimes (i (1- (length ordered)))
	  (set! keylist (merge-keylists (text->keylist (elt ordered (1+ i)) language)
					keylist)))
	keylist)))

;;;; Utility functions for keylists

(define (keylist-get-base concept keylist)
  (try (do ((scan (keylist-entries keylist) (cdr scan)))
	   ((or (null? scan)
		(overlaps? concept (keyentry-actual (car scan))))
	    (if (null? scan) (fail)
		(keyentry-base (car scan)))))
       (do ((scan (keylist-entries keylist) (cdr scan)))
	   ((or (null? scan)
		(overlaps? concept (keyentry-possible (car scan))))
	    (if (null? scan) (fail)
		(keyentry-base (car scan)))))))
(define (keylist-get-term concept keylist)
  (try (do ((scan (keylist-entries keylist) (cdr scan)))
	   ((or (null? scan)
		(overlaps? concept (keyentry-actual (car scan))))
	    (if (null? scan) (fail)
		(keyentry-term (car scan)))))
       (do ((scan (keylist-entries keylist) (cdr scan)))
	   ((or (null? scan)
		(overlaps? concept (keyentry-possible (car scan))))
	    (if (null? scan) (fail)
		(keyentry-term (car scan)))))))

(define (keylist-actual keylist)
  (keyentry-actual (elts (keylist-entries keylist))))
(define (keylist-possible keylist)
  (keyentry-possible (elts (keylist-entries keylist))))
(define (keylist-bases keylist)
  (keyentry-base (elts (keylist-entries keylist))))
(define (keylist-terms keylist)
  (keyentry-term (elts (keylist-entries keylist))))

(define (get-keyentry keylist concept)
  (filter-choices (entry (elts (keylist-entries keylist)))
    (overlaps? concept (keyentry-possible entry))))

(define (merge-two-keylists k1 k2)
  (let ((bases (keyentry-base (elts (keylist-entries k1))))
	(terms (keyentry-term (elts (keylist-entries k1))))
	(terms2 (keyentry-term (elts (keylist-entries k2)))))
    (cond ((empty? terms) k2)
	  ((empty? terms2) k1)
	  (else
	   (vector (qc (keylist-text k1) (keylist-text k2))
		   (qc (keylist-language k1) (keylist-language k2))
		   (append (removeif (lambda (e)
				       (and (fail? (keyentry-possible e))
					    (overlaps? (keyentry-base e) terms2)))
				     (keylist-entries k1))
			   (removeif (lambda (e)
				       (or (overlaps? (keyentry-base e) bases)
					   (and (fail? (keyentry-possible e))
						(overlaps? (keyentry-base e) terms))))
				     (keylist-entries k2))))))))

(define (keylist-merger list-of-keylists)
  (cond ((null? list-of-keylists) (vector (qc) (qc) '()))
	((null? (cdr list-of-keylists))
	 (if (ambiguous? (car list-of-keylists))
	     (keylist-merger (choice->list (car list-of-keylists)))
	     (car list-of-keylists)))
	((fail? (car list-of-keylists))
	 (keylist-merger (cdr list-of-keylists)))
	((ambiguous? (car list-of-keylists))
	 (keylist-merger
	  (cons (keylist-merger (choice->list (car list-of-keylists)))
		(cdr list-of-keylists))))
	((ambiguous? (cadr list-of-keylists))
	 (keylist-merger
	  (cons (merge-two-keylists (qc (car list-of-keylists))
				    (keylist-merger (choice->list (cadr list-of-keylists))))
		(cddr list-of-keylists))))
	(else (keylist-merger
	       (cons (merge-two-keylists (qc (car list-of-keylists))
					 (qc (cadr list-of-keylists)))
		     (cddr list-of-keylists))))))

(define (merge-keylists . list-of-keylists)
  (keylist-merger list-of-keylists))

(define (keylist/default keylist concepts)
  (vector (qc (keylist-text keylist))
	  (qc (keylist-language keylist))
	  (map (lambda (e) (keyentry/default e (qc concepts)))
	       (keylist-entries keylist))))

(define (keylist->map keylist)
  (let ((f (frame-create #f)))
    (doseq (e (keylist-entries keylist))
      (store! f (keyentry-term e) (keyentry-actual e)))
    f))

;;;; Expanding metakeys

(define expand-metakey
  (ambda (concepts)
	 (get concepts genls*)
	 (get (get concepts implies) genls*)
	 (get concepts partof*)))

;;; Tags

(define (tag-string x)
  (if (string? x) x (car x)))
(define (tag-concepts tag)
  (if (string? tag) {}
      (if (oid? tag) tag (reject (cdr tag) null?))))
(define (tag-elts tag)
  (if (pair? tag) 
      (choice tag (car tag) (reject (cdr tag) null?))
      tag))

(define (keyentry->tag entry)
  (cons (keyentry-term entry) (qc (try (keyentry-actual entry) '()))))

;;; Exports


(module-export!
 '{
   ;; breaks up a string and returns a keylist
   string->keylist
   ;; parses a sentence and generates a keylist including part-of-speech
   ;; constraints
   sentence->keylist
   ;; these functions just return a choice of keyentries
   parse->keyentries sentence->keyentries
   ;; combines NL parsing and simple scanning
   text->keylist texts->keylist
   ;; Takes initial concepts and creates a keylist from them
   concepts->keylist
   ;; Takes a string (or a quoted choice of strings) and generates a keylist
   strings->keylist
   ;; Takes a choice of strings and returns a keylist of recognized terms,
   ;;  ignoring terms which are subwords of other recognized terms
   fragments->keylist
   ;; accessors for keylist fields
   keylist-text keylist-language keylist-entries
   ;; Merging keylists
   merge-keylists})
(module-export!
 ;; constructors and accessors for keyentry fields
 '{make-keyentry
   term->keyentry
   variants->keyentry
   concept->keyentry
   keyentry-term
   keyentry-base
   keyentry-language
   keyentry-possible
   keyentry-candidates
   keyentry-actual
   keyentry-scores})
(module-export!
 ;; functions on keylist which aggregate keyentry fields
 '{keylist-get-term keylist-get-base
   keylist-bases keylist-terms
   keylist-actual keylist-possible
   keylist/default keylist->map})
(module-export!
 '{
   keyentry/scored
   keyentry/bias
   keyentry/useknown
   keyentry/reduce
   keyentry/disambig
   keyentry/apriori-prefetch})
;; These are legacy functions, soon to go 
(module-export!
 '{keyentry/apriori
   keyentry/norm
   keyentry/assign
   keyentry/unique
   keyentry/default})
(module-export!
 '{expand-metakey})

(module-export! '{get-wordlist get-norm-wordlist morph-frequency})

(module-export! '{tag-concepts
		  tag-string
		  tag-elts
		  keyentry->tag})

;;; Dead code

;; (define (recompound list)
;;   (stringout (doseq (word list i)
;; 	       (printout (if (> i 0) " ") word))))
;; (define (deglomify string)
;;   (if (position #\Space string)
;;       (apply append (segment string " "))
;;       (recompound (deglom string))))

