;;; -*- Mode: Scheme; Character-encoding: utf-8;  -*-

(in-module 'gnosys/disambiguate)

(define version "$Id$")

;;;; Tracing support

(define tracedetails #f)
(define traceresults #f)
(define tracetime #t)

(define logger comment)
;;(define logger logif)
(define %volatile '{logger traceresults tracedetails tracetime})

;;; This provides for text disambiguation

(use-module '{reflection texttools tagger})
(use-module '{brico brico/lookup})
(use-module '{morph gnosys parsetime rulesets ezrecords})

(module-export!
 '{
   make-ambentry     ;; Basic constructor, mostly used internally
   modify-ambentry   ;; For changing the things which change
   word->ambentry    ;; Make an ambentry for a word
   probe->ambentry    ;; Make an ambentry for a word if it has any meanings
   term->ambentry    ;; Make an ambentry for a term, possibly analyzing compounds
   ;; Makes an ambentry for a particular concept, using its norm as the term
   concept->ambentry
   ;; Make an ambentry for a compound term with explicit alternate phrasings
   alt->ambentry     
   ;; "Record" accessors
   ambentry-term ambentry-word ambentry-language
   ambentry-possible ambentry-resolved ambentry-resolved?
   ;; Accessors used in disambiguation
   ambentry-scores ambentry-candidates
   })

(module-export!
 '{ambentry/given     ;; Disambiguates against a known set of concepts
   ambentry/reduce    ;; Reduces meanings based on a known set of concepts
   ambentry/include   ;; Applies a custom term mapping to a collection of entries
   ambentry/exclude   ;; Applies a custom term mapping to a collection of entries
   ambentry/handmap   ;; Applies a custom term mapping to a collection of entries
   ambentry/scored    ;; Computes or recomputes a score for an entry
   ambentry/bias      ;; Applies a scoring map as a bias to the current scores
   ambentry/disambig  ;; Uses scores to disambiguate an entry in various ways
   ambentry/usefreq   ;; Use frequency information to reduce the number of possible meanings
   })

(module-export!
 '{disambiguate               ;; Takes a set of entries and disambiguates them together
   contextual-score           ;; Computes a "connection score" among a set of entries
   get-apriori-scores         ;; Gets scores based on word/concept frequency
   ambentry/apriori-prefetch! ;; Prefetch for get-apriori-scores
   disambiguate-prefetch!     ;; Prefetcher for disambiguate
   })

;;; Useful functions for other purposes
(module-export!
 '{disambig/correlator disambig/correlator* disambig/coincidences disambig/correlate})


;;; AMBENTRY data structure
;;;  Should use records when they're ready

(defrecord ambentry
  ;; This is the string whose ambiguities are described
  term
  ;; This is the base word for the term
  word
  ;; This is the language for the term/word
  language
  ;; These are all the possible meanings for the term
  possible
  ;; These are the meanings determined to actually apply
  resolved
  ;; If not #f, this is a table scoring possible meanings in [0,1]
  scores)

;; All the possible meanings should have AMBENTRY-WORD as words
;; The scores should be normalized so that all the scores add up to 1.0

;; These are almost accessors in that they do no significant computation
(define (ambentry-candidates entry)
  (try (ambentry-resolved entry) (ambentry-possible entry)))
(define (ambentry-resolved? entry) (exists? (ambentry-resolved entry)))

;; Term and scores should be singletons
(defambda (make-ambentry term word language possible resolved (scores #f))
  "Create an AMBENTRY from components"
  (cons-ambentry term word language possible resolved
		 (if (table? scores) scores
		     (and scores (get-apriori-scores possible language word)))))

(defambda (modify-ambentry entry possible actual (scores #f))
  "Modify an AMBENTRY's possible or resolved meanings or it scores"
  (cons-ambentry (ambentry-term entry)
		 (ambentry-word entry)
		 (ambentry-language entry)
		 possible actual
		 (if (table? scores) scores
		     (if scores
			 (get-apriori-scores possible
					     (ambentry-language entry)
					     (ambentry-word entry))
			 (ambentry-scores entry)))))


;;; Generating an AMBENTRY from a word or term

(define (word->ambentry x (language default-language) (tryhard #t))
  "Generates an AMBENTRY from a string using LOOKUP-WORD"
  (cond ((< (length x) 2) (fail))
	((char-numeric? (elt x 0)) (fail))
	(else (let ((possible (lookup-word x (or language default-language) tryhard)))
		;; (message "Making ambentry from " (write x) " for " possible)
		(make-ambentry x x (or language default-language)
			       possible (singleton possible))))))
(define (probe->ambentry x (language default-language) (tryhard #t))
  "Generates an AMBENTRY from a string using LOOKUP-WORD"
  (cond ((< (length x) 2) (fail))
	((char-numeric? (elt x 0)) (fail))
	(else (let ((possible (lookup-word x (or language default-language) tryhard)))
		;; (message "Making ambentry from " (write x) " for " possible)
		(tryif (exists? possible)
		       (make-ambentry x x (or language default-language)
				      possible (singleton possible)))))))

(define (term->ambentry x (language default-language) (tryhard #t))
  "Generates an AMBENTRY from a string, possibly compound, using PARSE-TERM"
  (cond ((< (length x) 2) (fail))
	((char-numeric? (elt x 0)) (fail))
	(else (let ((possible (parse-term x (or language default-language)
					  tryhard)))
		(if (exists? possible)
		    (make-ambentry x (car possible) (or language default-language) 
				   (cdr possible) (singleton (cdr possible)))
		    (make-ambentry x x (or language default-language)
				   (cdr possible) (singleton (cdr possible))))))))

(defambda (alt->ambentry x alt (language default-language) (tryhard #t))
  "Generates an AMBENTRY from a string and some alternative strings.  \
   The string maybe a compound, and possible meanings are found using LOOKUP-TERM."
  (cond ((< (length x) 2) (fail))
	((char-numeric? (elt x 0)) (fail))
	(else (let* ((possible (parse-term (choice x alt)
					   (or language default-language)
					    tryhard))
		     (asfound (try (car possible) x))
		     (meanings (cdr possible)))
		(make-ambentry x asfound (or language default-language)
			       meanings (singleton meanings))))))

(define (concept->ambentry concept (language default-language))
  (let ((norm (get-norm concept language)))
    (make-ambentry norm norm language concept concept)))


;;; AMBENTRY operations

(defambda (ambentry/reduce entry given (unique #t))
  "This reduces the possible meanings for an entry based on known set of concepts.  \
   The possible concepts are intersected with KNOWN.  This may disambiguate
   the entry if it reduces it to a singleton. "
  (for-choices entry
    (let ((reduced (if (hashset? given)
		       (pick (ambentry-possible entry) given)
		       (intersection (ambentry-possible entry) given))))
      (modify-ambentry entry reduced (singleton reduced)))))

(defambda (ambentry/given entry (given #f) (unique #t))
  "This disambiguates an entry based on overlap with known concepts. \
   If any possible meanings for entry overlap with GIVEN, they are \
   selected.  If UNIQUE is true, it is required that the overlap \
   be unique.  Unlike AMBENTRY/REDUCE, this does not reduce the \
   possible meanings, but only resolved meanings."
  (if (fail? given) entry
      (let ((given (or given (ambentry-resolved entry))))
	(for-choices entry
	  (if (exists? (ambentry-resolved entry)) entry
	      (let ((actual (intersection (ambentry-candidates entry) given)))
		(modify-ambentry
		 entry (ambentry-possible entry)
		 (if unique (singleton actual) actual))))))))

(defambda (ambentry/include entry (given #f) (unique #t))
  "This disambiguates an entry based on overlap with known concepts. \
   If any possible meanings for entry overlap with GIVEN, they are \
   selected.  If UNIQUE is true, it is required that the overlap \
   be unique.  Like AMBENTRY/REDUCE, this modifies the possible meanings \
   but only does so if that set of meanings is non-empty."
  (if (fail? given) entry
      (let ((given (or given (ambentry-resolved entry))))
	(for-choices entry
	  (if (exists? (ambentry-resolved entry)) entry
	      (let ((newposs (intersection (ambentry-candidates entry) given)))
		(if (exists? newposs)
		    (modify-ambentry entry newposs (if unique (singleton newposs) newposs))
		    entry)))))))

;; Future version could take predicate functions or key/value pairs for exclude.
(defambda (ambentry/exclude entry (exclude #f) (unique #t))
  "This disambiguates an entry based on excluding certain concepts. \
   If UNIQUE is true, it is required that the overlap be unique.  \
   This is like AMBENTRY/REDUCE in that it does not modify \
   the set of possible meanings."
  (if (fail? exclude) entry
      (for-choices entry
	(if (exists? (ambentry-resolved entry)) entry
	    (let ((actual (difference (ambentry-candidates entry) exclude)))
	      (modify-ambentry
	       entry (ambentry-possible entry)
	       (if unique (singleton actual) actual)))))))

;;; Frequency disambiguation

(defambda (reduce-possible possible opts (asvec #f))
  (let* ((topn (getopt opts 'topn #f))
	 (absthresh (getopt opts 'absthresh #f))
	 (maxthresh (getopt opts 'maxthresh 0.33))
	 (sumthresh (getopt opts 'maxthresh #f))
	 (freqfn (getopt opts 'freqfn getabsfreq))
	 (freqs (make-hashtable))
	 (freqmax 0)
	 (freqsum 0))
    (do-choices (p possible)
      (let ((freq (try (freqfn p) 1)))
	(store! freqs p freq)
	(when (> freq freqmax) (set! freqmax freq))
	(set! freqsum (+ freqsum freq))))
    (let* ((thresh
	    (largest (choice
		      (tryif absthresh absthresh)
		      (tryif maxthresh (* freqmax maxthresh))
		      (tryif sumthresh (* freqsum sumthresh)))))
	   (reduced (pick possible freqs > thresh)))
      (if asvec
	  (subseq (rsorted reduced freqs)
		  0 (and topn (min topn (choice-size reduced))))
	  (if (and topn (> (choice-size reduced) topn))
	      (elts (rsorted reduced freqs) 0 topn)
	      reduced)))))

(define (ambentry/usefreq entry (opts #[]) (resolve #t))
  "This uses frequency information to disambiguate an entry.   If \
   RESOLVE is true, the ambentry-resolved field is modified.  Otherwise \
   the AMBENTRY-POSSIBLE field is modified and (if it's a singleton) \
   AMBENTRY-RESOLVED may be changed.  Options include: \
    FREQFN: the function used for concept frequency, defaults to GETABSFREQ
    ABSTHRESH: a fixed threshold on frequency for a concept to be accepted
    SUMTHRESH: a ratio (< 1) of the total of all frequencies to be used as a threshold \
    MAXTHRESH: a ratio (< 1) of the largest frequency to be used as a threshold \
    TOPN: number of top results to return (defaults to all)"
  (let ((reduced (reduce-possible (ambentry-possible entry) opts)))
    (if (fail? reduced) entry
	(if resolve
	    (modify-ambentry entry (ambentry-possible entry) reduced)
	    (modify-ambentry entry reduced (singleton reduced))))))

;;; Other disambiguators

(defambda (ambentry/handmap entry handmap (language #f) (require #f))
  "This disambiguates a set of entries based on a table (the handmap) \
   explicilty mapping terms, term-concept pairs, or term-term pairs \
   into concepts.  If language is specified, it is assumed to be the \
   language for handmap, otherwise, "
  (if (fail? handmap) entry
      (let ((lang (or language default-language))
	    (contexta (ambentry-resolved entry))						
	    (contextb (for-choices entry
			(choice (get-noun-root (ambentry-word entry)
					       (ambentry-language entry))
				(get-verb-root (ambentry-word entry)
					       (ambentry-language entry))
				(ambentry-word entry)))))
	(for-choices entry
	  (if (or (ambentry-resolved? entry)
		  (and language (not (eq? (ambentry-language entry) language))))
	      entry
	      (let* ((word (ambentry-word entry))
		     (wordlang (ambentry-language entry))
		     (roots (choice (get-noun-root word wordlang)
				    (get-verb-root word wordlang)
				    word))
		     (meanings (pickoids
				(try (tryif language
					    (get handmap (cons* language roots contexta))
					    (get handmap (cons* language roots contextb))
					    (get handmap (cons language roots)))
				     (get handmap (cons roots contexta))
				     (get handmap (cons roots contextb))
				     (get handmap roots)))))
		(if (exists? meanings)
		    (ambentry/include entry meanings #t)
		    (if require (fail) entry))))))))

(define (ambentry/scored entry (rescore #f))
  "Computes a score for ENTRY or normalizes an existing score"
  (if (table? rescore)
      (modify-ambentry
       entry (ambentry-possible entry) (ambentry-resolved entry)
       (normalize-scores rescore (ambentry-candidates entry)))
      (if (and (ambentry-scores entry) (not rescore)) entry
	  (modify-ambentry
	   entry (ambentry-possible entry) (ambentry-resolved entry)
	   (get-apriori-scores
	    (ambentry-candidates entry)
	    (ambentry-language entry)
	    (ambentry-word entry))))))

;;; Biasing and disambiguating

(define (ambentry/bias entry bias)
  "Applies the table BIAS to the scores within ENTRY .
   This multiples the original scores by bias or zero."
  (modify-ambentry
   entry (ambentry-possible entry) (ambentry-resolved entry)
   (normalize-scores (ambentry-scores entry) 
		     (ambentry-candidates entry)
		     bias)))

(define (ambentry/disambig entry (disambig #t))
  (let* ((entry (ambentry/scored entry))
	 (scores (ambentry-scores entry)))
    (modify-ambentry
     entry (ambentry-possible entry)
     (if disambig
	 (try (ambentry-resolved entry)
	      (singleton (ambentry-possible entry))
	      (tryif (not (number? disambig)) (table-max scores))
	      (tryif (inexact? disambig)
		     (if (> disambig 0) (table-skim scores disambig)
			 (tryif (> (table-maxval scores) (- disambig))
				(table-max scores)))))
	 (ambentry-resolved entry)))))


;;; Score related functions

(defambda (normalize-scores scores (candidates #f) (bias #f))
  (if bias
      (let ((newscores (make-hashtable))
	    (candidates (or candidates (getkeys scores)))
	    (default-bias (/~ (choice-size candidates)))
	    (sum 0))
	(do-choices (key candidates)
	  (let ((new-score (* (get scores key) (try (get bias key) default-bias))))
	    ;; Not sure what the bias should be if it's not in
	    ;;  BIAS.  1 is almost certainly wrong but 0 might
	    ;; not be the right thing either.
	    (store! newscores key new-score)
	    (set! sum (+ sum new-score))))
	(hashtable-multiply! newscores candidates (/ 1.0 sum))
	newscores)
      (let ((sum (reduce-choice + (or candidates (getkeys scores)) 0 scores))
	    (newscores (make-hashtable)))
	(do-choices (candidate candidates)
	  (store! newscores candidate (/~ (get scores candidate) sum)))
	newscores)))

(defambda (get-apriori-scores concepts language term)
  (if (singleton? concepts)
      (frame-create #f concepts 1.0)
      (let ((scores (if (> (choice-size concepts) 7)
			(make-hashtable)
			(frame-create #f)))
	    (sum 0))
	(do-choices (c concepts)
	  (let ((cfreq (ilog (largest (concept-frequency c language term)))))
	    (set! sum (+ sum cfreq))
	    (table-increment! scores c cfreq)))
	(let ((sum (* 1.0 sum)))
	  (do-choices (c concepts)
	    (store! scores c (/ (get scores c) sum))))
	scores)))

(define (inv-weight slotid value)
  "Returns a number in [0,1] based on the frequency of a given slot value"
  ;; We use max to avoid negative values with incredibly common slot values
  (max 0.0 (- 1.0 (* .05 (ilog (choice-size (?? slotid value)))))))

(defambda (combine-apriori entries)
  (let ((combined (make-hashtable)))
    (do-choices (scores (pick (ambentry-scores entries) table?))
      (do-choices (key (getkeys scores))
	(let ((score (get scores key))
	      (max (try (get combined key) 0)))
	  (when (> score max) (store! combined key score)))))
    combined))

;;; This does the knowledge base prefetch for apriori scoring. 
(defambda (ambentry/apriori-prefetch! entries)
  (concept-frequency-prefetch
   (ambentry-possible entries)
   (ambentry-language entries)
   (ambentry-term entries)))


;;;; Context-based disambiguation

(define context-methods '())

(defambda (disambiguate entries (threshold #t))
  (let* ((entries (ambentry/scored (ambentry/given entries)))
	 (cxtscore (contextual-score entries))
	 (biased (ambentry/bias entries cxtscore)))
    (ambentry/disambig biased threshold)))


;;;; Computing contextual scores
;;;    (see algorithm description below)

(defambda (contextual-score
	   entries (justify #f) (methods context-methods))
  "Generates a scoring of all possible meanings among ENTRIES, using contextual\n\
    connections between the entries to determine the score.  When passed JUSTIFY,\n\
    the table will also include the paths which contributed to each concept's score\n\
    and when passed METHODS, it will override the default path methods."
  (let* ((scores (make-hashtable))
	 (entries (ambentry/scored entries))
	 (candidates (ambentry-candidates entries))
	 (apriori (combine-apriori entries)))
    (do-choices (candidate candidates)
      (let* ((pathscores (get-pathscores candidate entries apriori methods))
	     (paths (getkeys pathscores))) ;; (sum 0)
	(store! scores candidate (reduce-choice + paths 0 pathscores))
	(when justify
	  (do-choices (path paths)
	    (add! scores (list candidate)
		  (cons (get pathscores path) path))))))
    scores))

(define (find-paths method source context)
  "Returns the paths from SOURCE to nodes in CONTEXT.  Each path is a vector
   consisting of a weight, a target, and a justification."
  (let ((weight (second method))
	(handler (third method))
	(len (length method)))
    (cond ((slotid? handler)
	   (for-choices (cxtelt context)
	     (tryif (overlaps? source (?? handler cxtelt))
		    (vector (inv-weight handler cxtelt) cxtelt cxtelt))))
	  ((not (applicable? handler)) (fail))
	  ((= len 3)
	   (if (= (procedure-arity handler) 1)
	       (vector weight (qc (intersection (handler source) context)) #f)
	       (handler source (qc context))))
	  ((= len 4) (handler source (qc context) (fourth method)))
	  ((= len 5) (handler source (qc context) (fourth method) (fifth method)))
	  (else (apply handler source (qc context) (->list (subseq method 3)))))))

(defambda (get-pathscores concept entries apriori methods)
  "Returns a table mapping paths to weights, where a path is a method
    and a 'justification' which identifies the node enabling a particular path."
  (for-choices (concept concept)
    (let* ((justifications (make-hashtable))
	   ;; These are all non-competing candidates
	   (context (ambentry-candidates
		     (reject entries ambentry-candidates concept))))
      (doseq (method methods)
	(let ((paths (cachepoint find-paths method concept (qc context))))
	  (do-choices (path paths)
	    (let* ((target (second path))
		   (score (* (or (second method) 1)
			     (first path)
			     (largest (get apriori target))))
		   (key (vector (third path) (first method)))
		   (current (try (get justifications key) 0)))
	      (when (> score current) (store! justifications key score))))))
      justifications)))

;;; New design:
;;;  * works without corpus, using ontology
;;;  * can use corpus information
;;;  * incorporates other arbitrary rules
;;;
;;;  Simple model:
;;;   ap(w,l,c) is an a priori distribution, where
;;;    w is a word, l is a language, and c is a concept
;;;   Expanders map concepts into other related concepts.
;;;   E is a set of expanders, and weight(x,e,i) is
;;;    a weight associated with the expansion x=e(i).

;;;   The algorithm starts with a bunch of possible concepts and
;;;    computes an initial set of apriori scores.  It then
;;;    expands each of those concepts to other concepts and
;;;    multiplies the weight of the expansion by the a priori weight
;;;    of the expansion target.  The resulting contextual scores
;;;    are then multiplied by a priori scores to give a final score
;;;    and this is used to pick the preferred meaning.

;;;   cs(c) is a score function over concepts,
;;;    over w in words, c in concepts, e in expanders,
;;;      over o in e(c)
;;;        cs(o)=cs(o)+ap(w,l,c)*weight(o,e,c)
;;;   then, over w, return c with max cs
;;;   Expanders, for example, take wholes and expand to parts
;;;    or parts and expand to wholes, kinds to kindof, etc.
;;;   Problem is that disambiguation does a lot of fetching


;;; Simple direct methods

(set! context-methods
      (cons* (vector 'REGION 4 'REGION)
	     (vector 'COUNTRY 1 'COUNTRY)
	     (vector @?partof 2 'COUNTRY)
 	     context-methods))


;;;; Coincidence path methods

(define (disambig/coincidences c o slotid)
  (largest
   (for-choices (v (choice c (get c slotid)))
     (let ((coincidence (intersection (?? slotid v) o)))
       (tryif (exists? coincidence)
	      (vector (pick> (* (inv-weight slotid v) (max 1 (ilog (choice-size coincidence)))) 0)
		      (qc coincidence) v))))
   first))
      
(set! context-methods
      (cons* (vector 'IMPLIESMATCH 3 disambig/coincidences implies)
 	     context-methods))
(set! context-methods
      (cons* (vector 'GENLSMATCH 3 disambig/coincidences genls)
 	     context-methods))
; (set! context-methods
;        (cons* (vector 'GENLS*MATCH 2 disambig/coincidences genls*)
;   	     context-methods))
(set! context-methods
      (cons* (vector 'REGIONMATCH 2 disambig/coincidences 'region)
 	     context-methods))
(set! context-methods
      (cons* (vector 'COUNTRYMATCH 2 disambig/coincidences 'country)
 	     context-methods))
(set! context-methods
      (cons* (vector 'PARTOF 2 disambig/coincidences @?partof)
 	     context-methods))


;;;; Correlation path methods

(define (disambig/correlate c1 c2 slotid (index #f))
  (let ((c1hits (if index (find-frames index slotid c1) (?? slotid c1)))
	(c2hits (if index (find-frames index slotid c2)
		    (?? slotid c2))))
    (let ((c1size (choice-size c1hits))
	  (c2size (choice-size c2hits))	  
	  (cosize (choice-size (intersection c1hits c2hits))))
      (if (or (zero? c1size) (zero? c2size)) 0
	  (/~ (* cosize cosize) (* c1size c2size))))))

; (defambda (correlatefn c1hits c2 slotid (index #f))
;   (let ((c2hits (if index (find-frames index slotid c2)
; 		    (?? slotid c2))))
;     (let ((c1size (choice-size c1hits))
; 	  (c2size (choice-size c2hits))	  
; 	  (cosize (choice-size (intersection c1hits c2hits))))
;       (if (or (zero? c1size) (zero? c2size)) 0
; 	  (/~ (* cosize cosize) (* c1size c2size))))))

(defambda (correlatefn c1hits c2hits)
  (let ((c1size (choice-size c1hits))
	(c2size (choice-size c2hits))	  
	(cosize (choice-size (intersection c1hits c2hits))))
    (if (or (zero? c1size) (zero? c2size)) 0
	(/~ (* cosize cosize) (* c1size c2size)))))

(define (disambig/correlator c others slotid (index #f))
  (let ((correlations (make-hashtable))
	(chits (if index (find-frames index slotid c) (?? slotid c)))
	(otherhits (make-hashtable))
	(sum 0.0))
    (when (exists? chits)
      (do-choices (other others)
	(add! otherhits other
	      (if index (find-frames index slotid other) (?? slotid other)))))
    (tryif (exists? chits)
	   (for-choices (other (getkeys otherhits))
	     ;; The result is a vector of a score, the source of the match, and one example
	     (vector (pick> (correlatefn chits (get otherhits other)) 0.0001)
		     other
		     (smallest (intersection chits (get otherhits other))))))))

(define (disambig/correlator* c others slotid (index #f) (expander @?genls*))
  (let* ((correlations (make-hashtable))
	 (expansions (make-hashtable))
	 (c* (cond ((not expander) c)
		   ((test expander 'inverse)
		    (choice c (?? (get expander 'inverse) c)))
		   (else (choice c (get c expander)))))
	 (chits (find-frames index slotid c*))
	 (hitscache (make-hashtable))
	 (sum 0.0))
    (add! expansions c c*)
    (when (exists? chits)
      (do-choices (e c*) (add! hitscache e (find-frames index slotid e)))
      (do-choices (other others)
	(let ((e* (cond ((not expander) other)
			((test expander 'inverse)
			 (choice other (?? (get expander 'inverse) other)))
			(else (choice other (get other expander))))))
	  (add! expansions other e*)
	  (do-choices (e e*)
	    (add! hitscache e (find-frames index slotid e))))))
    (tryif (exists? chits)
	   ;; The result is a vector of a score, the source of the match, and one example
	   (for-choices (c (choice c c*))
	     (for-choices (other others)
	       (for-choices (o (get expansions other))
		 (vector (pick> (correlatefn (get hitscache c) (get hitscache o)) 0.0001)
			 other (smallest (intersection (get hitscache c) (get hitscache o))))))))))

(set! context-methods
      (cons* (vector 'SUMTERMSCORRELATE 8 disambig/correlator sumterms #f)
	     (vector 'REFTERMSCORRELATE 4 disambig/correlator refterms #f)
	     context-methods))

(define (replace-method new current)
  (if (null? current) (list new)
      (if (eq? (first (car current)) (first new))
	  (cons new (cdr current))
	  (cons (car current) (replace-method new (cdr current))))))

(defslambda (add-disambig-method! method)
  (set! context-methods (replace-method method context-methods)))
(module-export! 'add-disambig-method!)

(config-def! 'disambigmethods (ruleset-configfn context-methods))


;;;; Disambiguate prefetching

(define (get-path-args handler i (methods context-methods))
  (if (null? methods) (fail)
      (choice 
       (tryif (eq? (third (car methods)) handler) (elt (car methods) i))
       (get-path-args handler i (cdr methods)))))

(defambda (disambiguate-prefetch! entries (refcheck #f)
				  (methods context-methods))
  (let ((oids (ambentry-candidates entries))
	(path-slotids (pick (elts (map third methods)) oid?))
	(coincidence-slotids (get-path-args disambig/coincidences 3 methods))
	(correlation-slotids (get-path-args disambig/correlator 3 methods))
	(wfbases (tryif (config 'usewordforms #t)
			(ambentry-term entries)))
	(wflanguages (tryif (config 'usewordforms #t)
			    (ambentry-language entries))))
    (prefetch-oids! oids)
    (when refcheck
      ;; Do these prefetches when 'refchecking' whether prefetching
      ;;  gets everything.  Practically, these are one-time fetches
      ;;  that don't grow with the set, so it's not a pain to fetch them.
      (prefetch-oids! coincidence-slotids)
      (prefetch-oids! (get coincidence-slotids '{inverse slots}))
      (prefetch-oids! (get (pick (get coincidence-slotids 'slots) oid?) 'inverse)))
    (when (config 'usewordforms #t)
      (prefetch-keys! (choice (cons 'language wflanguages)
			      (cons 'word wfbases)
			      (cons 'of oids)))
      (prefetch-oids! (?? 'language wflanguages 'word wfbases 'of oids)))
    (prefetch-keys!
     (choice 
      ;; For simple slotid paths
      (cons path-slotids oids)
      ;; For correlation paths
      (cons correlation-slotids oids)
      (cons correlation-slotids (list oids))
      ;; For coincidence paths
      (cons coincidence-slotids oids)
      ;; For computing the coincidence slots themselves, when they have
      ;;  inverses
      (cons (choice (get (pick coincidence-slotids oid?) 'inverse)
		    (get (pick (get (pick coincidence-slotids oid?) 'slots) oid?)
			 'inverse))
	    oids)))
    ;; Now get the indcies we look up
    (prefetch-keys! (cons coincidence-slotids (get oids coincidence-slotids)))))

;;; For debugging prefetching

(comment ;; Replace with begin and eval to define
 (use-module 'trackrefs)
 (define (refcheck-disambig entries)
   (clearcaches) (disambiguate-prefetch! entries #t)
   (trackrefs (lambda () (disambiguate entries))))
 (module-export! 'refcheck-disambig))

