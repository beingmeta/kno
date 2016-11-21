;;; -*- Mode: Scheme; Character-encoding: utf-8;  -*-
;;; This is a version of the parser for FDScript.  It is slow, but is
;;; intended primarily for development with reliance on muparse and
;;; compiled tables for fast execution.

(use-module '{texttools side-effects reflection optimize})

(define trace #f)

(define (cdddr x) (cdr (cdr (cdr x))))

;;;; Dictionary access and tools

(define use-morphology #t)

(define dictionary #f)
(define fragments #f)

(define (word-downcase x)
  (if (string? x) (downcase x)
    (and (pair? x) (map downcase x))))

(define (posp string)
  (if (and (string? string) (capitalized? string))
      (try (get dictionary string)
	   (get dictionary (word-downcase string)))
      (get dictionary string)))
(define (known-word? word)
  (if (pair? word) #t (exists? (posp word))))

(define (word-suffix? suffix word)
  (if (string? word) (has-suffix word suffix)
    (and (pair? word)
	 (let ((final (elt word (1- (length word)))))
	   (or (has-suffix (elt word 0) suffix)
	       (has-suffix final suffix))))))

(define (has-part-of-speech word tag (plus 0))
  (try (+ plus (cdr (filter-choices (weight (get dictionary word))
		      (eq? tag (car weight)))))
       ;; If the word is capitalized, we increment it's non capitalize
       ;; parts of speech by one.
       (tryif (capitalized? word)
	      (+ 2 (cdr (filter-choices
			    (weight (get dictionary (downcase word)))
			  (eq? tag (car weight))))))
       #f))

(define (suffix-rule string suffix . args)
  (and (string? string)
       (let* ((slength (length string))
	      (offset (- slength (length suffix)))
	      (replacement (if (null? (cdr args)) #f (car args)))
	      (test (if (null? (cdr args)) (car args) (cadr args))))
	 (and (> offset 0)
	      (equal? (subseq string offset slength) suffix)
	      (if replacement
		  (test (append (subseq string 0 offset) replacement))
		  (test (subseq string 0 offset)))))))


;;;; Defining new arc types

(define (transform-test test)
  (if (number? (car test))
      `(let ((weight ,(cadr test)))
	 (and weight (exists? weight)
	      (if (number? weight) (+ weight ,(car test))
		  ,(car test))))
      `(let ((weight ,test))
	 (and (exists? weight) weight))))


(define define-word-arc-type
  (macro expr
    `(begin (define ,(cadr expr)
	      (or ,@(map transform-test (cddr expr))))
	    (clear-arc-cache!))))
(define define-word-arc-predicate
  (macro expr
    `(begin (define ,@(cdr expr)) (clear-arc-cache!))))


;;;; Representing state machines

(define all-arcs '())
(define all-nodes '())
(define node-properties (make-hashtable))
(define node-links (make-hashtable))

(define (substate-name substate)
  (let ((head (car substate)))
    (if (pair? head) (car head) head)))

(define (declare-state! name properties links substates)
  (let ((substate-names (map substate-name substates)))
    (add! node-properties name (elts properties))
    (unless (position name all-nodes) (set! all-nodes (cons name all-nodes)))
    (dolist (link links)
      (let ((test (car link))
	    (target (cadr link))
	    (args (cddr link)))
	(unless (or (eq? test 'epsilon) (position test all-arcs))
	  (set! all-arcs (cons test all-arcs)))
	(if (position target substate-names)
	    (set! target (list name target)))
	(add! node-links name (cons* test target args))))
    (dolist (substate substates)
      (let ((state-name (list name (substate-name substate))))
	(if (pair? (car substate))
	    (add! node-properties
			    state-name (elts (cdr (car substate)))))
	(unless (position state-name all-nodes)
	  (set! all-nodes (cons state-name all-nodes)))
	(dolist (link (cdr substate))
	  (let ((test (car link))
		(target (cadr link))
		(args (cddr link)))
	    (unless (or (eq? test 'epsilon) (position test all-arcs))
	      (set! all-arcs (cons test all-arcs)))
	    (if (position target substate-names)
		(set! target (list name target)))
	    (add! node-links state-name
		  (cons* test target args))))))))

(define define-state-machine
  (macro expr
    (let ((spec (second expr))
	  (links (third expr))
	  (substates (cdddr expr)))
      (let ((name (car spec)) (props (cdr spec)))
	`(declare-state! ',name ',props ',links ',substates)))))


;;;; Expanding states

(define (state-node s) (elt s 0))
(define (state-input s) (elt s 1))
(define (state-distance s) (elt s 2))
(define (state-origin s) (elt s 3))
(define (state-done? s) (elt s 4))
(define (state-word s) (elt s 5))

(define (make-state state-space node input distance from-state word)
  (let ((known (get (car state-space) (cons input node))))
    (when trace
      (lineout "Push state " node " @" input
	       ":" (car from-state) " (" distance ") "))
    (if (empty? known)
	(let ((new (vector node input distance from-state #f word)))
	  (when trace
	    (lineout "New state " node " @" input " (" distance ") "))
	  (add! (car state-space) (cons input node) new)
	  (insert-state! new state-space))
	(when (< distance (elt known 2)) ; (state-distance known)
	  (vector-set! known 2 distance)
	  (vector-set! known 3 from-state)
	  (vector-set! known 5 word)
	  (insert-state! known state-space)))))

(define (make-state-space input)
  (list (make-hashtable) input
	(vector '*start-state* 0 0 #f #f)))

(define (insert-state! state state-space)
  (let ((prev (cdr state-space))
	(current (cddr state-space))
	(d (elt state 2))) ; (state-distance state)
    (until (or (null? current)
	       (> (elt (car current) 2) d))
	   (set! prev current) (set! current (cdr prev)))
    (set-cdr! prev (cons state (cdr prev)))))

(define arc-cache (make-hashtable))
(define (clear-arc-cache!) (set! arc-cache (make-hashtable)))

(define (phrase->string wordlist)
  (stringout (doseq (word wordlist i)
	       (unless (equal? word "")
		 (printout (if (and (> i 0)
				    (not (char-punctuation? (elt word 0))))
			       " ")
			   word)))))
(define (test-arc predicate input)
  (try (get arc-cache (cons predicate input))
       (let* ((string (if (pair? input) (phrase->string input)
			  input))
	      (computed ((eval predicate) string)))
	 (store! arc-cache (cons predicate input) computed)
	 computed)))
(define (xzero? x) (and (number? x) (zero? x)))
(define (zero-arc predicate input)
  (satisfied? (xzero? (test-arc predicate input))))

(define (sentence-ref sentence i)
  (if (< i (length sentence)) (elt sentence i)
      (fail)))
(define (probe-compounds base inputs i)
  (let* ((word (sentence-ref inputs i))
	 (variants
	  (if (or (= i 0) (overlaps? (sentence-ref inputs (1- i))
				     {"." "?" ";" ":" "!"})
		  (uppercase? word))
	      (choice word (downcase word))
	      word))
	 (compound (append base " " variants))
	 (fragmentp (and fragments
			 (hashset-get fragments compound))))
    ;; (lineout "probing compound: " compound)
    (tryif (exists? word)
	   (choice (tryif (or (exists? (get dictionary compound))
			      (exists? (verb-root compound))
			      (exists? (noun-root compound)))
			  compound)
		   (tryif fragmentp
			  (probe-compounds compound inputs (1+ i)))))))
(define (probe-compounds base inputs i)
  (tryif (has-part-of-speech base 'prefix)
	 (->list (get dictionary (cons base (sentence-ref inputs (1+ i)))))))
(define (get-inputs inputs i)
  (let* ((word (sentence-ref inputs i))
	 (variants
	  (if (or (= i 0) (overlaps? (sentence-ref inputs (1- i))
				     {"." "?" ";" ":" "!"})
		  (uppercase? word))
	      (choice word (downcase word))
	      word)))
    (choice (tryif (overlaps? quote-chars word) (glob-quote inputs (1+ i)))
	    variants
	    (tryif (and (capitalized? word)
			(identical? (sentence-ref inputs (1+ i)) "."))
		   (list word "."))
	    (probe-compounds variants inputs i))))

(define quote-chars
  (elts "'\"`\u00AB\u00BB\u2018\u2019\u201c\u201d\u2039\u203a"))
(define (glob-quote wordlist i)
  (do ((j i (1+ j)) (lim (length wordlist)))
      ((or (>= i lim)
	   (overlaps? (elt wordlist j) quote-chars))
       (if (>= i lim) (fail)
	   (append (subseq wordlist i j) '(""))))))

(define (noise x)
  (cond ((some? char-alphanumeric? x) #f)
	((or (position #\' x) (position #\" x)) 1)
	(else #f)))

(define (expand-link link state state-space)
  (when trace
    (lineout "Expanding state " (state-node state)
	     "@" (state-input state)
	     " via " link))
  (let* ((inputs (cadr state-space))
	 (input-choice (try (get-inputs inputs (state-input state))
			    #f))
	 (predicate (car link))
	 (target (cadr link))
	 (extra (if (null? (cddr link)) 0 (third link))))
    (do-choices (input input-choice)
      (let* ((term (if (pair? input) (phrase->string input) input))
	     (weight (cond ((eq? predicate 'epsilon) 0)
			   ((not term) #f)
			   ((string? predicate)
			    (and (equal? predicate term) 0))
			   (use-morphology (test-arc predicate term))
			   (else #f))))
	(if (eq? weight #t) (set! weight 0))
	(when trace
	  (lineout "\t" predicate "(\"" term "\")=" weight))
	(when weight
	  (make-state state-space
		      (cadr link)
		      (if (eq? predicate 'epsilon)
			  (state-input state)
			(if (string? input) (+ 1 (state-input state))
			  (+ (length input) (state-input state))))
		      (+ (state-distance state) weight extra)
		      (cons predicate state)
		      (if (eq? predicate 'epsilon) #f term)))))))

(define (expand-state state state-space)
  (unless (state-done? state) 
    (when trace
      (lineout "Expanding state " (state-node state)
	       " @" (state-input state) ":"
	       (get-inputs (cadr state-space) (state-input state))
	       " (" (state-distance state) ")"))
    (let* ((inputs (cadr state-space))
	   (input (sentence-ref inputs (state-input state)))
	   (noise-weight (noise input)))
      (when (and (exists? noise-weight) noise-weight)
	(make-state state-space
		    (state-node state) (+ 1 (state-input state))
		    (+ (state-distance state) noise-weight)
		    (cons 'noise state)
		    input)))
    (do-choices (link (get node-links (state-node state)))
      (expand-link link state state-space))
    (vector-set! state 4 #t)))

(define (terminal-state? s state-space)
  (and (>= (state-input s) (length (cadr state-space)))
       (overlaps? 'terminal
		  (get node-properties (state-node s)))))


;;;; Searching the state space

(define (step-space state-space)
  (if (null? (cddr state-space)) 'fail
      (let ((top (third state-space)))
	(set-cdr! (cdr state-space)
		  (cdr (cdr (cdr state-space))))
	(if (terminal-state? top state-space)
	    top
	    (begin (expand-state top state-space)
		   (vector-set! top 4 #t)
		   #f)))))

(define (explore-space state-space)
  (let ((probe (step-space state-space)))
    (until probe (set! probe (step-space state-space)))
    probe))

(define (extract-history state)
  (let ((from (state-origin state)))
    (if from
	(if (eq? (car from) 'epsilon)
	    (cons (list (if (symbol? (state-node state))
			    (state-node state)
			    (elt (state-node state) 0))
			(state-distance state))
		  (extract-history (cdr from)))
	  (cons (list (state-node state)
		      (state-word state)
		      (state-distance state)
		      (car from))
		(extract-history (cdr from))))
      '())))
(define (extract-tags state)
  (let ((from (state-origin state)))
    (if from
	(if (eq? (car from) 'epsilon)
	    (extract-tags (cdr from))
	  (cons (list (state-word state) (car from) (state-distance state))
		(extract-tags (cdr from))))
      '())))
(define (segment-paragraphs string)
  (stringout
      (doseq (elt (segment string {"\n\n" "\r\n\r\n"}) i)
	(printout (if (> i 0) " | ") elt))))
(define (segment-text text)
  (let* ((wordlist (map stdspace
			(getwords (segment-paragraphs text) #t)))
	 (nullless (remove "" wordlist))
	 (merged (merge$ nullless)))
    (if (capitalized? (car merged))
	(cons (qc (car merged) (downcase (car merged)))
	      (cdr merged))
	merged)))

(define (merge$ words)
  (cond ((null? words) '())
	((null? (cdr words)) words)
	((or (equal? (car words) "$")
	     (and (equal? (cadr words) ".")
		  (or (position #\. (car words))
		      (and (capitalized? (car words))
			   (= (length (car words)) 1)))))
	 (if (null? (cdr (cdr words)))
	     (list (append (car words) (cadr words)))
	     (cons* (append (car words) (cadr words))
		    (qc (car (cddr words))
			(downcase (car (cddr words))))
		    (merge$ (cdr (cddr words))))))
	((and (= (length (car words)) 1)
	      (not (equal? (car words) ","))
	      (char-punctuation? (elt (car words) 0))
	      (capitalized? (cadr words)))
	 (cons (car words)
	       (cons (qc (cadr words) (downcase (cadr words)))
		     (merge$ (cddr words)))))
	((and (capitalized? (cadr words)) 
	      (has-suffix (car words) "."))
	 (cons (car words)
	       (cons (qc (cadr words) (downcase (cadr words)))
		     (merge$ (cddr words)))))
	(else (cons (car words) (merge$ (cdr words))))))

(define (dotag input)
  (let ((space (make-state-space (qc input))))
    (let ((state (difference (explore-space space) 'fail)))
      (reverse (extract-tags state)))))
(define (tagit . words)
  (if (null? (cdr words))
      (let ((space (make-state-space
		    (->vector (segment-text (car words))))))
	(let ((state (difference (explore-space space) 'fail)))
	  (reverse (extract-tags state))))
    (let ((space (make-state-space (->vector words))))
      (let ((state (difference (explore-space space) 'fail)))
	(reverse (extract-tags state))))))

(define (traceit . words)
  (if (null? (cdr words))
      (let ((space (make-state-space
		    (->vector (segment-text (car words))))))
	(let ((state (difference (explore-space space) 'fail)))
	  (reverse (extract-history state))))
    (let ((space (make-state-space (->vector words))))
      (let ((state (difference (explore-space space) 'fail)))
	(reverse (extract-history state))))))

(define (compare-taggings t1 t2)
  (cond ((and (null? t1) (null? t2)) '())
	((null? t1)
	 (map (lambda (x) `(,(car x) (#f ,(cadr x))))
	      t2))
	((null? t2)
	 (map (lambda (x) `(,(car x) (,(cadr t2) #f)))
	       t1))
	((not (equal? (first (car t1)) (first (car t2))))
	 (cons (cons (car t1) (car t2))
	       (compare-taggings (cdr t1) (cdr t2))))
	((equal? (second (car t1)) (second (car t2)))
	 (compare-taggings (cdr t1) (cdr t2)))
	(else
	 (cons
	  `(,(first (car t1)) ,(second (car t1)) ,(second (car t2)))
	  (compare-taggings (cdr t1) (cdr t2))))))

(define (tagtest-compare t1 t2)
  (let ((comparison (compare-taggings t1 t2)))
    (unless (null? comparison)
      (lineout "Tag inconsistency:")
      (pprint comparison)
      (lineout))))

(define (tagtest sentence (expected #f))
  (if expected
      (tagtest-compare (tagit sentence) expected)
      (begin
	(pprint `(tagtest ,sentence ',(tagit sentence)))
	(lineout))))

(define (testit sentence . expected-args)
  (if (null? expected-args)
      `(testit ,sentence ',(tagit sentence))
      (let ((expected (car expected-args))
	    (gotten   (tagit sentence)))
	(do ((e expected (cdr e))
	     (g gotten (cdr g)))
	    ((or (null? e) (null? g)))
	  (unless (equal? (car e) (car g))
	    (lineout ";; " (car e) " is now " (car g))))
	`(testit ,sentence ',(tagit sentence)))))


;;;; Updating the lexicon

(define (get-frags compound)
  (let ((wordvec (words->vector compound))
	(frags {}))
    (dotimes (i (- (length wordvec) 1))
      (set+! frags (->list (subseq wordvec 0 (+ 1 i)))))
    frags))

(define (define-word string tag (weight 0))
  (let ((current (filter-choices (datum (get dictionary string))
		   (eq? (car datum) tag))))
    (drop! dictionary string current)
    (add! dictionary string (cons tag weight))
    (when (compound? string)
      (let ((wordv (words->vector string)))
	(when (> (length wordv) 1)
	  (add! dictionary (first wordv) '(prefix . 0))
	  (add! dictionary (cons (first wordv)  (second wordv))
		wordv))))
    (clear-arc-cache!)))

;;;; Writing data for the C parser

(define (export-node node arcs)
  (let ((export (make-vector (+ 3 (length arcs)) '())))
    (vector-set! export 0 node)
    (vector-set! export 1 (position node all-nodes))
    (vector-set! export 2
		 (if (overlaps? 'terminal (get node-properties node))
		     1 0))
    (let ((links (get node-links node)))
      (do-choices (link links)
	(let* ((tag (car link))
	       (pos (position tag arcs))
	       (current (elt export (+ pos 3))))
	  (vector-set! export (+ pos 3)
		       (cons (cons (if (null? (cddr link)) 0 (third link))
				   (position (cadr link) all-nodes))
			     current)))))
    (dotimes (i (length arcs))
      (unless (null? (elt export (+ i 3)))
	(vector-set! export (+ i 3)
		     (cons (length (elt export (+ i 3)))
			   (elt export (+ i 3))))))
    export))

(define (write-state-machine file root)
  ;; Remove current state machine
  (when (file-exists? file)
    (system "rm " file))
  (if (position root all-nodes)
      (set! all-nodes (cons root (remove root all-nodes)))
      (lineout "Warning: root " root " doesn't exist in grammar"))
  (let ((out (open-output-file file))
	(arcs (cons* 'epsilon 'fragment all-arcs)))
    (dtype->file+ (->vector arcs) file)
    (dtype->file+ (length all-nodes) file)
    (dolist (node all-nodes)
      (dtype->file+ (export-node node arcs) file))))
(define (dump-state-machine root)
  (if (position root all-nodes)
      (set! all-nodes (cons root (remove root all-nodes)))
      (lineout "Warning: root " root " doesn't exist in grammar"))
  (let ((arcs (cons* 'epsilon 'fragment all-arcs)))
    (cons* (->vector arcs) (length all-nodes)
	   (map (lambda (node) (export-node node arcs))
		all-nodes))))

(define (arc-entry word arc)
  (if (eq? arc 'epsilon) 0
      (if (equal? word "") #f
	  (if (eq? arc 'prefix)
	      (if (has-part-of-speech word 'prefix) 0 #f)
	      (if (string? arc) (and (equal? arc word) 0)
		  (if (string? word)
		      (smallest ((eval arc) word))
		      (smallest ((eval arc) (seq->phrase word)))))))))
(define (arc-entry-byte word arc)
  (if (eq? arc 'epsilon) 0
      (if (equal? word "") 255
	  (if (eq? arc 'prefix)
	      (if (has-part-of-speech word 'prefix) 0 255)
	      (if (string? arc) (if (equal? arc word) 0 255)
		  (let ((weight (if (string? word)
				    (smallest ((eval arc) word))
				    (smallest ((eval arc) (seq->phrase word))))))
		    (if (and (number? weight) (>= 255 weight 0))
			weight
			(if (eq? weight #t) 0 255))))))))

(define (lexicon-entry word)
  (->vector (cons* 0 (if (has-part-of-speech word 'prefix) 0 #f)
		   (map (lambda (arc) (arc-entry word arc)) all-arcs))))
(define (make-lexicon-entry . props)
  (->vector (cons* 0 (if (position 'prefix props) 0 #f)
		   (map (lambda (arc)
			  (let ((slist (position arc props)))
			    (if slist (elt props (1+ slist)) #f)))
			all-arcs))))
(define (slotmap->lexentry slotmap)
  (->vector (cons* 0 (if (test slotmap 'prefix) 0 #f)
		   (map (lambda (arc) (try (get slotmap arc) #f))
			all-arcs))))

(define (segment-compound c)
  (if (position #\Space c) (segment c " ")
      (if (has-suffix c ".") (list (subseq c 0 -1) ".")
	  c)))

(define (write-lexicon-entry word index (packet #t))
  ;; (lineout "Writing lexicon entry for " word)
  (let* ((vec (->vector (cons* 0 
			       (if (has-part-of-speech word 'prefix) 0 (if packet 255 #f))
			       (map (if packet (lambda (arc) (arc-entry-byte word arc))
					(lambda (arc) (arc-entry word arc)))
				    all-arcs))))
	 (val (if packet (->packet vec) vec)))
    (if (compound? word)
	(let ((wordvec (words->vector word)))
	  (store! index wordvec vec)
	  (when (> (length wordvec) 1)
	    (add! index (cons (first wordvec) (second wordvec)) wordvec)))
	(store! index word vec))))

(define (lexer-entry c)
  (if (pair? c) c
      (if (position #\Space c) (segment c " ")
	  (if (has-suffix c ".") (list (subseq c 0 -1) ".")
	      (list c)))))

(comment
 (define (write-lexer-data index)
   (do-choices (term (if fragments (hashset-elts fragments) {}))
     (let ((segword (lexer-entry term)))
       (unless (test index segword) (store! index segword #f))))))

(define (pos->terms tags)
  (filter-choices (term (getkeys dictionary))
    (overlaps? (car (get dictionary term)) tags)))

(define (optimize-chopper)
  (optimize! word-downcase posp known-word? word-suffix?
	     has-part-of-speech suffix-rule transform-test)
  (optimize! substate-name declare-state! cdddr phrase->string
	     make-state insert-state! test-arc xzero? zero-arc
	     state-node state-input state-distance state-origin
	     state-done? state-word)
  (optimize! expand-link expand-state terminal-state?
	     step-space explore-space extract-history
	     extract-tags )
  (optimize! sentence-ref probe-compounds get-inputs
	     glob-quote noise segment-paragraphs segment-text
	     merge$)
  (optimize! dotag tagit traceit compare-taggings
	     tagtest-compare tagtest testit)
  (optimize! get-frags segment-compound
	     export-node write-state-machine dump-state-machine
	     arc-entry lexicon-entry make-lexicon-entry
	     slotmap->lexentry write-lexicon-entry lexer-entry
	     pos->terms))

(define (optimize-arcs)
  (dolist (arc all-arcs)
    (when (symbol? arc)
      (let ((v (eval arc)))
	(when (procedure? v) (optimize! v))))))
