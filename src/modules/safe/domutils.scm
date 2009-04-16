;;; -*- Mode: Scheme; -*-

(in-module 'domutils)

(use-module '{fdweb xhtml texttools ezrecords})

(module-export!
 '{
   domutils/textify
   domutils/set! domutils/add! domutils/append!
   domutils/selector domutils/match domutils/lookup domutils/find
   domutils/search domutils/strip!
   ->selector selector-tag selector-class selector-id})

;;; Textify

(define (domutils/textify node (embedded #f))
  (if embedded
      (if (string? node) (printout node)
	  (if (pair? node)
	      (dolist (elt node) (domutils/textify elt #t))
	      (if (table? node)
		  (dolist (elt (get node '%content))
		    (domutils/textify elt #t))
		  (printout node))))
      (stringout (domutils/textify node #t))))

;;; DOM editing

;;; The trick here is to update the XML representation so that
;;;  XML unparsing will reflect the changing.  This means updating
;;;  in multiple ways.

(define (varycase s)
  (if (symbol? s)
      (choice (symbol->string s) (downcase (symbol->string s)))))
(define (attrib-basename s)
  (if (or (pair? s) (and (vector? s) (> (length s) 0)))
      (if (and (string? (first s)) (position #\: (first s)))
	  (subseq (first s) (1+ (position #\: (first s))))
	  s)
      (if (string? s)
	  (if (and (string? s) (position #\: s))
	      (subseq s (1+ (position #\: s)))
	      s)
	  s)))
(define (attrib-namespace s)
  (and (string? s) (position #\: s)
       (subseq s 0 (position #\: s))))

(define (domutils/set! node attrib val (fdxml #f))
  (let* ((slotid (if (symbol? attrib) attrib
		     (string->lisp attrib)))
	 (aname (if (symbol? attrib) (downcase (symbol->string attrib))
		    attrib))
	 (anames (if (symbol? attrib) (varycase attrib) attrib))
	 (attribs (try (pick (get node '%attribs) first anames)
		       (pick (get node '%attribs) attrib-basename anames)))
	 (raw-attribs
	  (try (pick (get node '%%attribs) first anames)
	       (pick (get node '%%attribs) attrib-basename anames)))
	 (stringval (if (and (not fdxml) (string? val)) val
			(unparse-arg val))))
    (when (or (ambiguous? attribs) (ambiguous? raw-attribs))
      (error "AmbiguousDOMAttribute"
	     "DOMUTILS/SET of ambiguous attribute " attrib
	     (choice attribs raw-attribs)))
    (store! node slotid val)
    (add! node '%attribids slotid)
    (if (exists? attribs)
	(begin (drop! node '%attribs attribs)
	       (add! node '%attribs (cons (car attribs) stringval)))
	(add! node '%attribs (cons aname stringval)))
    (if (exists? raw-attribs)
	(begin
	  (drop! node '%%attribs raw-attribs)
	  (add! node '%%attribs
		(vector (first raw-attribs) (second raw-attribs)
			stringval)))
	(when (test node '%%name) ;; Kept raw XML info
	  (add! node '%%attribs
		(vector aname aname stringval))))))

(define (domutils/add! node attrib val (fdxml #f))
  (unless (test node (if (symbol? attrib) attrib
			 (string->lisp attrib))
		val)
    (let* ((slotid (if (symbol? attrib) attrib
		       (string->lisp attrib)))
	   (aname (if (symbol? attrib) (downcase (symbol->string attrib))
		      attrib))
	   (anames (if (symbol? attrib) (varycase attrib) attrib))
	   (attribs (try (pick (get node '%attribs) first anames)
			 (pick (get node '%attribs) attrib-basename anames)))
	   (raw-attribs
	    (try (pick (get node '%%attribs) first anames)
		 (pick (get node '%%attribs) attrib-basename anames)))
	   (stringval (if (and (not fdxml) (string? val)) val
			  (unparse-arg val))))
      (when (or (ambiguous? attribs) (ambiguous? raw-attribs))
	(error "AmbiguousDOMAttribute"
	       "DOMUTILS/SET of ambiguous attribute " attrib
	       (choice attribs raw-attribs)))
      (store! node slotid val)
      (add! node '%attribids slotid)
      (if (exists? attribs)
	  (begin (drop! node '%attribs attribs)
		 (add! node '%attribs
		       (cons (car attribs)
			     (string-append (cdr attribs) ";" stringval))))
	  (add! node '%attribs (cons aname stringval)))
      (if (exists? raw-attribs)
	  (begin
	    (drop! node '%%attribs raw-attribs)
	    (add! node '%%attribs
		  (vector (first raw-attribs) (second raw-attribs)
			  (string-append (third raw-attribs) ";" stringval))))
	  (when (test node '%%name) ;; Kept raw XML info
	    (add! node '%%attribs
		  (vector aname aname stringval)))))))

(define (domutils/append! node . content)
  (let ((current (try (get node '%content) '())))
    (dolist (elt content)
      (if (pair? elt)
	  (set! current (append current elt))
	  (if (and (string? elt) (has-prefix elt "<"))
	      (if (test node '%%name)
		  (xmlparse elt 'keepraw)
		  (xmlparse elt))
	      (set! current (append current (list elt))))))
    (store! node '%content current)))

;;; Selector functions

;;; Currently, we're ignoring attribs (just like the Javascript version)

(defrecord selector tag (class #f) (id #f) (attribs (frame-create #f)))

(define xmlid #((isalpha) (opt (isalnum+)) (* #("_" (opt (isalnum+))))))

(define selector-pattern
  `{#("." (label classname ,xmlid))
    #("#" (label idname ,xmlid))
    #((bol) (label tagname ,xmlid #t))})

(define (domutils/selector spec)
  (if (selector? spec) spec
      (if (string? spec)
	  (let ((match (text->frames selector-pattern spec)))
	    (cons-selector (try (get match 'tagname) #f)
			   (try (get match 'classname) #f)
			   (try (get match 'idname) #f)))
	  (if (table? spec)
	      ;; Assume this is an XML node
	      (cons-selector (try (get spec '%name) #f)
			     (try (get spec 'class) #f)
			     (try (get spec 'id) #f))
	      (fail)))))
(define ->selector domutils/selector)

(define (domutils/lookup table sel (dflt))
  (if (selector? sel)
      (try (tryif (and (selector-tag sel) (selector-class sel)
		       (selector-id sel))
		  (get table (stringout (selector-tag sel)
					"." (selector-class sel)
					"#" (selector-id sel))))
	   (tryif (and (selector-tag sel) (selector-id sel))
		  (get table (stringout (selector-tag sel)
					"#" (selector-id sel))))
	   (tryif (and (selector-tag sel) (selector-class sel))
		  (get table (stringout (selector-tag sel)
					"." (selector-class sel))))
	   (tryif (selector-id sel)
		  (get table (stringout "#" (selector-id sel))))
	   (tryif (selector-class sel)
		  (get table (stringout "#" (selector-class sel))))
	   (tryif (selector-tag sel)
		  (get table (selector-tag sel)))
	   (if (bound? dflt) dflt {}))
      (if (bound? dflt)
	  (domutils/lookup table (->selector sel) dflt)
	  (domutils/lookup table (->selector sel)))))

(define (domutils/match elt sel)
  (and (not (string? elt))
       (if (selector? sel)
	   (and (or (not (selector-tag sel))
		    (test elt '%name (selector-tag sel))
		    (test elt '%%name (selector-tag sel)))
		(or (not (selector-class sel))
		    (test elt 'class (selector-class sel)))
		(or (not (selector-id sel))
		    (test elt 'id (selector-id sel))))
	   (if (bound? dflt)
	       (domutils/match (->selector sel) dflt)
	       (domutils/match (->selector sel))))))

;;; Searching

(define (domutils/find under sel)
  "Finds all nodes matching SEL under UNDER"
  (if (selector? sel)
      (cond ((string? under) (fail))
	    ((pair? under)
	     (for-choices (elt (elts under)) (domutils/find sel elt)))
	    ((table? under)
	     (choice (tryif (domutils/match sel under) under)
		     (for-choices (elt (elts (get under '%content)))
		       (domutils/find sel elt)))))
      (domutils/find (->selector sel) under)))

(define (search-helper under pattern exitor)
  "Finds all text and nodes containing pattern under UNDER"
  (for-choices (elt (elts (get under '%content)))
    (if (string? elt)
	(if (textsearch pattern elt)
	    (if exitor (exitor (cons under elt)) (fail))
	    (fail))
	(if (table? elt)
	    (search-helper elt pattern exitor)
	    (fail)))))

(define (domutils/search under pattern (all #f))
  (if all
      (search-helper under pattern #f)
      (call/cc (lambda (exitor) (search-helper under pattern exitor)))))


;;; Stripping out some elements

(defambda (domutils/strip! under sel)
  "Removes all nodes matching SEL under UNDER"
  (if (fail? (reject sel selector?))
      (cond ((string? under) under)
	    ((pair? under)
	     (let ((stripped (strip-helper under sel)))
	       (dolist (elt stripped)
		 (unless (or (string? elt) (pair? elt))
		   (domutils/strip! elt sel)))))
	    ((table? under)
	     (let ((stripped (strip-helper (get under '%content) sel)))
	       (store! under '%content stripped)
	       (dolist (elt stripped)
		 (unless (or (string? elt) (pair? elt))
		   (domutils/strip! elt sel))))))
      (domutils/strip! (->selector sel) under)))

(defambda (strip-helper list sel)
  (if (pair? list)
      (if (domutils/match? (car list) sel)
	  (strip-helper (cdr list) sel)
	  (cons (car list) (strip-helper (cdr list) sel)))
      list))