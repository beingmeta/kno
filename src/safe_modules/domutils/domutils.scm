;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

(in-module 'domutils)

(use-module '{fdweb texttools reflection ezrecords
	      varconfig logger})

(define %used_modules '{varconfig ezrecords})
(define-init %loglevel %notice%)

(module-export!
 '{
   dom/textify dom/find
   dom/textual? dom/structural? dom/content?
   dom/hasclass? dom/addclass! dom/dropclass!
   dom/oidify dom/oidmap dom/sig dom/nodeid dom/eltref dom/updateid!
   dom/get dom/set-tag! dom/set! dom/add! dom/drop!
   dom/append! dom/prepend!
   dom/remove-child! dom/remove!
   dom/replace-child! dom/replace!
   dom/selector dom/match dom/lookup dom/selector? dom/selector->string
   dom/select dom/select->list dom/find->list
   dom/getrules dom/add-rule!
   dom/search dom/search/first dom/strip! dom/map dom/map! dom/combine!
   dom/findmeta dom/getmeta dom/findlinks dom/getlinks
   dom/gethead dom/getschemas 
   dom/count
   dom/split-space dom/split-semi dom/split-comma dom/split-semi+comma
   ->selector selector-tag selector-class selector-id
   *block-tags* *block-text-tags* *terminal-block-tags* *table-tags*
   *pre-tags* *wrapper-tags* *inline-tags* *empty-tags* *void-tags*  
   dom/block? dom/inline? dom/terminal?
   dom->id id->dom dom->xmlids
   dom/parent
   dom/clone})

(module-export! '{dompool/get dompool/done dompool/call})

;;;; Kinds of HTML tags

(define *block-text-tags*
  '{P LI DT DD BLOCKQUOTE
    DIV SECTION ASIDE DETAIL
    UL DL OL
    H1 H2 H3 H4 H5 H6 H7
    PRE})
(define *table-tags*
  (string->symbol '{"TABLE" "TBODY" "TR" "TD" "TH" "COL"}))
(define *inline-tags*
  '{a em strong i b span cite sup sub code kbd var dfn samp})
(define *pre-tags* '{pre})
(define *wrapper-tags* ;; semantic wrapper tags
  '{blockquote ul ol dl section aside  li dt dd detail})
(define *terminal-block-tags*
   '{p h1 h2 h3 h4 h5 h6 h7})
(define *block-tags*
  (choice *terminal-block-tags* *wrapper-tags* 'body 'div))

(define *head-tags* '{H1 H2 H3 H4 H5 H6 H7})
(define *void-tags*
  '{AREA BASE BR COL EMBED HR IMG INPUT KEYGEN LINK MENUITEM
    META PARAM SOURCE TRACK WBR
    FRAME BASEFONT ISINDEX NEXTID BGSOUND SPACER})
(define *empty-tags*
  '{IMG BR HR LINK META BASE INPUT OPTION
    COMMAND KEYGEN SOURCE
    FRAME EMBED PARAM AREA BASEFONT COL
    ISINDEX NEXTID BGSOUND SPACER
    WBR})

(define (dom/block? node)
  (and (table? node) (not (pair? node))
       (overlaps? (get node '%xmltag) *block-text-tags*)))

(define (dom/inline? node)
  (and (table? node) (not (pair? node))
       (overlaps? (get node '%xmltag) *inline-tags*)))

(define (dom/terminal? node)
  (or (string? node) (not (table? node))
      (test node '%xmltag *terminal-block-tags*)
      (and (test node '%xmltag *block-tags*)
	   (not (some? dom/block? (get node '%content))))))

;;;; Standard XML/XHTML schemas

(define-init stdschemas
  (for-choices (def '{(sbooks . "http://sbooks.net/")
		      (sbook . "http://sbooks.net/")
		      (olib . "http://openlibrary.org/")
		      (ia . "http://archive.org/")
		      (fdjt . "http://fdjt.org/")
		      (fd . "http://framerd.org/")
		      (kno . "http://knodules.org/")
		      (bm . "http://beingmeta.com/")
		      (beingmeta . "http://beingmeta.com/")
		      (bookhub . "http://bookhub.io/")
		      (pubtool . "http://beingmeta.com/pubtool/")})
    (choice def (cons (cdr def) (car def)))))
(config-def! 'stdschemas
	     (lambda (name (val))
	       (if (bound? val)
		   (if (or (and (pair? val)
				(symbol? (car val))
				(string? (cdr val)))
			   (and (pair? val)
				(symbol? (cdr val))
				(string? (car val))))
		       (set+! stdschemas
			      (choice (cons (cdr val) (car val)) val))
		       (error "Bad DOM schema spec" "stdschemas:config"
			      val))
		   stdschemas))
	     "Define standard schemas for DOM META and LINK elements")

;;; Random utilities

(define (dom/split-space string) (elts (segment string " ")))
(define (dom/split-semi string)
  (difference (stdspace (elts (segment string ";"))) ""))
(define (dom/split-comma string)
  (difference (stdspace (elts (segment string ","))) ""))
(define (dom/split-semi+comma string)
  (difference (stdspace (elts (segment string {";" ","}))) ""))

(define (find-parent node under)
  (let ((content (get under '%content)))
    (if (or (fail? content) (not (pair? content))) #f
	(if (position node content) under
	    (let ((parent #f) (child #f))
	      (while (and (pair? content) (not parent))
		(unless (string? (car content))
		  (set! child (car content))
		  (set! parent (find-parent node child)))
		(set! content (cdr content)))
	      parent)))))

(define (dom/parent node doc)
  (try (get node '%parent) (find-parent node doc)))

;;; DOM editing

;;; The trick here is to update the XML representation so that
;;;  XML unparsing will reflect the changing.  This means updating
;;;  in multiple ways.

(define (varycase s)
  (if (symbol? s)
      (choice (symbol->string s) (downcase (symbol->string s)))))
(define (lowername x) (downcase (first x)))
(define (lowerqname x) (downcase (second x)))
(define (unqname attrib)
  (let ((qname (first attrib)))
    (if (position #\: qname)
	(slice qname (1+ (position #\: qname)))
	qname)))
(define (lunqname attrib)
  (let ((qname (first attrib)))
    (downcase
     (if (position #\: qname)
	 (slice qname (1+ (position #\: qname)))
	 qname))))

(define (attrib-basename s)
  (if (or (pair? s) (and (vector? s) (> (length s) 0)))
      (if (string? (first s))
	  (if (position #\: (first s))
	      (subseq (first s) (1+ (position #\: (first s))))
	      (if (position #\} (first s))
		  (subseq (first s) (1+ (position #\} (first s))))
		  (first s)))
	  s)))
(define (attrib-namespace s)
  (and (string? s) (position #\: s)
       (subseq s 0 (position #\: s))))
(define (attrib-name s)
  (and (string? s) (position #\: s)
       (subseq s (1+ (position #\: s)))))

(define (dom/set-tag! node name (index))
  (default! index (try (get (get node '%doc) 'index) #f))
  (when index
    (drop! index (cons '{%xmltag %qname} (get node '{%xmltag %qname}))
	   node)
    (add! index (cons '{%xmltag %qname} name) node))
  (store! node '{%xmltag %qname} name)
  (store! node '%id (dom/nodeid node)))

(define (dom/set! node attrib val (index #f) (fdxml #f))
  (drop! node '%markup)
  (let* ((slotid (if (symbol? attrib) attrib (string->lisp attrib)))
	 (aname (if (symbol? attrib) (downcase (symbol->string attrib))
		    attrib))
	 (unq (tryif (position #\: aname)
		(slice aname (1+ (position #\: aname)))))
	 (allattribs (get node '%attribs))
	 (delta (if (ambiguous? allattribs) 1 2)))
    (let* ((attribs (try (pick allattribs first aname)
			 (pick allattribs second aname)
			 (pick allattribs lowername aname)
			 (pick allattribs unqname (try unq aname))
			 (pick allattribs lunqname (downcase (try unq aname)))))
	   (ids (get node '%attribids))
	   (stringval (if (and (not fdxml) (string? val)) val
			  (->domstring val)))
	   (qattrib (string->lisp (first (singleton attribs))))
	   (newattrib (vector (elt attribs 0) (elt attribs 1) stringval))
	   (xslotid (pick (difference (string->lisp (elt attribs 0)) slotid) node)))
      (when index
	(drop! index (cons (choice attrib xslotid slotid qattrib) (get node attrib)) node)
	(add! index (cons (choice attrib xslotid slotid qattrib) val) node)
	(add! index (cons 'has {slotid xslotid}) node))
      (when (ambiguous? attribs)
	(logwarn "AmbiguousDOMAttribute"
	  "DOM/SET! of ambiguous attribute " attrib
	  attribs)
	(drop! node '%attribs attribs)
	(set! ids (difference ids {qattrib slotid}))
	(set! allattribs (difference allattribs attribs))
	(set! attribs {}))
      (store! node {slotid xslotid} val)
      (if (exists? ids)
	  (unless (overlaps? ids (choice qattrib slotid))
	    (if (and (singleton? ids) (or (vector? ids) (pair? ids)))
		(if (not (exists position (choice qattrib slotid) ids))
		    (if (vector? ids)
			(store! node '%attribids `#(,(try qattrib slotid) ,@ids))
			(store! node '%attribids `(,(try qattrib slotid) ,@ids))))
		(add! node '%attribids (try qattrib slotid)))
	    (when index
	      (add! index (cons '%attribids (try qattrib slotid)) node)))
	  (add! node '%attribids slotid))
      (when (or (exists? allattribs) (string? attrib) (exists? unq))
	(if (exists? attribs)
	    (begin
	      (add! node '%attribs (difference newattrib attribs))
	      (drop! node '%attribs (difference attribs newattrib)))
	    (add! node '%attribs (vector aname #f stringval)))))
    (drop! node '%markup))
  (store! node '%attribs (simplify (get node '%attribs)))
  (dom/updateid! node))

(define (dom/drop! node attrib (value) (sep #f) (index #f))
  (drop! node '%markup)
  (let* ((slotid (if (symbol? attrib) attrib (string->lisp attrib)))
	 (aname (if (symbol? attrib) (downcase (symbol->string attrib))
		    attrib))
	 (anames (if (symbol? attrib) (varycase attrib) attrib))
	 (attribs (try (pick (get node '%attribs) first anames)
		       (pick (get node '%attribs) attrib-basename anames)))
	 (xslotid (pick (difference (string->lisp (elt attribs 0)) slotid) node))
	 (ids (get node '%attribids)))
    (when (ambiguous? attribs)
      (error "AmbiguousDOMAttribute"
	     "DOM/SET of ambiguous attribute " attrib
	     attribs))
    (if (bound? value)
	(let* ((val (try (second attribs) (get node slotid)))
	       (vals (if sep (segment val sep) (segment val)))
	       (newvals (remove value vals)))
	  (unless (equal? vals newvals)
	    (dom/set! node (try (car attribs) slotid)
		      (stringout
			(if sep
			    (doseq (v newvals i)
			      (printout (if (> i 0) sep) v))
			    (doseq (v newvals i)
			      (printout (if (> i 0) " ") v)))))))
	(begin
	  (drop! node {slotid xslotid})
	  (if (and (singleton? ids) (or (pair? ids) (vector? ids)))
	      (store! node '%attribids (remove slotid ids))
	      (drop! node '%attribids slotid))
	  (when (exists? attribs) (drop! node '%attribs attribs))
	  (when index
	    (drop! index (cons {slotid xslotid attrib} value) node)
	    (drop! index (cons 'has {slotid xslotid attrib}) node)))))
  (dom/updateid! node))

(define (dom/get node attrib)
  (if (symbol? attrib) (get node attrib)
      (third (pick (get node '%attribs) {first second} attrib))))

(define (dom/hasclass? node classname (attrib 'class))
  (or (test node attrib classname)
      (textsearch (if (string? classname) `(word ,classname)
		      (if (symbol? classname)
			  `(IC (WORD ,(downcase classname)))
			  `(WORD ,classname)))
		  (try (pickstrings (get node attrib)) ""))))
(define (dom/addclass! node classname (index #f))
  (if (and (string? classname) (not (empty-string? classname)))
      (if (test node 'class)
	  (let ((classes (segment (get node 'class) " ")))
	    (unless (position classname classes)
	      (dom/set! node 'class
			(stringout (doseq (class classes) (printout class " "))
			  (printout classname)))
	      (when index (index-frame index node 'classname classname))))
	  (begin (dom/set! node 'class classname)
	    (when index (index-frame index node 'classname classname))))
      (logwarn DOM/ADDCLASS "Invalid classname " (write classname))))
(define (dom/dropclass! node classname)
  (when (test node 'class)
    (let ((classes (segment (get node 'class) " ")))
      (when (position classname classes)
	(if (= (length classes) 1)
	    (dom/drop! node 'class)
	    (dom/set! node 'class
		      (stringout
			(doseq (class (remove classname classes) i)
			  (printout (if (> i 0) " ") class)))))))))

(define (->domstring value)
  (cond ((string? value) value)
	((oid? value) (oid->string value))
	((symbol? value) (symbol->string value))
	((uuid? value)  (uuid->string value))
	((number? value) (number->string value))
	(else (stringout value))))

(define (dom/add! node attrib value (sep ";") (index #f) (svalue))
  (drop! node '%markup)
  (let ((current (dom/get node attrib))
	(svalue (->domstring value)))
    (if (fail? current)
	(dom/set! node attrib value index)
	(let ((values (segment current sep)))
	  (unless (position svalue values)
	    (dom/set! node attrib (string-append svalue sep current))
	    (when index (add! index (cons attrib value) node)))))))

(define (dom/remove-child! node elt (recur #t))
  (drop! node '%markup)
  (and (table? node) (not (xmlempty? node))
       (let* ((content (get node '%content))
	      (newcontent (remove elt content)))
	 (cond ((equal? content newcontent)
		(if recur
		    (some? (lambda (x) (and (table? x) (dom/remove! x elt)))
			   content)
		    #f))
	       (else (store! node '%content newcontent)
		     #t)))))
(define (dom/replace-child! node cur new (recur #t))
  (drop! node '%markup)
  (and (table? node) (not (xmlempty? node))
       (let* ((content (get node '%content))
	      (newcontent
	       (->list (forseq (celt (->vector content))
			 (if (equal? cur celt) new celt)))))
	 (cond ((equal? content newcontent)
		(if recur
		    (some? (lambda (x)
			     (and (table? x) (dom/replace-child! x cur new #t)))
			   content)
		    #f))
	       (else (store! node '%content newcontent)
		     #t)))))

(define (dom/remove! elt (parent))
  (default! parent (get elt '%parent))
  (set! parent (dom/parent elt (try parent #f)))
  (when (and (exists? parent) (table? parent) (test parent '%content))
    (drop! parent '%markup)
    (let* ((content (get parent '%content))
	   (newcontent (remove elt content)))
      (store! parent '%content newcontent))))
(define (dom/replace! cur new (parent))
  (default! parent (get cur '%parent))
  (set! parent (dom/parent cur (try parent #f)))
  (when (and (exists? parent) (table? parent)
	     (not (xmlempty? parent)))
    (drop! parent '%markup)
    (let* ((content (get parent '%content))
	   (newcontent
	    (->list (forseq (celt (->vector content))
		      (if (equal? cur celt) new celt)))))
      (store! parent '%content newcontent))))

(define (dom/append! node . content)
  (drop! node '%markup)
  (let ((current (try (get node '%content) '())))
    (dolist (elt content)
      (when (exists? elt)
	(set! current
	      (append current
		      (if (pair? elt) elt
			  (if (and (string? elt) (has-prefix elt "<"))
			      (if (test node '%%xmltag)
				  (xmlparse elt 'keepraw)
				  (xmlparse elt))
			      (list elt)))))))
    (store! node '%content current)))

(define (dom/prepend! node . content)
  (let ((current (try (get node '%content) '())))
    (dolist (elt (reverse content))
      (when (exists? elt)
	(set! current
	      (append (if (pair? elt) elt
			  (if (and (string? elt) (has-prefix elt "<"))
			      (if (test node '%%xmltag)
				  (xmlparse elt 'keepraw)
				  (xmlparse elt))
			      (list elt)))
		      current))))
    (store! node '%content current)))

;;; Textmap functions

(define (dom/reverse-text! node)
  (if (string? node)
      (reverse (decode-entities node))
      (if (test node '%content)
	  (begin (store! node '%content
			 (->list
			  (forseq (elt (->vector (get node '%content)))
			    (if (string? elt) (reverse elt)
				(dom/reverse-text! elt)))))
	    node)
	  node)))

(define (dom/transform-text! node fn)
  (if (string? node)
      (fn (decode-entities node))
      (if (test node '%content)
	  (begin (store! node '%content
			 (->list
			  (forseq (elt (->vector (get node '%content)))
			    (if (string? elt) (fn elt)
				(dom/transform-text! elt fn)))))
	    node)
	  node)))

(module-export! '{dom/reverse-text! dom/transform-text!})

;;; Selector functions

(defrecord selector tag (class #f) (id #f) (attribs {}) (context '()))

(define xmlid #((isalpha) (opt (isalnum+)) (* #({"_" "-"} (opt (isalnum+))))))

(define (dom/selector? x) (selector? x))
(define (dom/selector->string sel)
  (stringout (if (selector-tag sel) (selector-tag sel))
    (if (selector-id sel) (printout "#" (selector-id sel)))
    (when (selector-class sel)
      (do-choices (cl (selector-class sel)) (printout "." cl)))
    (when (selector-attribs sel)
      (do-choices (a (selector-attribs sel))
	(if (null? (cdr a))
	    (printout "[" (car a) "]")
	    (printout "[" (car a) "=" (cdr a) "]"))))))

(define (strip-quotes s)
  (if (has-prefix s {"'" "\""}) (set! s (slice s 1)))
  (if (has-suffix s {"'" "\""}) (set! s (slice s 0 -1)))
  s)

(define selector-pattern
  `{#("." (label classname ,xmlid))
    #("#" (label idname ,xmlid))
    #((bol) (label tagname ,xmlid #t))
    (GREEDY
     #("[" (label name ,xmlid #t)
       (opt
	#((label cmp {"=" "=~"})
	  (label value (not> "]") ,strip-quotes)))
       "]"))})
(module-export! 'selector-pattern)

(define (combine-attribs attrib)
  (cons (get attrib 'name)
	(if (test attrib 'cmp)
	    (if (test attrib 'cmp "=~")
		`(IS (PHRASE ,(get attrib 'value)))
		(get attrib 'value))
	    '())))

(define (dom/selector spec)
  (if (selector? spec) spec
      (if (string? spec)
	  (for-choices (spec (if (position #\, spec)
				 (elts (segment spec ","))
				 spec))
	    (let ((cascade (reverse (map stdspace (segment spec " ")))))
	      (if (null? (cdr cascade))
		  (let ((match (text->frames selector-pattern (car cascade))))
		    (cons-selector (try (get match 'tagname) #f)
				   (try (difference (get match 'classname) "*") #f)
				   (try (get match 'idname) #f)
				   (combine-attribs (pick match 'name))
				   '()))
		  (let ((match (text->frames selector-pattern (car cascade))))
		    (cons-selector (try (get match 'tagname) #f)
				   (try (difference (get match 'classname) "*") #f)
				   (try (get match 'idname) #f)
				   (combine-attribs (pick match 'name))
				   (selector-parse-context (cdr cascade)))))))
	  (if (table? spec)
	      ;; Assume this is an XML node
	      (cons-selector (try (get spec '%xmltag) #f)
			     (try (get spec 'class) #f)
			     (try (get spec 'id) #f))
	      (fail)))))
(define ->selector dom/selector)

(define (selector-parse-context cascade)
  (let ((context '()))
    (dolist (cx cascade)
      (if (equal? cx ">")
	  (set! context (cons cx context))
	  (let ((match (text->frames selector-pattern cx)))
	    (set! context
		  (cons (cons-selector (try (get match 'tagname) #f)
				       (try (difference (get match 'classname) "*") #f)
				       (try (get match 'idname) #f)
				       (combine-attribs (pick match 'name)))
			context)))))
    (reverse context)))

(defambda (getsigs tag id classes)
  (let* ((tag (choice (->string tag) (downcase (->string tag))))
	 (tagid (choice tag (glom tag "#" id))))
    (choice tagid
	    (for-choices (c1 classes)
	      (choice
	       (glom tagid "." c1)
	       (glom "." c1)
	       (for-choices (c2 (difference classes c1))
		 (choice (glom tagid "." c1 "." c2)
			 (glom "." c1 "." c2)
			 (for-choices (c3 (difference classes c1 c2))
			   (glom tagid "." c1 "." c2 "." c3)
			   (glom "." c1 "." c2 "." c3)))))))))
(define (siglen x) (length (segment x {"." "#"})))

(define (dom/lookup table sel (dflt))
  (if (selector? sel)
      (let* ((allsigs (getsigs (or (selector-tag sel) {}) (or (selector-id sel) {})
			       (or (selector-class sel) {})))
	     (sigvec (append (rsorted (pick allsigs string-contains? "#") siglen)
			     (rsorted (reject allsigs string-contains? "#") siglen)))
	     (found {}) (i 0) (lim (length sigvec)))
	(while (and (< i lim) (fail? found))
	  (set! found (get table (elt sigvec i)))
	  (set! i (1+ i)))
	(try found (tryif (bound? dflt) dflt)))
      (if (bound? dflt)
	  (dom/lookup table (->selector sel) dflt)
	  (dom/lookup table (->selector sel)))))

(define (dom/getrules table node (dflt))
  (if (pair? table)
      (try (dom/getrules (car table) node)
	   (dom/getrules (cdr table) node)
	   (tryif (bound? dflt) dflt))
      (let* ((allsigs (getsigs (get node '%xmltag) (get node 'id)
			       (elts (segment (get node 'class) " "))))
	     (sigvec (append (rsorted (pick allsigs string-contains? "#") siglen)
			     (rsorted (pick (reject allsigs string-contains? "#")
					    string-contains? ".")
				      siglen)
			     (rsorted (reject (reject allsigs string-contains? "#")
					      string-contains? ".")
				      siglen)))
	     (found {}) (i 0) (lim (length sigvec)))
	(while (and (< i lim) (fail? found))
	  (set! found (get table (elt sigvec i)))
	  (set! i (1+ i)))
	(try found (tryif (bound? dflt) dflt)))))

(define (dom/add-rule! table selector value)
  (let* ((selector (->selector selector))
	 (tag (or (selector-tag selector) {}))
	 (class (or (selector-class selector) {}))
	 (id (or (selector-id selector) {})))
    (add! table (try (string-append (symbol->string tag) "." class "#" id)
		     (string-append "." class "#" id)
		     (string-append (symbol->string tag) "#" id)
		     (string-append (symbol->string tag) "." class)
		     (string-append "#" id)
		     (string-append "." class)
		     tag)
	  (cons selector value))))

(define (basematch elt sel)
  (and (or (not (selector-tag sel))
	   (test elt '%xmltag (selector-tag sel))
	   (test elt '%rawtag (selector-tag sel)))
       (or (not (selector-class sel))
	   (test elt 'class (selector-class sel))
	   (and (test elt 'class)
		(if (test elt 'classname)
		    (contains? (selector-class sel)
			       (get elt 'classname))
		    (if (test elt '%class)
			(contains? (selector-class sel)
				   (get elt '%class))
			(let ((classes (elts (segment (get elt 'class)))))
			  (store! elt '%class classes)
			  (contains? (selector-class sel) classes))))))
       (or (not (selector-id sel))
	   (test elt 'id (selector-id sel)))
       (fail?
	(try-choices (attrib (selector-attribs sel))
	  (if (null? (cdr attrib))
	      (tryif (not (test elt (car attrib))) attrib)
	      (tryif (not (if (string? (cdr attrib))
			      (or (test elt (car attrib) (cdr attrib))
				  (test elt (car attrib) (parse-arg (cdr attrib))))
			      (textsearch (cdr attrib) (get elt (car attrib)))))
		attrib))))))
(define (selmatch elt sel)
  (and (basematch elt sel)
       (or (null? (selector-context sel))
	   (match-context elt (selector-context sel)))))
(define (match-context elt context (stack #f))
  (or (null? context)
      (if (equal? (car context) ">")
	  (if stack
	      (and (pair? stack)
		   (exists basematch (car stack) (cadr context))
		   (match-context (car stack)  (cddr context) (cdr stack)))
	      (and (exists basematch (get elt '%parent) (cadr context))
		   (match-context (get elt '%parent) (cddr context))))
	  (if stack
	      (let ((scan stack))
		(while (and (pair? scan) (not (basematch (car scan) (car context))))
		  (set! scan (cdr scan)))
		(and (pair? scan) (match-context (car scan) (cdr context) (cdr scan))))
	      (let ((scan (get elt '%parent)))
		(while (and (exists? scan) (not (basematch scan (car context))))
		  (set! scan (get scan '%parent)))
		(and (exists? scan)
		     (match-context scan (cdr context))))))))

(defambda (dom/match elt sel)
  (for-choices elt
    (if (string? elt) #f
	(or (test elt '%xmltag (pick sel symbol?))
	    (exists (pick sel applicable?) elt)
	    (try (try-choices (sel {(pick sel selector?)
				    (->selector (pickstrings sel))})
		   (or (selmatch elt sel) {}))
		 #f)))))

;;; Finding using the index

(defambda (dom-index-find index sel (under #f))
  (cond (under
	 (for-choices under
	   (intersection (dom-index-find index sel)
			 (find-frames index 'parents under))))
	((ambiguous? sel) (for-choices sel (dom-index-find index sel)))
	((or (exists? (selector-attribs sel)) (not (null? (selector-context sel))))
	 (filter-choices (cand (dom-find-core index sel))
	   (dom/match cand sel)))
	(else (dom-find-core index sel))))

(define (dom-find-core index sel)
  (cond ((and (selector-tag sel) (selector-id sel)
	      (singleton? (selector-class sel))
	      (selector-class sel))
	 (find-frames index
	   '%xmltag (selector-tag sel)
	   'class (selector-class sel)
	   'id (selector-id sel)))
	((and (selector-tag sel) (singleton? (selector-class sel))
	      (selector-class sel))
	 (find-frames index
	   '%xmltag (selector-tag sel)
	   'class (selector-class sel)))
	((and (selector-tag sel) (selector-id sel)
	      (not (selector-class sel)))
	 (find-frames index
	   '%xmltag (selector-tag sel)
	   'id (selector-id sel)))
	((and (selector-id sel)
	      (not (selector-tag sel))
	      (not (selector-class sel)))
	 (find-frames index 'id (selector-id sel)))
	((and (selector-id sel)
	      (singleton? (selector-class sel))
	      (selector-class sel)
	      (not (selector-tag sel)))
	 (find-frames index
	   'class (selector-class sel)
	   'id (selector-id sel)))
	((ambiguous? (selector-class sel))
	 (apply intersection
		(cond ((and (selector-tag sel) (selector-id sel))
		       (find-frames index '%xmltag (selector-tag sel)
				    'id (selector-id sel)))
		      ((selector-tag sel) (find-frames index '%xmltag (selector-tag sel)))
		      ((selector-id sel) (find-frames index 'id (selector-id sel)))
		      (else (fail)))
		(map (lambda (class) (find-frames index 'class class))
		     (choice->list (selector-class sel)))))
	((selector-id sel) (find-frames index 'id (selector-id sel)))
	((selector-class sel)
	 (find-frames index 'class (selector-class sel)))
	((selector-tag sel)
	 (find-frames index '%xmltag (selector-tag sel)))
	((and (not (selector-tag sel)) (not (selector-class sel))
	      (not (selector-id sel))
	      (exists? (selector-attribs sel)))
	 (let ((cands #f) (selected {}))
	   (do-choices (attrib (selector-attribs sel))
	     (unless (fail? cands)
	       (set! selected
		     (if (null? (cdr attrib))
			 (find-frames index '{has %attribids} (car attrib))
			 (find-frames index (car attrib)
				      (choice (cdr attrib)
					      (parse-arg (cdr attrib))))))
	       (if cands (set! cands (intersection cands selected))
		   (set! cands selected))))
	   cands))
	(else (fail))))

;;; Searching

(defambda (dom/find under sel . args)
  (if (string? under) (fail)
      (if (null? args) (dom/select under sel)
	  (if (and (pair? args) (identical? (car args) #f))
	      (dom/select under sel #f)
	      (if (test under 'index)
		  (apply find-frames (get under 'index) (cons sel args))
		  (begin (logwarn "No index for " under)
		    (fail)))))))

(defambda (dom/select under sel (findall #t))
  "Finds all nodes matching SEL under UNDER, if FINDALL is true, \
   look under matching nodes for other matching nodes."
  (when (exists? (pickstrings sel)) (set! sel (->selector sel)))
  (cond ((string? under) (fail))
	((ambiguous? under)
	 (for-choices under (dom/select under sel)))
	((exists?  (get under 'index))
	 (for-choices (index (get under 'index)) (dom-index-find index sel)))
	((exists? (get (get under '%doc) 'index))
	 (let ((index (try (get under 'index) (get (get under '%doc) 'index))))
	   (for-choices index (dom-index-find index sel under))))
	((pair? under)
	 (for-choices (elt (elts under)) (dom/select elt sel findall)))
	((and (table? under) (dom/match under sel))
	 (choice under (tryif findall
			 (for-choices (elt (elts (get under '%content)))
			   (dom/select elt sel findall)))))
	((table? under)
	 (for-choices (elt (elts (get under '%content)))
	   (dom/select elt sel findall)))
	(else (fail))))

(defambda (dom/select->list under sel (findall #t))
  "Finds all nodes matching SEL under UNDER, if FINDALL is true, \
   look under matching nodes for other matching nodes."
  (unless (exists? (pickstrings sel)) (set! sel (->selector sel)))
  (cond ((string? under) '())
	((ambiguous? under)
	 (let ((nodes '()))
	   (do-choices under
	     (set! nodes (append nodes (dom/select->list under sel))))
	   nodes))
	((pair? under)
	 (let ((nodes '()))
	   (dolist (under under)
	     (set! nodes (append nodes (dom/select->list under  sel))))
	   nodes))
	((vector? under)
	 (let ((nodes '()))
	   (doseq (under under)
	     (set! nodes (append nodes (dom/select->list under  sel))))
	   nodes))
	((and (table? under) (dom/match under sel))
	 (list under))
	((and (table? under) (not (xmlempty? under)))
	 (let ((nodes '()))
	   (doseq (under (get under '%content))
	     (set! nodes (append nodes (dom/select->list under sel))))
	   nodes))
	(else '())))
(define dom/find->list dom/select->list)

;;; Text searching

(define (search-helper under path pattern foreach)
  "Finds all text and nodes containing pattern under UNDER"
  (for-choices (elt (elts (if (pair? under) under
			      (get under '%content))))
    (if (string? elt)
	(if (textsearch pattern elt)
	    (if foreach
		(if (foreach elt under path)
		    (if (pair? under) (cons elt path)
			(cons* elt under path))
		    {})
		(if (pair? under) (cons elt path)
		    (cons* elt under path)))
	    (fail))
	(if (table? elt)
	    (search-helper elt (cons under path) pattern foreach)
	    (fail)))))

(define (dom/search under pattern (foreach #f))
  (search-helper under '() pattern #f))

(define (dom/search/first under pattern (test #f) (return 'node))
  (unless (overlaps? return '{string node path})
    (error "Invalid return specifier"))
  (call/cc (lambda (exitor)
	     (search-helper under pattern '()
			    (lambda (string node path)
			      (if (and test (not (test string node path))) #f
				  (if (eq? return 'node) (exitor node)
				      (if (eq? return 'string) (exitor string)
					  (if (eq? return 'path) (exitor (cons node path))
					      (exitor (cons* string node path)))))))))))


;;; Counting nodes

(define (dom/count under (sel #f) (sum 0))
  (if (string? under) sum
      (if (pair? under)
	  (begin (doseq (child (->vector under))
		   (unless (or (string? child)
			       (and sel (not (dom/match child sel))))
		     (set! sum (dom/count child sel sum))))
	    sum)
	  (if (table? under)
	      (if (not (xmlempty? under))
		  (begin
		    (doseq (child (->vector (get under '%content)))
		      (unless (or (string? child)
				  (and sel (not (dom/match child sel))))
			(set! sum (dom/count child sel sum))))
		    (1+ sum))
		  (1+ sum))
	      sum))))

;;; Stripping out some elements

(defambda (dom/strip! under sel)
  "Removes all nodes matching SEL under UNDER"
  (if (fail? (reject sel selector?))
      (strip-under under sel)
      (dom/strip! under (->selector sel))))

(defambda (strip-under under sel)
  "Removes all nodes matching SEL under UNDER"
  (cond ((string? under) under)
	((pair? under) (strip-helper under sel))
	((and (table? under) (exists? (get under '%content)))
	 (let ((stripped (strip-helper (get under '%content) sel)))
	   (store! under '%content stripped)
	   under))
	(else under)))

(defambda (strip-helper content sel)
  (if (pair? content)
      (if (or (string? (car content))
	      (not (slotmap? (car content))))
	  (cons (car content) (strip-helper (cdr content) sel))
	  (if (dom/match (car content) sel)
	      (strip-helper (cdr content) sel)
	      (cons (strip-under (car content) sel)
		    (strip-helper (cdr content) sel))))
      content))

;;; Getting meta fields

(define (dom/gethead doc)
  (try (get doc 'head)
       (tryif (test doc '%xmltag 'head) doc)
       (let ((found (dom/select doc "HEAD")))
	 (when (exists? found)
	   (store! doc 'head found))
	 found)))

(define (dom/getschemas doc)
  (if (test doc '%schemas) (get doc '%schemas)
      (begin
	(when (exists? (get doc '%xschemas))
	  (add! doc '%schemas (get doc '%xschemas)))
	(do-choices (link (dom/select doc "LINK"))
	  (when (has-prefix (get link 'rel) "schema.")
	    (let* ((rel (get link 'rel))
		   (href (get link 'href))
		   (prefix (string->symbol (subseq rel 7))))
	      (add! doc '%schemas (cons prefix href))
	      (add! doc '%schemas (cons href prefix)))))
	(get doc '%schemas))))

(define (getnames doc field)
  (if (position #\. field)
      (let* ((sep (position #\. field))
	     (schemas (dom/getschemas doc))
	     (prefixstring (subseq field 0 sep))
	     (prefix (string->symbol prefixstring))
	     (uri (try (get schemas prefixstring)
		       (get schemas prefix)
		       (get stdschemas prefixstring)
		       (get stdschemas prefix)))
	     (prefixes (get schemas uri)))
	(if (fail? prefixes) field
	    (glom prefixes "." (subseq field (1+ sep)))))
      field))

(define (dom/findmeta doc field (xform) (dflt))
  (let* ((head (dom/gethead doc))
	 (meta (dom/select head "META"))
	 (names
	  (if (symbol? field)
	      (getnames doc (symbol->string field))
	      (getnames doc field))))
    (if (symbol? field)
	(pick meta lname (downcase names))
	(try (pick meta 'name names)
	     (pick meta 'name field)))))

(define (dom/getmeta doc field (xform) (dflt))
  (let ((elts (dom/findmeta doc field)))
    (try (if (and (bound? xform) xform)
	     (if (applicable? xform)
		 (xform (get elts 'content))
		 (if (eq? xform #t) (parse-arg (get elts 'content))
		     (textsubst (get elts 'content) (qc xform))))
	     (if (bound? xform) elts ;; Must be #f
		 (get elts 'content)))
	 (if (bound? dflt) dflt (fail)))))

(define (dom/findlinks doc field)
  (let* ((head (dom/gethead doc))
	 (links (dom/select head "LINK"))
	 (names
	  (if (symbol? field)
	      (getnames doc (symbol->string field))
	      (getnames doc field))))
    (if (symbol? field)
	(pick links lname (downcase names))
	(try (pick links 'rel names)
	     (pick links 'rel field)))))

(define (dom/getlinks doc field (dflt #f))
  (if dflt
      (try (get (dom/findlinks doc field) 'href) dflt)
      (get (dom/findlinks doc field) 'href)))

(define (lname x (name))
  (default! name (try (get x 'rel) (get x 'name)))
  (if (fail? name) {}
      (if (symbol? name) (downcase (symbol->string name))
	  (if (string? name) (downcase name)
	      (downcase (stringout name))))))

;;; Walking the DOM

(define (map0 node fn)
  (if (pair? node)
      (doseq (elt (->vector node)) (map0 elt fn))
      (begin (fn node)
	(when (and (table? node) (not (xmlempty? node)))
	  (doseq (elt (->vector (try (get node '%content) '())))
	    (when (table? elt) (map0 elt fn)))))))

(define (map1 node fn arg)
  (if (pair? node)
      (doseq (elt (->vector node)) (map1 elt fn (qc arg)))
      (begin (fn node)
	(when (and (table? node) (not (xmlempty? node)))
	  (doseq (elt (try (get node '%content) '()))
	    (when (table? elt) (map1 elt fn (qc arg))))))))

(define (mapn node fn args)
  (if (pair? node)
      (doseq (elt node) (mapn elt fn args))
      (begin (apply fn node args)
	(when (and (table? node) (not (xmlempty? node)))
	  (doseq (elt (->vector (try (get node '%content) '())))
	    (when (table? elt) (mapn elt fn args)))))))

(defambda (dom/map node fn (arg) . args)
  (do-choices node
    (do-choices fn
      (if (bound? arg)
	  (if (null? args) (map1 node fn arg)
	      (mapn node fn (cons (qc arg) args)))
	  (map0 node fn)))))

(define (map0! node fn)
  (cond ((string? node) node)
	((pair? node) (->list (map0! (->vector node) fn)))
	((vector? node)
	 (apply append
		(forseq (elt (->vector node))
		  (if (string? elt) (vector elt) (map0! elt fn)))))
	((not (xmlempty? node))
	 (let* ((content (->vector (get node '%content)))
		(new (apply append
			    (forseq (elt content)
			      (if (string? elt) (vector elt)
				  (->vector (map0! elt fn)))))))
	   (store! node '%content (->list new))
	   (fn node)))
	(else '())))

(define (mapn! node fn args)
  (cond ((string? node) node)
	((pair? node) (->list (mapn! (->vector node) fn args)))
	((vector? node)
	 (apply append
		(forseq (elt (->vector node))
		  (if (string? elt) (vector elt)
		      (->vector (mapn! elt fn args))))))
	((not (xmlempty? node))
	 (let* ((content (->vector (get node '%content)))
		(new (apply append
			    (forseq (elt content)
			      (if (string? elt) (vector elt)
				  (->vector (mapn! elt fn args)))))))
	   (store! node '%content (->list new))
	   (apply fn node args)))
	(else '())))

(defambda (dom/map! node fn . args)
  (do-choices node
    (do-choices fn
      (if (null? args)
	  (map0! node fn)
	  (mapn! node fn args)))))

;;; Combining

(define (dom/combine! target elements (extract "BODY"))
  (doseq (elt elements)
    (dom/append! target (dom/select->list elt extract))))

;;; Textify

(define *line-break-tags*
  (choice *block-text-tags* (string->symbol '{"PRE" "BR" "HR"})))
(define *no-text-tags* {})
(varconfig! DOM:TEXTFREE *no-text-tags* ->selector choice)

(define (dom/textify node (embedded #f) (cache #t) (skip #f))
  (if embedded
      (if (string? node)
	  (printout (if (position #\< node)
			(decode-entities (strip-markup node))
			(decode-entities node)))
	  (if (or (pair? node) (vector? node) (null? node))
	      (doseq (elt node) (dom/textify elt #t cache skip))
	      (if (table? node)
		  (unless (or (test node 'textfree)
			      (test node '%xmltag '%comment)
			      (dom/match node *no-text-tags*)
			      (and skip (dom/match node skip)))
		    (printout
		      (if (overlaps? (get node '%xmltag) *line-break-tags*)
			  "\n")
		      (if (test node '%text)
			  (get node '%text)
			  (when (and (test node '%content)
				     (not (null? (test node '%content))))
			    (dom/textify (get node '%content) #t #f skip)))))
		  (printout node))))
      (if (test node '%text)
	  (get node '%text)
	  (if (and cache (not (pair? node)) (not (string? node)))
	      (let ((s (stringout (dom/textify node #t #f skip))))
		(store! node '%text s)
		s)
	      (stringout (dom/textify node #t #f skip))))))

(define (dom/textual? node (lim #f))
  (and (test node '%content)
       (if lim
	   (do ((content (get node '%content) (cdr content))
		(len 0 (+ (if (string? (car content))
			      (if (has-prefix (car content) "<!--")
				  0 (isalphalen (car content)))
			      0)
			  len)))
	       ((or (null? content) (> len lim))
		(> len lim)))
	   (some? (lambda (x)
		    (and (string? x)
			 (not (empty-string? x))
			 (not (has-prefix x "<!--"))))
		  (get node '%content)))))

(define (dom/structural? node)
  "Returns true if a node only contains other blocks or empty strings"
  (and (test node '%content)
       (every? (lambda (x) (if (string? x) (empty-string? x)
			       (not (and (test x '%xmltag *inline-tags*)
					 (dom/textual? x)))))
	       (get node '%content))))

(define (dom/content? node)
  "Returns true if a node only contains other blocks or empty strings"
  (and (test node '%content)
       (some? (lambda (x) (if (string? x) (not (empty-string? x))
			      (dom/content? x)))
	       (get node '%content))))

;;; Cloning nodes

;; This is just like STORE! but won't store an empty choice
(defambda (dostore! f s v) (unless (fail? v) (store! f s v)))

(define (dom/clone real (withcontent #f))
  (let ((result `#[%XMLTAG ,(get real '%xmltag)]))
    (when (exists? (get real '%content))
      (store! result '%content
	      (if withcontent
		  (map (lambda (x) (if (string? x) x (dom/clone x withcontent)))
		       (get real '%content))
		  '())))
    (dostore! result '%attribids (get real '%attribids))
    (dostore! result '%attribs (get real '%attribs))
    (dostore! result '%xmlns (get real '%xmlns))
    (if (vector? (get real '%attribids))
	(doseq (attrib (get real '%attribids))
	  (store! result attrib (get real attrib)))
	(do-choices (attrib (get real '%attribids))
	  (store! result attrib (get real attrib))))
    result))

;;; OIDify

(define dompool #f)

(define (oidify/copy node pool (doc #f) (parent #f))
  (if (or (slotmap? node)  (schemap? node))
      (try (get node '%oid)
	   (let* ((oid (frame-create pool))
		  (slotids (getkeys node))
		  (cur #f) (prev #f))
	     (when parent (store! oid '%parent parent))
	     (store! oid '%oid oid)
	     (store! node '%oid oid)
	     (when doc (store! oid '%doc doc))
	     (when (not (xmlempty? node))
	       (store! oid '%content
		       (->list
			(forseq (elt (->vector (get node '%content)))
			  (when (or (slotmap? elt)  (schemap? elt))
			    (set! cur (oidify/copy elt pool doc oid))
			    (when (oid? cur)
			      (when prev
				(store! prev 'next cur)
				(store! cur 'prev prev))
			      (set! prev cur)))
			  (if (slotmap? elt) cur elt)))))
	     (do-choices (slotid (difference slotids '%content))
	       (store! oid slotid (%get node slotid)))
	     (unless (test oid '%id)
	       (store! oid '%id (dom/nodeid oid)))
	     (store! oid '%children
		     (pickoids (elts (get oid '%content))))
	     oid))
      (if (pair? node)
	  (map (lambda (child)
		 (if (string? child) child (oidify/copy child pool doc node)))
	       node)
	  node)))

(define (oidify/inplace node pool (doc #f) (parent #f))
  (if (or (slotmap? node)  (schemap? node))
      (try (get node '%oid)
	   (let ((oid (allocate-oids pool)) (cur #f) (prev #f)
		 (content (try (->vector (get node '%content)) #f)))
	     (logdebug "Allocated " oid " for " (get node 'id))
	     (store! node '%oid oid)
	     (set-oid-value! oid node #t)
	     (when parent (store! node '%parent parent))
	     (when doc (store! node '%doc doc))
	     (when content
	       (store! node '%content
		       (->list
			(forseq (elt content)
			  (if (or (slotmap? elt)  (schemap? elt))
			      (begin
				(set! cur (oidify/inplace elt pool doc oid))
				(when prev
				  (store! prev 'next cur)
				  (store! cur 'prev prev))
				(set! prev cur)
				cur)
			      elt)))))
	     (unless (test node '%id)
	       (store! oid '%id (dom/nodeid oid)))
	     (store! oid '%children
		     (pickoids (elts (get node '%content))))
	     oid))
      (if (pair? node)
	  (map (lambda (child)
		 (if (string? child) child (oidify/inplace child pool doc node)))
	       node)
	  node)))

(define (dom/oidify node (pool (try (threadget 'dompool) dompool))
		    (inplace #t) (doc #f) (parent #f) )
  (if (eq? pool #t) (set! pool dompool))
  (if (not pool) node
      (if inplace
	  (oidify/inplace node pool doc parent)
	  (oidify/copy node pool doc parent))))

;;; DOMPOOL functions

(define-init dompool-base @d03/0)
(define-init dompool-count 0)
(define-init all-dompools {})
(define-init dompools {})

(defslambda (dompool/get (cap (* 1024 256)))
  (try (let ((picked (pick-one (pick dompools pool-capacity cap))))
	 (tryif (exists? picked)
	   (prog1 picked (set! dompools (difference dompools picked)))))
       (let ((pool (make-mempool (glom "dompool" (1+ dompool-count))
				 dompool-base cap)))
	 (set+! all-dompools pool)
	 (set! dompool-base (oid-plus dompool-base cap))
	 (set! dompool-count (1+ dompool-count))
	 pool)))

(defslambda (dompool/done pool)
  (if (overlaps? pool all-dompools)
      (begin
	(reset-mempool pool)
	(set+! dompools pool))
      (irritant pool |UnallocatedDOMPool|)))

(define (dompool/call fun)
  (let ((dompool #f))
    (dynamic-wind
	(lambda () (set! dompool (dompool/get)))
	(lambda () (fun dompool))
	(lambda () (dompool/done dompool)))))

;;; Over OIDs

;; This returns a hashtable containing the OID value mappings for an
;;  OIDified DOM tree.  As a bit of a kludge, as long as this is
;;  around, cleaning the OID map won't do anything.
(define (dom/oidmap node (table (make-hashtable)))
  (if (oid? node)
      (if (test table node) table
	  (begin (store! table node (oid-value node))
		 (dom/oidmap (oid-value node) table)))
      (if (or (immediate? node) (number? node) (string? node))
	  table
	  (if (slotmap? node)
	      (let ((slotids (getkeys node)))
		(do-choices (slotid (getkeys node))
		  (set! table (dom/oidmap (%get node slotid) table)))
		table)
	      (if (pair? node)
		  (dom/oidify (cdr node) (dom/oidmap (car node) table))
		  (begin
		    (when (vector? node)
		      (doseq (elt node) (set! table (dom/oidmap elt table))))
		    table))))))

(define *sig-attribs* {})
(varconfig! dom:sigattribs *sig-attribs* #f choice)

(defambda (get-sig-attribs attribids)
  (intersection (choice (reject attribids vector?)
			(elts (pick attribids vector?)))
		*sig-attribs*))

(define (dom/sig elt (attribs #f) (showptr #t) (seen '{name rel class id}))
  (stringout
    (try (get elt '%%xmltag) (get elt '%xmltag))
    (when (test elt 'class)
      ;; (->string (get elt 'class))
      (printout "." (string-subst (get elt 'class) " " ".")))
    (when (test elt 'id) (printout "#" (get elt 'id)))
    (when (test elt 'name) (printout "[NAME=" (get elt 'name) "]"))
    (when (test elt 'rel) (printout "[REL=" (get elt 'rel) "]"))
    (do-choices (attrib (difference (get-sig-attribs (get elt '%attribids))
				    seen))
      (when (test elt attrib)
	(printout "[" attrib "=" (get elt attrib) "]")
	(set+! seen attrib)))
    (when attribs
      (if (and (singleton? (get elt '%attribids))
	       (vector? (get elt '%attribids)))
	  (doseq (attrib (get elt '%attribids))
	    (unless (overlaps? attrib seen)
	      (printout "[" attrib "]")))
	  (do-choices (attrib (difference (get elt '%attribids) seen))
	    (printout "[" attrib "]"))))
    (when (and showptr
	       (or (overlaps? showptr '{force always})
		   (not (test elt 'id))))
      (printout (hashref elt)))))
(define (dom/nodeid elt (attribs #t) (showptr #f))
  (dom/sig elt attribs showptr))
(define (dom/updateid! elt (attribs #t) (showptr #f))
  (store! elt '%id (dom/sig elt attribs showptr)))

(define (dom/eltref elt)
  (if (oid? elt)
      (stringout (oid->string elt) "\"" (dom/sig elt #f #f))
      (dom/sig elt #f 'force)))

(varconfig! DOMPOOL dompool use-pool)

;;;; DOM IDs

(define (dom->id elt)
  (if (or (oid? elt) (string? elt)) elt
      (try (get elt 'oid) (get elt 'id))))
(define (id->dom id index)
  (if (oid? id) id (find-frames index 'id id)))

(defambda (dom->xmlids obj)
  (cond ((fail? obj) obj)
	((ambiguous? obj) (for-choices (e obj) (dom->xmlids e)))
	((or (immediate? obj) (fixnum? obj)) obj)
	((oid? obj)
	 (if (test obj '%xmltag) (glom "#" (get obj 'id))
	     (if (test obj '_qid) (get obj '_qid) obj)
	     obj))
	((or (number? obj) (string? obj) (packet? obj)) obj)
	((and (table? obj) (test obj '%xmltag)) (get obj 'id))
	((pair? obj)
	 (if (and (pair? (cdr obj)) (proper-list? obj))
	     (map dom->xmlids obj)
	     (cons (dom->xmlids (car obj)) (dom->xmlids (cdr obj)))))
	((vector? obj) (map dom->xmlids obj))
	((hashset? obj)
	 (choice->hashset
	  (for-choices (e (hashset-elts obj)) (dom->xmlids e))))
	((or (hashtable? obj) (slotmap? obj) (schemap? obj))
	 (let ((keys (getkeys obj))
	       (table (if (hashtable? obj) (make-hashtable)
			  (frame-create #f))))
	   (do-choices (key keys)
	     (store! table (dom->xmlids key)
		     (dom->xmlids (get obj key))))
	   table))
	(else obj)))


;;;; Debugging

(define (show-attribs node (cxt #f) (attribs) (delta))
  (comment
   (default! attribs (get node '%attribs))
   (set! delta (if (ambiguous? attribs) 1 3))
   (lineout "[" (elapsed-time) "]" ;;  logwarn |DumpAttribs|
     (when cxt (printout cxt " "))
     (choice-size attribs)
     " attribs for " (dom/sig node)
     (when (test node 'id) (printout " #!x" (number->string (hashptr node) 16))))
   (do-choices (a attribs)
     (lineout "[" (elapsed-time) "]" ;; logwarn |Attrib|
       (when cxt (printout cxt " "))
       (refcount a delta) " refs to " a
       " #!x" (number->string (hashptr a)))))
   node)
(define (show-all-attribs node (cxt #f))
  (if (or (pair? node)  (vector? node))
      (doseq (elt node)
	(unless (string? elt) (show-all-attribs elt cxt)))
      (begin (show-attribs node cxt)
	(when (exists? (get node '%content))
	  (doseq (child (get node '%content))
	    (unless (string? child) (show-all-attribs child cxt))))))
  node)
(define (dump-attribs node (cxt #f))
  (show-all-attribs node cxt)
  node)
(define (dump-node-attribs node (cxt #f))
  (show-attribs node cxt)
  node)
(module-export! '{dump-attribs dump-node-attribs})
