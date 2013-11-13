;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc.  All rights reserved.

(in-module 'domutils/css)

(use-module '{reflection texttools varconfig logger domutils gpath})

(module-export! '{css-rules css-rule css/parse dom/getcss
		  css/selector/parse css/selector/norm
		  css/match css/matches
		  css/drop-class! css/drop-prop!
		  css/textout css/norm})

(define property-extract
  #((label property #((opt "-") (lword)))
    ":" (spaces*)
    (label value (not> {";" "}"}))))
(define property-match
  #(#((opt "-") (lword)) ":" (spaces*) (not> {";" "}"})))
(define (parse-properties string (into #[]))
  (let ((plist (gather->list property-match string))
	(ordered '()))
    (dolist (p plist)
      (do-choices (p (text->frames property-extract p))
	(store! into (get p 'property) (get p 'value))
	(set! ordered (cons (get p 'property) ordered))))
    (add! into 'properties ordered)
    (add! into 'proplist plist)
    into))

(define base-selector '(char-not " \t\n,{}><+()@/"))
(define selector
  `(GREEDY #(,base-selector
	     (* #({(spaces) #((spaces) {"+" ">"} (spaces))}
		  ,base-selector)))))

(define css-rule-match
  `#((GREEDY #(,selector
	       (* #((spaces*) "," (spaces*) ,selector))))
     (spaces*)
     "{" (* #((spaces*) ,property-match ";"))
     (spaces*) "}"))
(define css-rule-extract
  `#((greedy
      #((label selector ,selector)
	(* #((spaces*) "," (spaces*) (label selector ,selector)))))
     (spaces*) "{"
     (label props (* #((spaces*)  (char-not ";{}") ";")))
     (spaces*) "}"))

(define css-comment #("/*" (not> "*/") "*/"))

(define css-rule `{,css-rule-match ,css-comment})

(define css-media-rule
  `#("@media" (spaces) (label media (not> "{")  ,trim-spaces)
     "{" (label rules (* #((spaces) ,css-rule (spaces)))) "}"))

(define css-charset-rule
  `#("@charset" (spaces) (label charset (not> (eol)))))
(define css-include-rule
  `#("@include" (spaces) (label include (not> (eol)))))
(define css-page-rule
  `#("@page" (spaces) "{" (not> "}") "}"))
(define css-fontface-rule
  `#("@font-face" (spaces) "{" (not> "}") "}"))

(define css-rules `{,css-media-rule ,css-rule-extract
		    ,css-charset-rule ,css-include-rule
		    ,css-fontface-rule ,css-page-rule ,css-comment})

(define (parse-rule rule (media #f))
  (cond ((not (string? rule)) rule)
	((has-prefix rule {"/*" "@include" "@charset" "@page" "@font-face"}) rule)
	((has-prefix rule "@media")
	 (let ((parsed (text->frames css-media-rule rule)))
	   `#[media ,(get parsed 'media)
	      rules ,(difference
		      (map (lambda (x) (parse-rule x media))
			   (gather->list css-rules (get parsed 'rules)))
		      '())
	      text ,rule]))
	(else (parse-css-rule rule media))))

(define (parse-css-rule rule media)
  (for-choices (ex (text->frames css-rule-extract rule))
    (let* ((selectors (get ex 'selector))
	   (parsed (css/selector/parse selectors))
	   (matchers
	    (choice (reject parsed vector?)
		    (for-choices (v (pick parsed vector?))
		      (reject (elt v -1) string?))))
	   (match+ (for-choices (v (pick parsed vector?))
		     (reject (elts v) string?)))
	   (seltext (stdspace (slice rule 0 (position #\{ rule))))
	   (props (get ex 'props))
	   (base `#[selectors ,selectors parsed ,parsed
		    matchers ,matchers matchers+ ,match+ ex ,ex
		    rule ,rule props ,props sel ,seltext]))
      (when media (store! base 'media media))
      (parse-properties props base))))

(define (css/parse string (media #f))
  (map (lambda (x) (parse-rule x media))
       (remove '() (gather->list (qc css-rules) string))))

;;; Parsing selectors

(define selector-extract
  `(+ {#((bow) (label tag (char-not ".#:[ \t\n")))
       #("." (label class (char-not ".#:[ \t\n")))
       #("#" (label id (char-not ".#:[ \t\n")))
       #("[" (label attrib (not> "]")) "]")
       #(":" (label pseudo (char-not ".#:[ \t\n")))}))
(define (css/selector/parse string)
  (if (not (string? string)) string
      (let ((clauses (segment string)))
	(if (= (length clauses) 1)
	    (text->frame selector-extract string)
	    (map (lambda (x)
		   (if (overlaps? x {">" "+"}) x
		       (text->frame selector-extract x)))
		 (->vector clauses))))))
(define (css/selector/norm string)
  (let ((parsed (if (not (string? string)) string
		    (text->frame selector-extract string))))
    (stringout (try (get parsed 'tag) "")
      (doseq (id (sorted (get parsed 'id))) (printout "#" id))
      (doseq (class (sorted (get parsed 'class))) (printout "." class))
      (doseq (attrib (sorted (get parsed 'attrib)))
	(printout "[" attrib "]"))
      (doseq (pseudo (sorted (get parsed 'pseudo)))
	(printout ":" pseudo)))))

;;; Matching selectors

(define (css/match pat sample)
  (when (string? pat) (set! pat (css/selector/parse pat)))
  (when (string? sample) (set! sample (css/selector/parse sample)))
  (and (or (not (test pat 'tag))
	   (identical? (downcase (get sample 'tag))
		       (downcase (get pat 'tag))))
       (or (not (test pat 'class))
	   (contains? (get sample 'class) (get pat 'class)))
       (or (not (test pat 'id)) (test sample 'id (get pat 'id)))
       (or (not (test pat 'attrib))
	   (contains? (get sample 'attrib) (get pat 'attrib)))))

(define (css/matches rules pat (plus #f))
  (if (string? pat) (set! pat (css/selector/parse pat)))
  (let ((seen (make-hashset)))
    (remove-if
     (lambda (x)
       (or (not x)
	   (and (not (string? x))
		(or (hashset-get seen (css/norm x))
		    (begin (hashset-add! seen (css/norm x)) #f)))))
     (if (pair? (car rules))
	 ;; It's actually a list of sheets
	 (apply append (map (lambda (sheet) (css/matches (cdr sheet) pat))
			    rules))
	 (map (lambda (x)
		(and (not (string? x))
		     (or (exists css/match pat (get x 'matchers))
			 (exists css/match pat (get x 'matchers+)))
		     x))
	      rules)))))

;;; Manipulating sheets

(define (css/drop-class! sheet classname)
  (let ((csspat `#[class ,classname])
	(textpat `#("." ,classname {(isspace) "." "#" "[" ":"}))
	(rules (->vector (cdr sheet)))
	(deletions 0))
    (doseq (rule rules)
      (unless (string? rule)
	(when (and (test rule 'rule) (css/match pat rule))
	  (drop! rule 'matchers
		 (pick (get rule 'matchers) 'class classname))
	  (drop! rule 'matchers+
		 (pick (get rule 'matchers+) 'class classname))
	  (drop! rule 'selectors
		 (pick (get rule 'selectors) string-contains? textpat))
	  (drop! rule 'parsed
		 (filter-choices (p (get rule 'parsed))
		   (if (vector? p)
		       (exists? (pick (elts p) 'class classname))
		       (test p 'class classname))))
	  (drop! (get rule 'ex) 'selector
		 (pick (get (get rule 'ex) 'selector) string-contains? textpat)))))
    (doseq (rule rules i)
      (unless (string? rule)
	(when (and (test rule 'rule) (fail? (get rule 'selector)))
	  (store! rules i #f)
	  (set! deletions (1+ deletions)))))
    (unless (zero? deletions)
      (set-cdr! sheet (->list (remove #f rules))))))

(define (css/drop-prop! sheet propname)
  (let ((rules (->vector (cdr sheet)))
	(prefix (glom propname ":"))
	(deletions 0))
    (doseq (rule rules)
      (unless (string? rule)
	(when (and (test rule 'rule)
		   (position propname (get rule 'properties)))
	  (drop! rule propname)
	  (store! rule 'properties (remove propname (get rule 'properties)))
	  (store! rule 'proplist
		  (remove #f (map (lambda (x) (and (not (has-prefix x prefix)) x))))))))
    (doseq (rule rules i)
      (unless (string? rule)
	(when (and (test rule 'rule) (test rule 'properties '()))
	  (store! rules i #f)
	  (set! deletions (1+ deletions)))))
    (unless (zero? deletions)
      (set-cdr! sheet (->list (remove #f rules))))))

;;; Getting CSS from a parsed DOM

(define (getcss dom source (count 0))
  (remove
   #f (map (lambda (node)
	     (if (test node '%xmltag 'style)
		 (cons (try (glom "#" (get node 'id))
			    (begin (set! count (1+ count)) count))
		       (css/parse
			(textsubst
			 (apply glom (get node '%content))
			 {"<!--" "-->" "<![CDATA" "]>"} "")
			(try (get node 'media) #f)))
		 (and (test node 'rel "stylesheet")
		      (let* ((href (get node 'href))
			     (media (try (get node 'media) #f))
			     (absref (->gpath href source))
			     (content (onerror (gp/fetch absref)
					(lambda (x)
					  (logwarn "Couldn't access " absref)
					  #f))))
			(if (and (exists? content) (string? content))
			    (cons href (css/parse content media))
			    (list href))))))
	   (dom/find->list dom "style,link"))))
(define (dom/getcss dom source)
  (try (get dom 'css)
       (let ((computed (getcss dom source 0)))
	 (store! dom 'css computed)
	 computed)))

;;; Output parsed CSS rule

(define (css/textout entry (norm #t))
  (unless (null? entry)
    (if (pair? entry)
	(doseq (e entry)
	  (when e (css/textout e) (printout "\n")))
	(begin
	  (do-choices (sel (get entry 'parsed) i)
	    (printout (if (> i 0) ", ")
	      (if (vector? sel)
		  (doseq (elt sel i)
		    (printout (if (> i 0) " ")
		      (if (string? elt) elt (css/selector/norm elt))))
		  (css/selector/norm sel))))
	  (printout " {")
	  (doseq (prop (get entry 'properties))
	    (printout "\n\t" prop ": " (get entry prop) ";"))
	  (printout "}")))))

(define (css/norm entry)
  (stringout
    (doseq (sel (sorted (css/selector/norm (get entry 'parsed))) i)
      (printout (if (> i 0) ",") sel))
    " {" (doseq (prop (sorted (elts (get entry 'props))))
	   (printout prop ":" (get entry prop) ";"))
    "}"))

