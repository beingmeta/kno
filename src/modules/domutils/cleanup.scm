;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

(in-module 'domutils/cleanup)

(use-module '{fdweb xhtml texttools reflection ezrecords logger varconfig
	      domutils domutils/styles})

(define-init %loglevel %warning%)

(module-export! '{dom/cleanup! dom/mergestyles! dom/unipunct!
		  dom/cleanblocks! dom/raisespans!
		  dom/smartquote!})

(module-export!
 '{dom/mergebreaks/subst
   dom/mergebreaks
   dom/mergenbsp/subst
   dom/mergenbsp
   dom/mergespans!})

(module-export! '{dom/cleanup/mergelines dom/cleanup/unipunct
		  dom/cleanup/mergelines+unipunct
		  dom/cleanup/drop-imagesizes!})

;;; Rules

(define fix-bad-style
  #((isalnum) (subst (spaces) "; ") (lword) ":"))

(define default-stylefixes {})
(define default-classfixes {})

(define dom-cleanup-rules #[])
(config-def! 'dom:cleanup:rules
	     (lambda (var (val))
	       (if (bound? val)
		   (if (pair? val)
		       (store! dom-cleanup-rules (car val) (cdr val))
		       (if (and (procedure? val) (procedure-name val))
			   (store! dom-cleanup-rules
				   (string->symbol (procedure-name val))
				   val)
			   (error "Invalid cleanup rule")))
		   dom-cleanup-rules)))

(define keep-empty-attribs '{id name})

;;; Fixing punctuation to be prettier

(define (wrapdash string) (glom "&nbsp;" string " "))

(define (unipunct string)
  (textsubst
   (textsubst
    (textsubst
     (let ((n-quotes (length (gather->list "\"" string))))
       (if (even? n-quotes)
	   (stringout
	     (let* ((scan 0)
		    (start (position #\" string scan))
		    (end (and start (position #\" string (1+ start))))
		    (lim (length string)))
	       (while (and start end (< scan lim))
		 (printout (subseq string scan start) "\&ldquo;"
		   (subseq string (1+ start) end)
		   "\&rdquo;")
		 (set! scan (1+ end))
		 (set! start (position #\" string scan))
		 (set! end (and start (position #\" string (1+ start)))))
	       (when (and scan (< scan lim))
		 (printout (subseq string scan)))))
	   string))
     `(GREEDY #((spaces*)
		{(subst "-" "\&ndash;")
		 (subst "--" "\&mdash;")
		 (subst "---" "\&mdash;")
		 (subst #("..." (opt ".") (opt ".") (opt ".") (opt ".")) "\u2026")}
		{(spaces) (eos)})))
    `(GREEDY #({(islower) (ispunct)}
	       {(subst (+ "&mdash;") ,wrapdash)
		(subst (+ "\&mdash;") ,wrapdash)})))
   '(GREEDY (+ {"&nbsp;" #\u00a0})) "\&nbsp;"))
(define (dom/unipunct! arg)
  (if (string? arg) (unipunct arg)
      (if (pair? arg) (map dom/unipunct! arg)
	  (if (not (table? arg)) arg
	      (if (test arg '%content)
		  (begin (store! arg '%content
				 (map dom/unipunct! (get arg '%content)))
		    arg)
		  arg)))))

;;; Smart quotes

(define (dom/smartquote! node (state #f))
  (if (string? node)
      (let* ((scan 0) (qpos (position #\" node scan)))
	(while qpos
	  (printout (slice node scan qpos)
	    (if state "\&rdquo;" "\&ldquo;"))
	  (set! state (not state))
	  (set! scan (1+ qpos))
	  (set! qpos (position #\" node scan)))
	(printout (subseq node scan)))
      (when (and (not (test node '%xmltag '%pre))
		 (not (test node 'xml:space "preserve"))
		 (test node '%content) (exists? (get node '%content))
		 (pair? (get node '%content)))
	(store! node '%content
		(->list (forseq (elt (->vector (get node '%content)))
			  (if (and (string? elt) (position #\" elt))
			      (stringout
				(set! state (dom/smartquote! elt state)))
			      (if (string? elt) elt
				  (begin (set! state (dom/smartquote! elt state))
				    elt))))))))
  state)

;;; Merging nbsp runs

(define dom/mergenbsp/subst
  '(SUBST (+ {"&nbsp" #\u00a0}) "\&nbsp;"))
(define (dom/mergenbsp node)
  (if (string? node)
      (textsubst node dom/mergenbsp/subst)
      (if (test node '%content)
	  (if (or (test node '%xmltag 'pre)
		  (test node 'xml:space "preserve"))
	      node
	      (begin (store! node '%content
			     (map dom/mergenbsp (get node '%content)))
		node))
	  node)))

;;; Merging breaks

(define dom/mergebreaks/subst
  '(IC (IS (SUBST (GREEDY (+ #("<br" {(spaces) ""} {"/>" ">"} (spaces))))
		  "<br/>"))))

(define (mergebreaks content (output '()))
  (if (null? content) (reverse output)
      (if (string? (car content))
	  (mergebreaks (cdr content) (cons (car content) output))
	  (if (test (car content) '%xmltag 'br)
	      (do ((scan content (cdr scan)))
		  ((or (null? scan)
		       (not (if (string? (car scan))
				(empty-string? (car scan) #f #t)
				(test (car scan) '%xmltag 'br))))
		   (mergebreaks scan (cons (car content) output))))
	      (mergebreaks (cdr content) (cons (car content) output))))))

(define (dom/mergebreaks node)
  (if (pair? node) (mergebreaks node)
      (if (test node '%content)
	  (let* ((content (get node '%content))
		 (merged (mergebreaks content)))
	    (store! node '%content merged)
	    (doseq (elt content)
	      (unless (string? elt) (dom/mergebreaks elt)))
	    node)
	  node)))

;;;; Merge text

;;; This merges runs of strings in the %CONTENT of DOM nodes into
;;;  single strings, calling an optional TEXTFN on each combined
;;;  string.  The TEXTFN can normalize whitespace or insert indents.

(define (cleanup! node textfn dropfn unwrapfn dropempty classfixes stylefixes)
  (logdetail "Cleanup " (dom/eltref node))
  (if (or (test node '%xmltag '{pre script style})
	  (test node 'xml:space "preserve"))
      node
      (if (test node '%content)
	  (cleanup-content node textfn dropfn unwrapfn dropempty
			   classfixes stylefixes)
	  ;; Fix empty tags
	  (if (test node '%xmltag *void-tags*)
	      node
	      (begin (store! node '%content '())
		node)))))

(defambda (cleanup-content node textfn dropfn unwrapfn dropempty classfixes stylefixes)
  (let ((vec (->vector (get node '%content)))
	(newfn (and textfn
		    (not (try (get node 'keepspace) #f))
		    textfn))
	(isblock (test node '%xmltag *block-tags*))
	(strings '())
	(merged '()))
    (when (test node '%xmltag 'font) (fix-font-node node))
    (when (and classfixes (test node 'class))
      (dom/set! node 'class
		(if (eq? classfixes #t)
		    (stdspace (get node 'class))
		    (stdspace (textsubst (get node 'class)
					 (qc classfixes))))))
    (when (test node 'style)
      (let* ((style (get node 'style))
	     (nstyle (textsubst style (qc fix-bad-style))))
	(when stylefixes
	  (set! nstyle (dom/normstyle nstyle (qc stylefixes))))
	(unless (identical? style nstyle)
	  (logdetail "Converted " (write style) " to " (write nstyle))
	  (dom/set! node 'style nstyle)))
      (when (empty-string? (get node 'style)) (dom/drop! node 'style)))
    (doseq (child vec)
      (cond ((string? child)
	     (unless (= (length child) 0)
	       (set! strings (cons child strings))))
	    (else
	     (set! child
		   (cleanup! child textfn
			     (qc dropfn) (qc unwrapfn)
			     dropempty
			     (qc classfixes)
			     (qc stylefixes)))
	     (when (and dropempty (test child '%xmltag *block-tags*)
			(not (test child keep-empty-attribs))
			(not (has-prefix (first (get child '%attribs))
					 "DATA-"))
			(test child '%content)
			(or (null? (get child '%content))
			    (every? empty-child? (get child '%content))))
	       (loginfo |DeleteEmpty|
		 "Deleting empty node \n\t" (pprint child))
	       (set! child #f))
	     (when (null? child) (set! child #f))
	     (when (and child (exists? dropfn) dropfn (dom/match child dropfn))
	       (set! child #f))
	     (when (and child (not (null? strings)))
	       (set! merged
		     (cons (if textfn
			       (textfn (apply glom (reverse strings)))
			       (apply glom (reverse strings)))
			   merged))
	       (set! strings '()))
	     (if (pair? child)
		 (set! merged (append (reverse child) merged))
		 (when child (set! merged (cons child merged)))))))
    (unless (null? strings)
      (set! merged
	    (cons (if textfn (textfn (apply glom (reverse strings)))
		      (apply glom (reverse strings)))
		  merged))
      (set! strings '()))
    (when (and isblock (pair? merged))
      (if (not (string? (car merged)))
	  (set! merged (cons "\n" merged))
	  (unless (or (test node '%xmltag 'P)
		      (has-suffix (car merged) "\n"))
	    (set-car! merged (glom (car merged) "\n")))))
    (logdetail "Reversing " (length merged)
	       " merged content elements for "
	       (dom/eltref node))
    (set! merged (reverser merged))
    (when (and isblock (pair? merged))
      (if (not (string? (car merged)))
	  (set! merged (cons "\n" merged))
	  (if (not (has-prefix (car merged) "\n"))
	      (set-car! merged (glom "\n" (car merged))))))
    (store! node '%content merged)
    (if (or (and (exists? unwrapfn) unwrapfn (dom/match node unwrapfn))
	    (and (test node '%xmltag 'span)
		 (fail? (get node '%attribids))
		 (fail? (get node '%attribs))))
	merged
	node)))

(define (dom/cleanup! node (opts #[])
		      (textfn #f) (dropfn (config 'dom:drop #f)) 
		      (unwrapfn (config 'dom:unwrap #f)) 
		      (dropempty (config 'dom:dropempty #f))
		      (classfixes (config 'dom:classfix #f))
		      (stylefixes (config 'dom:stylefix #f)))
  (set! textfn (getopt opts 'dom:textfn textfn))
  (set! dropfn (getopt opts 'dom:drop dropfn))
  (set! unwrapfn (getopt opts 'dom:unwrap unwrapfn))
  (set! dropempty (getopt opts 'dom:dropempty dropempty))
  (set! classfixes (getopt opts 'dom:classfix classfixes))
  (set! stylefixes (getopt opts 'dom:stylefix stylefixes))
  (if (overlaps? classfixes #t) (set+! classfixes default-classfixes))
  (if (overlaps? stylefixes #t) (set+! stylefixes default-stylefixes))
  (notice%watch "DOM/CLEANUP!" 
    "NODE" (dom/nodeid node)
    dropempty classfixes stylefixes 
    dropfn unwrapfn textfn)
  (cleanup! node textfn dropfn unwrapfn dropempty
	    (and classfixes
		 (qc (get dom-cleanup-rules (pick classfixes symbol?))
		     (reject classfixes symbol?)))
	    (and stylefixes
		 (qc (get dom-cleanup-rules (pick stylefixes symbol?))
		     (reject stylefixes symbol?)))))

(define (reverser e (result '()))
  ;; This reverses e and also insures that blocks are preceded
  ;;  and succeeded by newlines
  (if (null? e) result
      (if (string? (car e))
	  (if (and (not (has-prefix (car e) "\n"))
		   (pair? (cdr e)) (not (string? (cadr e)))
		   (overlaps? (get (cadr e) '%xmltag) *block-tags*))
	      (reverser (cdr e) (cons (glom "\n" (car e)) result))
	      (reverser (cdr e) (cons (car e) result)))
	  (if (and (pair? result) (not (string? (car result)))
		   (test (car e) '%xmltag *block-tags*))
	      (reverser (cdr e) (cons* (car e) "\n" result))
	      (reverser (cdr e) (cons (car e) result))))))

(define (fix-font-node node (style))
  (default! style (try (get node 'style) ""))
  (store! node '%xmltag 'span)
  (when (exists? (get node 'size))
    (set! style (glom style " font-size: "
		  (+ 60 (* 20 (->number (get node 'size)))) 
		  "%;"))
    (dom/drop! node 'size))
  (when (exists? (get node 'face))
    (set! style (glom style " font-family: " (get node 'face) ";"))
    (dom/drop! node 'face))
  (when (exists? (get node 'color))
    (set! style (glom style " color: " (get node 'color) ";"))
    (dom/drop! node 'color))
  (dom/set! node 'style style)
  node)

(define (empty-child? x)
  (if (string? x) 
      (empty-string? x #f #t)
      (or (test x '%xmltag 'br)
	  (and (test x '%content)
	       (null? (get x '%content))
	       (test x '%xmltag *block-tags*)))))

;;; Text cleanup functions

(define (mergelines string)
  (if (empty-string? string #f #t)
      (if (position #\newline string) "\n" " ")
      (textsubst string #("\n" (+ #((spaces*) "\n"))) "\n\n")))

(define dom/cleanup/mergelines mergelines)
(define dom/cleanup/unipunct unipunct)
(define (dom/cleanup/mergelines+unipunct text)
  (unipunct (mergelines text)))

;;; Style cleanup functions

(define (dom/mergestyles! dom 
			  (classdefs (make-hashtable))
			  (stylemap (make-hashtable))
			  (prefix "fd__autostyle"))
  (let ((stylecount (try (get classdefs '%count) 1)))
    (dom/gather-styles! dom stylemap #t)
    (doseq (style (rsorted (getkeys stylemap)
			   (lambda (s) (choice-size (get stylemap s)))))
      (let ((classname (glom prefix stylecount)))
	(set! stylecount (1+ stylecount))
	(store! classdefs classname (cons (choice-size (get stylemap style)) style))
	(do-choices (node (get stylemap style))
	  (dom/addclass! node classname)
	  (dom/drop! node 'style))))
    (store! dom '%stylemap stylemap)
    (store! dom '%classdefs classdefs)
    (store! classdefs '%count stylecount)
    classdefs))

;;; Identifying empty nodes

(define (empty-text? x)
  (and (string? x) 
       (empty-string? (decode-entities x) #f #t)))

(define (empty-node? x)
  (or (and (string? x) (empty-string? x #f #t))
      (and (not (string? x))
	   (every? empty-text? (get x '%content)))))

;;;; Merging text (strings) in bodies

(define (merge-text in) (merge-strings #f in '()))

(define (merge-strings cur in out)
  (if (null? in)
      (if (empty-string? cur #f #t)
	  (reverse (if cur (cons (fix-whitespace cur) out) out))
	  (reverse (if cur (cons cur out) out)))
      (if (string? (car in))
	  (if cur
	      (merge-strings (glom cur (car in)) (cdr in) out)
	      (merge-strings (car in) (cdr in) out))
	  (if cur
	      (if (empty-string? cur #f #t)
		  (merge-strings #f (cdr in)
				 (cons* (car in) (fix-whitespace cur) out))
		  (merge-strings #f (cdr in) (cons* (car in) cur out)))
	      (merge-strings #f (cdr in) (cons (car in) out))))))

(define (fix-whitespace s)
  (if (= (length s) 0) s
      (if (position #\newline s) "\n"
	  " ")))

;;;; Merging spans

(define (mergespans content)
  (let ((merged '())
	(open #f) (children '())
	(openclass {}) (openstyle {})
	(opentitle {}) (openattribs {}))
    (doseq (node content)
      (if (string? node)
	  (if (empty-string? node #f #t)
	      (if open
		  (set! children (cons node children))
		  (set! merged (cons node merged)))
	      (begin (when open
		       (store! open '%content (reverse children))
		       (set! merged (cons open merged))
		       (set! open #f) (set! children '())
		       (set! openclass {}) (set! openstyle {})
		       (set! openattribs {}) (set! opentitle {}))
		(set! merged (cons node merged))))
	  (if (and open (test node '%xmltag 'span)
		   (identical? openclass (get node 'class))
		   (identical? openstyle (get node 'style))
		   (identical? opentitle (get node 'title))
		   (identical? openattribs (get node '%attribids)))
	      (set! children (append (reverse (get node '%content))
				     children))
	      (begin (when open
		       (store! open '%content (reverse children))
		       (set! merged (cons open merged))
		       (set! open #f) (set! children '())
		       (set! openclass {}) (set! openstyle {})
		       (set! openattribs {}) (set! opentitle {}))
		(if (test node '%xmltag 'span)
		    (begin (set! open node)
		      (set! openclass (get open 'class))
		      (set! openstyle (get open 'style))
		      (set! opentitle (get open 'title))
		      (set! openattribs (get open '%attribids))
		      (set! children (reverse (get node '%content))))
		    (set! merged (cons node merged)))))))
    (when open
      (store! open '%content (reverse children))
      (set! merged (cons open merged)))
    (reverse merged)))

(define (dom/mergespans! root (content))
  (set! content (get root '%content))
  (when (and (exists? content) (pair? content))
    (set! content (mergespans content))
    (store! root '%content content)
    (doseq (elt content)
      (unless (string? elt) (dom/mergespans! elt)))))

;;;; Elide wrappers

(define (dom/cleanblocks! arg (settings #f))
  (if (pair? arg)
      (->list (apply append (forseq (node (->vector arg))
			      (cleanblocks node settings))))
      (if (exists? (get arg '%content))
	  (begin
	    (store! arg '%content
		    (append->list
		     (forseq (node (->vector (get arg '%content)))
		       (cleanblocks node settings))))
	    arg)
	  arg)))
(define (append->list elts) (->list (apply append elts)))

(define (cleanblocks node (opts #f))
  (cond ((string? node) (vector node))
	((or (not (test node '%content)) (null? (get node '%content)))
	 (vector node))
	((or (test node '%xmltag *pre-tags*)
	     (test node 'xml:space "preserve"))
	 (vector node))
	((test node '%xmltag *inline-tags*) (vector node))
	((test node '%xmltag *terminal-block-tags*)
	 (promote-single-spans node)
	 (vector node))
	((test node '%xmltag *wrapper-tags*)
	 (promote-single-spans node)
	 (let* ((content (->vector (get node '%content)))
		(cleaned
		 (apply append
			(forseq (node content)
			  (cleanblocks node opts)))))
	   (store! node '%content (merge-text (->list cleaned)))
	   (vector node)))
	(else (let ((content (->vector (get node '%content))))
		(if (and (test node '%xmltag '{div span})
			 (every? (lambda (x)
				   (or (not (string? x)) (empty-string? x #f #t)))
				 content)
			 (not (test node 'style)) (not (test node 'class))
			 (not (test node 'id)))
		    (apply append (forseq (node content) (cleanblocks node opts)))
		    (let ((cleaned (apply append (forseq (node content)
						   (cleanblocks node opts)))))
		      (store! node '%content (merge-text (->list cleaned)))
		      (vector node)))))))

(define (promote-single-spans node (rules #f))
  (let* ((content (get node '%content))
	 (nodes (if (exists? content) (remove-if empty-text? content) '())))
    (when (and (= (length nodes) 1) (not (string? (car nodes)))
	       (test (car nodes) '%xmltag 'span)
	       (fail? (difference (get (car nodes) '%attribids)
				  '{style class})))
      (when (test (car nodes) 'style)
	(dom/set! node 'cstyle (get (car nodes) 'style))
	(if (test node 'style)
	    (dom/set! node 'style
		      (glom  (get node 'style) " " (get (car nodes) 'style)))
	    (dom/set! node 'style (get (car nodes) 'style))))
      (when (test (car nodes) 'class)
	(dom/set! node 'cclass (get (car nodes) 'class))
	(when (test node 'class)
	  (dom/set! node 'class
		    (stdspace (glom (get node 'class) " "
				(get (car nodes) 'class))))))
      (store! node '%content
	      (apply append
		     (map (lambda (x)
			    (if (string? x) (list x)
				(get (car nodes) '%content)))
			  (get node '%content)))))))

;;;; Raising spans

;;; If a node consists of just inline elements and empty text,
;;;  raise the dominant class/style

(define (dom/raisespans! node)
  (and (exists? (get node '%content))
       (every? (lambda (child)
		 (if (string? child)
		     (empty-string? child #f #t)
		     (dom/inline? child)))
	       (get node '%content))
       (raise-spans node)))

(define (raise-spans node)
  (let* ((content (get node '%content))
	 (spans (make-hashtable))
	 (spanscores (make-hashtable))
	 (len (score-styles content spans spanscores))
	 (maxval (table-maxval spanscores))
	 (top (table-max spanscores)))
    (when (and (exists? content) len (> len 0)
	       (exists? maxval) (> maxval (/ len 2))
	       (not (or (fail? top) (ambiguous? top)
			(equal? top '(#f . #f)))))
      (when (and (cdr top) (not (empty-string? (cdr top))))
	(if (and (test node 'style) (not (empty-string? (get node 'style))))
	    (dom/set! node 'style
		      (dom/normstyle (glom (get node 'style) " " (cdr top))))
	    (dom/set! node 'style (cdr top))))
      (when (and (car top) (not (empty-string? (car top))))
	(if (and (test node 'class) (not (empty-string? (get node 'class))))
	    (dom/set! node 'class (glom (get node 'class) " " (car top)))
	    (dom/set! node 'class (car top)))))
    (store! node '%content
	    (->list
	     (apply append
		    (map (lambda (elt)
			   (if (string? elt) (vector elt)
			       (if (test spans top elt)
				   (->vector (get elt '%content))
				   (vector elt))))
			 (->vector (get node '%content))))))))

(define (score-styles content spans scores (len 0))
  (if (null? content) len
      (let ((elt (car content)))
	(if (string? elt)
	    (if (empty-string? elt #f #t)
		(score-styles (cdr content) spans scores len)
		#f)
	    (and (dom/inline? elt)
		 (let ((width (length (dom/textify elt)))
		       (key (cons (try (get elt 'class) #f)
				  (try (get elt 'style) #f))))
		   (when (test elt '%xmltag 'span)
		     (hashtable-increment! scores key width)
		     (add! spans key elt))
		   (score-styles (cdr content) spans scores (+ len width))))))))

;;;; Dropping image dimensions

(define (dom/cleanup/drop-imagesizes! dom)
  (let* ((index (get dom 'index))
	 (images (pick (find-frames index '%xmltag 'img) '{width height})))
    (when (exists? images)
      (logdebug PUBTOOL/READEPUB "Dropping explicit dimensions from "
		(choice-size images) " image(s)."))
    (do-choices (img images)
      (when (test img 'width)
	(drop! index (cons 'width (get img 'width)) img)
	(add! index (cons 'has 'data-source-width) img)
	(add! index (cons 'data-source-width (get img 'width)) img)
	(dom/set! img 'data-source-width (get img 'width))
	(dom/drop! img 'width)
	(drop! index (cons 'has 'width) img))
      (when (test img 'height)
	(drop! index (cons 'height (get img 'height)) img)
	(add! index (cons 'has 'data-source-height) img)
	(add! index (cons 'data-source-height (get img 'height)) img)
	(dom/set! img 'data-source-height (get img 'height))
	(dom/drop! img 'height)
	(drop! index (cons 'has 'height) img)))))
