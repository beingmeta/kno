;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc.  All rights reserved.

(in-module 'domutils/cleanup)

(use-module '{fdweb xhtml texttools reflection ezrecords logger varconfig
	      domutils domutils/styles})

(define-init %loglevel %notice!)

(module-export! '{dom/cleanup! dom/mergestyles! dom/unipunct!})

(module-export!
 '{dom/mergeheads/subst
   dom/mergeheads
   dom/mergebreaks/subst
   dom/mergebreaks})

(module-export! '{dom/cleanup/mergelines dom/cleanup/unipunct
		  dom/cleanup/mergelines+unipunct})

;;; Fixing punctuation to be prettier

(define (wrapdash string) (glom "&nbsp;" string " "))

(define (unipunct string)
  (textsubst
   (textsubst
    (let ((n-quotes (length (gather->list "\"" string))))
      (if (even? n-quotes)
	  (stringout
	    (let* ((scan 0)
		   (start (position #\" string scan))
		   (end (and start (position #\" string (1+ start)))))
	      (while (and start end)
		(printout (subseq string scan start) "\&ldquo;"
		  (subseq string (1+ start) end)
		  "\&rdquo;")
		(set! scan (1+ end))
		(set! start (position #\" string scan))
		(set! end (and start (position #\" string (1+ start)))))
	      (when scan (printout (subseq string scan)))))
	  string))
    #((spaces*)
      {(subst "-" "\&ndash;")
       (subst "--" "\&mdash;")
       (subst "..." "\u2026;")}
      {(spaces) (eos)}))
   `#({(islower) (ispunct)}
      {(subst (+ "&mdash;") ,wrapdash)
       (subst (+ "\&mdash;") ,wrapdash)})))
(define (dom/unipunct! arg)
  (if (string? arg) (unipunct arg)
      (if (pair? arg) (map dom/unipunct! arg)
	  (if (not (table? arg)) arg
	      (if (test arg '%content)
		  (begin (store! arg '%content
				 (map dom/unipunct! (get arg '%content)))
		    arg)
		  arg)))))

;;; Merging heads

(define (wrap core prefix suffix) (string-append prefix core suffix))

(define (mergeheadrunrule head level)
  `(ic (subst (GREEDY #("<" ,head (not> ">") ">" (not> "</") "</" ,head ">"
			(+ #((spaces)
			     "<" ,head (not> ">") ">" (not> "</") "</" ,head ">"))))
	      ,wrap
	      ,(stringout "<div class='sbookterminal sbookid sbook"level"head'>\n")
	      "\n</div>\n")))
(define dom/mergeheadruns/subst
  '(subst  (greedy (+ #("<" {"H" "h"} (isdigit)
			(not> "</" {"H" "h"} (isdigit))
			"</" {"H" "h"} (isdigit) ">" (spaces))))
	  ,wrap "\n<hgroup>\n" "\n</hgroup>\n"))

(define heads '{h1 h2 h3 h4 h5 h6 h7 h8 h9})

(define (mergeheads content (output '()) (wrapper #[%XMLTAG HGROUP]))
  (if (null? content) (reverse output)
      (if (string? (car content))
	  (mergeheads (cdr content) (cons (car content) output))
	  (if (test (car content) '%xmltag heads)
	      (do ((headtag (get (car content) '%xmltag))
		   (scan (cdr content) (cdr scan))
		   (hgroup (list (car content))  (cons (car scan) hgroup))
		   (headcount 1 (if (string? (car scan)) headcount (1+ headcount))))
		  ((or (null? scan)
		       (not (if (string? (car scan))
				(empty-string? (car scan))
				(test (car scan) '%xmltag headtag))))
		   (when (and (> headcount 1)
			      (string? (car hgroup)) (empty-string? (car hgroup)))
		     (set! hgroup (cons "\n" (cdr hgroup))))
		   (if (> headcount 1)
		       (let ((node (deep-copy wrapper)))
			 (store! node '%content (cons "\n" (reverse hgroup)))
			 (mergeheads scan (cons* "\n" node "\n" output)))
		       (mergeheads (cdr content) (cons (car content) output)))))
	      (mergeheads (cdr content) (cons (car content) output))))))

(define (dom/mergeheads node (wrapper #[%XMLTAG HGROPU]))
  (if (pair? node) (mergeheads node '() wrapper)
      (if (test node '%xmltag 'hgroup)
	  node
	  (if (test node '%content)
	      (let* ((content (get node '%content))
		     (merged (mergeheads content)))
		(store! node '%content merged)
		(dolist (elt content)
		  (unless (string? elt) (dom/mergeheads elt wrapper)))
		node)
	      node))))

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
				(empty-string? (car scan))
				(test (car scan) '%xmltag 'br))))
		   (mergebreaks scan (cons (car content) output))))
	      (mergebreaks (cdr content) (cons (car content) output))))))

(define (dom/mergebreaks node)
  (if (pair? node) (mergebreaks node)
      (if (test node '%content)
	  (let* ((content (get node '%content))
		 (merged (mergebreaks content)))
	    (store! node '%content merged)
	    (dolist (elt content)
	      (unless (string? elt) (dom/mergebreaks elt)))
	    node)
	  node)))

;;;; Merge text

;;; This merges runs of strings in the %CONTENT of DOM nodes into
;;;  single strings, calling an optional TEXTFN on each combined
;;;  string.  The TEXTFN can normalize whitespace or insert indents.

(define *block-tags*
  '{DIV P SECTION ASIDE DETAIL FIGURE BLOCKQUOTE UL OL HEAD BODY DL})
(define *head-tags* '{H1 H2 H3 H4 H5 H6 H7})

(define (dom/cleanup! node (textfn #f) (dropfn #f) (dropempty #f)
		      (mergeheads #f)
		      (cleanstyles #f))
  (if (test node '%content)
      (let ((vec (->vector (get node '%content)))
	    (newfn (and textfn
			(not (or (try (get node 'keepspace) #f)
				 (test node '%xmltag 'pre)
				 (test node 'xml:space "preserve")))
			textfn))
	    (isblock (test node '%xmltag *block-tags*))
	    (strings '())
	    (hgroup '())
	    (merged '()))
	(when (test node '%xmltag 'font) (fix-font-node node))
	(when (and cleanstyles (test node 'style))
	  (dom/set! node 'style
		    (if (eq? cleanstyles #t)
			(dom/normstyle (get node 'style))
			(dom/normstyle (get node 'style) cleanstyles)))
	  (when (empty-string? (get node 'style))
	    (dom/drop! node 'style)))
	(doseq (child vec)
	  (if (string? child)
	      (if (or (null? hgroup) (not mergeheads))
		  (set! strings (cons child strings))
		  (if (empty-string? child)
		      (set! hgroup (cons child hgroup))
		      (begin
			(set! merged
			      (cons* `#[%xmltag HGROUP %content
					,(cons "\n" (reverse hgroup))]
				     "\n" merged))
			(set! hgroup '()))))
	      (unless (and dropfn (dropfn child))
		(set! child
		      (dom/cleanup! child textfn
				    (qc dropfn) dropempty
				    mergeheads (qc cleanstyles)))
		(when (and dropempty isblock
			   (test child '%content)
			   (or (null? (get child '%content))
			       (every? empty-child? (get child '%content))))
		  (set! child #f))
		(unless (or (not child) (and dropfn (dropfn child)))
		  (unless (null? strings)
		    (set! merged (cons (if textfn
					   (textfn (apply glom (reverse strings)))
					   (apply glom (reverse strings)))
				       merged))
		    (set! strings '()))
		  (if (test child '%xmltag *head-tags*)
		      (set! hgroup (cons child hgroup))
		      (set! merged (cons child merged)))))))
	(unless (null? strings)
	  (set! merged
		(cons (if textfn (textfn (apply glom (reverse strings)))
			  (apply glom (reverse strings)))
		      merged))
	  (set! strings '()))
	(unless (null? hgroup)
	  (set! merged
		(cons* `#[%xmltag HGROUP %content ,(cons "\n" (reverse hgroup))]
		       "\n" merged))
	  (set! hgroup '()))
	(when isblock
	  (if (not (string? (car merged)))
	      (set! merged (cons "\n" merged))
	      (if (not (has-suffix (car merged) "\n"))
		  (set-car! merged (glom (car merged) "\n")))))
	(set! merged (reverse merged))
	(when isblock
	  (if (not (string? (car merged)))
	      (set! merged (cons "\n" merged))
	      (if (not (has-prefix (car merged) "\n"))
		  (set-car! merged (glom "\n" (car merged))))))
	(store! node '%content merged)
	node)
      node))

(define (fix-font-node node (style))
  (default! style (try (get node 'style) ""))
  (store! node '%xmltag 'span)
  (when (exists? (get node 'size))
    (set! style (glom style " font-size: "
		  (+ 60 (* 20 (get node 'size))) 
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
  (if (string? x) (empty-string? x)
      (or (test x '%xmltag 'br)
	  (and (test x '%content)
	       (null? (get x '%content))
	       (test x '%xmltag *block-tags*)))))

;;; Text cleanup functions

(define (mergelines string)
  (if (empty-string? string)
      (if (position #\newline string) "\n" " ")
      (textsubst string #("\n" (+ #((spaces*) "\n"))) "\n\n")))

(define dom/cleanup/mergelines mergelines)
(define dom/cleanup/unipunct unipunct)
(define (dom/cleanup/mergelines+unipunct text)
  (unipunct (mergelines text)))

;;; Style cleanup functions

(define default-style-rules css/dropdecimals)

(define (dom/mergestyles!
	 dom (stylerules default-style-rules)
	 (classdefs (make-hashtable))
	 (stylemap (make-hashtable))
	 (prefix "fd__autostyle"))
  (let ((stylecount (try (get classdefs '%count) 1)))
    (dom/gather-styles! dom stylemap (qc stylerules))
    (doseq (style (rsorted (getkeys stylemap)
			   (lambda (s) (choice-size (get stylemap s)))))
      (let ((classname (glom prefix stylecount)))
	(set! stylecount (1+ stylecount))
	(store! classdefs classname (cons (choice-size (get stylemap style)) style))
	(do-choices (node (get stylemap style))
	  (dom/addclass! node classname)
	  (dom/drop! node 'style))))
    (store! classdefs '%count stylecount)
    classdefs))


