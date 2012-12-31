;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2012 beingmeta, inc.  All rights reserved.

(in-module 'domutils/cleanup)

(use-module '{fdweb xhtml texttools reflection ezrecords logger varconfig})

(define-init %loglevel %notice!)

(module-export!
 '{dom/unipunct
   dom/mergeheads/subst
   dom/mergeheads
   dom/mergebreaks/subst
   dom/mergebreaks
   dom/mergetext!})

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
(define (dom/unipunct arg)
  (if (string? arg) (unipunct arg)
      (if (pair? arg) (map dom/unipunct arg)
	  (if (not (table? arg)) arg
	      (if (test arg '%content)
		  (begin (store! arg '%content (map dom/unipunct (get arg '%content)))
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
		       (let ((node (deep-copy template)))
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

;;; Merge text

(define (dom/mergetext! node)
  (when (test node '%content)
    (let ((vec (->vector (get node '%content)))
	  (merged (list #f)))
      (doseq (elt vec)
	(if (and (string? elt) (string? (car merged)))
	    (set! merged (cons (glom (car merged) elt) (cdr merged)))
	    (set! merged (cons elt merged)))
	(when (table? elt) (dom/mergetext! elt)))
      (store! node '%content (cdr (reverse merged))))))
