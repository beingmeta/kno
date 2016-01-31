;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

(in-module 'domutils/hyphenate)

(use-module '{fdweb xhtml texttools reflection ezrecords logger varconfig
	      domutils hyphenate})

(module-export! '{dom/hyphenate!})

(define-init %loglevel %notice%)

(define (hyphenate-string string)
  (encode-entities
   (string-subst (hyphenate (decode-entities string))
		 "­­" "­")))

(define (dom/hyphenate! node)
  (if (string? node) (hyphenate-string node)
      (if (not (or (test node '%xmltag 'pre)
		   (test node 'xml:space "preserve")))
	  (if (test node '%content)
	      (if (string? (get node '%content))
		  (begin (store! node '%content
				 (hyphenate-string (get node '%content)))
		    node)
		  (if (pair? (get node '%content))
		      (begin (store! node '%content
				     (map dom/hyphenate! (get node '%content)))
			node)
		      node))
	      node)
	  ;; Spaces matter, so don't bother hyphenating
	  node)))







