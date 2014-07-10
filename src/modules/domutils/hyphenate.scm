;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2014 beingmeta, inc.  All rights reserved.

(in-module 'domutils/hyphenate)

(use-module '{fdweb xhtml texttools reflection ezrecords logger varconfig
	      domutils hyphenate})

(module-export! '{dom/hyphenate!})

(define-init %loglevel %notice%)

(define (dom/hyphenate! node)
  (if (string? node) (hyphenate node)
      (if (test node '%content)
	  (if (string? (get node '%content))
	      (begin (store! node '%content (hyphenate (get node '%content)))
		node)
	      (if (pair? (get node '%content))
		  (begin (store! node '%content
				 (map dom/hyphenate! (get node '%content)))
		    node)
		  node))
	  node)))





