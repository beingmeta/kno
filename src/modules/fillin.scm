;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc.  All rights reserved.

(in-module 'fillin)

(use-module '{texttools logger})

(define-init %loglevel %notify%)

(module-export! 'fillin)

(define (fillin template bindings)
  (debug%watch
      (textsubst template
		 `(SUBST {#("{{" (isalnum+) "}}")
			  #("{{" (isalnum+) "|" (not> "}") "}}")}
			 ,(lambda (x) (dosubst x bindings))))
    template bindings))

(define (dosubst string table)
  (let* ((bar (position #\| string))
	 (sym (intern (upcase (slice string 2 (or bar -2))))))
    (if (exists? (get table sym))
	(stringout (get table sym))
	(if bar (slice string (1+ bar) -2)
	    (slice string 2 -2)))))

