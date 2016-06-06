;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

(in-module 'lexml)

;;; Implements handling of custom lexicons in lexml

(use-module '{fdweb texttools tagger})

(define-init %loglevel 5)

(module-export!
 '{lexml->lexicon get-custom-lexicon reload-lexicon!})

;;;; Custom Lexicon support

(unless (bound? lexicon-cache)
  (define lexicon-cache (make-hashtable)))

(define read-custom-lexicon
  (slambda (url)
    (try (get lexicon-cache url)
	 (let ((lexicon (lexml->lexicon (urlxml url))))
	   (store! lexicon-cache url lexicon)
	   lexicon))))

(define (get-custom-lexicon url)
  (try (get lexicon-cache url) (read-custom-lexicon url)))

(define (reload-lexicon! url)
  (let ((lexicon (lexml->lexicon (urlxml url))))
    (store! lexicon-cache url lexicon)
    #t))

(define arc-types (elts (lextags)))

(define (def->weights xml)
  (let ((result {}))
    (do-choices (tag (intersection (get xml '%attribids) arc-types))
      (set+! result (cons tag (parse-arg (get xml tag)))))
    (when (test xml 'noun-root)
      (set+! result (cons 'noun-root (get xml 'noun-root))))
    (when (test xml 'verb-root)
      (set+! result (cons 'verb-root (get xml 'verb-root))))
    result))

(define (infer-weights weights)
  (choice (tryif (and (not (test weights 'proper-modifier))
		      (test weights 'proper-name)
		      (get weights 'proper-name))
		 `(PROPER-MODIFIER . ,(get weights 'proper-name)))
	  (tryif (and (not (test weights 'inflected-verb))
		      (test weights 'verb)
		      (test weights 'verb-root))
		 `(INFLECTED-VERB . , (get weights 'verb)))
	  (tryif (and (not (test weights 'plural-noun))
		      (test weights 'noun)
		      (test weights 'noun-root))
		 `(PLURAL-NOUN . , (get weights 'noun)))
	  weights))

(define (add-fragments table)
  (do-choices (key (getkeys table))
    (when (and (string? key) (compound? key))
      (let* ((words (getwords key))
	     (len (length words)))
	(add! table words (get table key))
	(dotimes (i (1- len))
	  (let ((key (subseq words 0 (1+ i))))
	    (unless (test table key)
	      (store! table key #f)))))))
  table)

(define (lexml->lexicon xml (slotmap #f))
  (let ((lexicon (if slotmap (frame-create #f) (make-hashtable))))
    (do-choices (lex (xmlget xml 'lexicon))
      (let ((language (get lex 'language)))
	(do-choices (defword (xmlget lex 'defwords))
	  (let ((weights (def->weights defword)))
	    (doseq (item (get defword '%content))
	      (if (string? item)
		  (doseq (word (map stdspace (segment item "\n")))
		    (unless (equal? word "")
		      (add! lexicon word (infer-weights (qc weights)))))
		  (if (test item '%name 'word)
		      (let* ((overrides (def->weights item))
			     (word (difference
				    (stdspace (elts (get item '%content))) ""))
			     (combined (choice (reject weights (car overrides))
					       overrides)))
			(add! lexicon word
			      (infer-weights
			       (qc (reject combined '{noun-root verb-root}))))
			(when (test combined 'noun-root)
			  (add! lexicon (cons 'noun-root word) (get combined 'noun-root)))
			(when (test combined 'verb-root)
			  (add! lexicon (cons 'verb-root word) (get combined 'verb-root)))))))))))
    (add-fragments lexicon)))

