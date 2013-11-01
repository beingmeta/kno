;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc.  All rights reserved.

(in-module 'domutils/css)

(use-module '{reflection texttools domutils varconfig logger})

(module-export! '{css-rules css-rule domutils/css})

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
  `(GREEDY #(,base-selector (* #({(spaces) #((spaces) {"+" ">"} (spaces))} ,base-selector)))))

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

(define css-rules `{,css-media-rule ,css-rule-extract ,css-comment})

(define (parse-rule rule (media #f))
  (if (not (string? rule)) rule
      (if (has-prefix rule "/*")
	  `#[comment ,rule]
	  (if (has-prefix rule "@media")
	      (let ((parsed (text->frames css-media-rule rule)))
		`#[media ,(get parsed 'media)
		   rules ,(difference
			   (map (lambda (x) (parse-rule x media))
				(gather->list css-rules (get parsed 'rules)))
			   '())])
	      (for-choices (ex (text->frames css-rule-extract rule))
		(parse-properties (get ex 'props)
				  (if media
				      `#[match ,(get ex 'selector) media ,media]
				      `#[match ,(get ex 'selector)])))))))

(define (domutils/css string (media #f))
  (map parse-rule (remove '() (gather->list (qc css-rules) string))))


