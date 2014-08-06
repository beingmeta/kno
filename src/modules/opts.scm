;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2014 beingmeta, inc. All rights reserved.

(in-module 'opts)

(module-export! '{setopt! setopt+! mergeopts saveopt checkopts})

(defambda (setopt! opts opt val)
  (store! (if (pair? opts) (car opts) opts) opt val))
(defambda (setopt+! opts opt val)
  (add! (if (pair? opts) (car opts) opts) opt val))

(defambda (mergeopts . settings)
  (if (null? settings) (fail)
      (if (or (fail? (car settings)) (not (car settings)))
	  (apply mergeopts (cdr settings))
	  (if (ambiguous? (car settings))
	      (for-choices (s (car settings))
		(apply mergeopts s (cdr settings)))
	      (if (pair? (car settings))
		  (if (null? (car settings))
		      (try (cons (car (car settings))
				 (apply mergeopts (cdr settings)))
			   (car (car settings)))
		      (try (cons (car (car settings))
				 (apply mergeopts (cdr (car settings))
					(cdr settings)))
			   (car (car settings))))
		  (if (table? (car settings))
		      (try (cons (car settings)
				 (apply mergeopts (cdr settings)))
			   (car settings))
		      (if (symbol? (car settings))
			  (cons `#[,(car settings) ,(car settings)]
				(apply mergeopts (cdr settings)))
			  (apply mergeopts (cdr settings)))))))))

(define (checkopts settings table)
  (if (pair? settings)
      (or (checkopts (car settings) table)
	  (checkopts (cdr settings) table))
      (eq? settings table)))

;;; Save opt

(define (_saveopt settings opt thunk)
  (try (getopt settings opt {})
       (let ((v (thunk)))
	 (if (pair? settings)
	     (store! (car settings) opt v)
	     (store! settings opt v))
	 v)))
(define saveopt
  (macro expr
    `(,_saveopt ,(second expr) ,(third expr)
		(lambda () ,(fourth expr)))))

