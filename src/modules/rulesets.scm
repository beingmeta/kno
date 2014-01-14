;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc.  All rights reserved.

(in-module 'rulesets)

;;; Rulesets are just lists or sets (choices) whose entries are named.
;;; Adding an item to a ruleset either adds the item to the collection
;;; or replaces the identically named item on the list (it actually
;;; conses a new list, so you have to use the return value).

(module-export! '{ruleset+ ruleset-configfn ruleconfig!})

(define (ruleset-replace-list rule name rule-list (fcn first))
  (if (null? rule-list) (fail)
      (if (equal? (fcn (car rule-list)) name)
	  (cons rule (cdr rule-list))
	  (cons (car rule-list)
		(ruleset-replace-list rule name (cdr rule-list) fcn)))))
(define (ruleset-replace-choice rule name ruleset (fcn first))
  (let ((existing (pick ruleset fcn name)))
    (choice rule (difference ruleset existing))))

(defambda (ruleset+ rule rules (fcn first))
  "This adds an entry to a rule set.  It takes a rule and a ruleset \
   and returns a new ruleset.  The caller is responsible for putting \
   the new rulset back where it belongs.  \
   The optional third argument is a name accessor, which is a function, \
   and defaults to FIRST."
  (tryif (or (pair? rule) (vector? rule))
    (let ((name (fcn rule)))
      (if (and (exists? rules) (singleton? rules)
	       (or (and (pair? rules)
			(or (pair? (cdr rules)) (null? (cdr rules))))
		   (null? rules) (not rules)))
	  ;; This handles rulesets which are lists
	  (if (or (fail? name) (not name))
	      (cons rule (or rules '()))
	      (if (or (not rules) (null? rules))
		  (list rule)
		  (try (ruleset-replace-list rule name rules fcn)
		       (cons rule rules))))
	  ;; This handles rulesets which are choices
	  (if (or (fail? name) (not name))
	      (choice rule rules)
	      (if (fail? rules) rule
		  (try (ruleset-replace-choice rule name rules fcn)
		       (choice rule rules))))))))

(define ruleset-configfn
  (macro ruleconfigexpr
    "This returns a CONFIG handler for adding to a ruleset.  It requires \
     the name of a local variable which stores the ruleset and optionally \
     takes a second argument of a function to call on inputs to get \
     the object to be added.\n\
     If the rule list is false, it is taken as '() so the result is the \
     list of the single rule."

    (let ((rulevar (get-arg ruleconfigexpr 1))
	  (ruleconverter (get-arg ruleconfigexpr 2 #f)))
      `(sambda (var (value))
	 (if (not (bound? value)) ,rulevar
	     (set! ,rulevar
		   (,ruleset+ ,(if ruleconverter 
				   `(,ruleconverter value)
				   'value)
			      ,rulevar)))))))
(define ruleconfig!
  (macro expr
    (let ((confname (cadr expr))
	  (confbody (cddr expr)))
      `(config-def! ',confname (ruleset-configfn ,@confbody)))))

