(in-module 'rulesets)

;;; Rulesets are just lists whose entries are named.  Adding an item to a ruleset
;;; either conses the item onto the list or replace the identically named item
;;; on the list (it actually conses a new list, so you have to use the return value).

;;; If the rule list is false, it is taken as '() so the result is the list of the single rule.

(define (ruleset-replace-list rule name rule-list (fcn first))
  (if (null? rule-list) (fail)
      (if (equal? (fcn (car rule-list)) name)
	  (cons rule (cdr rule))
	  (cons (car rule-list)
		(ruleset-replace rule name (cdr rule-list) fcn)))))
(define (ruleset-replace-choice rule name ruleset (fcn first))
  (let ((existing (pick ruleset fcn name)))
    (choice rule (difference ruleset existing))))

(defambda (ruleset+ rule rules (fcn first))
  (let ((name (fcn rule)))
    (if (and (exists? rules) (singleton? rules)
	     (or (pair? rules) (null? rules) (not rules)))
	(if (or (fail? name) (not name))
	    (cons rule (or rules '()))
	    (if (or (not rules) (null? rules))
		(list rules)
		(try (ruleset-replace-list rule name rules fcn)
		     (cons rule rules))))
	(if (or (fail? name) (not name))
	    (choice rule rules)
	    (if (fail? rules) rule
		(try (ruleset-replace-choice rule name rules fcn)
		     (choice rule rules)))))))

(module-export! 'ruleset+)

