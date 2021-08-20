;;; -*- Mode: Scheme; text-encoding: utf-8 -*-

(load-component "common.scm")

(use-module '{reflection binio varconfig text/stringfmts bench/randobj})

(define (xcons (num #required #number_type) (string #required #string_type))
  (cons num string))

(applytest 2 procedure-min-arity xcons)
(applytest '(3 . "three") xcons 3 "three")
(applytest 'err xcons "three" 3)

(if (exists? errors)
    (begin (message (choice-size errors)
		    " Errors during TYPES testing")
	   (error 'tests-failed))
    (message "TYPES testing successfuly completed"))

(test-finished "TYPES")


