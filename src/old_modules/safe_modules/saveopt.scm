;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

;;; Uses the FramerD option functions but caches the default value.
(in-module 'saveopt)

(module-export! 'saveopt)

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

