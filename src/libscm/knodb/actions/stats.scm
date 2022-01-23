;;; -*- Mode: Scheme -*-

(in-module 'knodb/actions/stats)

(module-export! '{stats main})

(defimport main 'knodb/actions/dbstats)
(defimport dbstats 'knodb/actions/dbstats)

(define stats (fcn/alias dbstats))
