;;; -*- Mode: Scheme; -*-

(in-module 'postgres)

(use-module 'sqldb)

(module-export! '{pq/open})

(defimport pq/open 'pqprims)
