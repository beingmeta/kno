;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

(use-module '{sqldb sqloids db})

(define dbmodule (get-module (config 'DBMODULE 'sqlite)))

(use-module dbmodule)

(define dbopen (get dbmodule (config 'DBOPEN 'sqlite/open)))

(define dbspec (config 'dbspec (get-component "test.sqlite")))

