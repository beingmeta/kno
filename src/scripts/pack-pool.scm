;;; -*- Mode: Scheme -*-

(use-module '{optimize knodb/actions/packpool})

(optimize-modules! (get (get-module 'knodb/actions/packpool) '%optimize))

