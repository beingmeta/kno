;;; -*- Mode: Scheme -*-

(use-module '{optimize knodb/actions/packindex})

(optimize-modules! (get (get-module 'knodb/actions/packindex) '%optimize))

