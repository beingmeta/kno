#!/usr/bin/knox
;;; -*- Mode: Scheme; -*-

(use-module '{logger texttools stringfmts})

(define (main)
  (fileout "/app/loop.pid" (config 'pid))
  (until (file-exists? "/app/stop")
    (lognotice |Looping| "Here we are")
    (sleep 30)))
