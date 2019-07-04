;;; -*- Mode: Scheme; -*-

(define done-file (get-component "apptest.done"))

(when (file-exists? done-file) (remove-file done-file))

(define (startup)
  (logwarn |Startup| "Just testing. This was evaluated."))
(define (onstart)
  (logwarn |Startup| "Just testing. This was applied"))
(config! 'appeval onstart)

(define (doit) 
  (logwarn |Shutdown| "That's all folks")
  (fileout done-file (config 'sessionid) ))

