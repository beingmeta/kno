;;; -*- Mode: Scheme; -*-

(define done-file (get-component "apptest.done"))

(when (file-exists? done-file) (remove-file done-file))

(define (startup)
  (logwarn |Startup| "Just testing. This was evaluated."))
(define (onstart)
  (logwarn |Startup| "Just testing. This was applied"))
(config! 'appeval onstart)

(define (doit) 
  (applytest pair? config 'appmods)
  (applytest pair? config 'appload)
  (applytest pair? config 'appeval)
  (logwarn |Shutdown| "That's all folks")
  (fileout done-file (config 'sessionid) ))

