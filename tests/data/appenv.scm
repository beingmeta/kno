;;; -*- Mode: Scheme; -*-

(define done-file (get-component "apptest.done"))

(when (file-exists? done-file) (remove-file done-file))

(define (startup)
  (logwarn |Startup| "Just testing. This was evaluated."))
(define (onstart)
  (logwarn |Startup| "Just testing. This was applied"))
(define (onstartenv (env #f))
  (applytest #t environment? env)
  (logwarn |Startup| "Just testing. This was applied and checked that it got an environment argument"))
(define (onstartbad) (error 'just-because-i-can))
(config! 'appeval onstart)
(config! 'appeval onstartenv)
(config! 'appeval onstartbad)

(define multiply-file (get-component "multiply.scm"))

(define (doit) 
  (config! 'appmods "logctl;bench/miscfns;bench/miscfns")
  (errtest (config! 'appmods #"packetin"))
  ;;(errtest (config! 'appmods '(#"packetin")))
  ;;(errtest (config! 'appload '(#"packetin")))
  (config! 'appload multiply-file)
  (applytest pair? config 'appmods)
  (applytest pair? config 'appload)
  (applytest pair? config 'appeval)
  (logwarn |Shutdown| "That's all folks")
  (fileout done-file (config 'sessionid) ))

