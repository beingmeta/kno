
(define (startup)
  (logwarn |Startup| "Just testing"))
(define (doit) 
  (logwarn |Shutdown| "That's all folks")
  (exit))
