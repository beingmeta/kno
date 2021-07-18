(in-module 'actions/getconfig)

(module-export! 'main)

(define (main confname)
  (lineout (config confname)))
