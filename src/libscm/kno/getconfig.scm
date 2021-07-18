(in-module 'kno/getconfig)

(module-export! 'main)

(define (main confname)
  (lineout (config confname)))
