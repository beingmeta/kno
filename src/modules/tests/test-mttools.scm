(config! 'bricosource "/data/bg/brico")
(config! 'cachelevel 2)
(use-module '{brico mttools})
(define all-slots (make-hashset))
(define (dotest)
  (do-choices-mt (f (pool-elts brico-pool) 4 8192 mttools/fetchoids)
     (hashset-add! all-slots (getslots f))))

