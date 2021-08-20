(define (main (i 0) (n 40))
  (message "main i=" i "; n=" n)
  (if (< i n) (chain (1+ i) n)))
