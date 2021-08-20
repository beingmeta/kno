(define (countup n (i 0))
  (if (= n 0) (dbg i) (countup (-1+ n) (1+ i))))

(define (reduce-string string)
  (if (= (length string) 0) 
      (dbg string)
      (reduce-string (slice string 1))))
