(in-module 'samplefns)

(module-export! '{fibr fibi factr facti})

(define (fibr n)
  (if (< n 3) 1 (+ (fibr (- n 1)) (fibr (- n 2)))))

(define (fib-iter i cur prev)
  (if (= i 1) cur (fib-iter (-1+ i) (+ cur prev) cur)))
(define (fibi n)
  (if (= n 0) 0 (fib-iter n 1 0)))

(define (factr n)
  (if (= n 0) 1 (* n (factr (-1+ n)))))

(define (fact-iter n accum)
  (if (= n 0) accum
      (fact-iter (-1+ n) (* n accum))))
(define (facti n) (fact-iter n 1))