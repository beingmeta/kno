(config! 'cachelevel 2)
(define default-dir (get-component "english/compiled/"))
(load "english.scm")
(define (main (dir #f))
  (write-lexdata (or dir default-dir) #f)
  (commit))


