(use-module '{optimize reflection})
(config! 'cachelevel 2)
(define default-dir (get-component "english/compiled/"))
(load "english.scm")
(define (main (dir #f))
  (optimize-compilation)
  (optimize-arcs)
  (write-lexdata (or dir default-dir) (config 'full #f))
  (commit))
