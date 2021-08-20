(use-module '{hunspell logger})

(define speller (hunspell/open "en_US"))

(applytest #t (hunspell/check "dog" speller))
(applytest #t (hunspell/check "dig" speller))
(applytest #t (hunspell/check "dug" speller))
(applytest #f (hunspell/check "dzg" speller))

(define spellers
  (let ((s {}))
    (dotimes (i 32) (set+! s (hunspell/open "en_US")))
    s))

(define base-letters (elts "abcdefghijklmnopqrstuvwxyz"))

(define (random-word (len 3))
  (let ((letters '()))
    (dotimes (i len)
      (set! letters (cons (pick-one base-letters) letters)))
    (if (zero? (random 4))
	(capitalize (apply glom letters))
	(apply glom letters))))

(define (run-threadtest size (wait))
  (default! wait (/~ 1 100 size))
  (let ((count 0)
	(word (random-word (+ 3 (random 3))))
	(processed 0))
    (while (< count size)
      (unless (hunspell/check word (pick-one spellers))
	(length (hunspell/suggest word (pick-one spellers))))
      (set! processed (+ (length word) processed))
      (set! count (1+ count))
      (set! word (random-word (+ 3 (random 3)) ))
      (sleep (* wait (random 5))))
    processed))

(define (run-threadtests (size 4000) (nthreads 4))
  (let ((threads {}))
    (dotimes (i nthreads)
      (set+! threads (thread/call run-threadtest size)))
    (thread/wait threads)))

(evaltest #t (positive? (run-threadtest 100)))
(evaltest #t (fail? (pick (run-threadtests 50 (* 4 (rusage 'ncpus))) thread/error?)))

