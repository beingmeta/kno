;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

(use-module '{regex texttools})

(define in-angles (regex "<[^>]+>"))

(applytest #t regex? in-angles)
(applytest #f regex? "<[^>]+>")

(define test-string "<P>This is a <em>simple</em> sentence</P>")

(applytest #f regex/match in-angles test-string)
(applytest #t regex/match in-angles (slice test-string 0 3))
(applytest 3 regex/matchlen in-angles test-string)
(applytest "<P>" regex/matchstring in-angles test-string)
(applytest 12 (regex/search in-angles (slice test-string 1)))
(applytest 0 (regex/search in-angles test-string))

(errtest (regex/match 'test-string))
(errtest (regex/match 33))
(errtest (regex/matchlen in-angles 'symbol))
(errtest (regex/matchstring in-angles 'symbol))
(errtest (regex/search in-angles 'symbol))

(applytest {"<P>" "</P>" "<em>" "</em>"} gather in-angles test-string)
(applytest "<p>" pick {"<p>" ".p" "paragraph"} in-angles)
(applytest {".p" "paragraph"} reject {"<p>" ".p" "paragraph"} in-angles)

(test-finished "regex")
