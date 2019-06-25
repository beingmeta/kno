;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

(use-module '{regex texttools testcapi})

(define in-angles (regex "<[^>]+>"))
(define in-atsigns (regex "@[^@]+@"))

(applytester #t regex? in-angles)
(applytester #f regex? "<[^>]+>")

(errtester (regex "<[^]>"))

(define test-string "<P>This is a <em>simple</em> sentence</P>")

(applytester #f regex/match in-angles test-string)
(applytester #t regex/match in-angles (slice test-string 0 3))
(applytester 3 regex/matchlen in-angles test-string)
(applytester #f regex/matchlen in-atsigns test-string)
(applytester "<P>" regex/matchstring in-angles test-string)
(applytester #f regex/matchstring in-atsigns test-string)
(applytester 12 regex/search in-angles (slice test-string 1))
(applytester #f regex/search in-atsigns (slice test-string 1))
(applytester '(0 . 3) regex/matchspan in-angles test-string)
(applytester #f regex/matchspan in-atsigns test-string)
(applytester 0 regex/search in-angles test-string)

(applytester in-angles parser/roundtrip in-angles)

(errtester (regex/match 'test-string))
(errtester (regex/match 33))
(errtester (regex/matchlen in-angles 'symbol))
(errtester (regex/matchstring in-angles 'symbol))
(errtester (regex/search in-angles 'symbol))

(errtester (regex/matchlen in-angles test-string -4))
(errtester (regex/matchstring in-angles test-string -9))
(errtester (regex/search in-angles test-string -17))

(applytester {"<P>" "</P>" "<em>" "</em>"} gather in-angles test-string)
(applytester "<p>" pick {"<p>" ".p" "paragraph"} in-angles)
(applytester {".p" "paragraph"} reject {"<p>" ".p" "paragraph"} in-angles)

(regex/testcapi)

(test-finished "regex")
