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

(applytester 'err regex/match in-angles test-string -5)
(applytester 'err regex/matchlen in-angles test-string -5)
(applytester 'err regex/matchstring in-atsigns test-string -5)
(applytester 'err regex/search in-atsigns test-string -5)

(applytester in-angles parser/roundtrip in-angles)

(applytest 'err regex/match in-angles (slice test-string 0 3) "ef")
(applytester 'err regex/matchspan in-angles test-string "ef")

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

(define regex-search 1)
(define regex-zeromatch 2)
(define regex-matchlen 3)
(define regex-exactmatch 4)
(define regex-matchspan 5)
(define regex-matchstring 5)

(applytest 1 regex/rawop regex-exactmatch in-angles "<foo>" 0)
(applytest #f regex/rawop regex-exactmatch in-angles ">foo>" 0)
(applytest 5 regex/rawop regex-matchlen in-angles "<foo>345" 0)
(applytest #f regex/rawop regex-matchlen in-angles ">foo>345" 0)
(applytest #f regex/rawop regex-exactmatch in-angles ">foo<" 0)
(applytest 'err regex/rawop regex-matchstring in-angles "<foo>345" 0)
(applytest 'err regex/rawop regex-matchspan in-angles "<foo>345" 0)

(test-finished "regex")
