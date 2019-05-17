(use-module '{webtools jsonout})

(define string1 "{\"c\": \":SEA\", \"b\": \"bee\", \"a\": 3}")

(applytest ":SEA" (get (jsonparse string1) 'c))
(applytest {} (get (jsonparse string1 #f) 'c))
(applytest ":SEA" (get (jsonparse string1 #f) "c"))
(applytest 'SEA (get (jsonparse string1 #t) 'c))
(applytest 'SEA (get (jsonparse string1 '(colonize)) 'c))
(applytest {} (get (jsonparse string1 'colonize) 'c))
(applytest 'SEA (get (jsonparse string1 'colonize) "c"))
(applytest 'SEA (get (jsonparse string1 '#{colonize symbolize}) 'c))
