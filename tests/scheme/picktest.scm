;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

(use-module 'reflection)

(define empty-hashset (make-hashset))
(applytest {} pick 'foo empty-hashset)
(applytest {} pick '{foo bar} empty-hashset)
(applytest {} pick {"foo" "bar"} empty-hashset)
(applytest 'foo reject 'foo empty-hashset)
(applytest '{foo bar} reject '{foo bar} empty-hashset)
(applytest {"foo" "bar"} reject {"foo" "bar"} empty-hashset)

(define fq3 (choice->hashset '{foo "quux" 3}))
(applytest {} pick 'bar fq3)
(applytest 'foo pick 'foo fq3)
(applytest 'foo pick '{foo quux} fq3)
(applytest "quux" pick '{"quux" bar} fq3)
(applytest 3 pick '{1 2 3} fq3)

(test-finished "PICKTEST")

