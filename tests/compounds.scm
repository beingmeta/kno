;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

(use-module 'ezrecords)

(defrecord type1
  x y (z 3) (q 4))

(defrecord (type2 mutable)
  x y (a 8) (b 9))

(defrecord (type3 opaque)
  x y (m 8) (n 9))

(define type1.1 (cons-type1 11 12))
(define type2.1 (cons-type2 33 99))
(define type3.1 (cons-type3 2000 42))

(applytest #t compound? type1.1)
(applytest #t compound? type1.1 'type1)
(applytest #f compound? type1.1 'type2)

(applytest #t compound? type2.1)
(applytest #t compound? type2.1 'type2)
(applytest #f compound? type2.1 'type1)

(applytest #t compound? type3.1)
(applytest #t exists compound? type1.1 '{type1 type2})
(applytest #t exists compound? type2.1 '{type1 type2})
(applytest #f exists compound? type3.1 '{type1 type2})

(applytest #t type1? type1.1)
(applytest #f compound-mutable? type1.1)
(applytest #f compound-opaque? type1.1)
(applytest 'type1 compound-tag type1.1)
(applytest 4 compound-length type1.1)
(applytest 3 type1-z type1.1)
(applytest 11 type1-x type1.1)

(applytest #t compound-mutable? type2.1)
(applytest #f compound-opaque? type2.1)
(applytest 'type2 compound-tag type2.1)
(applytest 4 compound-length type2.1)
(applytest 8 type2-a type2.1)
(applytest 33 type2-x type2.1)
(applytest 'err type1-x type2.1)

(applytest #f compound-mutable? type3.1)
(applytest #t compound-opaque? type3.1)
(applytest 'type3 compound-tag type3.1)
(applytest 4 compound-length type3.1)

(applytest 33 compound-ref type2.1 0)
(applytest 33 compound-ref type2.1 0 'type2)
(applytest 'err compound-ref type2.1 0 'type1)
(applytest 'err compound-ref type2.1 22 'type2)

(errtest (sequence->compound 'foo 'type4))
(applytest #%(TYPE4 A B C) sequence->compound #(A B C) 'type4)
(applytest #%(TYPE4 A B C) sequence->compound '(A B C) 'type4)

