(load-component "common.scm")

(use-module 'fdweb)

(define xml-test-input-1
  "<p title='check if x > 8'>x is a value.</p>")
(define xml-test-input-2
  "<p>x is <?fdeval x?> supposedly.</p>")
(define xml-test-input-3
  "<p title='check if x > 8'>x is <?fdeval x?> supposedly.</p>")
(define xml-test-input-4
  "<p> We have <em>embedded</em> markup <strong>to many <em>many</em> levels</strong></p>")
(define xml-test-input-5
  "<body><p> We have <em>embedded</em> markup <strong>to many <em>many</em> levels</strong></p>")

(define (try-xmlparse s)
  (onerror (xmlparse s) #f))

(applytest #t pair? (xmlparse xml-test-input-1))
(applytest #t pair? (xmlparse xml-test-input-2))
(applytest #t pair? (xmlparse xml-test-input-3))
(applytest #t pair? (xmlparse xml-test-input-4))
(applytest '() try-xmlparse xml-test-input-5)

(test-finished "XMLTEST")
