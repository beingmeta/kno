(load-component "common.scm")

(use-module 'webtools)

(define xml-test-input-1
  "<p title='check if x > 8'>x is a value.</p>")
(define xml-test-input-2
  "<p>x is <?knoeval x?> supposedly.</p>")
(define xml-test-input-3
  "<p title='check if x > 8'>x is <?knoeval x?> supposedly.</p>")
(define xml-test-input-4
  "<p> We have <em>embedded</em> markup <strong>to many <em>many</em> levels</strong></p>")
(define xml-test-input-5
  "<body><p> We have <em>embedded</em> markup <strong>to many <em>many</em> levels</strong></p>")
(define xml-test-input-6 "<p> We have")

(define (try-xmlparse s (flags #default))
  (onerror (xmlparse s flags) #f))

(applytest #t pair? (xmlparse xml-test-input-1))
(applytest #t pair? (xmlparse xml-test-input-2))
(applytest #t pair? (xmlparse xml-test-input-3))
(applytest #t pair? (xmlparse xml-test-input-4))
(applytest #f try-xmlparse xml-test-input-5)
(applytest #t pair? (try-xmlparse xml-test-input-5 'sloppy))
(try-xmlparse xml-test-input-6 'sloppy)

(test-finished "XMLTEST")
