(load-component "common.scm")

(use-module 'fdweb)

(define xml-test-input-1
  "<p title='check if x > 8'>x is a value.</p>")
(define xml-test-input-2
  "<p>x is <?fdeval x?> supposedly.</p>")
(define xml-test-input-3
  "<p title='check if x > 8'>x is <?fdeval x?> supposedly.</p>")


(applytest #t pair? (xmlparse xml-test-input-1))
(applytest #t pair? (xmlparse xml-test-input-2))
(applytest #t pair? (xmlparse xml-test-input-3))

(test-finished "XMLTEST")
