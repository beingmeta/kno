(in-module 'facebook/fbml)

(define version "$Id$")

(use-module '{fdweb xhtml})

(define (fb:name (id #f))
  (if id (xmlelt "fb:name" 'uid (stringout id))
       (xmlelt "fb:name")))

(module-export! 'fb:name)

