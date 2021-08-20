;;; -*- Mode: scheme; text-encoding: utf-8; -*-

(load-component "common.scm")

(config! 'hexpacket #f)

(define sample-text (filestring (get-component "r4rs.scm")))

(define (test-compression string method)
  (applytest string uncompress (compress string method)
	     method #t))

(test-compression sample-text 'zstd)
(test-compression sample-text 'zstd9)
(test-compression sample-text 'zstd19)
(test-compression sample-text 'zlib)
(test-compression sample-text 'zlib9)
(test-compression sample-text 'snappy)
(test-compression sample-text 'no)

(test-finished "COMPRESSTEST")
