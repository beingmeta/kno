;;; -*- Mode: scheme; text-encoding: utf-8; -*-

(define (streaming (file "reversed.text"))
  (when (file-exists? file) (remove-file file))
  (let ((line (getline))
	(output (and file (open-output-file file))))
    (while (string? line)
      (printout-to output (reverse line) "\n")
      (set! line (getline)))))

(define main streaming)
