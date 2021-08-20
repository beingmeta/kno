;;; -*- Mode: Scheme; -*-

(load-component "common.scm")

(applytest (config 'loadpath) string->lisp (run->string 'getconfig "LOADPATH"))
(applytest string? (run->string "ls"))
(run->file "tmp.log" "ls")
(proc/run [stdout "tmp.log"] "ls" #("." ".."))

