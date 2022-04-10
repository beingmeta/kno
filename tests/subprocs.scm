;;; -*- Mode: Scheme; -*-

(load-component "common.scm")

(config! 'knox (get-component "testknox"))

(applytest (config 'knoversion) trim-spaces (run->string 'getconfig "KNOVERSION"))
(applytest string? (run->string "ls"))
(run->file "tmp.log" "ls")
(proc/run [stdout "tmp.log"] "ls" #("." ".."))

