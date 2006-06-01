#define FD_DEFAULT_DLOADPATH \
  "/usr/lib/%.dylib:/usr/lib/lib%.dylib:/lib/%.dylib:/lib/lib%.dylib"

#define FD_DEFAULT_LOADPATH \
  "/usr/share/framerd/scm/%/module.scm:/usr/share/framerd/scm/%.scm"

#define FD_DEFAULT_SAFE_LOADPATH \
  "/usr/share/framerd/scm/safe/%/module.scm:/usr/share/framerd/scm/safe/%.scm"

#define FD_CONFIG_FILE_PATH \
  "/usr/share/framerd/config/%"
#define FD_USER_CONFIG_FILE_PATH \
  "~/.fdconfig/%:/usr/share/framerd/config/%"

#define FD_INIT_SCHEME_BUILTINS() \
  fd_init_fdscheme(); fd_init_schemeio(); u8_init_chardata_c()
