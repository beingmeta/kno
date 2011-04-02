FD_EXPORT fdtype fd_history_ref(fdtype history,int ref);
FD_EXPORT int fd_history_set(fdtype history,int ref,fdtype value);
FD_EXPORT int fd_history_find(fdtype history,fdtype value,int equal);
FD_EXPORT int fd_history_push(fdtype history,fdtype value);
FD_EXPORT int fd_histpush(fdtype value);
FD_EXPORT fdtype fd_histref(int ref);
FD_EXPORT int fd_hist_top(void);
FD_EXPORT int fd_histfind(fdtype value);
FD_EXPORT void fd_histinit(int size);
FD_EXPORT void fd_histclear(int size);

