FD_EXPORT lispval fd_history_ref(lispval history,int ref);
FD_EXPORT int fd_history_set(lispval history,int ref,lispval value);
FD_EXPORT int fd_history_find(lispval history,lispval value,int equal);
FD_EXPORT int fd_history_push(lispval history,lispval value);
FD_EXPORT int fd_histpush(lispval value);
FD_EXPORT lispval fd_histref(int ref);
FD_EXPORT int fd_hist_top(void);
FD_EXPORT int fd_histfind(lispval value);
FD_EXPORT void fd_histinit(int size);
FD_EXPORT void fd_histclear(int size);

