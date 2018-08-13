FD_EXPORT lispval fd_history_ref(lispval history,lispval ref);
FD_EXPORT lispval fd_history_add(lispval history,lispval value,lispval ref);
FD_EXPORT lispval fd_history_find(lispval history,lispval val);
FD_EXPORT int fd_histpush(lispval value);
FD_EXPORT lispval fd_histref(int ref);
FD_EXPORT lispval fd_histfind(lispval value);
FD_EXPORT void fd_histinit(int size);
FD_EXPORT void fd_histclear(int size);

