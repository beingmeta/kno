KNO_EXPORT lispval kno_history_ref(lispval history,lispval ref);
KNO_EXPORT lispval kno_history_add(lispval history,lispval value,lispval ref);
KNO_EXPORT lispval kno_history_find(lispval history,lispval val);
KNO_EXPORT int kno_histpush(lispval value);
KNO_EXPORT lispval kno_histref(int ref);
KNO_EXPORT lispval kno_histfind(lispval value);
KNO_EXPORT void kno_histinit(int size);
KNO_EXPORT void kno_histclear(int size);

