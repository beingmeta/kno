int knotc_setoid(KNOTC *cache,lispval oid,lispval val);
int knotc_setadjunct(KNOTC *cache,lispval oid,lispval slotid,lispval val);
int knotc_index_add(KNOTC *cache,lispval ix,lispval key,lispval oids);
int knotc_index_cache(KNOTC *cache,lispval ix,lispval key,lispval oids);
int knotc_bgadd(KNOTC *cache,lispval key,lispval oids);
