struct { u8_string name; int code; }
  initial_sockopts[] = {
    {"affinity", ZMQ_AFFINITY },
    { "routing_id",  ZMQ_ROUTING_ID },
    { "subscribe",  ZMQ_SUBSCRIBE },
    { "unsubscribe",  ZMQ_UNSUBSCRIBE },
    { "rate",  ZMQ_RATE },
    { "recovery_ivl",  ZMQ_RECOVERY_IVL },
    { "sndbuf",  ZMQ_SNDBUF },
    { "rcvbuf",  ZMQ_RCVBUF },
    { "rcvmore",  ZMQ_RCVMORE },
    { "fd",  ZMQ_FD },
    { "events",  ZMQ_EVENTS },
    { "type",  ZMQ_TYPE },
    { "linger",  ZMQ_LINGER },
    { "reconnect_ivl",  ZMQ_RECONNECT_IVL },
    { "backlog",  ZMQ_BACKLOG },
    { "reconnect_ivl_max",  ZMQ_RECONNECT_IVL_MAX },
    { "maxmsgsize",  ZMQ_MAXMSGSIZE },
    { "sndhwm",  ZMQ_SNDHWM },
    { "rcvhwm",  ZMQ_RCVHWM },
    { "multicast_hops",  ZMQ_MULTICAST_HOPS },
    { "rcvtimeo",  ZMQ_RCVTIMEO },
    { "sndtimeo",  ZMQ_SNDTIMEO },
    { "last_endpoint",  ZMQ_LAST_ENDPOINT },
    { "router_mandatory",  ZMQ_ROUTER_MANDATORY },
    { "tcp_keepalive",  ZMQ_TCP_KEEPALIVE },
    { "tcp_keepalive_cnt",  ZMQ_TCP_KEEPALIVE_CNT },
    { "tcp_keepalive_idle",  ZMQ_TCP_KEEPALIVE_IDLE },
    { "tcp_keepalive_intvl",  ZMQ_TCP_KEEPALIVE_INTVL },
    { "immediate",  ZMQ_IMMEDIATE },
    { "xpub_verbose",  ZMQ_XPUB_VERBOSE },
    { "router_raw",  ZMQ_ROUTER_RAW },
    { "ipv6",  ZMQ_IPV6 },
    { "mechanism",  ZMQ_MECHANISM },
    { "plain_server",  ZMQ_PLAIN_SERVER },
    { "plain_username",  ZMQ_PLAIN_USERNAME },
    { "plain_password",  ZMQ_PLAIN_PASSWORD },
    { "curve_server",  ZMQ_CURVE_SERVER },
    { "curve_publickey",  ZMQ_CURVE_PUBLICKEY },
    { "curve_secretkey",  ZMQ_CURVE_SECRETKEY },
    { "curve_serverkey",  ZMQ_CURVE_SERVERKEY },
    { "probe_router",  ZMQ_PROBE_ROUTER },
    { "req_correlate",  ZMQ_REQ_CORRELATE },
    { "req_relaxed",  ZMQ_REQ_RELAXED },
    { "conflate",  ZMQ_CONFLATE },
    { "zap_domain",  ZMQ_ZAP_DOMAIN },
    { "router_handover",  ZMQ_ROUTER_HANDOVER },
    { "tos",  ZMQ_TOS },
    { "connect_routing_id",  ZMQ_CONNECT_ROUTING_ID },
    { "gssapi_server",  ZMQ_GSSAPI_SERVER },
    { "gssapi_principal",  ZMQ_GSSAPI_PRINCIPAL },
    { "gssapi_service_principal",  ZMQ_GSSAPI_SERVICE_PRINCIPAL },
    { "gssapi_plaintext",  ZMQ_GSSAPI_PLAINTEXT },
    { "handshake_ivl",  ZMQ_HANDSHAKE_IVL },
    { "socks_proxy",  ZMQ_SOCKS_PROXY },
    { "xpub_nodrop",  ZMQ_XPUB_NODROP },
    { "blocky",  ZMQ_BLOCKY },
    { "xpub_manual",  ZMQ_XPUB_MANUAL },
    { "xpub_welcome_msg",  ZMQ_XPUB_WELCOME_MSG },
    { "stream_notify",  ZMQ_STREAM_NOTIFY },
    { "invert_matching",  ZMQ_INVERT_MATCHING },
    { "heartbeat_ivl",  ZMQ_HEARTBEAT_IVL },
    { "heartbeat_ttl",  ZMQ_HEARTBEAT_TTL },
    { "heartbeat_timeout",  ZMQ_HEARTBEAT_TIMEOUT },
    { "xpub_verboser",  ZMQ_XPUB_VERBOSER },
    { "connect_timeout",  ZMQ_CONNECT_TIMEOUT },
    { "tcp_maxrt",  ZMQ_TCP_MAXRT },
    { "thread_safe",  ZMQ_THREAD_SAFE },
    { "multicast_maxtpdu",  ZMQ_MULTICAST_MAXTPDU },
    { "vmci_buffer_size",  ZMQ_VMCI_BUFFER_SIZE },
    { "vmci_buffer_min_size",  ZMQ_VMCI_BUFFER_MIN_SIZE },
    { "vmci_buffer_max_size",  ZMQ_VMCI_BUFFER_MAX_SIZE },
    { "vmci_connect_timeout",  ZMQ_VMCI_CONNECT_TIMEOUT },
    { "use_fd",  ZMQ_USE_FD },
    {NULL, -1}};

