package internalpb

// MaxMsgBytes is the largest message Logic's gRPC servers accept (gRPC's
// default is 4MB). A ReconcileEnd lists every conn_id its gateway tracks,
// 8 bytes each, and at the 600k benchmark that is 4.8MB (spec: Reconcile,
// pass end: "the limit raised"; P6b1I-1).
const MaxMsgBytes = 16 << 20

// MaxTrackedConnIDs is the most conn_ids a ReconcileEnd can carry under
// MaxMsgBytes, keeping 64KB for the rest of the message. A gateway tracks at
// most MaxConns open connections plus LogoutQueue closed ones awaiting their
// Logout, and refuses a config whose sum is above this.
const MaxTrackedConnIDs = (MaxMsgBytes - 64<<10) / 8
