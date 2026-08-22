package nixcache

// SetMidMark installs a hook running between GC's mark and sweep.
func SetMidMark(n *Node, f func()) { n.midMark = f }
