package raft

// Min returns minimum of two ints
func Min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}

// Max returns the maximum of two ints
func Max(a, b int) int {
	if a <= b {
		return b
	}
	return a
}
