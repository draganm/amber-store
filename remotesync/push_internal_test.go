package remotesync

import "testing"

func TestSplitJobs(t *testing.T) {
	for _, tc := range []struct{ jobs, checkers, uploaders int }{
		{1, 1, 1}, // pipeline floor: the one case allowed to exceed the budget
		{2, 1, 1},
		{3, 1, 2},
		{4, 1, 3},
		{8, 2, 6},
		{16, 4, 12},
	} {
		c, u := splitJobs(tc.jobs)
		if c != tc.checkers || u != tc.uploaders {
			t.Errorf("splitJobs(%d) = (%d, %d), want (%d, %d)",
				tc.jobs, c, u, tc.checkers, tc.uploaders)
		}
	}
}
