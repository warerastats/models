package reports

import "strconv"

// itoa is a brief alias for strconv.Itoa used in composite document keys.
func itoa(n int) string {
	return strconv.Itoa(n)
}
