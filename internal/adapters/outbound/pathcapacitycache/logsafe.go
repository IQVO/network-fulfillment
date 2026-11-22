package pathcapacitycache

import (
	"strings"
)

// Event-derived values (CloudEvent types, payload fields, decode errors) are
// external input: strip CR and LF before they reach a log attribute so a
// crafted event cannot forge a log line.

// sanitizeErr is the CR/LF strip for an error's text ("" for a nil error).
func sanitizeErr(err error) string {
	if err == nil {
		return ""
	}
	return strings.ReplaceAll(strings.ReplaceAll(err.Error(), "\r", ""), "\n", "")
}
