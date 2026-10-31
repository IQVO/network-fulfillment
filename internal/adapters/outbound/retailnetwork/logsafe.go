package retailnetwork

import "strings"

// logReplacer strips the characters that let a value forge a log line.
var logReplacer = strings.NewReplacer("\r", "", "\n", "")

// sanitizeLog removes CR and LF from a value that came off the wire before it
// is logged or put in an error that will be (CodeQL go/log-injection).
func sanitizeLog(s string) string { return logReplacer.Replace(s) }
