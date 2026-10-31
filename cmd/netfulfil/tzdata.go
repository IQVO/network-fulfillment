package main

// The zone database is embedded so the IANA timezone a CPT schedule names
// resolves on any base image, not only one that ships tzdata: the
// process-path capability cache reads every cutoff's local time in that zone
// (ADR 0031).
import _ "time/tzdata"
