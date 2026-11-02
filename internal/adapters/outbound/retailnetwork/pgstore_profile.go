package retailnetwork

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func (s *PGStore) NodeProfileState(ctx context.Context, siteCode string) (NodeProfileState, bool, error) {
	st := NodeProfileState{SiteCode: siteCode}
	err := s.pool.QueryRow(ctx, `
		SELECT revision, attempt, fingerprint, content
		FROM network_node_profiles WHERE site_code = $1`, siteCode).
		Scan(&st.Revision, &st.Attempt, &st.Fingerprint, &st.Content)
	if errors.Is(err, pgx.ErrNoRows) {
		return NodeProfileState{}, false, nil
	}
	if err != nil {
		return NodeProfileState{}, false, err
	}
	return st, true, nil
}

// AdvanceNodeProfile is one guarded upsert: the row changes only when the new
// (revision, attempt) pair is greater than the stored one, so two writers racing
// (two replicas, or a restart overlapping a slow call) can never lower it. The
// state in force is read back afterwards, which is the caller's answer to "did I
// win".
func (s *PGStore) AdvanceNodeProfile(ctx context.Context, st NodeProfileState) (NodeProfileState, error) {
	if st.Revision < 1 || st.Attempt < 1 || st.Fingerprint == "" || len(st.Content) == 0 {
		return NodeProfileState{}, fmt.Errorf("retailnetwork: a profile state needs a revision and attempt of at least 1, a fingerprint and its content (got %d/%d)", st.Revision, st.Attempt)
	}
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO network_node_profiles (site_code, revision, attempt, fingerprint, content)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (site_code) DO UPDATE
		SET revision = EXCLUDED.revision, attempt = EXCLUDED.attempt,
		    fingerprint = EXCLUDED.fingerprint, content = EXCLUDED.content, updated_at = now()
		WHERE (network_node_profiles.revision, network_node_profiles.attempt) < (EXCLUDED.revision, EXCLUDED.attempt)`,
		st.SiteCode, st.Revision, st.Attempt, st.Fingerprint, st.Content); err != nil {
		return NodeProfileState{}, mapForeignKeyViolation(err)
	}
	got, ok, err := s.NodeProfileState(ctx, st.SiteCode)
	if err != nil {
		return NodeProfileState{}, err
	}
	if !ok {
		return NodeProfileState{}, fmt.Errorf("retailnetwork: profile state of site %q vanished after the write", st.SiteCode)
	}
	return got, nil
}
