package auth_test

import (
	"testing"

	"github.com/Hoot-Code/pubsub-broker/internal/auth"
)

// TestRolePermissions verifies that each role has exactly the correct set of
// permissions — no more, no less.
func TestRolePermissions(t *testing.T) {
	t.Parallel()

	cases := []struct {
		role Role
		want []auth.Permission
	}{
		{
			role: auth.RoleAdmin,
			want: []auth.Permission{
				auth.PermPublish, auth.PermSubscribe, auth.PermFetch,
				auth.PermCommit, auth.PermCreateTopic, auth.PermDeleteTopic,
				auth.PermListTopics, auth.PermSeek, auth.PermAdmin,
			},
		},
		{
			role: auth.RoleProducer,
			want: []auth.Permission{auth.PermPublish, auth.PermListTopics},
		},
		{
			role: auth.RoleConsumer,
			want: []auth.Permission{
				auth.PermSubscribe, auth.PermFetch, auth.PermCommit,
				auth.PermSeek, auth.PermListTopics,
			},
		},
		{
			role: auth.RoleViewer,
			want: []auth.Permission{auth.PermListTopics},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(string(tc.role), func(t *testing.T) {
			t.Parallel()
			got := auth.RolePermissions[tc.role]
			if len(got) != len(tc.want) {
				t.Errorf("role %s: got %d perms, want %d: got=%v want=%v",
					tc.role, len(got), len(tc.want), got, tc.want)
				return
			}
			wantSet := make(map[auth.Permission]bool, len(tc.want))
			for _, p := range tc.want {
				wantSet[p] = true
			}
			for _, p := range got {
				if !wantSet[p] {
					t.Errorf("role %s has unexpected permission %q", tc.role, p)
				}
			}
		})
	}
}

// TestTopicACL verifies that an identity with a topic allowlist returns
// Can=false for topics outside the list, regardless of role.
func TestTopicACL(t *testing.T) {
	t.Parallel()

	id := &auth.Identity{
		ClientID: "svc-orders",
		Role:     auth.RoleAdmin, // even admin is restricted by topic allowlist
		Topics:   []string{"orders"},
	}

	t.Run("allowed_topic", func(t *testing.T) {
		if !id.Can(string(auth.PermPublish), "orders") {
			t.Error("expected Can=true for allowed topic 'orders'")
		}
	})

	t.Run("denied_topic", func(t *testing.T) {
		if id.Can(string(auth.PermPublish), "payments") {
			t.Error("expected Can=false for disallowed topic 'payments'")
		}
	})

	t.Run("denied_different_prefix", func(t *testing.T) {
		if id.Can(string(auth.PermSubscribe), "orders-dlq") {
			t.Error("expected Can=false for 'orders-dlq' when allowlist is ['orders']")
		}
	})

	t.Run("empty_topic_bypasses_acl", func(t *testing.T) {
		// A non-topic-specific check (topic=="") should not be blocked by
		// the topic allowlist — the topic gate only fires when topic != "".
		if !id.Can(string(auth.PermAdmin), "") {
			t.Error("expected Can=true when topic is empty (non-topic-specific check)")
		}
	})
}

// TestAdminHasAll verifies that RoleAdmin identity returns Can=true for every
// defined Permission.
func TestAdminHasAll(t *testing.T) {
	t.Parallel()

	id := &auth.Identity{
		ClientID: "admin-svc",
		Role:     auth.RoleAdmin,
	}

	allPerms := []auth.Permission{
		auth.PermPublish, auth.PermSubscribe, auth.PermFetch, auth.PermCommit,
		auth.PermCreateTopic, auth.PermDeleteTopic, auth.PermListTopics,
		auth.PermSeek, auth.PermAdmin,
	}

	for _, p := range allPerms {
		p := p
		t.Run(string(p), func(t *testing.T) {
			t.Parallel()
			if !id.Can(string(p), "") {
				t.Errorf("RoleAdmin should have permission %q", p)
			}
		})
	}
}

// TestViewerDenied verifies that RoleViewer cannot publish, subscribe, etc.
func TestViewerDenied(t *testing.T) {
	t.Parallel()

	id := &auth.Identity{Role: auth.RoleViewer, ClientID: "monitor"}
	denied := []auth.Permission{
		auth.PermPublish, auth.PermSubscribe, auth.PermFetch,
		auth.PermCommit, auth.PermCreateTopic, auth.PermDeleteTopic,
		auth.PermSeek, auth.PermAdmin,
	}
	for _, p := range denied {
		if id.Can(string(p), "") {
			t.Errorf("RoleViewer should not have permission %q", p)
		}
	}
	if !id.Can(string(auth.PermListTopics), "") {
		t.Error("RoleViewer must have PermListTopics")
	}
}

// TestProducerCannotSubscribe verifies that RoleProducer is denied subscribe.
func TestProducerCannotSubscribe(t *testing.T) {
	t.Parallel()

	id := &auth.Identity{Role: auth.RoleProducer, ClientID: "svc"}
	if id.Can(string(auth.PermSubscribe), "") {
		t.Error("RoleProducer must not have PermSubscribe")
	}
	if id.Can(string(auth.PermDeleteTopic), "") {
		t.Error("RoleProducer must not have PermDeleteTopic")
	}
	if !id.Can(string(auth.PermPublish), "") {
		t.Error("RoleProducer must have PermPublish")
	}
}

// Role is a convenience alias used only within this test file so that test
// cases can reference auth.Role without a package qualifier.
type Role = auth.Role
