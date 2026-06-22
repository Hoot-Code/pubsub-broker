// Package auth provides authentication and role-based access control for the
// pubsub broker.  Each connected client is assigned an Identity that encodes
// its Role and optional topic allowlist.  The Identity.Can method is the
// single authoritative gate for all permission decisions.
package auth

// ─── Roles ───────────────────────────────────────────────────────────────────

// Role classifies a client's capabilities at a coarse level.
type Role string

const (
	// RoleAdmin grants every permission.
	RoleAdmin Role = "admin"
	// RoleProducer allows publishing and listing topics.
	RoleProducer Role = "producer"
	// RoleConsumer allows subscribing, fetching, committing, seeking, and
	// listing topics.
	RoleConsumer Role = "consumer"
	// RoleViewer grants read-only access: listing topics only.
	RoleViewer Role = "viewer"
)

// ─── Permissions ─────────────────────────────────────────────────────────────

// Permission is a fine-grained capability that a Role may or may not include.
type Permission string

const (
	// PermPublish allows publishing messages to a topic.
	PermPublish Permission = "publish"
	// PermSubscribe allows subscribing to a topic.
	PermSubscribe Permission = "subscribe"
	// PermFetch allows fetching messages from a topic.
	PermFetch Permission = "fetch"
	// PermCommit allows committing consumer-group offsets.
	PermCommit Permission = "commit"
	// PermCreateTopic allows creating topics.
	PermCreateTopic Permission = "create_topic"
	// PermDeleteTopic allows deleting topics.
	PermDeleteTopic Permission = "delete_topic"
	// PermListTopics allows listing topics and reading topic metadata.
	PermListTopics Permission = "list_topics"
	// PermSeek allows seeking a consumer group to an offset or timestamp.
	PermSeek Permission = "seek"
	// PermAdmin grants administrative operations (topic management, cluster
	// inspection, etc.). Implied by RoleAdmin.
	PermAdmin Permission = "admin"
)

// RolePermissions maps each Role to its complete set of allowed Permissions.
// The sets are disjoint-by-design: a permission is granted iff it appears
// here, so adding a new Permission requires an explicit decision for every
// Role.
var RolePermissions = map[Role][]Permission{
	RoleAdmin: {
		PermPublish,
		PermSubscribe,
		PermFetch,
		PermCommit,
		PermCreateTopic,
		PermDeleteTopic,
		PermListTopics,
		PermSeek,
		PermAdmin,
	},
	RoleProducer: {
		PermPublish,
		PermListTopics,
	},
	RoleConsumer: {
		PermSubscribe,
		PermFetch,
		PermCommit,
		PermSeek,
		PermListTopics,
	},
	RoleViewer: {
		PermListTopics,
	},
}

// ─── Identity ─────────────────────────────────────────────────────────────────

// Identity represents an authenticated connection's principal and its RBAC
// attributes.
type Identity struct {
	// ClientID is the caller-supplied identifier from the AUTH request.
	ClientID string
	// Role is the RBAC role assigned to this client by the Authenticator.
	Role Role
	// Topics is an optional allowlist of topic names this identity may access.
	// An empty slice means all topics are permitted.
	Topics []string
}

// Can reports whether this identity is permitted to perform perm on topic.
//
// The check has two independent gates both of which must pass:
//
//  1. Role gate: the identity's Role must include perm in RolePermissions.
//  2. Topic gate: when Topics is non-empty and topic is non-empty, topic must
//     appear in the allowlist.
//
// perm should be one of the PermXxx string constants, e.g. string(PermPublish).
// topic may be empty for permission checks that are not topic-specific.
func (id *Identity) Can(perm, topic string) bool {
	// Topic allowlist gate.
	if topic != "" && len(id.Topics) > 0 {
		found := false
		for _, t := range id.Topics {
			if t == topic {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	// Role permission gate.
	for _, p := range RolePermissions[id.Role] {
		if string(p) == perm {
			return true
		}
	}
	return false
}

// roleFromLegacy derives a Role from a legacy Permissions string slice.
// This is used when loading API key entries that use the old "permissions"
// field instead of the new "role" field.
func roleFromLegacy(perms []string) Role {
	for _, p := range perms {
		switch p {
		case "admin":
			return RoleAdmin
		case "publish":
			return RoleProducer
		case "subscribe":
			return RoleConsumer
		}
	}
	return RoleViewer
}
