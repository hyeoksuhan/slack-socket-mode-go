package socketmode

import (
	"errors"
	"fmt"
)

// ErrorCode mirrors the codes the Node client attaches to its errors, so that
// code being ported over can keep switching on the same values.
type ErrorCode string

const (
	// ErrSendWhileDisconnected means Send was called with no connection at all.
	ErrSendWhileDisconnected ErrorCode = "socketmode_send_while_disconnected"

	// ErrSendWhileNotReady means a connection exists but is not usable yet.
	ErrSendWhileNotReady ErrorCode = "socketmode_send_while_not_ready"

	// ErrWebsocket wraps a transport-level failure.
	ErrWebsocket ErrorCode = "socketmode_websocket_error"

	// ErrNoReplyReceived means Slack accepted a message but never answered.
	ErrNoReplyReceived ErrorCode = "socketmode_no_reply_received_error"

	// ErrPlatform wraps a Slack API error returned while opening a connection.
	ErrPlatform ErrorCode = "socketmode_platform_error"
)

// Error is a socket-mode failure carrying one of the codes above.
type Error struct {
	Code ErrorCode
	Msg  string
	Err  error
}

func (e *Error) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Msg, e.Err)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Msg)
}

func (e *Error) Unwrap() error { return e.Err }

// Is lets errors.Is compare by code alone.
func (e *Error) Is(target error) bool {
	var t *Error
	if !errors.As(target, &t) {
		return false
	}
	return t.Code == e.Code
}

func newError(code ErrorCode, msg string, err error) *Error {
	return &Error{Code: code, Msg: msg, Err: err}
}

// unrecoverableStartErrors are the Slack API errors that will never succeed on
// retry. Everything else is treated as temporary and reconnected through.
//
// 🚨 Retrying past these burns your rate limit and never recovers. The Node
//
//	client carries the same list, and it is worth keeping the two in step.
var unrecoverableStartErrors = map[string]bool{
	"invalid_auth":                true,
	"account_inactive":            true,
	"token_revoked":               true,
	"token_expired":               true,
	"no_permission":               true,
	"org_login_required":          true,
	"ekm_access_denied":           true,
	"missing_scope":               true,
	"not_allowed_token_type":      true,
	"method_deprecated":           true,
	"deprecated_endpoint":         true,
	"two_factor_setup_required":   true,
	"team_added_to_org":           true,
	"invalid_args":                true,
	"internal_error":              false, // retryable, listed for contrast
	"team_access_not_granted":     true,
	"enterprise_is_restricted":    true,
	"invalid_token":               true,
	"connection_limit_reached":    false, // retryable: another instance may exit
	"ratelimited":                 false, // retryable after a wait
	"service_unavailable":         false, // retryable
	"fatal_error":                 false, // retryable
	"request_timeout":             false, // retryable
	"upgrade_required":            true,
	"token_type_not_supported":    true,
	"not_authed":                  true,
	"app_missing_action_url":      true,
	"user_removed_from_team":      true,
	"team_disabled":               true,
	"unknown_method":              true,
	"missing_charset":             false,
	"superfluous_charset":         false,
	"invalid_charset":             false,
	"invalid_form_data":           false,
	"invalid_post_type":           false,
	"missing_post_type":           false,
	"invalid_json":                false,
	"json_not_object":             false,
	"request_entity_too_large":    false,
	"accesslimited":               true,
	"invalid_arg_name":            true,
	"invalid_array_arg":           true,
	"team_not_found":              true,
	"unsupported_arguments":       true,
	"not_an_enterprise_install":   true,
	"cannot_find_service_account": true,
}

// IsUnrecoverable reports whether a Slack error code means "stop trying".
func IsUnrecoverable(slackErrorCode string) bool {
	return unrecoverableStartErrors[slackErrorCode]
}
