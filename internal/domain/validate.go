package domain

import (
	"fmt"
	"net/mail"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

// Content limits per channel, counted in runes so that multibyte
// characters (Turkish letters, emoji) count as one character each.
var maxContentRunes = map[Channel]int{
	ChannelSMS:   160,
	ChannelEmail: 10000,
	ChannelPush:  256,
}

const (
	maxPushTokenLen  = 512
	maxScheduleAhead = 30 * 24 * time.Hour
)

var e164 = regexp.MustCompile(`^\+[1-9]\d{7,14}$`)

// ValidationError lists every invalid field with a human readable reason.
type ValidationError struct {
	Fields map[string]string
}

func (e *ValidationError) Error() string {
	keys := make([]string, 0, len(e.Fields))
	for k := range e.Fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = k + ": " + e.Fields[k]
	}
	return "validation failed: " + strings.Join(parts, "; ")
}

// Validate checks a create request and returns a *ValidationError
// describing every invalid field, or nil when the input is valid.
func Validate(in CreateInput, now time.Time) error {
	fields := map[string]string{}
	ch := Channel(in.Channel)

	if !ch.Valid() {
		fields["channel"] = "must be one of sms, email, push"
	} else {
		if msg := validateRecipient(ch, in.Recipient); msg != "" {
			fields["recipient"] = msg
		}
		if strings.TrimSpace(in.Content) == "" {
			fields["content"] = "is required"
		} else if limit := maxContentRunes[ch]; utf8.RuneCountInString(in.Content) > limit {
			fields["content"] = fmt.Sprintf("must be at most %d characters for %s", limit, ch)
		}
	}

	if in.Priority != "" && !Priority(in.Priority).Valid() {
		fields["priority"] = "must be one of high, normal, low"
	}

	if in.ScheduledAt != nil {
		if !in.ScheduledAt.After(now) {
			fields["scheduled_at"] = "must be in the future"
		} else if in.ScheduledAt.Sub(now) > maxScheduleAhead {
			fields["scheduled_at"] = "must be within 30 days"
		}
	}

	if len(fields) > 0 {
		return &ValidationError{Fields: fields}
	}
	return nil
}

func validateRecipient(ch Channel, r string) string {
	switch ch {
	case ChannelSMS:
		if !e164.MatchString(r) {
			return "must be an E.164 phone number, e.g. +905551234567"
		}
	case ChannelEmail:
		addr, err := mail.ParseAddress(r)
		if err != nil || addr.Address != r {
			return "must be a valid email address"
		}
	case ChannelPush:
		if strings.TrimSpace(r) == "" || len(r) > maxPushTokenLen {
			return fmt.Sprintf("must be a device token of 1-%d characters", maxPushTokenLen)
		}
	}
	return ""
}
