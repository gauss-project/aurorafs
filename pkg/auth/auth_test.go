package auth_test

import (
	"errors"
	"golang.org/x/crypto/bcrypt"
	"io"
	"testing"
	"time"

	"github.com/gauss-project/aurorafs/pkg/auth"
	"github.com/gauss-project/aurorafs/pkg/logging"
)

const (
	encryptionKey = "mZIODMvjsiS2VdK1xgI1cOTizhGVNoVz"
	passwordHash  = "$2a$12$mZIODMvjsiS2VdK1xgI1cOTizhGVNoVz2Xn48H8ddFFLzX2B3lD3m"
)

func TestPass(t *testing.T) {
	pass := "123"
	password, err := bcrypt.GenerateFromPassword([]byte(pass), 5)
	if err != nil {
		t.Error(err)
	}
	println(string(password))
}

func TestAuthorize(t *testing.T) {
	a, err := auth.New(encryptionKey, passwordHash, nil)
	if err != nil {
		t.Error(err)
	}

	tt := []struct {
		desc     string
		pass     string
		expected bool
	}{
		{
			desc:     "correct credentials",
			pass:     "test",
			expected: true,
		}, {
			desc:     "wrong password",
			pass:     "notTest",
			expected: false,
		},
	}
	for _, tC := range tt {
		t.Run(tC.desc, func(t *testing.T) {
			res := a.Authorize(tC.pass)
			if res != tC.expected {
				t.Error("unexpected result", res)
			}
		})
	}
}

func TestExpiry(t *testing.T) {
	a, err := auth.New(encryptionKey, passwordHash, logging.New(io.Discard, 0))
	if err != nil {
		t.Error(err)
	}

	key, err := a.GenerateKey("consumer", 1)
	if err != nil {
		t.Errorf("expected no error, got: %v", err)
	}

	time.Sleep(2 * time.Second)

	result, err := a.Enforce(key, "/bytes/1", "GET")
	if !errors.Is(err, auth.ErrTokenExpired) {
		t.Errorf("expected token expired error, got: %v", err)
	}

	if result {
		t.Errorf("expected %v, got %v", false, result)
	}
}

func TestEnforce(t *testing.T) {
	a, err := auth.New(encryptionKey, passwordHash, nil)
	if err != nil {
		t.Error(err)
	}

	tt := []struct {
		desc                   string
		role, resource, action string
		expected               bool
	}{
		{
			desc:     "success",
			role:     "maintainer",
			resource: "/pingpong/someone",
			action:   "POST",
			expected: true,
		}, {
			desc:     "success with query param",
			role:     "creator",
			resource: "/bzz?name=some-name",
			action:   "POST",
			expected: true,
		},
		{
			desc:     "bad role",
			role:     "consumer",
			resource: "/pingpong/some-other-peer",
			action:   "POST",
		},
		{
			desc:     "bad resource",
			role:     "maintainer",
			resource: "/i-dont-exist",
			action:   "POST",
		},
		{
			desc:     "bad action",
			role:     "maintainer",
			resource: "/pingpong/someone",
			action:   "DELETE",
		},
	}

	for _, tC := range tt {
		t.Run(tC.desc, func(t *testing.T) {
			apiKey, err := a.GenerateKey(tC.role, 1)

			if err != nil {
				t.Errorf("expected no error, got: %v", err)
			}

			result, err := a.Enforce(apiKey, tC.resource, tC.action)

			if err != nil {
				t.Errorf("expected no error, got: %v", err)
			}

			if result != tC.expected {
				t.Errorf("request from user with %s on object %s: expected %v, got %v", tC.role, tC.resource, tC.expected, result)
			}
		})
	}
}
