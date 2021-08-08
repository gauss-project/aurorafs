// Copyright (c) 2018, Janoš Guljaš <janos@resenje.org>
// All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package web

import "net/http"

// RoundTripperFunc type is an adapter to allow the use of
// ordinary functions as RoundTripper interfaces. If f is
// a function with the appropriate signature,
// RoundTripperFunc(f) is a RoundTripper that calls f.
//
// Example to set User-Agent header for every request made by Client:
//
// 	httpClient := &http.Client{
//		Transport: RoundTripperFunc(func(r *http.Request) (*http.Response, error) {
//			r.Header.Set("User-Agent", "a clockwork orange")
//			return http.DefaultTransport.RoundTrip(r)
//		}),
//	}
type RoundTripperFunc func(*http.Request) (*http.Response, error)

// RoundTrip calls f(r).
func (f RoundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}
