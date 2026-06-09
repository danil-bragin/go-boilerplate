// Package testingexamples contains reference test templates demonstrating each
// layer of the testing pyramid used in this repository.
//
// Copy these patterns when writing tests for a new service or feature. Each
// file is a self-contained, runnable example with extensive comments explaining
// the intent, the doubles in use, and the assertions style.
//
// Files:
//
//   - unit_example_test.go      — pure unit test, moq mocks, zero I/O
//   - functional_example_test.go — slice-across-collaborators test, fakes +
//     mockhttp, no containers
//
// See docs/testing.md for the full guide including integration and E2E patterns.
package testingexamples
