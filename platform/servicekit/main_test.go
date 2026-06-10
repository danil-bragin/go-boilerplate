package servicekit

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"go-boilerplate/platform/run"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeApp implements the App contract Main drives.
type fakeApp struct {
	startErr error
	started  atomic.Bool
	closed   atomic.Bool
	closer   *run.Closer
}

func newFakeApp(startErr error) *fakeApp {
	a := &fakeApp{startErr: startErr, closer: run.NewCloser()}
	a.closer.Add("fake", func(context.Context) error {
		a.closed.Store(true)
		return nil
	})
	return a
}

func (a *fakeApp) Start() error {
	a.started.Store(true)
	return a.startErr
}
func (a *fakeApp) Closer() *run.Closer { return a.closer }

// TestRunMain_HappyPath: build → Start → block on signal → close → exit 0.
// SIGTERM is delivered to the test process; run.Run's NotifyContext catches it.
func TestRunMain_HappyPath(t *testing.T) {
	// Guard handler: registering ANY handler for SIGTERM disables the default
	// kill-the-process action, so a SIGTERM that lands before run.Run has
	// installed its NotifyContext cannot terminate the test binary.
	guard := make(chan os.Signal, 8)
	signal.Notify(guard, syscall.SIGTERM)
	defer signal.Stop(guard)

	app := newFakeApp(nil)

	done := make(chan int, 1)
	go func() {
		done <- runMain(func(context.Context) (App, error) { return app, nil })
	}()

	require.Eventually(t, app.started.Load, 5*time.Second, 10*time.Millisecond,
		"Main must call Start")

	// Signal delivery races run.Run's handler installation: resend until Main
	// observes it and returns.
	var code int
	require.Eventually(t, func() bool {
		require.NoError(t, syscall.Kill(syscall.Getpid(), syscall.SIGTERM))
		select {
		case code = <-done:
			return true
		case <-time.After(200 * time.Millisecond):
			return false
		}
	}, 10*time.Second, 10*time.Millisecond, "Main did not return after SIGTERM")

	assert.Equal(t, 0, code, "clean shutdown must exit 0")
	assert.True(t, app.closed.Load(), "Main must run the closer exactly via run.Run — no double teardown")
}

// fatalApp is a fakeApp that also exposes a fatal-error channel, like
// *Service does (FatalNotifier).
type fatalApp struct {
	fakeApp
	fatal chan error
}

func newFatalApp() *fatalApp {
	a := &fatalApp{fatal: make(chan error, 1)}
	a.closer = run.NewCloser()
	a.closer.Add("fake", func(context.Context) error {
		a.closed.Store(true)
		return nil
	})
	return a
}

func (a *fatalApp) Fatal() <-chan error { return a.fatal }

// TestRunMain_FatalServeError: a fatal server error after a successful Start
// (e.g. the public listener dying) must tear the app down via the closer and
// exit non-zero — NOT leave the pod sitting NotReady forever (readiness only
// removes it from endpoints; livez stays 200, so nothing would restart it).
func TestRunMain_FatalServeError(t *testing.T) {
	// Guard handler: see TestRunMain_HappyPath.
	guard := make(chan os.Signal, 8)
	signal.Notify(guard, syscall.SIGTERM)
	defer signal.Stop(guard)

	app := newFatalApp()

	done := make(chan int, 1)
	go func() {
		done <- runMain(func(context.Context) (App, error) { return app, nil })
	}()

	require.Eventually(t, app.started.Load, 5*time.Second, 10*time.Millisecond,
		"Main must call Start")

	// The serve watcher reports a fatal error (listener died).
	app.fatal <- errors.New("accept tcp 0.0.0.0:8080: use of closed network connection")

	var code int
	select {
	case code = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Main did not return after a fatal server error")
	}

	assert.Equal(t, 1, code, "fatal serve error must exit non-zero so the orchestrator restarts the pod")
	assert.True(t, app.closed.Load(), "fatal serve error must run the closer (clean teardown) before exiting")
}

// TestRunMain_BuildFailure: build error → exit code 1, Start never called.
func TestRunMain_BuildFailure(t *testing.T) {
	code := runMain(func(context.Context) (App, error) {
		return nil, errors.New("boom")
	})
	assert.Equal(t, 1, code)
}

// TestRunMain_StartFailure: Start error → resources closed, exit code 1.
func TestRunMain_StartFailure(t *testing.T) {
	app := newFakeApp(errors.New("bind failed"))
	code := runMain(func(context.Context) (App, error) { return app, nil })
	assert.Equal(t, 1, code)
	assert.True(t, app.closed.Load(), "Start failure must still close already-built resources")
}
