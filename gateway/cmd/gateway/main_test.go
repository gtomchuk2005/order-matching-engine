package main

import (
	"os"
	"os/exec"
	"testing"
)

// Re-exec the test binary as a subprocess so mustEnv's log.Fatalf (which calls os.Exit)
// doesn't kill the test runner itself.
func TestMustEnvFatalsOnMissingOrdersTopic(t *testing.T) {
	if os.Getenv("GATEWAY_TEST_MUST_ENV_SUBPROCESS") == "1" {
		mustEnv("ORDERS_TOPIC")
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestMustEnvFatalsOnMissingOrdersTopic")
	cmd.Env = append(os.Environ(), "GATEWAY_TEST_MUST_ENV_SUBPROCESS=1", "ORDERS_TOPIC=")
	err := cmd.Run()
	if exitErr, ok := err.(*exec.ExitError); ok && !exitErr.Success() {
		return
	}
	t.Fatalf("expected mustEnv(\"ORDERS_TOPIC\") to exit non-zero when unset, got err=%v", err)
}
