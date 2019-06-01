package zumount

import (
	"fmt"
	"os"
	"testing"
)

func TestZumount(t *testing.T) {
	fmt.Println("TEST")
	if 1 != 0 {
		t.Errorf("oh no")
	}
}

func setup() {
	fmt.Println("SETUP")
}

func shutdown() {
	fmt.Println("SHUTDOWN")
}

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	shutdown()
	os.Exit(code)
}
