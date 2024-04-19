package natskv_test

import (
	"context"
	"fmt"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/nats-io/nats.go"
	"github.com/sagikazarmark/crypt/backend/natskv"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"
)

func init() {
	_ = os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
}

func TestClient_Get(t *testing.T) {
	ctx := testContext(t)
	natsAddress := runNatsContainer(ctx, t)

	// Prepare test
	conn, err := nats.Connect(natsAddress)
	if err != nil {
		t.Fatal(err)
	}

	js, err := conn.JetStream()
	if err != nil {
		t.Fatal(err)
	}

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: "viper",
	})
	if err != nil {
		t.Fatal(err)
	}

	wantConfig := []byte("foo: bar")
	if _, err = kv.Put("config.yaml", wantConfig); err != nil {
		t.Fatal(err)
	}

	// Execute test
	client, err := natskv.New([]string{natsAddress})
	if err != nil {
		t.Fatalf("Failed to create natskv - %v", err)
	}

	gotConfig, err := client.Get("config.yaml")
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(gotConfig, []byte("foo: bar")) {
		t.Fatalf("Unexpected config - got: %v, want: %v", gotConfig, wantConfig)
	}
}

func TestClient_List(t *testing.T) {
	ctx := testContext(t)
	natsAddress := runNatsContainer(ctx, t)

	// Prepare test
	conn, err := nats.Connect(natsAddress)
	if err != nil {
		t.Fatal(err)
	}

	js, err := conn.JetStream()
	if err != nil {
		t.Fatal(err)
	}

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: "viper",
	})
	if err != nil {
		t.Fatal(err)
	}

	wantConfigs := map[string][]byte{
		"config.yaml":        []byte("foo: bar"),
		"second_config.yaml": []byte("foo: baz"),
	}
	for name, value := range wantConfigs {
		if _, err = kv.Put(name, value); err != nil {
			t.Fatal(err)
		}
	}

	// Execute test
	client, err := natskv.New([]string{natsAddress})
	if err != nil {
		t.Fatalf("Failed to create natskv - %v", err)
	}

	kvPairs, err := client.List("viper")
	if err != nil {
		t.Fatal(err)
	}

	for _, kvPair := range kvPairs {
		if !reflect.DeepEqual(wantConfigs[kvPair.Key], kvPair.Value) {
			t.Fatalf("Unexpected config - got: %v, want: %v", kvPair.Value, wantConfigs[kvPair.Key])
		}
	}
}

func TestClient_Watch(t *testing.T) {
	ctx := testContext(t)
	natsAddress := runNatsContainer(ctx, t)

	// Prepare test
	conn, err := nats.Connect(natsAddress)
	if err != nil {
		t.Fatal(err)
	}

	js, err := conn.JetStream()
	if err != nil {
		t.Fatal(err)
	}

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: "viper",
	})
	if err != nil {
		t.Fatal(err)
	}

	wantConfig := []byte("foo: bar")
	if _, err = kv.Put("config.yaml", wantConfig); err != nil {
		t.Fatal(err)
	}

	// Execute test
	client, err := natskv.New([]string{natsAddress})
	if err != nil {
		t.Fatalf("Failed to create natskv - %v", err)
	}

	quit := make(chan bool)
	respChan := client.Watch("config.yaml", quit)

	updateWG := new(sync.WaitGroup)
	updateWG.Add(1)

	termWG := new(sync.WaitGroup)
	termWG.Add(1)

	updatedConfig := []byte("foo: update")
	go func() {
		defer termWG.Done()
		for resp := range respChan {
			if !reflect.DeepEqual(resp.Value, updatedConfig) {
				t.Errorf("Unexpected config - got: %v, want: %v", resp.Value, updatedConfig)
			}
			updateWG.Done()
		}
	}()

	if _, err = kv.Put("config.yaml", updatedConfig); err != nil {
		t.Fatal(err)
	}
	updateWG.Wait()

	close(quit)

	termWG.Wait()
}

func testContext(t *testing.T) context.Context {
	ctx := context.Background()
	deadline, ok := t.Deadline()
	if ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(context.Background(), deadline)
		t.Cleanup(cancel)
	}

	return ctx
}

func runNatsContainer(ctx context.Context, t testing.TB) string {
	req := testcontainers.ContainerRequest{
		Image:        "nats:latest",
		ExposedPorts: []string{"4222/tcp", "8222/tcp"},
		Entrypoint:   []string{"/nats-server"},
		Cmd:          []string{"-js", "-m", "8222"},
		WaitingFor: wait.NewHTTPStrategy("/varz").
			WithAllowInsecure(true).
			WithPort("8222/tcp").
			WithTLS(false).
			WithStartupTimeout(15 * time.Second),
		HostConfigModifier: func(config *container.HostConfig) {
			config.AutoRemove = true
		},
	}
	natsContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})

	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := natsContainer.Terminate(ctx); err != nil {
			t.Fatalf("Failed to terminate nats container, error = %v", err)
		}
	})

	ip, err := natsContainer.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}

	var port nat.Port
	if port, err = natsContainer.MappedPort(ctx, "4222/tcp"); err != nil {
		t.Fatal(err)
	}

	return fmt.Sprintf("%s:%s", ip, port.Port())
}
