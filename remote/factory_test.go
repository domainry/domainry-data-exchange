package remote

import (
	"context"
	"io"
	"strings"
	"testing"

	dataexchange "github.com/domainry/domainry-data-exchange-sdk"
	"github.com/domainry/domainry-data-exchange-sdk/modulehost"
	"github.com/domainry/domainry-foundation/modulehttp"
)

type remoteTestHost struct{}

func (remoteTestHost) ImportProvider(string) (modulehost.ImportProvider, bool) { return nil, false }
func (remoteTestHost) ExportProvider(string) (modulehost.ExportProvider, bool) { return nil, false }

type remoteTestTransport struct {
	connected modulehost.Host
	source    io.Reader
}

func (t *remoteTestTransport) Connect(_ context.Context, _ dataexchange.ApplicationRef, h modulehost.Host) error {
	t.connected = h
	return nil
}
func (*remoteTestTransport) Descriptor(context.Context, dataexchange.ApplicationRef) (dataexchange.Descriptor, error) {
	return dataexchange.Descriptor{ProtocolVersion: dataexchange.ProtocolVersionV1, Mode: dataexchange.DeploymentModeSaaS}, nil
}
func (t *remoteTestTransport) SubmitImport(_ context.Context, _ dataexchange.ApplicationRef, r dataexchange.ImportRequest) (dataexchange.Job, bool, error) {
	t.source = r.Source
	return dataexchange.Job{ID: "remote-job", Status: "queued"}, false, nil
}
func (*remoteTestTransport) SubmitExport(context.Context, dataexchange.ApplicationRef, dataexchange.ExportRequest) (dataexchange.Job, bool, error) {
	return dataexchange.Job{}, false, nil
}
func (*remoteTestTransport) Job(context.Context, dataexchange.ApplicationRef, dataexchange.JobRequest) (dataexchange.Job, error) {
	return dataexchange.Job{}, nil
}
func (*remoteTestTransport) Cancel(context.Context, dataexchange.ApplicationRef, dataexchange.JobRequest) (dataexchange.Job, error) {
	return dataexchange.Job{}, nil
}
func (*remoteTestTransport) Download(context.Context, dataexchange.ApplicationRef, dataexchange.JobRequest) (dataexchange.Artifact, error) {
	return dataexchange.Artifact{}, nil
}
func (*remoteTestTransport) Close(context.Context, dataexchange.ApplicationRef) error { return nil }

func TestSaaSBindingConnectsProviderBridgeAndPassesSourceStream(t *testing.T) {
	transport := &remoteTestTransport{}
	binding, err := NewFactory(transport).OpenSaaS(t.Context(), dataexchange.ApplicationRef{ApplicationID: "app", RuntimeID: "runtime"}, remoteTestHost{})
	if err != nil {
		t.Fatal(err)
	}
	if transport.connected == nil {
		t.Fatal("provider host was not connected")
	}
	source := strings.NewReader("id\n1\n")
	job, _, err := binding.SubmitImport(t.Context(), dataexchange.ImportRequest{Source: source})
	if err != nil || job.ID != "remote-job" {
		t.Fatalf("job=%+v err=%v", job, err)
	}
	if transport.source != source {
		t.Fatal("Remote Binding replaced or buffered source reader")
	}
	provider, ok := binding.(modulehttp.Provider)
	if !ok || len(provider.HTTPSurfaces()) != 1 {
		t.Fatal("Data Exchange SaaS binding does not expose the owner HTTP surface")
	}
}
