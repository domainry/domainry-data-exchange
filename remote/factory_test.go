package remote

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	dataexchange "github.com/domainry/domainry-data-exchange-sdk"
	"github.com/domainry/domainry-data-exchange-sdk/modulehost"
	sourcecapability "github.com/domainry/domainry-data-exchange/capability"
	"github.com/domainry/domainry-foundation/modulecapability"
	"github.com/domainry/domainry-foundation/modulecapability/contracttest"
	"github.com/domainry/domainry-foundation/modulehttp"
)

type remoteTestHost struct{}

func (remoteTestHost) ImportProvider(string) (modulehost.ImportProvider, bool) { return nil, false }
func (remoteTestHost) ExportProvider(string) (modulehost.ExportProvider, bool) { return nil, false }

type remoteTestTransport struct {
	modulecapability.Binding
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
	host := remoteTestHost{}
	direct, err := sourcecapability.Open(sourcecapability.Inputs{})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := modulecapability.NewHTTPHandler(direct, func(*http.Request) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	summary, err := direct.CapabilitySummary(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	remoteCapability, err := modulecapability.OpenRemote(t.Context(), modulecapability.RemoteConfig{
		BaseURL: server.URL, Client: server.Client(), ExpectedModuleKey: "data_exchange", ExpectedContractSHA256: summary.Identity.ContractSHA256,
	})
	if err != nil {
		t.Fatal(err)
	}
	transport := &remoteTestTransport{Binding: remoteCapability}
	binding, err := NewFactory(transport).OpenSaaS(t.Context(), dataexchange.ApplicationRef{ApplicationID: "app", RuntimeID: "runtime"}, host)
	if err != nil {
		t.Fatal(err)
	}
	contracttest.VerifyBinding(t, binding)
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
	if !ok || len(provider.HTTPAdapters()) != 1 {
		t.Fatal("Data Exchange SaaS binding does not expose the owner HTTP adapter")
	}
}

func TestSaaSBindingRejectsDifferentCapabilityBeforeConnecting(t *testing.T) {
	different, err := contracttest.NewFixtureBinding("data_exchange")
	if err != nil {
		t.Fatal(err)
	}
	transport := &remoteTestTransport{Binding: different}
	_, err = NewFactory(transport).OpenSaaS(t.Context(), dataexchange.ApplicationRef{ApplicationID: "app", RuntimeID: "runtime"}, remoteTestHost{})
	if err == nil || !strings.Contains(err.Error(), "contract_mismatch") {
		t.Fatalf("OpenSaaS() error = %v", err)
	}
	if transport.connected != nil {
		t.Fatal("Data Exchange connected a SaaS transport before capability verification")
	}
}
