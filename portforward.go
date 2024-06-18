package main

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"net/http"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/httpstream"
	v1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
)

type servicePortForward struct {
	localPort uint16
	stopChan  chan struct{}
	errChan   chan error
}

func openServicePortForward(ctx context.Context, cfg *rest.Config, localPort, destPort uint16, serviceKey types.NamespacedName) (*servicePortForward, error) {
	cl, err := v1.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	svc, err := cl.Services(serviceKey.Namespace).Get(ctx, serviceKey.Name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	destPodPort := -1
	for _, svcPort := range svc.Spec.Ports {
		if svcPort.Port != int32(destPort) {
			continue
		}
		destPodPort = int(svcPort.TargetPort.IntVal)
		break
	}
	if destPodPort == -1 {
		return nil, fmt.Errorf("service %s/%s does not expose port %d", serviceKey.Namespace, serviceKey.Name, destPort)
	}
	endpoints, err := cl.Endpoints(serviceKey.Namespace).Get(ctx, serviceKey.Name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	var pods []string
	for _, subset := range endpoints.Subsets {
		for _, port := range subset.Ports {
			if port.Port != int32(destPodPort) {
				continue
			}
			for _, addr := range subset.Addresses {
				if addr.TargetRef != nil && addr.TargetRef.Kind == "Pod" {
					pods = append(pods, addr.TargetRef.Name)
				}
			}
		}
	}
	if len(pods) == 0 {
		return nil, fmt.Errorf("no available pod endpoints found for service %s/%s", serviceKey.Namespace, serviceKey.Name)
	}

	pod := pods[rand.IntN(len(pods))]
	url := cl.RESTClient().Post().Resource("pods").Namespace(serviceKey.Namespace).Name(pod).SubResource("portforward").URL()
	transport, upgrader, err := spdy.RoundTripperFor(cfg)
	if err != nil {
		return nil, err
	}
	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, "POST", url)
	if cmdutil.PortForwardWebsockets.IsEnabled() {
		tunnelingDialer, err := portforward.NewSPDYOverWebsocketDialer(url, cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to create websockets dialer: %w", err)
		}
		// First attempt tunneling (websocket) dialer, then fallback to spdy dialer.
		dialer = portforward.NewFallbackDialer(tunnelingDialer, dialer, httpstream.IsUpgradeFailure)
	}
	stopChan, readyChan := make(chan struct{}, 1), make(chan struct{}, 1)
	out, errOut := new(bytes.Buffer), new(bytes.Buffer)

	fw, err := portforward.New(dialer, []string{fmt.Sprintf("%d:%d", localPort, destPodPort)}, stopChan, readyChan, out, errOut)
	if err != nil {
		return nil, err
	}

	errChan := make(chan error)
	go func() {
		defer close(errChan)
		errChan <- fw.ForwardPorts()
	}()
	select {
	case err := <-errChan:
		return nil, fmt.Errorf("port forwarding failed: %w (%s)", err, errOut.String())
	case <-readyChan:
	}
	forwardedPorts, err := fw.GetPorts()
	if err != nil {
		return nil, err
	}
	return &servicePortForward{
		localPort: forwardedPorts[0].Local,
		stopChan:  stopChan,
		errChan:   errChan,
	}, nil
}

func (pf *servicePortForward) LocalPort() uint16 {
	return pf.localPort
}

func (pf *servicePortForward) Close() error {
	close(pf.stopChan)
	if err, ok := <-pf.errChan; ok {
		return err
	}
	return nil
}
