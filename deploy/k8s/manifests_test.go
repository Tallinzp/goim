// Package k8s_test checks deploy/k8s on any machine (Plan 6d1, #150.7):
// every manifest decodes strictly into its k8s.io/api type, none sets a
// test-only flag (P5bI-3), and the fields the spec relies on are set.
package k8s_test

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	"sigs.k8s.io/yaml"
)

var kinds = map[string]func() any{
	"Namespace": func() any { return &corev1.Namespace{} }, "Service": func() any { return &corev1.Service{} },
	"StatefulSet": func() any { return &appsv1.StatefulSet{} }, "Deployment": func() any { return &appsv1.Deployment{} },
	"PodDisruptionBudget":     func() any { return &policyv1.PodDisruptionBudget{} },
	"HorizontalPodAutoscaler": func() any { return &autoscalingv2.HorizontalPodAutoscaler{} },
	"NetworkPolicy":           func() any { return &networkingv1.NetworkPolicy{} },
}

// load decodes every document of the .yaml files under dir but the
// kustomization.yaml files; an unknown field or kind fails the test.
func load(t *testing.T, dir string) (docs []any) {
	t.Helper()
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || filepath.Ext(path) != ".yaml" || d.Name() == "kustomization.yaml" {
			return err
		}
		b, err := os.ReadFile(path)
		for i, part := range bytes.Split(b, []byte("\n---\n")) {
			var tm struct{ Kind string }
			_ = yaml.Unmarshal(part, &tm)
			mk, ok := kinds[tm.Kind]
			if !ok {
				t.Errorf("%s #%d: kind %q is not checked here", path, i, tm.Kind)
				continue
			}
			obj := mk()
			if err := yaml.UnmarshalStrict(part, obj); err != nil {
				t.Errorf("%s #%d: %v", path, i, err)
				continue
			}
			docs = append(docs, obj)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return docs
}

func only[T any](docs []any) (out []T) {
	for _, d := range docs {
		if o, ok := d.(T); ok {
			out = append(out, o)
		}
	}
	return out
}

func TestManifestsDecodeStrictly(t *testing.T) {
	if n := len(load(t, ".")); n != 14 {
		t.Fatalf("%d documents, want the base's 11 and the local overlay's 3", n)
	}
}

// The fault API is unauthenticated on the admin port; -clock-offset
// shifts the wall clock (P5bI-3).
func TestNoTestOnlyFlags(t *testing.T) {
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || filepath.Ext(path) == ".go" {
			return err
		}
		b, err := os.ReadFile(path)
		for _, flag := range []string{"-faults", "-clock-offset"} {
			if bytes.Contains(b, []byte(flag)) {
				t.Errorf("%s sets %s", path, flag)
			}
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

// Kicks reach a draining gateway through the headless Service gateway;
// every other Service, the client one above all, has Ready pods only.
func TestNotReadyAddresses(t *testing.T) {
	found := false
	for _, s := range only[*corev1.Service](load(t, ".")) {
		if s.Name == "gateway" && s.Spec.ClusterIP == corev1.ClusterIPNone {
			found = s.Spec.PublishNotReadyAddresses
		} else if s.Spec.PublishNotReadyAddresses {
			t.Errorf("Service %s publishes NotReady addresses", s.Name)
		}
	}
	if !found {
		t.Error("the headless Service gateway does not publish NotReady addresses")
	}
}

// R5S-O5: one gateway at a time, Logic never below minReplicas, gateway
// pods not evicted by the autoscaler.
func TestDisruptionBudgets(t *testing.T) {
	docs := load(t, "base")
	pdbs := map[string]*policyv1.PodDisruptionBudget{}
	for _, p := range only[*policyv1.PodDisruptionBudget](docs) {
		pdbs[p.Spec.Selector.MatchLabels["app"]] = p
	}
	if p := pdbs["gateway"]; p == nil || p.Spec.MaxUnavailable == nil || p.Spec.MaxUnavailable.String() != "1" || p.Spec.MinAvailable != nil {
		t.Errorf("gateway PDB %+v, want maxUnavailable 1 only", p)
	}
	hpa, dep := only[*autoscalingv2.HorizontalPodAutoscaler](docs), only[*appsv1.Deployment](docs)
	if len(hpa) != 1 || hpa[0].Spec.MinReplicas == nil || len(dep) != 1 || dep[0].Spec.Replicas == nil {
		t.Fatalf("want one HPA with minReplicas and one Deployment with replicas, got %d, %d", len(hpa), len(dep))
	}
	min := int(*hpa[0].Spec.MinReplicas)
	if p := pdbs["logic"]; p == nil || p.Spec.MinAvailable == nil || p.Spec.MinAvailable.IntValue() != min || p.Spec.MaxUnavailable != nil {
		t.Errorf("logic PDB %+v, want minAvailable %d (minReplicas) only", p, min)
	}
	if r := int(*dep[0].Spec.Replicas); r != min {
		t.Errorf("logic replicas %d, want minReplicas %d", r, min)
	}
	for _, s := range only[*appsv1.StatefulSet](docs) {
		if v := s.Spec.Template.Annotations["cluster-autoscaler.kubernetes.io/safe-to-evict"]; v != "false" {
			t.Errorf("StatefulSet %s: safe-to-evict %q, want \"false\"", s.Name, v)
		}
	}
}

// The pod name is the gateway id and its Kick DNS name, the grace period
// outlasts the drain (-drain-timeout 85s), and both binaries are probed on
// /readyz.
func TestGatewayPods(t *testing.T) {
	docs := load(t, "base")
	sts := only[*appsv1.StatefulSet](docs)
	if len(sts) != 1 || sts[0].Spec.ServiceName != "gateway" {
		t.Fatalf("want one StatefulSet with serviceName gateway, got %d", len(sts))
	}
	pod := sts[0].Spec.Template.Spec
	if g := pod.TerminationGracePeriodSeconds; g == nil || *g <= 85 {
		t.Errorf("terminationGracePeriodSeconds %v, want > 85", g)
	}
	if e := pod.Containers[0].Env; len(e) == 0 || e[0].Name != "POD_NAME" || e[0].ValueFrom == nil ||
		e[0].ValueFrom.FieldRef == nil || e[0].ValueFrom.FieldRef.FieldPath != "metadata.name" {
		t.Errorf("gateway env %+v, want POD_NAME from metadata.name first", e)
	}
	specs := []corev1.PodSpec{pod}
	for _, d := range only[*appsv1.Deployment](docs) {
		specs = append(specs, d.Spec.Template.Spec)
	}
	for _, s := range specs {
		if p := s.Containers[0].ReadinessProbe; p == nil || p.HTTPGet == nil || p.HTTPGet.Path != "/readyz" || p.HTTPGet.Port.IntValue() != 9090 {
			t.Errorf("%s: readiness probe %+v, want GET /readyz on 9090", s.Containers[0].Name, p)
		}
	}
}

// #85: each binary's pods are selected by a policy that admits 9090 only
// from the monitoring namespace and keeps its other ports open to all.
func TestAdminPortPolicy(t *testing.T) {
	want := map[string][]int{"gateway": {8000, 9100}, "logic": {9000, 9001}}
	for _, np := range only[*networkingv1.NetworkPolicy](load(t, "base")) {
		app := np.Spec.PodSelector.MatchLabels["app"]
		open, ok := want[app]
		delete(want, app)
		var opened []int
		for _, r := range np.Spec.Ingress {
			if len(r.Ports) == 0 {
				t.Errorf("%s: a rule without ports opens every port", np.Name)
			}
			for _, p := range r.Ports {
				if p.Port.IntValue() != 9090 {
					if len(r.From) == 0 {
						opened = append(opened, p.Port.IntValue())
					}
				} else if f := r.From; len(f) != 1 || f[0].PodSelector != nil || f[0].IPBlock != nil || f[0].NamespaceSelector == nil ||
					len(f[0].NamespaceSelector.MatchLabels) != 1 || f[0].NamespaceSelector.MatchLabels["kubernetes.io/metadata.name"] != "monitoring" {
					t.Errorf("%s: 9090 admitted from %+v, want the monitoring namespace only", np.Name, f)
				}
			}
		}
		if !ok || !slices.Equal(opened, open) {
			t.Errorf("NetworkPolicy %s (app %q): open to all %v, want %v", np.Name, app, opened, open)
		}
	}
	if len(want) != 0 {
		t.Errorf("no NetworkPolicy for %v", want)
	}
}
