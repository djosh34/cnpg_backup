// Copyright 2026 cnpg_backup contributors. All rights reserved.
package cnpgi

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/djosh34/cnpg_backup/internal/configuration"
	"github.com/djosh34/cnpg_backup/internal/recoveryguard"
	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const ownedAnnotation = "cnpg-backup.djosh34.github.io/owner"
const configAnnotation = "cnpg-backup.djosh34.github.io/config"

func ptr[T any](v T) *T     { return &v }
func jsonText(v any) string { b, _ := json.Marshal(v); return string(b) }

type patch struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value any    `json:"value"`
}

// Place patches only the container/volume lists and owned metadata. It never
// deletes Pods; EVALUATE returns the same deterministic patch for CNPG rollout.
func Place(ctx context.Context, api *API, c Cluster, object []byte, image string) ([]byte, error) {
	if err := api.VerifyCluster(ctx, c); err != nil {
		return nil, err
	}
	var typ meta.TypeMeta
	if len(object) > 2<<20 || json.Unmarshal(object, &typ) != nil {
		return nil, errors.New("invalid lifecycle object")
	}
	var metadata *meta.ObjectMeta
	var spec *core.PodSpec
	var base, metadataBase string
	switch {
	case typ.Kind == "Pod" && typ.APIVersion == "v1":
		var pod core.Pod
		if err := json.Unmarshal(object, &pod); err != nil {
			return nil, err
		}
		spec = &pod.Spec
		metadata = &pod.ObjectMeta
		base = "/spec"
		metadataBase = "/metadata"
	case typ.Kind == "Job" && typ.APIVersion == "batch/v1":
		var job batch.Job
		if err := json.Unmarshal(object, &job); err != nil {
			return nil, err
		}
		if job.Namespace != c.Metadata.Namespace {
			return nil, errors.New("Job namespace mismatch")
		}
		spec = &job.Spec.Template.Spec
		metadata = &job.Spec.Template.ObjectMeta
		base = "/spec/template/spec"
		metadataBase = "/spec/template/metadata"
	default:
		return nil, errors.New("unsupported lifecycle resource")
	}
	if metadata.Namespace != "" && metadata.Namespace != c.Metadata.Namespace {
		return nil, errors.New("Pod namespace mismatch")
	}
	before := spec.DeepCopy()
	oldMeta := metadata.DeepCopy()
	if err := inject(ctx, api, c, spec, metadata, image); err != nil {
		return nil, err
	}
	changes := []patch{}
	for _, field := range []struct {
		name          string
		before, after any
	}{{"initContainers", before.InitContainers, spec.InitContainers}, {"containers", before.Containers, spec.Containers}, {"volumes", before.Volumes, spec.Volumes}} {
		if !reflect.DeepEqual(field.before, field.after) {
			changes = append(changes, patch{"add", base + "/" + field.name, field.after})
		}
	}
	if !reflect.DeepEqual(oldMeta.Annotations, metadata.Annotations) {
		changes = append(changes, patch{"add", metadataBase + "/annotations", metadata.Annotations})
	}
	if len(changes) == 0 {
		return nil, nil
	}
	return json.Marshal(changes)
}
func inject(ctx context.Context, api *API, c Cluster, spec *core.PodSpec, metadata *meta.ObjectMeta, image string) error {
	if spec.HostPID || (spec.ShareProcessNamespace != nil && *spec.ShareProcessNamespace) {
		return errors.New("private main PID namespace is required")
	}
	if spec.SecurityContext == nil || spec.SecurityContext.RunAsUser == nil || *spec.SecurityContext.RunAsUser != c.Spec.PostgresUID || spec.SecurityContext.RunAsGroup == nil || *spec.SecurityContext.RunAsGroup != c.Spec.PostgresGID {
		return errors.New("CNPG UID/GID does not match Pod security context")
	}
	mainIndex := slices.IndexFunc(spec.Containers, func(v core.Container) bool {
		return v.Name == "postgres" || v.Name == "full-recovery" || v.Name == "initdb" || v.Name == "join"
	})
	if mainIndex < 0 && len(spec.Containers) == 1 {
		mainIndex = 0
	}
	if mainIndex < 0 {
		return errors.New("CNPG main container not found")
	}
	main := &spec.Containers[mainIndex]
	if s := main.SecurityContext; s != nil && ((s.RunAsUser != nil && *s.RunAsUser != c.Spec.PostgresUID) || (s.RunAsGroup != nil && *s.RunAsGroup != c.Spec.PostgresGID)) {
		return errors.New("main container identity override is unsupported")
	}
	original := append(slices.Clone(main.Command), main.Args...)
	if len(original) > 2 && original[0] == recoveryguard.HelperPath && original[1] == "recovery-guard" && original[2] == "--" {
		original = original[3:]
	}
	if len(original) < 3 || original[0] != "/controller/manager" || original[1] != "instance" || !slices.Contains([]string{"run", "init", "join", "restore"}, original[2]) {
		return errors.New("unexpected CNPG main argv")
	}
	recovery := original[2] == "restore"
	if metadata.Labels["cnpg.io/jobRole"] == "full-recovery" && !recovery {
		return errors.New("recovery Job argv was replaced")
	}
	destination, source, err := c.Repositories()
	if err != nil {
		return err
	}
	dst, err := api.Repository(ctx, c.Metadata.Namespace, destination)
	if err != nil {
		return err
	}
	var src *configuration.Spec
	if recovery {
		if source == "" {
			return errors.New("recovery source Repository is required")
		}
		s, err := api.Repository(ctx, c.Metadata.Namespace, source)
		if err != nil {
			return err
		}
		src = &s
		if src.RepositoryID == dst.RepositoryID {
			return errors.New("source/destination lineage overlap")
		}
	}
	owned := metadata.Annotations[ownedAnnotation] == string(c.Metadata.UID)
	index := slices.IndexFunc(spec.InitContainers, func(v core.Container) bool { return v.Name == "cnpg-backup" })
	if slices.ContainsFunc(spec.Containers, func(v core.Container) bool { return v.Name == "cnpg-backup" }) || (index >= 0 && !owned) {
		return errors.New("sidecar name collision")
	}
	targets := map[string]string{"pgdata": "/var/lib/postgresql/data"}
	if len(c.Spec.WALStorage) > 0 && string(c.Spec.WALStorage) != "null" {
		targets["pg-wal"] = "/var/lib/postgresql/wal"
	}
	for _, t := range c.Spec.Tablespaces {
		name := t.Name
		if strings.HasPrefix(name, "_") {
			name = "1" + name[1:]
		}
		name = strings.ToLower(strings.NewReplacer("_", "-", "$", "-").Replace(name))
		targets["tbs-"+name] = "/var/lib/postgresql/tablespaces/" + t.Name
	}
	guard := recoveryguard.Config{ClusterUID: string(c.Metadata.UID), OperationUID: c.OperationUID()}
	dataMounts := []core.VolumeMount{}
	for _, mount := range main.VolumeMounts {
		expected, ok := targets[mount.Name]
		if !ok {
			continue
		}
		if mount.MountPath != expected || mount.SubPath != "" || mount.SubPathExpr != "" || mount.ReadOnly || mount.MountPropagation != nil {
			return errors.New("unsupported target mount")
		}
		volumeIndex := slices.IndexFunc(spec.Volumes, func(v core.Volume) bool { return v.Name == mount.Name })
		if volumeIndex < 0 || spec.Volumes[volumeIndex].PersistentVolumeClaim == nil || spec.Volumes[volumeIndex].PersistentVolumeClaim.ReadOnly {
			return errors.New("target is not a writable PVC")
		}
		if recovery {
			object, err := api.Get(ctx, coreResource("persistentvolumeclaims"), c.Metadata.Namespace, spec.Volumes[volumeIndex].PersistentVolumeClaim.ClaimName)
			if err != nil {
				return errors.New("target PVC identity unavailable; retry after claim creation")
			}
			var pvc core.PersistentVolumeClaim
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.Object, &pvc); err != nil {
				return err
			}
			owner := meta.GetControllerOf(&pvc)
			if owner == nil || owner.UID != c.Metadata.UID || pvc.DeletionTimestamp != nil || pvc.UID == "" || (pvc.Spec.VolumeMode != nil && *pvc.Spec.VolumeMode != core.PersistentVolumeFilesystem) {
				return errors.New("PVC is not an exclusively Cluster-owned filesystem target")
			}
			guard.Targets = append(guard.Targets, recoveryguard.Target{PVCUID: string(pvc.UID), Mount: mount.MountPath})
		}
		dataMounts = append(dataMounts, mount)
		delete(targets, mount.Name)
	}
	if len(targets) != 0 {
		return errors.New("missing PGDATA, WAL or tablespace target mount")
	}
	if recovery {
		if err := guard.Validate(); err != nil {
			return err
		}
		slices.SortFunc(guard.Targets, func(a, b recoveryguard.Target) int { return strings.Compare(a.PVCUID, b.PVCUID) })
		wrapped, err := recoveryguard.WrapArgv(original, nil, len(c.Spec.WALStorage) > 0 && string(c.Spec.WALStorage) != "null")
		if err != nil {
			return err
		}
		main.Command = wrapped
		main.Args = nil
		podUID := core.EnvVar{Name: "POD_UID", ValueFrom: &core.EnvVarSource{FieldRef: &core.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.uid"}}}
		envIndex := slices.IndexFunc(main.Env, func(v core.EnvVar) bool { return v.Name == "POD_UID" })
		if envIndex >= 0 {
			if !reflect.DeepEqual(main.Env[envIndex], podUID) {
				return errors.New("main POD_UID environment collision")
			}
		} else {
			main.Env = append(main.Env, podUID)
		}
		if err := api.EnsureProjection(ctx, c, c.Metadata.Name+"-cb-targets", map[string]string{"guard.json": jsonText(guard)}); err != nil {
			return err
		}
	}
	projection := map[string]string{"destination.json": jsonText(dst)}
	if src != nil {
		projection["source.json"] = jsonText(src)
	}
	sum := sha256.Sum256([]byte(jsonText(projection)))
	configName := fmt.Sprintf("%s-cb-%x", c.Metadata.Name, sum[:6])
	if err := api.EnsureProjection(ctx, c, configName, projection); err != nil {
		return err
	}
	sources := []core.VolumeProjection{}
	addRole := func(role string, s configuration.Spec) {
		sources = append(sources, core.VolumeProjection{ConfigMap: &core.ConfigMapProjection{LocalObjectReference: core.LocalObjectReference{Name: configName}, Items: []core.KeyToPath{{Key: role + ".json", Path: role + "/repository.json"}}}})
		for _, ref := range []struct {
			selector *configuration.Selector
			path     string
		}{{&s.S3.AccessKeySecret, "accessKey"}, {&s.S3.SecretKeySecret, "secretKey"}, {s.S3.SessionTokenSecret, "sessionToken"}} {
			if ref.selector != nil {
				sources = append(sources, core.VolumeProjection{Secret: &core.SecretProjection{LocalObjectReference: core.LocalObjectReference{Name: ref.selector.Name}, Items: []core.KeyToPath{{Key: ref.selector.Key, Path: role + "/" + ref.path}}}})
			}
		}
		if ref := s.S3.CAConfigMap; ref != nil {
			sources = append(sources, core.VolumeProjection{ConfigMap: &core.ConfigMapProjection{LocalObjectReference: core.LocalObjectReference{Name: ref.Name}, Items: []core.KeyToPath{{Key: ref.Key, Path: role + "/ca.crt"}}}})
		}
	}
	addRole("destination", dst)
	if src != nil {
		addRole("source", *src)
	}
	// Native capture uses only the replication client identity and public server
	// CA. Never mirror the main container's application/superuser/server-key mounts.
	if !recovery {
		replication, ca := c.Spec.Certificates.ReplicationTLSSecret, c.Spec.Certificates.ServerCASecret
		if replication == "" {
			replication = c.Metadata.Name + "-replication"
		}
		if ca == "" {
			ca = c.Metadata.Name + "-ca"
		}
		for _, ref := range []struct {
			name  string
			items []core.KeyToPath
		}{{replication, []core.KeyToPath{{Key: "tls.crt", Path: "native/tls.crt"}, {Key: "tls.key", Path: "native/tls.key"}}}, {ca, []core.KeyToPath{{Key: "ca.crt", Path: "native/ca.crt"}}}} {
			if _, err := api.Get(ctx, coreResource("secrets"), c.Metadata.Namespace, ref.name); err != nil {
				return errors.New("native replication Secret is missing or not allowlisted")
			}
			sources = append(sources, core.VolumeProjection{Secret: &core.SecretProjection{LocalObjectReference: core.LocalObjectReference{Name: ref.name}, Items: ref.items}})
		}
	}
	volumes := []core.Volume{
		{Name: "cnpg-backup-plugins", VolumeSource: core.VolumeSource{EmptyDir: &core.EmptyDirVolumeSource{SizeLimit: ptr(resource.MustParse("1Mi"))}}},
		{Name: "cnpg-backup-bin", VolumeSource: core.VolumeSource{EmptyDir: &core.EmptyDirVolumeSource{SizeLimit: ptr(resource.MustParse("128Mi"))}}},
		{Name: "cnpg-backup-state", VolumeSource: core.VolumeSource{EmptyDir: &core.EmptyDirVolumeSource{SizeLimit: ptr(resource.MustParse("16Mi"))}}},
		{Name: "cnpg-backup-projection", VolumeSource: core.VolumeSource{Projected: &core.ProjectedVolumeSource{DefaultMode: ptr(int32(0440)), Sources: sources}}},
		{Name: "cnpg-backup-work", VolumeSource: core.VolumeSource{Ephemeral: &core.EphemeralVolumeSource{VolumeClaimTemplate: &core.PersistentVolumeClaimTemplate{Spec: core.PersistentVolumeClaimSpec{AccessModes: []core.PersistentVolumeAccessMode{core.ReadWriteOnce}, VolumeMode: ptr(core.PersistentVolumeFilesystem), StorageClassName: &dst.Workspace.StorageClassName, Resources: core.VolumeResourceRequirements{Requests: core.ResourceList{core.ResourceStorage: resource.MustParse(dst.Workspace.Size)}}}}}}},
	}
	if recovery {
		volumes = append(volumes, core.Volume{Name: "cnpg-backup-config", VolumeSource: core.VolumeSource{ConfigMap: &core.ConfigMapVolumeSource{LocalObjectReference: core.LocalObjectReference{Name: c.Metadata.Name + "-cb-targets"}, DefaultMode: ptr(int32(0440))}}})
	}
	for _, volume := range volumes {
		i := slices.IndexFunc(spec.Volumes, func(v core.Volume) bool { return v.Name == volume.Name })
		if i >= 0 {
			if !owned {
				return errors.New("owned volume name collision")
			}
			spec.Volumes[i] = volume
		} else {
			spec.Volumes = append(spec.Volumes, volume)
		}
	}
	addMount := func(container *core.Container, mount core.VolumeMount) error {
		i := slices.IndexFunc(container.VolumeMounts, func(v core.VolumeMount) bool { return v.Name == mount.Name || v.MountPath == mount.MountPath })
		if i >= 0 {
			if !owned {
				return errors.New("owned mount/path collision")
			}
			container.VolumeMounts[i] = mount
		} else {
			container.VolumeMounts = append(container.VolumeMounts, mount)
		}
		return nil
	}
	for _, mount := range []core.VolumeMount{{Name: "cnpg-backup-plugins", MountPath: "/plugins", SubPath: "plugins"}, {Name: "cnpg-backup-bin", MountPath: "/cnpg-backup/bin", ReadOnly: true}, {Name: "cnpg-backup-state", MountPath: "/cnpg-backup/state", ReadOnly: true}} {
		if err := addMount(main, mount); err != nil {
			return err
		}
	}
	sidecarMounts := append(dataMounts, core.VolumeMount{Name: "cnpg-backup-plugins", MountPath: "/plugins", SubPath: "plugins"}, core.VolumeMount{Name: "cnpg-backup-bin", MountPath: "/cnpg-backup/bin"}, core.VolumeMount{Name: "cnpg-backup-state", MountPath: "/cnpg-backup/state"}, core.VolumeMount{Name: "cnpg-backup-projection", MountPath: "/cnpg-backup/projection", ReadOnly: true}, core.VolumeMount{Name: "cnpg-backup-work", MountPath: "/cnpg-backup/work"})
	if recovery {
		mount := core.VolumeMount{Name: "cnpg-backup-config", MountPath: "/cnpg-backup/config", ReadOnly: true}
		if err := addMount(main, mount); err != nil {
			return err
		}
		sidecarMounts = append(sidecarMounts, mount)
	}
	mode := "instance"
	if recovery {
		mode = "recovery-job"
	}
	sidecar := core.Container{Name: "cnpg-backup", Image: image, ImagePullPolicy: core.PullIfNotPresent, Command: []string{"/usr/local/bin/cnpg-backup", mode}, RestartPolicy: ptr(core.ContainerRestartPolicyAlways), Resources: dst.Resources, VolumeMounts: sidecarMounts,
		Env:                    []core.EnvVar{{Name: "POD_UID", ValueFrom: &core.EnvVarSource{FieldRef: &core.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.uid"}}}},
		TerminationMessagePath: "/dev/termination-log", TerminationMessagePolicy: core.TerminationMessageReadFile,
		SecurityContext: &core.SecurityContext{RunAsUser: &c.Spec.PostgresUID, RunAsGroup: &c.Spec.PostgresGID, RunAsNonRoot: ptr(true), ReadOnlyRootFilesystem: ptr(true), AllowPrivilegeEscalation: ptr(false), Capabilities: &core.Capabilities{Drop: []core.Capability{"ALL"}}, SeccompProfile: &core.SeccompProfile{Type: core.SeccompProfileTypeRuntimeDefault}},
		StartupProbe:    &core.Probe{ProbeHandler: core.ProbeHandler{Exec: &core.ExecAction{Command: []string{"/usr/local/bin/cnpg-backup", mode, "--probe"}}}, PeriodSeconds: 1, TimeoutSeconds: 4, FailureThreshold: 60, SuccessThreshold: 1},
	}
	preparer := core.Container{Name: "cnpg-backup-socket-init", Image: image, ImagePullPolicy: core.PullIfNotPresent,
		Command: []string{"/usr/local/bin/cnpg-backup", "instance", "--prepare-socket"}, Resources: dst.Resources,
		SecurityContext: sidecar.SecurityContext.DeepCopy(), VolumeMounts: []core.VolumeMount{{Name: "cnpg-backup-plugins", MountPath: "/cnpg-backup/socket-root"}},
		TerminationMessagePath: "/dev/termination-log", TerminationMessagePolicy: core.TerminationMessageReadFile}
	prepareIndex := slices.IndexFunc(spec.InitContainers, func(v core.Container) bool { return v.Name == preparer.Name })
	if prepareIndex >= 0 && (!owned || index < 0 || prepareIndex >= index) {
		return errors.New("socket preparation ordering/collision")
	}
	if index >= 0 && prepareIndex < 0 {
		return errors.New("owned socket preparer missing")
	}
	if index >= 0 {
		spec.InitContainers[index] = sidecar
		spec.InitContainers[prepareIndex] = preparer
	} else {
		spec.InitContainers = append(spec.InitContainers, preparer, sidecar)
	}
	if metadata.Annotations == nil {
		metadata.Annotations = map[string]string{}
	}
	metadata.Annotations[ownedAnnotation] = string(c.Metadata.UID)
	metadata.Annotations[configAnnotation] = configName
	return nil
}
