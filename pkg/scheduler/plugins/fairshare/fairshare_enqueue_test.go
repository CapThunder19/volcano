/*
Copyright 2025 The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package fairshare

import (
	"testing"

	v1 "k8s.io/api/core/v1"

	"volcano.sh/apis/pkg/apis/scheduling"
	schedulingv1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/cmd/scheduler/app/options"
	"volcano.sh/volcano/pkg/scheduler/actions/enqueue"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/uthelper"
	"volcano.sh/volcano/pkg/scheduler/util"
)

func TestJobTotalResource_SumsTasksWhenPresent(t *testing.T) {
	job := api.NewJobInfo("ns/j",
		&api.TaskInfo{UID: "t1", Job: "ns/j", Resreq: api.NewResource(api.BuildResourceList("1", "1G"))},
		&api.TaskInfo{UID: "t2", Job: "ns/j", Resreq: api.NewResource(api.BuildResourceList("2", "1G"))},
	)
	job.SetPodGroup(&api.PodGroup{PodGroup: scheduling.PodGroup{
		Spec: scheduling.PodGroupSpec{MinResources: ptrResourceList(api.BuildResourceList("10", "1G"))},
	}})

	if got := jobTotalResource(job, v1.ResourceCPU); got != 3000 {
		t.Errorf("jobTotalResource() = %v, want 3000 (sum of tasks, not MinResources)", got)
	}
}

func TestJobTotalResource_FallsBackToMinResourcesWithoutTasks(t *testing.T) {
	job := api.NewJobInfo("ns/j")
	job.SetPodGroup(&api.PodGroup{PodGroup: scheduling.PodGroup{
		Spec: scheduling.PodGroupSpec{MinResources: ptrResourceList(api.BuildResourceList("2", "1G"))},
	}})

	if got := jobTotalResource(job, v1.ResourceCPU); got != 2000 {
		t.Errorf("jobTotalResource() = %v, want 2000 (from MinResources)", got)
	}
}

func TestJobTotalResource_NoTasksNoMinResources(t *testing.T) {
	job := api.NewJobInfo("ns/j")
	job.SetPodGroup(&api.PodGroup{})

	if got := jobTotalResource(job, v1.ResourceCPU); got != 0 {
		t.Errorf("jobTotalResource() = %v, want 0", got)
	}
}

func ptrResourceList(rl v1.ResourceList) *v1.ResourceList {
	return &rl
}

func TestEnqueueGate(t *testing.T) {
	options.Default()

	trueValue := true
	tiers := []conf.Tier{{Plugins: []conf.PluginOption{{
		Name:               PluginName,
		EnabledJobOrder:    &trueValue,
		EnabledJobEnqueued: &trueValue,
		Arguments: map[string]interface{}{
			"fairshare.targetQueues":      "q1",
			"fairshare.resourceKey":       "cpu",
			"fairshare.enableEnqueueGate": "true",
		},
	}}}}

	oneCPU := api.BuildResourceList("1", "1G")
	runningJobs := []string{"a1", "a2", "a3", "a4"}

	basePodGroups := func() []*schedulingv1.PodGroup {
		pgs := make([]*schedulingv1.PodGroup, 0, len(runningJobs)+2)
		for _, n := range runningJobs {
			pgs = append(pgs, util.BuildPodGroupWithMinResources(n, "ns-a", "q1", 1, nil, oneCPU, schedulingv1.PodGroupRunning))
		}
		return append(pgs,
			util.BuildPodGroupWithMinResources("a5", "ns-a", "q1", 1, nil, oneCPU, schedulingv1.PodGroupPending),
			util.BuildPodGroupWithMinResources("b1", "ns-b", "q1", 1, nil, oneCPU, schedulingv1.PodGroupPending),
		)
	}
	runningPods := func() []*v1.Pod {
		pods := make([]*v1.Pod, 0, len(runningJobs)+2)
		for _, n := range runningJobs {
			pods = append(pods, util.BuildPod("ns-a", "p-"+n, "n1", v1.PodRunning, oneCPU, n, nil, nil))
		}
		return pods
	}
	nodes := func() []*v1.Node { return []*v1.Node{util.BuildNode("n1", api.BuildResourceList("4", "4G"), nil)} }
	queues := func() []*schedulingv1.Queue {
		return []*schedulingv1.Queue{util.BuildQueue("q1", 1, api.BuildResourceList("4", "4G"))}
	}
	expect := map[api.JobID]scheduling.PodGroupPhase{
		"ns-a/a5": scheduling.PodGroupPending,
		"ns-b/b1": scheduling.PodGroupInqueue,
	}

	tests := []uthelper.TestCommonStruct{
		{
			Name:      "pending jobs already have pods",
			PodGroups: basePodGroups(),
			Pods: append(runningPods(),
				util.BuildPod("ns-a", "p-a5", "", v1.PodPending, oneCPU, "a5", nil, nil),
				util.BuildPod("ns-b", "p-b1", "", v1.PodPending, oneCPU, "b1", nil, nil),
			),
			Nodes:        nodes(),
			Queues:       queues(),
			ExpectStatus: expect,
		},
		{
			Name:         "pending jobs have no pods yet (vcjob)",
			PodGroups:    basePodGroups(),
			Pods:         runningPods(),
			Nodes:        nodes(),
			Queues:       queues(),
			ExpectStatus: expect,
		},
	}

	for i, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			defer saveAndResetState(t)()
			test.Plugins = map[string]framework.PluginBuilder{PluginName: New}
			test.RegisterSession(tiers, nil)
			defer test.Close()
			test.Run([]framework.Action{enqueue.New()})
			if err := test.CheckAll(i); err != nil {
				t.Fatal(err)
			}
		})
	}
}
