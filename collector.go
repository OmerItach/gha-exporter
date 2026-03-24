package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/bradleyfalzon/ghinstallation/v2"
	"github.com/google/go-github/v79/github"
	"github.com/gravitational/trace"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	workflowRunnerSecondsVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gha_workflow_runner_seconds",
		},
		[]string{"repo", "ref", "event_type", "workflow"},
	)
	workflowElapsedSecondsVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gha_workflow_elapsed_seconds",
		},
		[]string{"repo", "ref", "event_type", "workflow"},
	)
	jobRunnerSecondsVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gha_job_runner_seconds",
		},
		[]string{"repo", "ref", "event_type", "workflow", "job"},
	)
	stepRunnerSecondsVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gha_step_runner_seconds",
		},
		[]string{"repo", "ref", "event_type", "workflow", "job", "step"},
	)
	workflowRunCountVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gha_workflow_run_count",
		},
		[]string{"repo", "ref", "event_type", "workflow", "conclusion"},
	)
	jobRunCountVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gha_job_run_count",
		},
		[]string{"repo", "ref", "event_type", "workflow", "job", "conclusion"},
	)
	stepRunCountVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gha_step_run_count",
		},
		[]string{"repo", "ref", "event_type", "workflow", "job", "step", "conclusion"},
	)
	workflowRunStatusVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "gha_workflow_run_status",
			Help: "Status of the last workflow run (1 for success, 0 for failure)",
		},
		[]string{"repo", "ref", "event_type", "workflow"},
	)
	workflowLastRunDurationVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "gha_workflow_last_run_duration_seconds",
			Help: "Duration of the last completed workflow run in seconds",
		},
		[]string{"repo", "ref", "event_type", "workflow"},
	)
	workflowLastRunIDVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "gha_workflow_last_run_id",
			Help: "ID of the last completed workflow run",
		},
		[]string{"repo", "ref", "event_type", "workflow"},
	)

)

type Collector struct {
	cfg    *CLI
	client *github.Client
	// seenRuns tracks which completed workflow run IDs we've counted this session,
	// keyed by short repo name. Evicted periodically to prevent unbounded growth.
	seenRuns map[string]map[int64]bool
	// oldestIncomplete tracks per-repo the earliest start time of a workflow run that we've
	// seen that was not completed. This becomes the cutoff for querying github
	// each collect loop.
	oldestIncomplete map[string]time.Time
	// highWaterMark[owner/repo] = max run ID known to be persisted in Prometheus.
	// Populated on startup by querying the Prometheus HTTP API. Any completed run
	// with ID <= this value has already been counted and must be skipped, even after
	// a pod restart when seenRuns is empty.
	highWaterMark map[string]int64
}

func NewCollector(cfg *CLI) *Collector {
	prometheus.MustRegister(workflowRunnerSecondsVec)
	prometheus.MustRegister(workflowElapsedSecondsVec)
	prometheus.MustRegister(jobRunnerSecondsVec)
	prometheus.MustRegister(stepRunnerSecondsVec)
	prometheus.MustRegister(workflowRunCountVec)
	prometheus.MustRegister(jobRunCountVec)
	prometheus.MustRegister(stepRunCountVec)
	prometheus.MustRegister(workflowRunStatusVec)
	prometheus.MustRegister(workflowLastRunDurationVec)
	prometheus.MustRegister(workflowLastRunIDVec)

	return &Collector{
		cfg:              cfg,
		seenRuns:         make(map[string]map[int64]bool),
		oldestIncomplete: make(map[string]time.Time),
		highWaterMark:    make(map[string]int64),
	}
}

// loadStateFromPrometheus queries the deployed Prometheus instance for the
// maximum workflow run ID it has ever scraped per repo. This is used to seed
// highWaterMark so that runs already counted before a pod restart are not
// double-counted after the in-memory seenRuns map is wiped.
func (c *Collector) loadStateFromPrometheus(ctx context.Context) {
	if c.cfg.PrometheusURL == "" {
		log.Print("GHA_PROMETHEUS_URL not set — starting with empty state. Runs in initialWindow may be double-counted after pod restarts.")
		return
	}

	query := `max by (repo) (gha_workflow_last_run_id)`
	apiURL := strings.TrimSuffix(c.cfg.PrometheusURL, "/") + "/api/v1/query?query=" + url.QueryEscape(query)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		log.Printf("Warning: failed to build Prometheus state query: %v", err)
		return
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("Warning: failed to query Prometheus for initial state: %v", err)
		return
	}
	defer resp.Body.Close()

	var result struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Metric map[string]string  `json:"metric"`
				Value  [2]json.RawMessage `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		log.Printf("Warning: failed to decode Prometheus response: %v", err)
		return
	}
	if result.Status != "success" {
		log.Printf("Warning: Prometheus returned non-success status: %s", result.Status)
		return
	}

	for _, r := range result.Data.Result {
		repo := r.Metric["repo"]
		if repo == "" {
			continue
		}
		var valStr string
		if err := json.Unmarshal(r.Value[1], &valStr); err != nil {
			continue
		}
		var runID int64
		if _, err := fmt.Sscanf(valStr, "%d", &runID); err != nil || runID <= 0 {
			continue
		}
		c.highWaterMark[repo] = runID
		log.Printf("Restored from Prometheus: repo=%s lastRunID=%d", repo, runID)
	}
	log.Printf("Loaded %d repo states from Prometheus", len(c.highWaterMark))
}

// isAlreadySeen returns true if this run has already been counted, either
// in the current session (seenRuns) or before a pod restart (highWaterMark).
func (c *Collector) isAlreadySeen(repo string, runID int64) bool {
	if c.seenRuns[repo][runID] {
		return true
	}
	fullRepo := c.cfg.Owner + "/" + repo
	hwm, ok := c.highWaterMark[fullRepo]
	return ok && runID <= hwm
}

// markSeen records a run ID as counted and updates the per-repo high water mark.
func (c *Collector) markSeen(repo string, runID int64) {
	if c.seenRuns[repo] == nil {
		c.seenRuns[repo] = make(map[int64]bool)
	}
	c.seenRuns[repo][runID] = true
	// Keep the in-process high water mark growing so eviction works correctly.
	fullRepo := c.cfg.Owner + "/" + repo
	if runID > c.highWaterMark[fullRepo] {
		c.highWaterMark[fullRepo] = runID
	}
}

// evictOldSeenRuns removes entries from seenRuns[repo] whose IDs are strictly
// less than the current high water mark. Those runs are guaranteed to be in
// Prometheus already (or skipped), so keeping them in memory is wasteful.
func (c *Collector) evictOldSeenRuns(repo string) {
	fullRepo := c.cfg.Owner + "/" + repo
	hwm := c.highWaterMark[fullRepo]
	if hwm == 0 {
		return
	}
	evicted := 0
	for id := range c.seenRuns[repo] {
		if id < hwm {
			delete(c.seenRuns[repo], id)
			evicted++
		}
	}
	if evicted > 0 {
		log.Printf("Evicted %d old seenRuns entries for repo %s (hwm=%d)", evicted, repo, hwm)
	}
}

func (c *Collector) Run(ctx context.Context) {
	log.Print("Collector started")
	defer func() { log.Print("Collector finished: ", ctx.Err()) }()

	// Restore high water marks from Prometheus on startup so runs already
	// counted before this pod restarted are not double-counted.
	c.loadStateFromPrometheus(ctx)

	for ; ctx.Err() == nil; time.Sleep(c.cfg.Sleep) {
		repos := c.cfg.Repos
		if len(repos) == 0 {
			var err error
			repos, err = c.discoverRepos(ctx)
			if err != nil {
				log.Printf("Failed to discover repos: %v", err)
				continue
			}
		}

		for _, repo := range repos {
			if err := c.collectRepo(ctx, repo); err != nil {
				log.Print(err)
			}
		}
	}
}

func (c *Collector) discoverRepos(ctx context.Context) ([]string, error) {
	// Recreate client on demand
	if c.client == nil {
		client, err := newGHClient(c.cfg.Owner, c.cfg.AppID, []byte(c.cfg.AppKey))
		if err != nil {
			return nil, trace.Wrap(err, "failed to create client for discovery")
		}
		c.client = client
	}

	var allRepos []string
	opts := &github.ListOptions{PerPage: 100}

	// We need to get the installation ID first, which newGHClient already does but doesn't expose easily.
	// However, newGHClient sets c.client which is an authenticated client for the installation.
	// For GitHub Apps, we can list repositories for the current installation.
	for {
		repos, resp, err := c.client.Apps.ListRepos(ctx, opts)
		if err != nil {
			return nil, trace.Wrap(err, "failed to list app repositories")
		}
		for _, r := range repos.Repositories {
			// Ensure it belongs to the owner we care about
			if strings.EqualFold(r.GetOwner().GetLogin(), c.cfg.Owner) {
				allRepos = append(allRepos, r.GetName())
			}
		}
		if resp.NextPage == 0 {
			break
		}
		opts.Page = resp.NextPage
	}

	return allRepos, nil
}

func (c *Collector) collectRepo(ctx context.Context, repo string) error {
	log.Printf("Started collecting repo: %s", repo)
	defer func() { log.Printf("Finished collecting repo: %s", repo) }()

	// Recreate client on demand after errors and on startup
	if c.client == nil {
		client, err := newGHClient(c.cfg.Owner, c.cfg.AppID, []byte(c.cfg.AppKey))
		if err != nil {
			return trace.Wrap(err, "collection failed for repo: %q", repo)
		}
		c.client = client
	}

	ignoreCompleted := false
	cutoff, ok := c.oldestIncomplete[repo]
	if !ok {
		cutoff = time.Now().UTC().Add(-c.cfg.InitialWindow)
		// This is the first time collecting from this repo. We normally ignore
		// completed runs and only collect run time from runs that complete
		// after we have started, but if the `--backfill` flag is given, then
		// we collect run times from all completed runs in the initial window.
		ignoreCompleted = !c.cfg.Backfill
	}

	created := cutoff.Format("created:>=2006-01-02T15:04:05+00:00")
	opts := &github.ListWorkflowRunsOptions{
		Created:     created,
		ListOptions: github.ListOptions{PerPage: 100},
	}

	actions := c.client.Actions
	var allRuns []*github.WorkflowRun
	for {
		runs, response, err := actions.ListRepositoryWorkflowRuns(ctx, c.cfg.Owner, repo, opts)
		if err != nil {
			return trace.Wrap(err, "failed to list workflow runs")
		}
		allRuns = append(allRuns, runs.WorkflowRuns...)

		opts.Page = response.NextPage
		if opts.Page == 0 {
			break
		}
	}

	newCutoff := time.Now().UTC()
	// Process runs from oldest to newest (GitHub API returns newest first)
	for i := len(allRuns) - 1; i >= 0; i-- {
		run := allRuns[i]
		newRunCutoff, err := c.collectRun(ctx, repo, run, newCutoff, ignoreCompleted)
		if err != nil {
			return trace.Wrap(err, "failed to collect workflow runs")
		}
		newCutoff = newRunCutoff
	}
	c.oldestIncomplete[repo] = newCutoff
	// Evict seenRuns entries whose IDs are below the high water mark.
	// This prevents the map from growing without bound over long uptime.
	c.evictOldSeenRuns(repo)
	return nil
}

func (c *Collector) collectRun(ctx context.Context, repo string, run *github.WorkflowRun, cutoff time.Time, ignoreCompleted bool) (time.Time, error) {
	var err error
	switch {
	case run.GetStatus() != "completed":
		switch {
		case run.CreatedAt == nil:
			// Ignore runs without a created timestamp
		case run.CreatedAt.Before(cutoff):
			cutoff = run.CreatedAt.Time
			log.Printf("open: %s/%d: %s (new cutoff: %v)",
				repo, run.GetID(), run.GetName(), cutoff)
		default:
			log.Printf("open: %s/%d: %s", repo, run.GetID(), run.GetName())
		}
	default:
		// Always update gauges for completed runs, even if already seen or ignoring
		// for counters. This ensures Gauges (which are in-memory only) are restored
		// after a restart from the newest runs in the list.
		c.updateGauges(repo, run)

		if c.isAlreadySeen(repo, run.GetID()) {
			log.Printf("seen: %s/%d: %s", repo, run.GetID(), run.GetName())
		} else {
			if !ignoreCompleted {
				err = c.collectJobs(ctx, repo, run)
			}
			if err == nil {
				c.markSeen(repo, run.GetID())
				log.Printf("done: %s/%d: %s\n", repo, run.GetID(), run.GetName())
			}
		}
	}

	return cutoff, err
}

func (c *Collector) collectJobs(ctx context.Context, repo string, run *github.WorkflowRun) error {
	opts := &github.ListWorkflowJobsOptions{
		ListOptions: github.ListOptions{PerPage: 100},
	}
	actions := c.client.Actions
	for {
		jobs, response, err := actions.ListWorkflowJobs(ctx, c.cfg.Owner, repo, run.GetID(), opts)
		if err != nil {
			return trace.Wrap(err, "failed to list workflow run jobs")
		}

		c.countJobs(repo, run, jobs.Jobs)

		opts.Page = response.NextPage
		if opts.Page == 0 {
			break
		}
	}
	return nil
}

func (c *Collector) countJobs(repoName string, run *github.WorkflowRun, jobs []*github.WorkflowJob) {
	workflowName := path.Base(run.GetPath())
	workflowName = strings.TrimSuffix(workflowName, path.Ext(workflowName))
	repo := c.cfg.Owner + "/" + repoName
	ref := makeRef(run)
	eventType := run.GetEvent()

	var workflowRunTime time.Duration

	for _, job := range jobs {
		var jobRunTime time.Duration
		for _, step := range job.Steps {
			stepRunCountVec.WithLabelValues(
				repo, ref, eventType, workflowName, job.GetName(), step.GetName(), step.GetConclusion(),
			).Add(1)


			if step.GetConclusion() != "success" {
				continue
			}
			if step.StartedAt == nil || step.CompletedAt == nil {
				continue
			}
			stepRunTime := step.CompletedAt.Sub(step.StartedAt.Time)
			stepRunnerSecondsVec.WithLabelValues(
				repo, ref, eventType, workflowName, job.GetName(), step.GetName(),
			).Add(stepRunTime.Seconds())
			jobRunTime += stepRunTime
		}
		jobRunCountVec.WithLabelValues(
			repo, ref, eventType, workflowName, job.GetName(), job.GetConclusion(),
		).Add(1)

		if job.GetConclusion() != "success" {
			continue
		}
		jobRunnerSecondsVec.WithLabelValues(
			repo, ref, eventType, workflowName, job.GetName(),
		).Add(jobRunTime.Seconds())

		workflowRunTime += jobRunTime
	}

	// The tool doesn't currently account for more than one run attempt.
	// This brings a multitude of issues that are near-impossible to
	// account for due to GH's API design.
	if run.GetRunAttempt() > 1 {
		return
	}

	workflowRunCountVec.WithLabelValues(repo, ref, eventType, workflowName, run.GetConclusion()).Add(1)

	if run.GetConclusion() == "success" {
		workflowRunnerSecondsVec.WithLabelValues(repo, ref, eventType, workflowName).Add(workflowRunTime.Seconds())
		if run.UpdatedAt != nil && run.CreatedAt != nil {
			elapsed := run.GetUpdatedAt().Sub(run.GetCreatedAt().Time)
			workflowElapsedSecondsVec.WithLabelValues(repo, ref, eventType, workflowName).Add(elapsed.Seconds())
		}
	}
}

func (c *Collector) updateGauges(repoName string, run *github.WorkflowRun) {
	if run.GetConclusion() == "" {
		return
	}

	workflowName := path.Base(run.GetPath())
	workflowName = strings.TrimSuffix(workflowName, path.Ext(workflowName))
	repo := c.cfg.Owner + "/" + repoName
	ref := makeRef(run)
	eventType := run.GetEvent()

	workflowLastRunIDVec.WithLabelValues(repo, ref, eventType, workflowName).Set(float64(run.GetID()))

	statusValue := 0.0
	if run.GetConclusion() == "success" {
		statusValue = 1.0
	}
	workflowRunStatusVec.WithLabelValues(repo, ref, eventType, workflowName).Set(statusValue)

	if run.UpdatedAt != nil && run.CreatedAt != nil {
		elapsed := run.GetUpdatedAt().Sub(run.GetCreatedAt().Time)
		workflowLastRunDurationVec.WithLabelValues(repo, ref, eventType, workflowName).Set(elapsed.Seconds())
	}
}

func makeRef(run *github.WorkflowRun) string {
	eventName := run.GetEvent()
	if strings.HasPrefix(eventName, "pull_request") {
		// Attempt to tie the workflow to a PR
		headSha := run.GetHeadSHA()
		headBranch := run.GetHeadBranch()

		if headSha == "" && headBranch == "" {
			return "error-missing-head-ref"
		}

		for _, pr := range run.PullRequests {
			prHeadBranch := pr.GetHead()

			if prHeadBranch.GetSHA() == headSha ||
				prHeadBranch.GetRef() == headBranch {
				return pr.GetBase().GetRef()
			}
		}

		return headBranch
	}

	if strings.HasPrefix(eventName, "merge_group") {
		headBranch := run.GetHeadBranch()
		if headBranch == "" {
			return "error-head-branch-is-nil"
		}

		mergeBranch := strings.TrimPrefix(headBranch, "gh-readonly-queue/")
		mergeBranch = strings.SplitN(mergeBranch, "/pr-", 2)[0]
		return mergeBranch
	}

	headBranch := run.GetHeadBranch()
	if headBranch == "" {
		return "error-head-branch-is-nil"
	}

	return headBranch
}

func newGHClient(owner string, appID int64, appKey []byte) (*github.Client, error) {
	// Create a retryable http client / roundtripper, but turn off logging
	// for now as it is a bit noisy. We should use a logger that implements
	// retryablehttp.LevelledLogger so we can turn on/off debug logging as needed.
	rtclient := retryablehttp.NewClient()
	rtclient.Logger = nil // too noisy right now
	rt := &retryablehttp.RoundTripper{Client: rtclient}

	// Create a temporary github client that can list the app installations
	// so we can get the installation ID for the proper github client.
	tmpTransport, err := ghinstallation.NewAppsTransport(rt, appID, appKey)
	if err != nil {
		return nil, err
	}
	gh := github.NewClient(&http.Client{Transport: tmpTransport})

	var instID int64
	listOptions := github.ListOptions{PerPage: 100}
findInst:
	for {
		installations, response, err := gh.Apps.ListInstallations(context.Background(), &listOptions)
		if err != nil {
			return nil, trace.Wrap(err, "Failed to list installations")
		}

		for _, inst := range installations {
			if inst.GetAccount().GetLogin() == owner {
				instID = inst.GetID()
				break findInst
			}
		}

		if response.NextPage == 0 {
			return nil, trace.NotFound("No such installation found")
		}

		listOptions.Page = response.NextPage
	}

	transport, err := ghinstallation.New(rt, appID, instID, appKey)
	if err != nil {
		return nil, trace.Wrap(err, "Failed creating authenticated transport")
	}

	gh = github.NewClient(&http.Client{Transport: transport})

	return gh, nil
}
