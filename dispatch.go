package main

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"io/ioutil"
	"log"
	"net/http"
	"os"
)

var api_key_path = "/home/kojan/tf/api-key"
var api_key string
var gacek_home = "/mnt/nfs/gacek"
var jobs_dir = gacek_home + "/jobs"
var queues_dir = gacek_home + "/queues"

type Request struct {
	ApiKey string `json:"api_key,omitempty"`
	Test   struct {
		Tmt struct {
			Url        string `json:"url"`
			Ref        string `json:"ref"`
			PlanFilter string `json:"plan_filter"`
			Settings   struct {
				RecognizeErrors bool `json:"recognize-errors"`
			} `json:"settings"`
		} `json:"tmt"`
	} `json:"test"`
	Environments [1]struct {
		Arch string `json:"arch"`
		Os   struct {
			Compose string `json:"compose"`
		} `json:"os"`
		Artifacts []Artifact `json:"artifacts,omitempty"`
	} `json:"environments"`
}

type Artifact struct {
	Type    string `json:"type"`
	Id      string `json:"id"`
	Install bool   `json:"install"`
}

type Subject struct {
	Url     string `xml:"url"`
	Ref     string `xml:"ref"`
	Filter  string `xml:"filter"`
	Compose string `xml:"compose"`
	Arch    string `xml:"arch"`
}

type SubjectArtifact struct {
	NVR    string `xml:"nvr"`
	TaskId string `xml:"taskId"`
}

func get_new_jobs() []string {
	dir, err := os.Open(queues_dir + "/new")
	if err != nil {
		log.Fatal(err)
	}
	defer dir.Close()
	jobs, err := dir.Readdirnames(0)
	if err != nil {
		log.Fatal(err)
	}
	return jobs
}

func move_job(job string) {
	new_path := queues_dir + "/new/" + job
	pending_path := queues_dir + "/pending/" + job
	err := os.Rename(new_path, pending_path)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Job %s marked as pending\n", job)
}

func get_subject(job string) Subject {
	job_dir := jobs_dir + "/" + job
	bytes, err := os.ReadFile(job_dir + "/subject.xml")
	if err != nil {
		log.Fatal(err)
	}
	subject := Subject{}
	if err := xml.Unmarshal(bytes, &subject); err != nil {
		log.Fatal(err)
	}
	return subject
}

func get_artifacts(job string) []SubjectArtifact {
	job_dir := jobs_dir + "/" + job
	bytes, err := os.ReadFile(job_dir + "/artifacts.xml")
	if err != nil {
		log.Fatal(err)
	}
	artifacts := struct {
		Artifacts []SubjectArtifact `xml:"artifact"`
	}{}
	if err := xml.Unmarshal(bytes, &artifacts); err != nil {
		log.Fatal(err)
	}
	return artifacts.Artifacts
}

func prepare_tf_request(subject Subject, artifacts []SubjectArtifact) Request {
	tf := Request{}
	tf.Test.Tmt.Url = subject.Url
	tf.Test.Tmt.Ref = subject.Ref
	tf.Test.Tmt.PlanFilter = subject.Filter
	tf.Test.Tmt.Settings.RecognizeErrors = true
	tf.Environments[0].Arch = subject.Arch
	tf.Environments[0].Os.Compose = subject.Compose
	for _, artifact := range artifacts {
		log.Printf("Artifact NVR %s, Koji task ID: %s\n", artifact.NVR, artifact.TaskId)
		tf.Environments[0].Artifacts = append(tf.Environments[0].Artifacts, Artifact{"fedora-koji-build", artifact.TaskId, false})
	}
	u, err := json.MarshalIndent(tf, "          ", "  ")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("TF request: %s", string(u))
	return tf
}

func submit_tf_request(r Request) string {
	r.ApiKey = api_key
	body, err := json.Marshal(r)
	if err != nil {
		log.Fatal(err)
	}
	req, err := http.NewRequest("POST", "https://api.dev.testing-farm.io/v0.1/requests", bytes.NewBuffer(body))
	if err != nil {
		log.Fatal(err)
	}
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("TF response HTTP code: %d\n", resp.StatusCode)
	bytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		log.Printf("TF response body: %s\n", string(bytes))
		log.Fatal("TF request failed")
	}
	tfResp := struct {
		Id string `json:"id"`
	}{}
	if err := json.Unmarshal(bytes, &tfResp); err != nil {
		log.Fatal(err)
	}
	log.Printf("TF ID: %s\n", tfResp.Id)
	return tfResp.Id
}

func write_dispatch_info(job string, tf_id string) {
	dispatch := struct {
		XMLName xml.Name `xml:"dispatch"`
		TfId    string   `xml:"tfId"`
	}{TfId: tf_id}
	bytes, err := xml.MarshalIndent(dispatch, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	err = os.WriteFile(jobs_dir+"/"+job+"/tf-dispatch.xml", bytes, 0644)
	if err != nil {
		log.Fatal(err)
	}
}

func dispatch_job(job string) {
	log.Printf("Attempting to dispatch job %s\n", job)
	subject := get_subject(job)
	artifacts := get_artifacts(job)
	tf_req := prepare_tf_request(subject, artifacts)
	tf_id := submit_tf_request(tf_req)
	write_dispatch_info(job, tf_id)
	move_job(job)
}

func main() {
	log.Printf("Dispatch started\n")
	bytes, err := os.ReadFile(api_key_path)
	if err != nil {
		log.Fatal(err)
	}
	api_key = string(bytes)
	for _, job := range get_new_jobs() {
		dispatch_job(job)
	}
	log.Printf("Dispatch complete\n")
}
