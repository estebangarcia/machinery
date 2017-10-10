package backends

import (
	"github.com/estebangarcia/machinery/v1/tasks"
	"github.com/valyala/fasthttp"
	"fmt"
	"github.com/estebangarcia/machinery/v1/config"
	"encoding/json"
	"time"
	"strings"
)

type APIBackend struct {
	client   *fasthttp.Client
	cnf      *config.Config
}

type TaskArguments []map[string] interface{}

type UpdateTask struct {
	Status string `json:"Status"`
	Result []*tasks.TaskResult `json:"Result"`
	Error string `json:"Error"`
}

type Task struct{
	UUID string `json:"UUID"`
	Name string `json:"Name"`
	Status string `json:"Status"`
	Args TaskArguments `json:"Args"`
	Result []*tasks.TaskResult `json:"Result"`
	Error string `json:"Error"`
	JobUUID *string `json:"JobUUID"`
	ExecutionOrder int `json:"ExecutionOrder"`
	CreatedAt time.Time `json:"CreatedAt"`
	UpdatedAt time.Time `json:"UpdatedAt"`
	DeletedAt *time.Time `json:"DeletedAt"`
}

type Job struct{
	UUID string `json:"UUID"`
	Name string `json:"Name"`
	Status string `json:"Status"`
	Tasks []Task `json:"Tasks"`
	Type string `json:"Type"`
	CreatedAt time.Time `json:"CreatedAt"`
	UpdatedAt time.Time `json:"UpdatedAt"`
	DeletedAt *time.Time `json:"DeletedAt"`
}

func NewAPIBackend(cnf *config.Config) Interface {
	return &APIBackend{
		cnf: cnf,
		client: &fasthttp.Client{},
	}
}

// Group related functions
func (a *APIBackend) InitGroup(groupUUID string, taskUUIDs []string) error {
	return nil
}

func (a *APIBackend) GroupCompleted(groupUUID string, groupTaskCount int) (bool, error){
	job, err := a.doJobRequest(groupUUID, "GET")

	if err != nil {
		return false, err
	}

	var countSuccessTasks = 0
	for _, task := range job.Tasks {
		if task.Status == tasks.StateSuccess || task.Status == tasks.StateFailure {
			countSuccessTasks++
		}
	}

	return countSuccessTasks == groupTaskCount, nil
}
func (a *APIBackend) GroupTaskStates(groupUUID string, groupTaskCount int) ([]*tasks.TaskState, error){
	job, err := a.doJobRequest(groupUUID, "GET")

	if err != nil {
		return nil, err
	}


	var taskStates []*tasks.TaskState
	for _, task := range job.Tasks {
		taskStates = append(taskStates, &tasks.TaskState{
			TaskUUID: task.UUID,
			State: task.Status,
			Results: task.Result,
			Error: task.Error,
		})
	}

	return taskStates, nil
}
func (a *APIBackend) TriggerChord(groupUUID string) (bool, error){
	return true, nil
}
// Setting / getting task state
func (a *APIBackend) SetStatePending(signature *tasks.Signature) error{

	UpdateTaskPayload := UpdateTask{}
	UpdateTaskPayload.Status = tasks.StatePending

	_, err := a.doTaskRequest(signature.UUID, "PUT", &UpdateTaskPayload)

	if err != nil {
		return err
	}

	return nil
}
func (a *APIBackend) SetStateReceived(signature *tasks.Signature) error{
	UpdateTaskPayload := UpdateTask{}
	UpdateTaskPayload.Status = tasks.StateReceived

	_, err := a.doTaskRequest(signature.UUID, "PUT", &UpdateTaskPayload)


	if err != nil {
		return err
	}

	return nil
}
func (a *APIBackend) SetStateStarted(signature *tasks.Signature) error{
	UpdateTaskPayload := UpdateTask{}
	UpdateTaskPayload.Status = tasks.StateStarted

	_, err := a.doTaskRequest(signature.UUID, "PUT", &UpdateTaskPayload)


	if err != nil {
		return err
	}

	return nil
}
func (a *APIBackend) SetStateRetry(signature *tasks.Signature) error{
	UpdateTaskPayload := UpdateTask{}
	UpdateTaskPayload.Status = tasks.StateRetry

	_, err := a.doTaskRequest(signature.UUID, "PUT", &UpdateTaskPayload)


	if err != nil {
		return err
	}

	return nil
}
func (a *APIBackend) SetStateSuccess(signature *tasks.Signature, results []*tasks.TaskResult) error{
	UpdateTaskPayload := UpdateTask{}
	UpdateTaskPayload.Status = tasks.StateSuccess
	UpdateTaskPayload.Result = results

	_, err := a.doTaskRequest(signature.UUID, "PUT", &UpdateTaskPayload)


	if err != nil {
		return err
	}

	return nil
}

func (a *APIBackend) SetStateFailure(signature *tasks.Signature, err string) error{

	UpdateTaskPayload := UpdateTask{}
	UpdateTaskPayload.Status = tasks.StateFailure
	UpdateTaskPayload.Error = err

	_, errReq := a.doTaskRequest(signature.UUID, "PUT", &UpdateTaskPayload)


	if errReq != nil {
		return errReq
	}

	return nil
}

func (a *APIBackend) GetState(taskUUID string) (*tasks.TaskState, error){
	task, err := a.doTaskRequest(taskUUID, "GET", nil)

	if err != nil {
		return nil, err
	}

	taskState := &tasks.TaskState{
		TaskUUID: task.UUID,
		State: task.Status,
		Results: task.Result,
		Error: task.Error,
	}

	return taskState, nil

}
// Purging stored stored tasks states and group meta data
func (a *APIBackend) PurgeState(taskUUID string) error{
	return nil
}
func (a *APIBackend) PurgeGroupMeta(groupUUID string) error{
	return nil
}

func (a *APIBackend) doJobRequest(jobUUID string, method string) (Job, error) {
	job := Job{}

	bytes, err := a.doRequest(fmt.Sprintf("/jobs/%s", jobUUID), method, nil)
	if err != nil {
		return job, err
	}

	err = json.Unmarshal(bytes, &job)
	if err != nil {
		return job, fmt.Errorf("error parsing task %s", jobUUID)
	}

	return job, nil

}

func (a *APIBackend) doTaskRequest(taskUUID string, method string, body *UpdateTask) (Task, error){
	task := Task{}


	bytes, err := a.doRequest(fmt.Sprintf("/tasks/%s", taskUUID), method, body)

	if err != nil {
		return task, err
	}

	err = json.Unmarshal(bytes, &task)
	if err != nil {
		return task, fmt.Errorf("error parsing task %s", taskUUID)
	}

	return task, nil
}

func (a *APIBackend) doRequest(uri string, method string, body interface{}) ([]byte, error) {
	req := fasthttp.AcquireRequest()
	req.SetRequestURI(fmt.Sprintf("%s%s",a.cnf.APIConfig.BaseURL, uri))
	req.Header.SetMethod(strings.ToUpper(method))
	req.Header.SetContentType("application/json")

	if strings.ToUpper(method) != "GET" && body != nil {
		bodyBytes, err := json.Marshal(body)

		if err != nil {
			return nil, fmt.Errorf("error parsing to json while calling %s%s", method, uri)
		}

		req.AppendBody(bodyBytes)
	}

	resp := fasthttp.AcquireResponse()

	a.client.Do(req, resp)

	if resp.StatusCode() != 200 {
		return nil, fmt.Errorf("error calling to %s %s with status code %v", method, uri, resp.StatusCode())
	}

	return resp.Body(), nil
}