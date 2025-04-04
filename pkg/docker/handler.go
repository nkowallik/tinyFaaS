package docker

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"sync"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/archive"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/google/uuid"

	"github.com/OpenFogStack/tinyFaaS/pkg/manager"
	"github.com/OpenFogStack/tinyFaaS/pkg/util"
)

const (
	TmpDir           = "./tmp"
	containerTimeout = 1
)

type dockerHandler struct {
	name         string
	env          string
	uniqueName   string
	filePath     string
	client       *client.Client
	network      string
	functionsMux sync.Mutex
	functions    map[string]string
}

type DockerBackend struct {
	client     *client.Client
	tinyFaaSID string
}

func New(tinyFaaSID string) *DockerBackend {
	// create docker client
	client, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		log.Fatalf("error creating docker client: %s", err)
		return nil
	}

	return &DockerBackend{
		client:     client,
		tinyFaaSID: tinyFaaSID,
	}
}

func (db *DockerBackend) Stop() error {
	return nil
}

func (db *DockerBackend) BuildImage(name string, env string, filedir string) (manager.Handler, error) {

	// make a unique function name by appending uuid string to function name
	uuid, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}

	dh := &dockerHandler{
		name:      name,
		env:       env,
		client:    db.client,
		functions: make(map[string]string),
		//containers: make([]string, 0),
		//handlerIPs: make([]string, 0),
	}

	dh.uniqueName = name + "-" + uuid.String()
	log.Println("creating function", name, "with unique name", dh.uniqueName)

	// make a folder for the function
	// mkdir <folder>
	dh.filePath = path.Join(TmpDir, dh.uniqueName)

	err = os.MkdirAll(dh.filePath, 0777)
	if err != nil {
		return nil, err
	}

	// copy Docker stuff into folder
	// cp runtimes/<env>/* <folder>

	err = util.CopyDirFromEmbed(runtimes, path.Join(runtimesDir, dh.env), dh.filePath)
	if err != nil {
		return nil, err
	}

	log.Println("copied runtime files to folder", dh.filePath)

	// copy function into folder
	// into a subfolder called fn
	// cp <file> <folder>/fn
	err = os.MkdirAll(path.Join(dh.filePath, "fn"), 0777)
	if err != nil {
		return nil, err
	}

	err = util.CopyAll(filedir, path.Join(dh.filePath, "fn"))
	if err != nil {
		return nil, err
	}

	// build image
	// docker build -t <image> <folder>
	tar, err := archive.TarWithOptions(dh.filePath, &archive.TarOptions{})
	if err != nil {
		return nil, err
	}

	r, err := db.client.ImageBuild(
		context.Background(),
		tar,
		types.ImageBuildOptions{
			Tags:       []string{dh.uniqueName},
			Remove:     true,
			Dockerfile: "Dockerfile",
			Labels: map[string]string{
				"tinyfaas-function": dh.name,
				"tinyFaaS":          db.tinyFaaSID,
			},
		},
	)
	if err != nil {
		return nil, err
	}

	defer r.Body.Close()
	scanner := bufio.NewScanner(r.Body)
	for scanner.Scan() {
		log.Println(scanner.Text())
	}

	log.Println("built image", dh.uniqueName)

	// remove folder
	// rm -rf <folder>
	err = os.RemoveAll(dh.filePath)
	if err != nil {
		return nil, err
	}

	log.Println("removed folder", dh.filePath)

	return dh, nil
}

func (db *DockerBackend) SpawnFunction(dh *dockerHandler, envs map[string]string) (string, error) {
	// create network
	// docker network create <network>
	if dh.network == "" {
		network, err := db.client.NetworkCreate(
			context.Background(),
			dh.uniqueName,
			network.CreateOptions{
				Labels: map[string]string{
					"tinyfaas-function": dh.name,
					"tinyFaaS":          db.tinyFaaSID,
				},
			},
		)
		if err != nil {
			return "", err
		}

		dh.network = network.ID

		log.Println("created network", dh.uniqueName, "with id", network.ID)
	}

	e := make([]string, 0, len(envs))

	for k, v := range envs {
		e = append(e, fmt.Sprintf("%s=%s", k, v))
	}

	// create containers
	// docker run -d --network <network> --name <container> <image>
	dh.functionsMux.Lock()
	defer dh.functionsMux.Unlock()
	uniqueContainerName := dh.uniqueName + fmt.Sprintf("-%d", len(dh.functions))

	container, err := db.client.ContainerCreate(
		context.Background(),
		&container.Config{
			Image: dh.uniqueName,
			Labels: map[string]string{
				"tinyfaas-function": dh.name,
				"tinyFaaS":          db.tinyFaaSID,
			},
			Env: e,
		},
		&container.HostConfig{
			NetworkMode: container.NetworkMode(dh.uniqueName),
		},
		nil,
		nil,
		uniqueContainerName,
	)

	if err != nil {
		return "", err
	}

	log.Println("created container", container.ID)

	//dh.containers = append(dh.containers, container.ID)
	dh.functions[container.ID] = ""

	// remove folder
	// rm -rf <folder>
	if dh.filePath != "" {
		err = os.RemoveAll(dh.filePath)
		if err != nil {
			return "", err
		}

		log.Println("removed folder", dh.filePath)
		dh.filePath = ""
	}
	return container.ID, nil
}

func (db *DockerBackend) KillContainer(id string) error {
	err := db.client.ContainerStop(context.Background(), id, container.StopOptions{})
	if err != nil {
		return err
	}

	err = db.client.ContainerRemove(context.Background(), id, container.RemoveOptions{
		Force:         true,
		RemoveVolumes: true,
	})
	if err != nil {
		return err
	}
	log.Printf("Destroyed fn container %s", id)
	return nil
}

func (db *DockerBackend) Append(dh manager.Handler, envs map[string]string) (string, string, error) {
	cid, err := db.SpawnFunction(dh.(*dockerHandler), envs)
	if err != nil {
		return "", "", err
	}
	ip, err := db.StartContainerById(dh.(*dockerHandler), cid)
	if err != nil {
		return "", "", err
	}
	maxRetries := 10
	for {
		maxRetries--
		if maxRetries == 0 {
			// container did not start properly!
			// give people some logs to look at
			log.Printf("container %s (ip %s) not ready after 10 retries", cid, ip)
			log.Printf("getting logs for container %s", cid)
			logs, err := dh.(*dockerHandler).getContainerLogs(cid)

			if err != nil {
				log.Print(fmt.Errorf("container %s not ready after 10 retries, error encountered when getting logs %s", ip, err))
			}

			log.Println(logs)

			log.Printf("end of logs for container %s", cid)

			log.Print(fmt.Errorf("container %s not ready after 10 retries", ip))
		}

		// timeout of 1 second
		client := http.Client{
			Timeout: 3 * time.Second,
		}

		resp, err := client.Get("http://" + ip + ":8000/health")
		if err != nil {
			log.Println(err)
			log.Println("retrying in 1 second")
			time.Sleep(1 * time.Second)
			continue
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			log.Println("container", ip, "is ready")
			break
		}
		log.Println("container", ip, "is not ready yet, retrying in 1 second")
		time.Sleep(1 * time.Second)
	}
	dh.(*dockerHandler).functions[cid] = ip
	return cid, ip, nil
}

func (db *DockerBackend) ShutdownContainer(cid string) {
	db.KillContainer(cid)
}

func (db *DockerBackend) StartContainerById(h manager.Handler, id string) (string, error) {
	dh := h.(*dockerHandler)
	err := db.client.ContainerStart(context.Background(), id, container.StartOptions{})
	if err != nil {
		return "", err
	}
	c, err := db.client.ContainerInspect(
		context.Background(),
		id,
	)
	if err != nil {
		return "", err
	}
	ip := c.NetworkSettings.Networks[dh.uniqueName].IPAddress
	log.Println(dh)
	log.Printf("IP_ADDR := %s", ip)
	return ip, nil
}

func (db *DockerBackend) Create(name string, env string, filedir string, envs map[string]string) (manager.Handler, error) {
	dh, err := db.BuildImage(name, env, filedir)
	if err != nil {
		log.Fatal("Unable to build image", err)
	}
	if dh == nil {
		log.Fatal("NIL in return value")
	}
	log.Println(dh)
	_, err = db.SpawnFunction(dh.(*dockerHandler), envs)
	if err != nil {
		log.Fatal("Unable to spawn function instance", err)
	}
	log.Println(dh)
	return dh, nil
}

func (dh *dockerHandler) IPs() []util.IpWrapper {
	ips := make([]util.IpWrapper, 0)
	for k, v := range dh.functions {
		ips = append(ips, util.IpWrapper{Ip: v, Cid: k})
	}
	return ips
}

func (dh *dockerHandler) Start() error {
	log.Printf("dh: %+v", dh)

	// start containers
	// docker start <container>

	wg := sync.WaitGroup{}
	for c, _ := range dh.functions {
		wg.Add(1)
		go func(c string) {
			err := dh.client.ContainerStart(
				context.Background(),
				c,
				container.StartOptions{},
			)
			wg.Done()
			if err != nil {
				log.Printf("error starting container %s: %s", c, err)
				return
			}

			log.Println("started container", c)
		}(c)
	}
	wg.Wait()

	// get container IPs
	// docker inspect <container>
	for container, _ := range dh.functions {
		c, err := dh.client.ContainerInspect(
			context.Background(),
			container,
		)
		if err != nil {
			return err
		}

		dh.functions[container] = c.NetworkSettings.Networks[dh.uniqueName].IPAddress
		//dh.handlerIPs = append(dh.handlerIPs, c.NetworkSettings.Networks[dh.uniqueName].IPAddress)

		log.Println("got ip", c.NetworkSettings.Networks[dh.uniqueName].IPAddress, "for container", container)
		return nil
	}

	// wait for the containers to be ready
	// curl http://<container>:8000/ready
	for cid, ip := range dh.functions {
		log.Println("waiting for container", ip, "to be ready")
		maxRetries := 10
		for {
			maxRetries--
			if maxRetries == 0 {
				// container did not start properly!
				// give people some logs to look at
				log.Printf("container %s (ip %s) not ready after 10 retries", cid, ip)
				log.Printf("getting logs for container %s", cid)
				logs, err := dh.getContainerLogs(cid)

				if err != nil {
					return fmt.Errorf("container %s not ready after 10 retries, error encountered when getting logs %s", ip, err)
				}

				log.Println(logs)

				log.Printf("end of logs for container %s", cid)

				return fmt.Errorf("container %s not ready after 10 retries", ip)
			}

			// timeout of 1 second
			client := http.Client{
				Timeout: 3 * time.Second,
			}

			resp, err := client.Get("http://" + ip + ":8000/health")
			if err != nil {
				log.Println(err)
				log.Println("retrying in 1 second")
				time.Sleep(1 * time.Second)
				continue
			}
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				log.Println("container", ip, "is ready")
				break
			}
			log.Println("container", ip, "is not ready yet, retrying in 1 second")
			time.Sleep(1 * time.Second)
		}
	}

	return nil
}

func (dh *dockerHandler) Destroy() error {
	log.Println("destroying function", dh.name)
	log.Printf("dh: %+v", dh)

	wg := sync.WaitGroup{}
	//log.Printf("stopping containers: %v", dh.containers)
	for c, _ := range dh.functions {
		log.Println("removing container", c)

		wg.Add(1)
		go func(c string) {
			log.Println("stopping container", c)

			timeout := 1 // seconds

			err := dh.client.ContainerStop(
				context.Background(),
				c,
				container.StopOptions{
					Timeout: &timeout,
				},
			)
			if err != nil {
				log.Printf("error stopping container %s: %s", c, err)
			}

			log.Println("stopped container", c)

			err = dh.client.ContainerRemove(
				context.Background(),
				c,
				container.RemoveOptions{},
			)
			wg.Done()
			if err != nil {
				log.Printf("error removing container %s: %s", c, err)
			}
		}(c)

		log.Println("removed container", c)
	}
	wg.Wait()

	// remove network
	// docker network rm <network>
	err := dh.client.NetworkRemove(
		context.Background(),
		dh.network,
	)
	if err != nil {
		return err
	}

	log.Println("removed network", dh.network)

	// remove image
	// docker rmi <image>
	/*_, err = dh.client.ImageRemove(
		context.Background(),
		dh.uniqueName,
		image.RemoveOptions{},
	)

	if err != nil {
		return err
	}

	log.Println("removed image", dh.uniqueName)
	*/
	return nil
}

func (dh *dockerHandler) getContainerLogs(c string) (string, error) {
	logs := ""

	l, err := dh.client.ContainerLogs(
		context.Background(),
		c,
		container.LogsOptions{
			ShowStdout: true,
			ShowStderr: true,
			Timestamps: true,
		},
	)
	if err != nil {
		return logs, err
	}

	var lstdout bytes.Buffer
	var lstderr bytes.Buffer

	_, err = stdcopy.StdCopy(&lstdout, &lstderr, l)

	l.Close()

	if err != nil {
		return logs, err
	}

	// add a prefix to each line
	// function=<function> handler=<handler> <line>
	scanner := bufio.NewScanner(&lstdout)

	for scanner.Scan() {
		logs += fmt.Sprintf("function=%s handler=%s %s\n", dh.name, c, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		return logs, err
	}

	// same for stderr
	scanner = bufio.NewScanner(&lstderr)

	for scanner.Scan() {
		logs += fmt.Sprintf("function=%s handler=%s %s\n", dh.name, c, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		return logs, err
	}

	return logs, nil
}

func (dh *dockerHandler) Logs() (io.Reader, error) {
	// get container logs
	// docker logs <container>
	var logs bytes.Buffer

	for c, _ := range dh.functions {
		l, err := dh.getContainerLogs(c)
		if err != nil {
			return nil, err
		}

		logs.WriteString(l)
	}

	return &logs, nil
}
