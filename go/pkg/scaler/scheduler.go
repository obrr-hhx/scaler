package scaler

import (
	"container/list"
	"context"
	"fmt"
	"github.com/AliyunContainerService/scaler/go/pkg/config"
	model "github.com/AliyunContainerService/scaler/go/pkg/model"
	platform_client "github.com/AliyunContainerService/scaler/go/pkg/platform_client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	"sync"
	"time"

	pb "github.com/AliyunContainerService/scaler/proto"
	"github.com/google/uuid"
)

type Scheduler struct {
	config         *config.Config
	metaData       *model.Meta
	platformClient platform_client.Client
	mu             sync.Mutex
	wg             sync.WaitGroup
	instances      map[string]*model.Instance
	idleInstance   *list.List
}

func New(metaData *model.Meta, config *config.Config) Scaler {
	client, err := platform_client.New(config.ClientAddr)
	if err != nil {
		log.Fatalf("client init with error: %s", err.Error())
	}
	scheduler := &Scheduler{
		config:         config,
		metaData:       metaData,
		platformClient: client,
		mu:             sync.Mutex{},
		wg:             sync.WaitGroup{},
		instances:      make(map[string]*model.Instance),
		idleInstance:   list.New(),
	}
	log.Printf("New scaler for app: %s is created", metaData.Key)
	scheduler.wg.Add(1)
	go func() {
		defer scheduler.wg.Done()
		scheduler.gcLoop()
		log.Printf("gc loop for app: %s is started", metaData.Key)
	}()
	return scheduler
}

func (s *Scheduler) Assign(ctx context.Context, request *pb.AssignRequest) (*pb.AssignReply, error) {
	start := time.Now()
	instanceId := uuid.New().String()

	defer func() {
		log.Printf("Assign cost: %s", time.Since(start))
	}()

	log.Printf("Assign request id: %s", request.RequestId)
	s.mu.Lock()
	// reuse idle instance if idle instance is available
	if s.idleInstance.Len() > 0 {
		e := s.idleInstance.Front()
		instance := e.Value.(*model.Instance)
		instance.Busy = true
		instanceId = instance.Id
		s.idleInstance.Remove(e)
		s.mu.Unlock()
		log.Printf("Assign request id: %s, instance %s reused", request.RequestId, instanceId)
		return &pb.AssignReply{
			Status:     pb.Status_Ok,
			Assigment: &pb.Assignment{
				RequestId: 	request.RequestId,
				MetaKey:  	instance.Meta.Key,
				InstanceId: instanceId,
			},
			ErrorMessage: nill,
		}, nil
	}
	s.mu.Unlock()

	// create new instance
	resourceConfig := &model.SlotResourceConfig{
		ResourceConfig: pb.ResourceConfig{
			MemoryInMegabytes: request.MetaData.MemoryInMb,
		},
	}
	slot, err := s.platformClient.CreateSlot(ctx, request.RequestId, &resourceConfig)
	if err != nil {
		errorMessage := fmt.Sprintf("Assign request id: %s, create slot error: %s", request.RequestId, err.Error())
		log.Printf(errorMessage)
		return nil, status.Error(codes.Internal, errorMessage)
	}

	meta := &model.Meta{
		Meta: pb.Meta{
			Key: 			request.MetaData.Key,
			Runtime: 		request.MetaData.Runtime,
			TimeoutInSecs: 	request.MetaData.TimeoutInSecs,
		},
	}
	instance, err := s.platformClient.Init(ctx, request.RequestId, instanceId, slot, meta)
	if err != nil {
		errorMessage := fmt.Sprintf("Assign request id: %s, init instance error: %s", request.RequestId, err.Error())
		log.Printf(errorMessage)
		return nil, status.Error(codes.Internal, errorMessage)
	}
	// add instance to instances map
	s.mu.Lock()
	instance.Busy = true
	s.instances[instance.Id] = instance
	s.mu.Unlock()
	log.Printf("Assign request id: %s, instance %s created for app %s, init latency: %dms", request.RequestId, instance.Id, request.MetaData.Key, instance.InitDurationInMs)

	return &pb.AssignReply{
		Status: pb.Status_Ok,
		Assigment: &pb.Assignment{
			RequestId: 	request.RequestId,
			MetaKey:  	instance.Meta.Key,
			InstanceId: instance.Id,
		},
		ErrorMessage: nil,
	}, nil
}