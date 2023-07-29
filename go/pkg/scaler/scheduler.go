package scaler

import (
	"container/list"
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/AliyunContainerService/scaler/go/pkg/config"
	model "github.com/AliyunContainerService/scaler/go/pkg/model"
	platform_client "github.com/AliyunContainerService/scaler/go/pkg/platform_client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/AliyunContainerService/scaler/proto"
	"github.com/google/uuid"
)

const (
	MAXIDLEINSTANCE = 30000 // max idle instance number
	MINIDLEINSTANCE = 1000  // min idle instance number

	THRESHOLD  = 500                     // bursty threshold
	BUSRTYTIME = 1500 * time.Millisecond // the interval time that the request type is bursty

	PRECREATE = 150 // the number of instances that can be created in advance

	REQUESTOLDTIMEOUT = 100 * time.Second // the timeout of the request that is too old
)

type Scheduler struct {
	config          *config.Config
	metaData        *model.Meta
	platformClient  platform_client.Client
	mu              sync.Mutex
	wg              sync.WaitGroup
	instances       map[string]*model.Instance
	idleInstance    *list.List
	lastRequestTime time.Time
	requestInterval time.Duration
	requestNum      int64
}

func New(metaData *model.Meta, config *config.Config) Scaler {
	client, err := platform_client.New(config.ClientAddr)
	if err != nil {
		log.Fatalf("client init with error: %s", err.Error())
	}
	scheduler := &Scheduler{
		config:          config,
		metaData:        metaData,
		platformClient:  client,
		mu:              sync.Mutex{},
		wg:              sync.WaitGroup{},
		instances:       make(map[string]*model.Instance),
		idleInstance:    list.New(),
		lastRequestTime: time.Now(),
		requestInterval: 0 * time.Millisecond,
		requestNum:      0,
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

/* check if the request type is bursty
 * if yes, we create some new idle instances to wait for the following requests
 */
func (s *Scheduler) waitFollowingRequest(ctx context.Context, request *pb.AssignRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// set a expression to determine if the request type is bursty based on the request interval and request number
	// the score of the result is higher if the request interval is shorter and the request number is larger
	// if the score is higher than the threshold, we think the request type is bursty
	busrty := false
	if s.requestInterval < BUSRTYTIME && s.requestNum > THRESHOLD {
		busrty = true
	}

	if !busrty {
		return
	}

	// create some new instances to wait for the following requests and add them to idle instance list
	for i := 0; i < PRECREATE; i++ {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			resourceConfig := &model.SlotResourceConfig{
				ResourceConfig: pb.ResourceConfig{
					MemoryInMegabytes: request.MetaData.MemoryInMb,
				},
			}
			slot, err := s.platformClient.CreateSlot(ctx, request.RequestId, resourceConfig)
			if err != nil {
				log.Printf("Assign request id: %s, init slot error: %s", request.RequestId, err.Error())
				return
			}
			meta := &model.Meta{
				Meta: pb.Meta{
					Key:           request.MetaData.Key,
					Runtime:       request.MetaData.Runtime,
					TimeoutInSecs: request.MetaData.TimeoutInSecs,
				},
			}
			instance, err := s.platformClient.Init(ctx, request.RequestId, uuid.New().String(), slot, meta)
			if err != nil {
				log.Printf("Assign request id: %s, init instance error: %s", request.RequestId, err.Error())
				return
			}
			s.mu.Lock()
			s.instances[instance.Id] = instance
			s.idleInstance.PushFront(instance)
			s.mu.Unlock()
			log.Printf("Assign request id: %s, instance %s created for wait following app %s request, init latency: %dms", request.RequestId, instance.Id, request.MetaData.Key, instance.InitDurationInMs)
		}()
	}
}

func (s *Scheduler) Assign(ctx context.Context, request *pb.AssignRequest) (*pb.AssignReply, error) {
	start := time.Now()
	instanceId := uuid.New().String()

	s.mu.Lock()
	if s.requestNum == 0 {
		s.lastRequestTime = time.Now()
	} else {
		s.requestNum++
		interval := time.Since(s.lastRequestTime)
		s.requestInterval = (s.requestInterval*time.Duration(s.requestNum-1) + interval) / time.Duration(s.requestNum)
	}
	s.mu.Unlock()

	defer func() {
		if s.idleInstance.Len() >= MAXIDLEINSTANCE {
			return
		}
		s.waitFollowingRequest(ctx, request)
	}()

	defer func() {
		log.Printf("Assign cost: %s", time.Since(start))
	}()

	log.Printf("Assign request id: %s", request.RequestId)

	s.mu.Lock()
	if s.idleInstance.Len() == 0 {
		s.mu.Unlock()
		// create new instance
		resourceConfig := &model.SlotResourceConfig{
			ResourceConfig: pb.ResourceConfig{
				MemoryInMegabytes: request.MetaData.MemoryInMb,
			},
		}
		s.mu.Lock()
		if s.idleInstance.Len() > 0 {
			goto use_idle_instance
		}
		s.mu.Unlock()
		slot, err := s.platformClient.CreateSlot(ctx, request.RequestId, resourceConfig)
		if err != nil {
			errorMessage := fmt.Sprintf("Assign request id: %s, create slot error: %s", request.RequestId, err.Error())
			log.Printf(errorMessage)
			return nil, status.Error(codes.Internal, errorMessage)
		}
		meta := &model.Meta{
			Meta: pb.Meta{
				Key:           request.MetaData.Key,
				Runtime:       request.MetaData.Runtime,
				TimeoutInSecs: request.MetaData.TimeoutInSecs,
			},
		}
		s.mu.Lock()
		if s.idleInstance.Len() > 0 {
			go func() {
				s.deleteSlot(ctx, request.RequestId, slot.Id, instanceId, request.MetaData.Key, "before initializing instance find idle instance")
			}()
			goto use_idle_instance
		}
		s.mu.Unlock()
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
		log.Printf("Assign request id: %s, no idle instance", request.RequestId)

		return &pb.AssignReply{
			Status: pb.Status_Ok,
			Assigment: &pb.Assignment{
				RequestId:  request.RequestId,
				MetaKey:    instance.Meta.Key,
				InstanceId: instance.Id,
			},
			ErrorMessage: nil,
		}, nil
	}

use_idle_instance:
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
			Status: pb.Status_Ok,
			Assigment: &pb.Assignment{
				RequestId:  request.RequestId,
				MetaKey:    instance.Meta.Key,
				InstanceId: instanceId,
			},
			ErrorMessage: nil,
		}, nil
	}
	// s.mu.Unlock()

	return nil, nil
}

func (s *Scheduler) Idle(ctx context.Context, request *pb.IdleRequest) (*pb.IdleReply, error) {
	if request.Assigment == nil {
		return nil, status.Error(codes.InvalidArgument, "assignment is nil")
	}
	reply := &pb.IdleReply{
		Status:       pb.Status_Ok,
		ErrorMessage: nil,
	}
	start := time.Now()
	instanceId := request.Assigment.InstanceId
	defer func() {
		log.Printf("Idle request id: %s, instance id: %s, cost: %dus", request.Assigment.RequestId, instanceId, time.Since(start).Microseconds())
	}()
	needDestroy := false
	slotId := ""
	if request.Result != nil && request.Result.NeedDestroy != nil && *request.Result.NeedDestroy {
		needDestroy = true
	}
	defer func() {
		if needDestroy {
			s.deleteSlot(ctx, request.Assigment.RequestId, slotId, instanceId, request.Assigment.MetaKey, "bad instance")
		}
	}()
	log.Printf("Idle request id:%s", request.Assigment.RequestId)

	s.mu.Lock()
	defer s.mu.Unlock()

	instance := s.instances[instanceId]
	if instance == nil {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("request id %s, instance %s not found", request.Assigment.RequestId, instanceId))
	}

	slotId = instance.Slot.Id
	instance.LastIdleTime = time.Now()
	if needDestroy {
		log.Printf("request id %s needs to destroy instance %s", request.Assigment.RequestId, instanceId)
		return reply, nil
	}

	if instance.Busy == false {
		log.Printf("request id %s instance %s is already freed", request.Assigment.RequestId, instanceId)
		return reply, nil
	}
	instance.Busy = false
	s.idleInstance.PushFront(instance)
	return reply, nil
}

func (s *Scheduler) deleteSlot(ctx context.Context, requestId, slotId, instanceId, metaKey, reason string) {
	if slotId == "" {
		return
	}
	log.Printf("request id %s, delete slot %s, instance %s, reason: %s", requestId, slotId, instanceId, reason)
	err := s.platformClient.DestroySLot(ctx, requestId, slotId, reason)
	if err != nil {
		log.Printf("request id %s, delete instance %s (Slot %s) for app %s error: %s", requestId, instanceId, slotId, metaKey, err.Error())
	}
}

func (s *Scheduler) gcLoop() {
	log.Printf("gc loop for app: %s is started", s.metaData.Key)
	ticker := time.NewTicker(s.config.GcInterval)

	for range ticker.C {
		for {
			s.mu.Lock()
			// check if last request is too old delete all idle instances
			if time.Since(s.lastRequestTime) > REQUESTOLDTIMEOUT {
				for element := s.idleInstance.Back(); element != nil; element = s.idleInstance.Back() {
					instance := element.Value.(*model.Instance)
					s.idleInstance.Remove(element)
					delete(s.instances, instance.Id)
					go func() {
						reason := fmt.Sprintf("idle duration %fs exceeds threshold %fs", time.Since(instance.LastIdleTime).Seconds(), REQUESTOLDTIMEOUT.Seconds())
						ctx := context.Background()
						ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
						defer cancel()
						s.deleteSlot(ctx, uuid.NewString(), instance.Slot.Id, instance.Id, instance.Meta.Key, reason)
					}()
				}
				break
			}

			if element := s.idleInstance.Back(); element != nil {
				instance := element.Value.(*model.Instance)
				idleDuration := time.Since(instance.LastIdleTime)
				if idleDuration > s.config.IdleDurationBeforeGC {
					// start to remove idle instance
					s.idleInstance.Remove(element)
					delete(s.instances, instance.Id)
					s.mu.Unlock()
					go func() {
						reason := fmt.Sprintf("idle duration %fs exceeds threshold %fs", idleDuration.Seconds(), s.config.IdleDurationBeforeGC.Seconds())
						ctx := context.Background()
						ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
						defer cancel()
						s.deleteSlot(ctx, uuid.NewString(), instance.Slot.Id, instance.Id, instance.Meta.Key, reason)
					}()
					continue
				}
			}
			s.mu.Unlock()
			break
		}
	}
}

func (s *Scheduler) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Stats{
		TotalInstance:     len(s.instances),
		TotalIdleInstance: s.idleInstance.Len(),
	}
}
