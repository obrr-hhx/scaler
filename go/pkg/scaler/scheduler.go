package scaler

import (
	"container/list"
	"context"
	"errors"
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

type Scheduler struct {
	config            *config.Config
	metaData          *model.Meta
	platformClient    platform_client.Client
	mu                sync.Mutex
	wg                sync.WaitGroup
	instances         map[string]*model.Instance
	idleInstance      *list.List
	pre_warm_window   int64
	keep_alive_window int64
}

func New(metaData *model.Meta, config *config.Config) Scaler {
	client, err := platform_client.New(config.ClientAddr)
	if err != nil {
		log.Fatalf("client init with error: %s", err.Error())
	}

	scheduler := &Scheduler{
		config:            config,
		metaData:          metaData,
		platformClient:    client,
		mu:                sync.Mutex{},
		wg:                sync.WaitGroup{},
		instances:         make(map[string]*model.Instance),
		idleInstance:      list.New(),
		pre_warm_window:   int64(PolicyMap[metaData.Key]["pre_warm_window"]),
		keep_alive_window: int64(PolicyMap[metaData.Key]["keep_alive_window"]),
	}

	log.Printf("New scaler for app: %s is created", metaData.Key)
	scheduler.wg.Add(1)
	go func() {
		defer scheduler.wg.Done()
		// scheduler.gcLoop()
		scheduler.policyLoop()
		log.Printf("gc loop for app: %s is started", metaData.Key)
	}()
	return scheduler
}

func (s *Scheduler) idleUseInstance(request *pb.AssignRequest) (*pb.AssignReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if element := s.idleInstance.Front(); element != nil {
		instance := element.Value.(*model.Instance)
		instance.Busy = true
		s.idleInstance.Remove(element)
		// log.Printf("[Idle Instance] Assign, request id: %s, type %s, instance %s reused, request interval %dms, num %d", request.RequestId, request.MetaData.Key, instance.Id, s.requestInterval, s.requestNum)
		instanceId := instance.Id
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
	return nil, errors.New("no idle instance")
}

func (s *Scheduler) Assign(ctx context.Context, request *pb.AssignRequest) (*pb.AssignReply, error) {
	start := time.Now()
	instanceId := uuid.New().String()

	defer func() {
		log.Printf("Assign cost: %s", time.Since(start))
	}()

	log.Printf("Assign request id: %s", request.RequestId)

	// check if there is idle instance
	if reply, err := s.idleUseInstance(request); err == nil {
		return reply, nil
	}

	// create new instance
	resourceConfig := &model.SlotResourceConfig{
		ResourceConfig: pb.ResourceConfig{
			MemoryInMegabytes: request.MetaData.MemoryInMb,
		},
	}
	create_start := time.Now().UnixMilli()
	slot, err := s.platformClient.CreateSlot(ctx, request.RequestId, resourceConfig)
	create_end := time.Now().UnixMilli()
	create_slot := create_end - create_start
	log.Printf("create slot %s cost: %dms for memory size: %dMb", slot.Id, create_slot, request.MetaData.MemoryInMb)
	if err != nil {
		errorMessage := fmt.Sprintf("create slot failed with: %s", err.Error())
		log.Printf(errorMessage)
		return nil, status.Errorf(codes.Internal, errorMessage)
	}

	if reply, err := s.idleUseInstance(request); err == nil {
		go func() {
			reason := "before init find idle instance"
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(request.MetaData.TimeoutInSecs)*time.Second)
			defer cancel()
			s.deleteSlot(ctx, request.RequestId, slot.Id, " ", request.MetaData.Key, reason)
		}()
		return reply, nil
	}

	meta := &model.Meta{
		Meta: pb.Meta{
			Key:           request.MetaData.Key,
			Runtime:       request.MetaData.Runtime,
			TimeoutInSecs: request.MetaData.TimeoutInSecs,
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
			RequestId:  request.RequestId,
			MetaKey:    instance.Meta.Key,
			InstanceId: instance.Id,
		},
		ErrorMessage: nil,
	}, nil

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
		log.Printf("Idle request id: %s, type: %s, instance id: %s, cost: %dus, now idle instance num: %d", request.Assigment.RequestId, s.metaData.Key, instanceId, time.Since(start).Microseconds(), s.idleInstance.Len())
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

	if s.keep_alive_window == 0 {
		needDestroy = true
	}

	if needDestroy {
		log.Printf("request id %s needs to destroy instance %s", request.Assigment.RequestId, instanceId)
		return reply, nil
	}

	if !instance.Busy {
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

// func (s *Scheduler) gcLoop() {
// 	log.Printf("gc loop for app: %s is started", s.metaData.Key)
// 	ticker := time.NewTicker(s.config.GcInterval)

// 	count := 0
// 	var lastRequestNum uint64 = s.requestNum
// 	for range ticker.C {
// 		count++
// 		for {
// 			s.mu.Lock()
// 			if s.deleteAll {
// 				// prevent high source usage from bursty but little requests
// 				reason := fmt.Sprintf("delete all idle instances for app %s", s.metaData.Key)
// 				for element := s.idleInstance.Front(); element != nil; element = element.Next() {
// 					instance := element.Value.(*model.Instance)
// 					s.idleInstance.Remove(element)
// 					delete(s.instances, instance.Id)
// 					go func() {
// 						defer s.wg.Done()
// 						ctx := context.Background()
// 						ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
// 						defer cancel()
// 						s.deleteSlot(ctx, uuid.NewString(), instance.Slot.Id, instance.Id, instance.Meta.Key, reason)
// 					}()
// 				}
// 				s.deleteAll = false
// 			}
// 			// check if last request is too old delete all idle instances
// 			if count == 50 {
// 				count = 0
// 				// set a random number between 1 and idleInstance.Len() as the number of idle instances to be deleted
// 				var num int = s.idleInstance.Len()
// 				if s.idleInstance.Len() > 0 {
// 					// cannot make num equal to idleInstance.Len(), do not delete all idle instances
// 					for num == s.idleInstance.Len() {
// 						// set random seed
// 						// rand.Seed(time.Now().UnixNano())
// 						rand.Seed(time.Now().UnixNano())
// 						num = rand.Intn(s.idleInstance.Len()) + 1
// 					}
// 				}
// 				if s.requestNum == lastRequestNum && s.idleInstance.Len() > 0 && num != s.idleInstance.Len() {
// 					// fmt.Printf("request type %s num is not changed, delete %d idle instances\n", s.metaData.Key, num)
// 					for element := s.idleInstance.Front(); element != nil; element = element.Next() {
// 						if num == 0 {
// 							break
// 						}
// 						instance := element.Value.(*model.Instance)
// 						s.idleInstance.Remove(element)
// 						delete(s.instances, instance.Id)

// 						s.wg.Add(1)
// 						go func() {
// 							defer s.wg.Done()
// 							ctx := context.Background()
// 							ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
// 							defer cancel()
// 							s.deleteSlot(ctx, uuid.NewString(), instance.Slot.Id, instance.Id, instance.Meta.Key, "pick random num to delete idle instances")
// 						}()
// 						num--
// 					}
// 					break
// 				}
// 				lastRequestNum = s.requestNum
// 			}

// 			for element := s.idleInstance.Front(); element != nil; element = element.Next() {
// 				instance := element.Value.(*model.Instance)
// 				idleDuration := time.Since(instance.LastIdleTime)
// 				if idleDuration > s.config.IdleDurationBeforeGC {
// 					// start to remove idle instance
// 					s.idleInstance.Remove(element)
// 					delete(s.instances, instance.Id)
// 					// s.mu.Unlock()
// 					s.wg.Add(1)
// 					go func() {
// 						defer s.wg.Done()
// 						reason := fmt.Sprintf("idle duration %fs exceeds threshold %fs", idleDuration.Seconds(), s.config.IdleDurationBeforeGC.Seconds())
// 						ctx := context.Background()
// 						ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
// 						defer cancel()
// 						s.deleteSlot(ctx, uuid.NewString(), instance.Slot.Id, instance.Id, instance.Meta.Key, reason)
// 					}()
// 				} else {
// 					continue
// 				}
// 			}
// 			s.mu.Unlock()
// 			break
// 		}
// 	}
// }

func (s *Scheduler) policyLoop() {
	log.Printf("gc loop for app: %s is started", s.metaData.Key)
	ticker := time.NewTicker(s.config.GcInterval)
	for range ticker.C {
		for {
			s.mu.Lock()
			if element := s.idleInstance.Back(); element != nil {
				instance := element.Value.(*model.Instance)
				idleDuration := time.Since(instance.LastIdleTime).Milliseconds()
				if idleDuration >= s.keep_alive_window {
					// start to remove idle instance
					s.idleInstance.Remove(element)
					delete(s.instances, instance.Id)
					s.mu.Unlock()
					s.wg.Add(1)
					go func() {
						defer s.wg.Done()
						reason := fmt.Sprintf("idle duration %dms exceeds threshold %dms", idleDuration, s.keep_alive_window)
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
