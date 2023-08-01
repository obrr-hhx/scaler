package scaler

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
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

	THRESHOLD  = 500  // bursty threshold
	BUSRTYTIME = 2000 // the interval time that the request type is bursty

	PRECREATE = 15 // the number of instances that can be created in advance

	REQUESTOLDTIMEOUT = 100 * time.Second // the timeout of the request that is too old
)

var InitTime uint64 = 16000 // the time of initialization for each instance
var InitNum uint64 = 0

type Scheduler struct {
	config          *config.Config
	metaData        *model.Meta
	platformClient  platform_client.Client
	mu              sync.Mutex
	wg              sync.WaitGroup
	instances       map[string]*model.Instance
	idleInstance    *list.List
	idleSlot        *list.List
	deleteAll       bool
	lastRequestTime uint64
	requestInterval uint64
	requestNum      uint64
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
		idleSlot:        list.New(),
		deleteAll:       bool(false),
		lastRequestTime: 0,
		requestInterval: 0,
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

	if s.idleInstance.Len() > MAXIDLEINSTANCE {
		return
	}

	interval_ms := s.requestInterval
	if s.idleInstance.Len() > MAXIDLEINSTANCE {
		// too many idle instances, we don't need to create new instances
		return
	}

	busrty := false
	if interval_ms < BUSRTYTIME && s.requestNum > THRESHOLD {
		busrty = true
	}
	if !busrty && s.requestNum > THRESHOLD {
		if interval_ms <= InitTime*2 {
			s.deleteAll = true
			return // the request interval is too small, we cannot create new instances, but we need delete all instances when them idle
		}
		go func() {
			// wait the interval time to new a instance
			time.Sleep(time.Duration(interval_ms-InitNum*2) * time.Millisecond)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(request.MetaData.TimeoutInSecs)*time.Second)
			defer cancel()
			resourceConfig := &model.SlotResourceConfig{
				ResourceConfig: pb.ResourceConfig{
					MemoryInMegabytes: request.MetaData.MemoryInMb,
				},
			}

			var slot *model.Slot
			if s.idleSlot.Len() > 0 {
				slot = s.idleSlot.Front().Value.(*model.Slot)
			} else {
				var err error
				slot, err = s.platformClient.CreateSlot(ctx, request.RequestId, resourceConfig)
				if err != nil {
					log.Printf("[Sparse Bursty] Assign request id: %s, init slot error: %s", request.RequestId, err.Error())
					return
				}
			}

			meta := &model.Meta{
				Meta: pb.Meta{
					Key:           request.MetaData.Key,
					Runtime:       request.MetaData.Runtime,
					TimeoutInSecs: request.MetaData.TimeoutInSecs,
				},
			}

			instance, err := s.platformClient.Init(ctx, request.RequestId, uuid.New().String(), slot, meta)
			InitNum++
			InitTime = (InitTime*(InitNum-1) + uint64(instance.InitDurationInMs)) / InitNum
			if err != nil {
				log.Printf("[Sparse Bursty] Assign request id: %s, init instance error: %s", request.RequestId, err.Error())
				return
			}

			s.mu.Lock()
			s.instances[instance.Id] = instance
			s.idleInstance.PushFront(instance)
			s.mu.Unlock()
			log.Printf("[Sparse Bursty] Assign request id: %s, instance %s created for wait following app %s request, init latency: %dms, idle instances num: %d", request.RequestId, instance.Id, request.MetaData.Key, instance.InitDurationInMs, s.idleInstance.Len())

		}()
	}

	if !busrty {
		s.deleteAll = true
		return
	}

	// create some new instances to wait for the following requests and add them to idle instance list

	for i := 0; i < PRECREATE; i++ {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(request.MetaData.TimeoutInSecs)*time.Second)
			defer cancel()
			resourceConfig := &model.SlotResourceConfig{
				ResourceConfig: pb.ResourceConfig{
					MemoryInMegabytes: request.MetaData.MemoryInMb,
				},
			}
			// slot, err := s.platformClient.CreateSlot(ctx, request.RequestId, resourceConfig)

			// if err != nil {
			// log.Printf("[Bursty] Assign request id: %s, init slot error: %s", request.RequestId, err.Error())
			// return
			// }
			var slot *model.Slot
			if s.idleSlot.Len() > 0 {
				slot = s.idleSlot.Front().Value.(*model.Slot)
			} else {
				var err error
				slot, err = s.platformClient.CreateSlot(ctx, request.RequestId, resourceConfig)
				if err != nil {
					log.Printf("[Sparse Bursty] Assign request id: %s, init slot error: %s", request.RequestId, err.Error())
					return
				}
			}

			meta := &model.Meta{
				Meta: pb.Meta{
					Key:           request.MetaData.Key,
					Runtime:       request.MetaData.Runtime,
					TimeoutInSecs: request.MetaData.TimeoutInSecs,
				},
			}
			instance, err := s.platformClient.Init(ctx, request.RequestId, uuid.New().String(), slot, meta)
			InitNum++
			InitTime = (InitTime*(InitNum-1) + uint64(instance.InitDurationInMs)) / InitNum
			if err != nil {
				log.Printf("[Bursty] Assign request id: %s, init instance error: %s", request.RequestId, err.Error())
				return
			}
			s.mu.Lock()
			s.instances[instance.Id] = instance
			s.idleInstance.PushFront(instance)
			s.mu.Unlock()
			log.Printf("[Bursty] Assign request id %s app %s, instance %s created for wait following , init latency: %dms, idle instances num: %d", request.RequestId, request.MetaData.Key, instance.Id, instance.InitDurationInMs, s.idleInstance.Len())
		}()
	}
}

func (s *Scheduler) idleUse(request *pb.AssignRequest) (*pb.AssignReply, error) {
	if element := s.idleInstance.Front(); element != nil {
		instance := element.Value.(*model.Instance)
		instance.Busy = true
		s.idleInstance.Remove(element)
		log.Printf("[Idle Instance] Assign, request id: %s, type %s, instance %s reused, request interval %dms, num %d", request.RequestId, request.MetaData.Key, instance.Id, s.requestInterval, s.requestNum)
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
	idle_use := false
	s.mu.Lock()
	if s.requestNum == 0 {
		s.lastRequestTime = request.Timestamp
	} else {
		interval := request.Timestamp - s.lastRequestTime
		// calculate the average request interval
		s.requestInterval = (s.requestInterval*(s.requestNum-1) + interval) / s.requestNum
	}
	s.requestNum++
	s.mu.Unlock()

	defer func() {
		if !idle_use {
			return
		}
		go s.waitFollowingRequest(ctx, request)
	}()

	defer func() {
		log.Printf("Assign cost: %s", time.Since(start))
	}()

	log.Printf("Assign request id: %s", request.RequestId)

	resourceConfig := &model.SlotResourceConfig{
		ResourceConfig: pb.ResourceConfig{
			MemoryInMegabytes: request.MetaData.MemoryInMb,
		},
	}
	meta := &model.Meta{
		Meta: pb.Meta{
			Key:           request.MetaData.Key,
			Runtime:       request.MetaData.Runtime,
			TimeoutInSecs: request.MetaData.TimeoutInSecs,
		},
	}

	// use idle instance if have idle instance
	s.mu.Lock()
	if replyIdle, err := s.idleUse(request); err == nil {
		idle_use = true
		return replyIdle, err
	}
	s.mu.Unlock()

	s.mu.Lock()
	var slot *model.Slot
	if is := s.idleSlot.Front(); is != nil {
		// use idle slot
		slot = is.Value.(*model.Slot)
		s.idleSlot.Remove(is)
		s.mu.Unlock()
	} else {
		s.mu.Unlock()
		// create new slot
		var err error
		slot, err = s.platformClient.CreateSlot(ctx, request.RequestId, resourceConfig)
		if err != nil {
			errorMessage := fmt.Sprintf("Assign request id: %s, create slot error: %s", request.RequestId, err.Error())
			log.Printf(errorMessage)
			return nil, status.Error(codes.Internal, errorMessage)
		}

		s.mu.Lock()
		if s.idleInstance.Len() > 0 {
			s.idleSlot.PushFront(slot)
			log.Printf("[Back Slot]Assign request id: %s, type %s, back slot %s, idle slot num: %d", request.RequestId, request.MetaData.Key, slot.Id, s.idleSlot.Len())
			if replyIdle, err := s.idleUse(request); err == nil {
				idle_use = true
				s.mu.Unlock()
				return replyIdle, err
			}
		}
		s.mu.Unlock()
	}

	instance, err := s.platformClient.Init(ctx, request.RequestId, instanceId, slot, meta)
	if err != nil {
		errorMessage := fmt.Sprintf("Assign request id: %s, init instance error: %s", request.RequestId, err.Error())
		log.Printf(errorMessage)
		return nil, status.Error(codes.Internal, errorMessage)
	}

	InitNum++
	InitTime = (InitTime*(InitNum-1) + uint64(instance.InitDurationInMs)) / InitNum

	// add instance to instances map
	s.mu.Lock()
	instance.Busy = true
	s.instances[instance.Id] = instance
	s.mu.Unlock()
	log.Printf("[No Idle Instance] Assign request id: %s, request interval: %dms Num: %d, instance %s created for app %s, init latency: %dms", request.RequestId, s.requestInterval, s.requestNum, instance.Id, request.MetaData.Key, instance.InitDurationInMs)

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
	// log.Printf("Idle request id:%s", request.Assigment.RequestId)

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

	if s.idleInstance.Len() >= MAXIDLEINSTANCE {
		log.Printf("request id %s, idle instances is up to max, destroy instance %s", request.Assigment.RequestId, instanceId)
		needDestroy = true
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

func (s *Scheduler) gcLoop() {
	log.Printf("gc loop for app: %s is started", s.metaData.Key)
	ticker := time.NewTicker(s.config.GcInterval)

	count := 0
	var lastRequestNum uint64 = s.requestNum
	for range ticker.C {
		count++
		for {
			s.mu.Lock()
			if s.deleteAll {
				// prevent high source usage from bursty but little requests
				reason := fmt.Sprintf("delete all idle instances for app %s", s.metaData.Key)
				for element := s.idleInstance.Front(); element != nil; element = element.Next() {
					instance := element.Value.(*model.Instance)
					s.idleInstance.Remove(element)
					delete(s.instances, instance.Id)
					go func() {
						defer s.wg.Done()
						ctx := context.Background()
						ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
						defer cancel()
						s.deleteSlot(ctx, uuid.NewString(), instance.Slot.Id, instance.Id, instance.Meta.Key, reason)
					}()
				}
				s.deleteAll = false
			}
			// check if last request is too old delete all idle instances
			if count == 10 {
				count = 0
				// set a random number between 1 and idleInstance.Len() as the number of idle instances to be deleted
				var num int = s.idleInstance.Len()
				if s.idleInstance.Len() > 0 {
					// cannot make num equal to idleInstance.Len(), do not delete all idle instances
					for num == s.idleInstance.Len() {
						// set random seed
						// rand.Seed(time.Now().UnixNano())
						rand.Seed(time.Now().UnixNano())
						num = rand.Intn(s.idleInstance.Len()) + 1
					}
				}
				if s.requestNum == lastRequestNum && s.idleInstance.Len() > 0 && num != s.idleInstance.Len() {
					fmt.Printf("request type %s num is not changed, delete %d idle instances, and back them slot\n", s.metaData.Key, num)
					for element := s.idleInstance.Front(); element != nil; element = element.Next() {
						if num == 0 {
							break
						}
						instance := element.Value.(*model.Instance)
						s.idleInstance.Remove(element)
						delete(s.instances, instance.Id)

						slot := instance.Slot
						s.idleSlot.PushFront(slot)

						num--
					}
					break
				}
				lastRequestNum = s.requestNum
			}

			for element := s.idleInstance.Front(); element != nil; element = element.Next() {
				instance := element.Value.(*model.Instance)
				idleDuration := time.Since(instance.LastIdleTime)
				if idleDuration > s.config.IdleDurationBeforeGC {
					// start to remove idle instance
					s.idleInstance.Remove(element)
					delete(s.instances, instance.Id)
					// s.mu.Unlock()
					s.wg.Add(1)
					go func() {
						defer s.wg.Done()
						reason := fmt.Sprintf("idle duration %fs exceeds threshold %fs", idleDuration.Seconds(), s.config.IdleDurationBeforeGC.Seconds())
						ctx := context.Background()
						ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
						defer cancel()
						s.deleteSlot(ctx, uuid.NewString(), instance.Slot.Id, instance.Id, instance.Meta.Key, reason)
					}()
				} else {
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
