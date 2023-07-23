package scaler

import (
	"container/list"
	"context"
	"fmt"
	"log"
	"sort"
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
	// max idle instance number
	MAXIDLEINSTANCE = 300000
	// the number of request types that can be cached
	THRESHOLDCACHE = 100
	// the number of request type's instance can be destroyed
	THRESHOLDDESTROY = 10
	// the number of instances that can be created in advance
	PRECREATE = 10
	// the number of request access to add
	ACCESS_SCORE = 1
	// the number of request access to subtract
	SUBSCORE = 10
	// the number of gcloop need waiting intervals to delete cache and wingman list
	DELETECACHEINTERVALS = 2
)

// store the request type and life time
// the life time is used to determine how long the request can be cached
// in other words, the lifetime can present if the specific request type is bursty
// if the request type is bursty, we do not free the instance immediately and create some new instances to wait for the following requests
type RequestCache struct {
	requestType string
	lifeTime    int64
}

type Scheduler struct {
	config               *config.Config
	metaData             *model.Meta
	platformClient       platform_client.Client
	mu                   sync.Mutex
	wg                   sync.WaitGroup
	instances            map[string]*model.Instance
	idleInstance         *list.List
	requestCaches        [100]*RequestCache
	requestCachesWingman *list.List
}

func New(metaData *model.Meta, config *config.Config) Scaler {
	client, err := platform_client.New(config.ClientAddr)
	if err != nil {
		log.Fatalf("client init with error: %s", err.Error())
	}
	scheduler := &Scheduler{
		config:               config,
		metaData:             metaData,
		platformClient:       client,
		mu:                   sync.Mutex{},
		wg:                   sync.WaitGroup{},
		instances:            make(map[string]*model.Instance),
		idleInstance:         list.New(),
		requestCaches:        [100]*RequestCache{},
		requestCachesWingman: list.New(),
	}
	for i := 0; i < len(scheduler.requestCaches); i++ {
		scheduler.requestCaches[i] = nil
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

func (s *Scheduler) flushRequestCache(request *pb.AssignRequest) {
	// If the request type is not in the wingman list, we add it to the wingman list.
	// If the request type is in the wingman list, we add it's life time and add it to cache
	requestType := request.MetaData.Key

	// search the request type in the cache array firstly
	for i := 0; i < len(s.requestCaches); i++ {
		if s.requestCaches[i] != nil && s.requestCaches[i].requestType == requestType {
			s.requestCaches[i].lifeTime += ACCESS_SCORE
			return
		}
	}

	// search the request type in the wingman list if the request type is not in the cache array
	for e := s.requestCachesWingman.Front(); e != nil; e = e.Next() {
		requestCache := e.Value.(*RequestCache)
		if requestCache.requestType == requestType {
			requestCache.lifeTime += ACCESS_SCORE
			if requestCache.lifeTime < THRESHOLDCACHE {
				return
			}
			var evict bool = true
			for i := 0; i < len(s.requestCaches); i++ {
				if s.requestCaches[i] == nil {
					s.requestCaches[i] = requestCache
					evict = false
					s.requestCachesWingman.Remove(e)
					break
				}
			}
			if evict {
				sort.Slice(s.requestCaches[:], func(i, j int) bool {
					return s.requestCaches[i].lifeTime > s.requestCaches[j].lifeTime
				})
				evictRequestCache := s.requestCaches[len(s.requestCaches)-1]
				s.requestCachesWingman.PushFront(evictRequestCache)
				for i := 0; i < len(s.requestCaches); i++ {
					if requestCache.lifeTime > s.requestCaches[i].lifeTime {
						// insert the request cache to the cache list and rearrange the cache list
						for j := len(s.requestCaches) - 1; j > i; j-- {
							s.requestCaches[j] = s.requestCaches[j-1]
						}
						s.requestCaches[i] = requestCache
					}
				}
			}
			return
		}
	}
	// Add the request type to the wingman list
	s.requestCachesWingman.PushFront(&RequestCache{
		requestType: requestType,
		lifeTime:    ACCESS_SCORE,
	})
}

/* check if the request type is bursty
 * if yes, we create some new idle instances to wait for the following requests
 */
func (s *Scheduler) waitFollowingRequest(ctx context.Context, request *pb.AssignRequest) {
	s.mu.Lock()
	ok := func() bool {
		for i := 0; i < len(s.requestCaches); i++ {
			if s.requestCaches[i] != nil && s.requestCaches[i].requestType == request.MetaData.Key {
				return true
			}
		}
		return false
	}()
	s.mu.Unlock()

	if !ok {
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
				errorMessage := fmt.Sprintf("Assign request id: %s, create slot error: %s", request.RequestId, err.Error())
				log.Printf(errorMessage)
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
				errorMessage := fmt.Sprintf("Assign request id: %s, init instance error: %s", request.RequestId, err.Error())
				log.Printf(errorMessage)
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
	s.flushRequestCache(request)
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
	for s.idleInstance.Len() == 0 {
		s.mu.Unlock()
		// create new instance
		resourceConfig := &model.SlotResourceConfig{
			ResourceConfig: pb.ResourceConfig{
				MemoryInMegabytes: request.MetaData.MemoryInMb,
			},
		}
		s.mu.Lock()
		if s.idleInstance.Len() > 0 {
			break
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
			break
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
	s.mu.Unlock()

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
	// if the request type in wingman list and life time is less than THRESHOLDDESTROY, we free the instance
	if needDestroy = func() bool {
		for e := s.requestCachesWingman.Front(); e != nil; e = e.Next() {
			requestCache := e.Value.(*RequestCache)
			if requestCache.requestType == request.Assigment.MetaKey && requestCache.lifeTime < THRESHOLDDESTROY {
				return true
			}
		}
		return false
	}(); needDestroy {
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

/* decrement request cache and wingman list life time
 * if the life time in cache array less than THRESHOLDCACHE, we remove it from cache array to wingman list
 * if the life time in wingman list equals to 0, we remove it from wingman list
 */
func (s *Scheduler) deleteCache() {
	for i := 0; i < len(s.requestCaches); i++ {
		if s.requestCaches[i] != nil {
			s.requestCaches[i].lifeTime = s.requestCaches[i].lifeTime - SUBSCORE
			if s.requestCaches[i].lifeTime < THRESHOLDCACHE && s.requestCaches[i].lifeTime > 0 {
				s.requestCachesWingman.PushFront(s.requestCaches[i])
			}
			s.requestCaches[i] = nil
		}
	}
	for e := s.requestCachesWingman.Front(); e != nil; e = e.Next() {
		requestCache := e.Value.(*RequestCache)
		requestCache.lifeTime = requestCache.lifeTime - SUBSCORE
		if requestCache.lifeTime <= 0 {
			s.requestCachesWingman.Remove(e)
		}
	}
}

func (s *Scheduler) gcLoop() {
	log.Printf("gc loop for app: %s is started", s.metaData.Key)
	ticker := time.NewTicker(s.config.GcInterval)
	var cacheCount int = 0
	for range ticker.C {
		cacheCount++
		for {
			s.mu.Lock()

			if cacheCount == DELETECACHEINTERVALS {
				cacheCount = 0
				s.deleteCache()
			}
			if element := s.idleInstance.Back(); element != nil {
				instance := element.Value.(*model.Instance)
				idleDuration := time.Now().Sub(instance.LastIdleTime)
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
