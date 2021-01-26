package pubsub

type operation int

const (
	sub operation = iota
	subOnce
	subOnceEach
	pub
	tryPub
	unsub
	unsubAll
	closeTopic
	shutdown
)

// PubSub struct with a collection of topics
type PubSub struct {
	eventChan chan event
	capacity  int
}

type event struct {
	op     operation
	topics []string
	ch     chan interface{}
	msg    interface{}
}

// New creates a new PubSub and starts a goroutine for handling operations
func New(capacity int) *PubSub {
	ps := &PubSub{make(chan event), capacity}
	go ps.start()
	return ps
}

// Sub returns a channel on which messages published on any of the specified topics can be received.
func (ps *PubSub) Sub(topics ...string) chan interface{} {
	return ps.sub(sub, topics...)
}

// SubOnce is similar to Sub, but only the first message published, after subscription,
// on any of the specified topics can be received.
func (ps *PubSub) SubOnce(topics ...string) chan interface{} {
	return ps.sub(subOnce, topics...)
}

// SubOnceEach returns a channel on which callers receive, at most, one message for each topic.
func (ps *PubSub) SubOnceEach(topics ...string) chan interface{} {
	return ps.sub(subOnceEach, topics...)
}

// AddSub adds subscriptions to an existing channel.
func (ps *PubSub) AddSub(ch chan interface{}, topics ...string) {
	ps.eventChan <- event{op: sub, topics: topics, ch: ch}
}

// AddSubOnceEach adds subscriptions to an existing channel with SubOnceEach behavior.
func (ps *PubSub) AddSubOnceEach(ch chan interface{}, topics ...string) {
	ps.eventChan <- event{op: subOnceEach, topics: topics, ch: ch}
}

// Pub publishes the given message to all subscribers of the specified topics.
func (ps *PubSub) Pub(msg interface{}, topics ...string) {
	ps.eventChan <- event{op: pub, topics: topics, msg: msg}
}

// TryPub publishes the given message to all subscribers of the specified topics if the topic has buffer space.
func (ps *PubSub) TryPub(msg interface{}, topics ...string) {
	ps.eventChan <- event{op: tryPub, topics: topics, msg: msg}
}

// Unsub unsubscribes the given channel from the specified topics. If no topic is specified, it is unsubscribed from all topics.
// Unsub must be called from a goroutine that is different from the subscriber.
// The subscriber must consume messages from the channel until it reaches the end. Not doing so can result in a deadlock. (todo: improve it)
func (ps *PubSub) Unsub(ch chan interface{}, topics ...string) {
	if len(topics) == 0 {
		ps.eventChan <- event{op: unsubAll, ch: ch}
	} else {
		ps.eventChan <- event{op: unsub, topics: topics, ch: ch}
	}
}

// Close closes all channels currently subscribed to the specified topics.
// If a channel is subscribed to multiple topics, some of which is not specified, it is not closed.
func (ps *PubSub) Close(topics ...string) {
	ps.eventChan <- event{op: closeTopic, topics: topics}
}

// Shutdown closes all subscribed channels and terminates the goroutine.
func (ps *PubSub) Shutdown() {
	ps.eventChan <- event{op: shutdown}
}

func (ps *PubSub) sub(op operation, topics ...string) chan interface{} {
	ch := make(chan interface{}, ps.capacity)
	ps.eventChan <- event{op: op, topics: topics, ch: ch}
	return ch
}

func (ps *PubSub) start() {
	reg := registry{
		topics:    make(map[string]map[chan interface{}]subType),
		revTopics: make(map[chan interface{}]map[string]bool),
	}

loop:
	for event := range ps.eventChan {
		if event.topics == nil {
			switch event.op {
			case unsubAll:
				reg.removeChannel(event.ch)
			case shutdown:
				break loop
			}
			continue loop
		}

		for _, topic := range event.topics {
			switch event.op {
			case sub:
				reg.add(topic, event.ch, normal)
			case subOnce:
				reg.add(topic, event.ch, onceAny)
			case subOnceEach:
				reg.add(topic, event.ch, onceEach)
			case tryPub:
				reg.sendNoWait(topic, event.msg)
			case pub:
				reg.send(topic, event.msg)
			case unsub:
				reg.remove(topic, event.ch)
			case closeTopic:
				reg.removeTopic(topic)
			}
		}
	}

	for topic, chans := range reg.topics {
		for ch := range chans {
			reg.remove(topic, ch)
		}
	}
}

// registry maintains the current subscription state. It's not safe to access a registry from multiple goroutines.
type registry struct {
	topics    map[string]map[chan interface{}]subType
	revTopics map[chan interface{}]map[string]bool
}

type subType int

const (
	onceAny subType = iota
	onceEach
	normal
)

func (reg *registry) add(topic string, ch chan interface{}, st subType) {
	if reg.topics[topic] == nil {
		reg.topics[topic] = make(map[chan interface{}]subType)
	}
	reg.topics[topic][ch] = st

	if reg.revTopics[ch] == nil {
		reg.revTopics[ch] = make(map[string]bool)
	}
	reg.revTopics[ch][topic] = true
}

func (reg *registry) send(topic string, msg interface{}) {
	for ch, st := range reg.topics[topic] {
		ch <- msg
		switch st {
		case onceAny:
			for topic := range reg.revTopics[ch] {
				reg.remove(topic, ch)
			}
		case onceEach:
			reg.remove(topic, ch)
		}
	}
}

func (reg *registry) sendNoWait(topic string, msg interface{}) {
	for ch, st := range reg.topics[topic] {
		select {
		case ch <- msg:
			switch st {
			case onceAny:
				for topic := range reg.revTopics[ch] {
					reg.remove(topic, ch)
				}
			case onceEach:
				reg.remove(topic, ch)
			}
		default:
		}

	}
}

func (reg *registry) removeTopic(topic string) {
	for ch := range reg.topics[topic] {
		reg.remove(topic, ch)
	}
}

func (reg *registry) removeChannel(ch chan interface{}) {
	for topic := range reg.revTopics[ch] {
		reg.remove(topic, ch)
	}
}

func (reg *registry) remove(topic string, ch chan interface{}) {
	if _, ok := reg.topics[topic]; !ok {
		return
	}

	if _, ok := reg.topics[topic][ch]; !ok {
		return
	}

	delete(reg.topics[topic], ch)
	delete(reg.revTopics[ch], topic)

	if len(reg.topics[topic]) == 0 {
		delete(reg.topics, topic)
	}

	if len(reg.revTopics[ch]) == 0 {
		close(ch)
		delete(reg.revTopics, ch)
	}
}
