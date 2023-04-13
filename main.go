package main

import (
	"encoding/json"
	"log"
	"strconv"
	"strings"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	idGen := IdGenerator{currentId: 1}
	localNumbers := LocalNumbers{NumList: make(map[float64]struct{})}
	// := Neighbors{List: make(map[string]struct{})}

	n.Handle("echo", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type to return back.
		body["type"] = "echo_ok"

		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		nodeId := msg.Dest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "generate_ok"
		body["id"] = idGen.generateGlobalId(nodeId)

		return n.Reply(msg, body)
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		number := body["message"].(float64)
		if localNumbers.isNewNumber(number) {
			localNumbers.addNumber(number)
			for _, node := range n.NodeIDs() {
				if node != msg.Src && node != n.ID() {
					n.Send(node, body)
				}
			}
		}

		body["type"] = "broadcast_ok"
		delete(body, "message")

		return n.Reply(msg, body)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["messages"] = localNumbers.getNumbers()
		body["type"] = "read_ok"
		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		// for k, v := range body["topology"].(map[string]interface{}) {
		// 	if k == n.ID() {
		// 		for _, v := range v.([]interface{}) {
		// 			neigh.List[v.(string)] = struct{}{}
		// 		}
		// 	}
		// }

		body["type"] = "topology_ok"
		delete(body, "topology")
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type IdGenerator struct {
	mu        sync.Mutex
	currentId uint64
}

func (i *IdGenerator) generateLocalId() string {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.currentId++
	return strconv.FormatUint(i.currentId, 10)
}

func (i *IdGenerator) generateGlobalId(nodeId string) string {
	builder := strings.Builder{}
	builder.WriteString(nodeId)
	builder.WriteString(i.generateLocalId())
	return builder.String()
}

type LocalNumbers struct {
	mu      sync.Mutex
	NumList map[float64]struct{}
}

func (l *LocalNumbers) addNumber(number float64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.NumList[number] = struct{}{}
}

func (l *LocalNumbers) getNumbers() []float64 {
	l.mu.Lock()
	result := make([]float64, len(l.NumList))
	defer l.mu.Unlock()
	for elem := range l.NumList {
		result = append(result, elem)
	}
	return result
}
func (l *LocalNumbers) isNewNumber(number float64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.NumList[number]
	return !ok
}

type BroadcastBody struct {
	Type    string `json:"type,omitempty"`
	Message string `json:"message"`
}

type Neighbors struct {
	mu   sync.Mutex
	List map[string]struct{}
}

func (n *Neighbors) getNeighbors() []string {
	n.mu.Lock()
	result := make([]string, len(n.List))
	defer n.mu.Unlock()
	for elem := range n.List {
		result = append(result, elem)
	}
	return result
}
