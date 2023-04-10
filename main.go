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
	localNumbers := LocalNumbers{numList: make(map[float64]struct{})}

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
		localNumbers.addNumber(number)

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
	numList map[float64]struct{}
}

func (l *LocalNumbers) addNumber(number float64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.numList[number] = struct{}{}
}

func (l *LocalNumbers) getNumbers() []float64 {
	l.mu.Lock()
	result := make([]float64, len(l.numList))
	defer l.mu.Unlock()
	for elem := range l.numList {
		result = append(result, elem)
	}
	return result
}
