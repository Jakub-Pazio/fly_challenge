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
