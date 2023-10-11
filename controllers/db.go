package controllers

import (
	"log"
	"time"

	"github.com/asynched/kvd/managers"
	"github.com/gofiber/fiber/v2"
	"github.com/hashicorp/raft"
)

type DbController struct {
	manager *managers.KeyValueManager
}

func (controller *DbController) Join(c *fiber.Ctx) error {
	var body struct {
		Address   string `json:"address"`
		LastIndex uint64 `json:"lastIndex"`
	}

	if err := c.BodyParser(&body); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"message": err.Error(),
		})
	}

	log.Printf("Received join request from %s, trying to add it to the cluster.\n", body.Address)

	raftNode := controller.manager.GetRaft()

	f := raftNode.AddVoter(raft.ServerID(body.Address), raft.ServerAddress(body.Address), body.LastIndex, time.Second*10)

	if err := f.Error(); err != nil {
		log.Println("Failed to add voter:", err)

		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"message": err.Error(),
		})
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"message": "success",
	})
}

func (controller *DbController) GetAll(c *fiber.Ctx) error {
	return c.Status(fiber.StatusOK).JSON(controller.manager.GetAll())
}

func (controller *DbController) GetOne(c *fiber.Ctx) error {
	key := c.Params("key")

	value, ok := controller.manager.Get(key)

	if !ok {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"message": "not found",
		})
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"key":   key,
		"value": value,
	})
}

func (controller *DbController) Create(c *fiber.Ctx) error {
	var body struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}

	if err := c.BodyParser(&body); err != nil {
		return err
	}

	if err := controller.manager.Set(body.Key, body.Value); err != nil {
		return err
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"message": "success",
	})
}

func (controller *DbController) Delete(c *fiber.Ctx) error {
	key := c.Params("key")

	if err := controller.manager.Delete(key); err != nil {
		return err
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"message": "success",
	})
}

func NewDbController(
	manager *managers.KeyValueManager,
) *DbController {
	return &DbController{
		manager: manager,
	}
}
