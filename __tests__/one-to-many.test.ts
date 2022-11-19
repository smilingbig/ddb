import { UpdateItemCommand } from "@aws-sdk/client-dynamodb"
import { faker } from "@faker-js/faker"
import {
  dump,
  populateDatabase,
  setupDatabase,
  TableAttributes,
  teardownDatabase,
  send,
} from "../src/main"

const TABLE_NAME = "Table"

describe("One to many", () => {
  describe("Denormalisation with complex attribute", () => {
    const maxCount = 3
    const primaryKey: TableAttributes[] = [
      {
        KeyType: "HASH",
        AttributeName: "PK",
        AttributeType: "S",
      },
      {
        KeyType: "RANGE",
        AttributeName: "SK",
        AttributeType: "S",
      },
    ]

    // We prepopulate with a single item that has a list with two items
    const writeItems = Array.from({ length: 1 }).map(() => {
      return {
        PK: { S: "USER#smilingbig" },
        SK: { S: "USER#smilingbig" },
        count: { N: 2 },
        list: {
          L: Array.from({ length: 2 }).map(() => ({
            S: faker.internet.userName(),
          })),
        },
      }
    })

    const command = {
      TableName: TABLE_NAME,
      Key: {
        PK: {
          S: "USER#smilingbig",
        },
        SK: {
          S: "USER#smilingbig",
        },
      },
      ConditionExpression: "#count < :maxCount",
      UpdateExpression:
        "SET #count = #count + :inc, #list = list_append(#list, :listItem)",
      ExpressionAttributeNames: {
        "#count": "count",
        "#list": "list",
      },
      ExpressionAttributeValues: {
        ":maxCount": { N: String(maxCount) },
        ":inc": { N: "1" },
        ":listItem": { L: [{ S: faker.internet.userName() }] },
      },
    }

    beforeAll(setupDatabase(TABLE_NAME, primaryKey))
    beforeAll(populateDatabase(TABLE_NAME, writeItems))
    afterAll(teardownDatabase(TABLE_NAME))

    it("should allow you to insert a list item if its less than the max count allowed", async () => {
      const predump = await dump(TABLE_NAME)

      expect(predump.Count).toBe(1)
      expect(predump.Items[0].count).toBe(2)
      expect(predump.Items[0].list).toHaveLength(2)

      await expect(send(new UpdateItemCommand(command))).resolves.not.toThrow()

      const postdump = await dump(TABLE_NAME)

      expect(postdump.Items[0].count).toBe(3)
      expect(postdump.Items[0].list).toHaveLength(3)
      expect(postdump.Count).toBe(1)
    })

    it("should not allow us to insert more list items once teh amount value has been reached", async () => {

      const predump = await dump(TABLE_NAME)

      expect(predump.Count).toBe(1)
      expect(predump.Items[0].count).toBe(3)
      expect(predump.Items[0].list).toHaveLength(3)

      await expect(send(new UpdateItemCommand(command))).rejects.toThrow(
        "The conditional request failed",
      )

      const postdump = await dump(TABLE_NAME)

      expect(postdump.Items[0].count).toBe(3)
      expect(postdump.Items[0].list).toHaveLength(3)
      expect(postdump.Count).toBe(1)
    })
  })

  describe("Denormalization by duplicating data", () => {
    it.todo(
      "should not allow us to insert more list items once teh amount value has been reached",
    )
  })
})
