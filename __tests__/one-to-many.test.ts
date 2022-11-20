import {
  UpdateItemCommand,
  QueryCommand,
  GetItemCommand,
} from "@aws-sdk/client-dynamodb"
import { faker } from "@faker-js/faker"
import {
  dump,
  populateDatabase,
  setupDatabase,
  TableAttributes,
  teardownDatabase,
  send,
} from "../src/main"
import { unmarshall } from "@aws-sdk/util-dynamodb"

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

    // We prepopulate with a single item that has a list with two items, to
    // illustrate using a complex data types to store additional data
    const writeItems = [
      {
        PK: { S: "USER#smilingbig" },
        SK: { S: "USER#smilingbig" },
        count: { N: 2 },
        list: {
          L: Array.from({ length: 2 }).map(() => ({
            S: faker.internet.userName(),
          })),
        },
      },
    ]

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

  // TODO
  // If duplicate data required updating, what strategies could be used
  describe("Denormalization by duplicating data", () => {
    const primaryKey: TableAttributes[] = [
      {
        KeyType: "HASH",
        AttributeName: "AuthorName",
        AttributeType: "S",
      },
      {
        KeyType: "RANGE",
        AttributeName: "BookName",
        AttributeType: "S",
      },
    ]

    // Hierarchically structured items that have attributes won't require updating often
    const writeItems = [
      {
        AuthorName: { S: "Stephen King" },
        BookName: { S: "It" },
        AuthorBirthdate: { S: "September 21, 1947" },
        ReleaseYear: { S: "1986" },
      },
      {
        AuthorName: { S: "Stephen King" },
        BookName: { S: "The Shining" },
        AuthorBirthdate: { S: "September 21, 1947" },
        ReleaseYear: { S: "1977" },
      },

      {
        AuthorName: { S: "J.K Rowling" },
        BookName: { S: "Hairy bull shit" },
        AuthorBirthdate: { S: "September 21, 1947" },
        ReleaseYear: { S: "1986" },
      },
    ]

    const command = {
      TableName: TABLE_NAME,
      Key: {
        AuthorName: {
          S: "Stephen King",
        },
      },
      KeyConditionExpression: "#authorName = :authorName",
      ExpressionAttributeNames: {
        "#authorName": "AuthorName",
      },
      ExpressionAttributeValues: {
        ":authorName": { S: "Stephen King" },
      },
      Select: "ALL_ATTRIBUTES",
    }

    beforeAll(setupDatabase(TABLE_NAME, primaryKey))
    beforeAll(populateDatabase(TABLE_NAME, writeItems))
    afterAll(teardownDatabase(TABLE_NAME))

    it("should return all items related to the partition key", async () => {
      expect(((await send(new QueryCommand(command))) as any).Count).toBe(2)
    })
  })

  describe("Composite primary key + the Query API action", () => {
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

    // Hierarchically structured items that have attributes won't require updating often
    const writeItems = [
      {
        PK: { S: "ORG#MICROSOFT" },
        SK: { S: "METADATA#MICROSOFT" },
        Name: { S: "Microsoft" },
        Type: { S: "Enterprise" },
      },
      {
        PK: { S: "ORG#MICROSOFT" },
        SK: { S: "USER#BILLGATES" },
        Name: { S: "Bill Gates" },
        Type: { S: "Member" },
      },
      {
        PK: { S: "ORG#MICROSOFT" },
        SK: { S: "USER#SATYANADELLA" },
        Name: { S: "Satya Nadella" },
        Type: { S: "Admin" },
      },
      {
        PK: { S: "ORG#AMAZON" },
        SK: { S: "METADATA#AMAZON" },
        Name: { S: "Amazon" },
        Type: { S: "Pro" },
      },
      {
        PK: { S: "ORG#AMAZON" },
        SK: { S: "USER#MEH" },
        Name: { S: "Meh" },
        Type: { S: "Admin" },
      },
    ]

    beforeAll(setupDatabase(TABLE_NAME, primaryKey))
    beforeAll(populateDatabase(TABLE_NAME, writeItems))
    afterAll(teardownDatabase(TABLE_NAME))

    it("query all parition keys (Query) with all relevant data", async () => {
      const command = {
        TableName: TABLE_NAME,
        Key: {
          PK: {
            S: "ORG#MICROSOFT",
          },
        },
        KeyConditionExpression: "#pk = :pk",
        ExpressionAttributeNames: {
          "#pk": "PK",
        },
        ExpressionAttributeValues: {
          ":pk": { S: "ORG#MICROSOFT" },
        },
        Select: "ALL_ATTRIBUTES",
      }

      const result: any = await send(new QueryCommand(command))

      expect(result.Count).toBe(3)
      expect(
        result.Items.filter((x: any) => x.PK.S === "ORG#MICROSOFT"),
      ).toHaveLength(3)
    })

    it("query all parition key items (GetItem) with a specific sort key type", async () => {
      const command = {
        TableName: TABLE_NAME,
        Key: {
          PK: {
            S: "ORG#MICROSOFT",
          },
          SK: {
            S: "METADATA#MICROSOFT",
          },
        },
        Select: "ALL_ATTRIBUTES",
      }

      // This can be a single get item if you know there will only be a single
      // record or multiple ones for additional metadata
      const result: any = await send(new GetItemCommand(command))

      expect(result.Item.Name.S).toBe("Microsoft")
    })

    it("query only specific items (Query begins_with) with the sort key prefix", async () => {
      const command = {
        TableName: TABLE_NAME,
        Key: {
          PK: {
            S: "ORG#MICROSOFT",
          },
        },
        KeyConditionExpression: "#pk = :pk AND begins_with ( #sk , :user )",
        ExpressionAttributeNames: {
          "#pk": "PK",
          "#sk": "SK",
        },
        ExpressionAttributeValues: {
          ":pk": { S: "ORG#MICROSOFT" },
          ":user": { S: "USER#" },
        },
        Select: "ALL_ATTRIBUTES",
      }

      const result: any = await send(new QueryCommand(command))

      expect(result.Count).toBe(2)
      expect(result.Items.map((x: any) => unmarshall(x))).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ Name: "Bill Gates" }),
          expect.objectContaining({ Name: "Satya Nadella" }),
        ]),
      )
    })
  })
})
