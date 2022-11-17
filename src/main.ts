import {
  DynamoDBClient,
  CreateTableCommand,
  CreateTableCommandInput,
  DeleteTableCommand,
  DeleteTableCommandInput,
  AttributeDefinition,
  KeySchemaElement,
  BatchWriteItemCommand,
  BatchWriteItemCommandInput,
  WriteRequest,
  ScanCommand,
} from "@aws-sdk/client-dynamodb"
import { faker } from "@faker-js/faker"

const client = new DynamoDBClient({ endpoint: "http://localhost:8000" })

interface TableAttributes {
  AttributeName: string
  AttributeType:
    | "B"
    | "BOOL"
    | "BS"
    | "L"
    | "M"
    | "N"
    | "NS"
    | "NULL"
    | "S"
    | "SS"
  KeyType: "HASH" | "RANGE"
}

export function generateTableSchema(tableAttributes: TableAttributes[]): {
  AttributeDefinitions: AttributeDefinition[]
  KeySchema: KeySchemaElement[]
} {
  const reduceFn = (accu: any, current: any) => {
    return {
      AttributeDefinitions: [
        ...accu.AttributeDefinitions,
        {
          AttributeName: current.AttributeName,
          AttributeType: current.AttributeType,
        },
      ],
      KeySchema: [
        ...accu.KeySchema,
        { AttributeName: current.AttributeName, KeyType: current.KeyType },
      ],
    }
  }

  return tableAttributes.reduce(reduceFn, {
    AttributeDefinitions: [],
    KeySchema: [],
  })
}

export function generateBatchWriteRequests<T>(items: T[]): WriteRequest[] {
  return items.map((Item) => ({ ["PutRequest" || "DeleteRequest"]: { Item } }))
}

export async function createTable(params: CreateTableCommandInput) {
  const command = new CreateTableCommand(params)
  return await client.send(command)
}

export async function deleteTable(params: DeleteTableCommandInput) {
  const command = new DeleteTableCommand(params)
  return await client.send(command)
}

export async function batchWriteItems<T>({
  TableName,
  writeItems,
}: {
  TableName: string
  writeItems: T[]
}) {
  console.log("writeItems", writeItems)
  console.log("wrappedWriteItems", generateBatchWriteRequests(writeItems))

  const params: BatchWriteItemCommandInput = {
    RequestItems: {
      [TableName]: [...generateBatchWriteRequests(writeItems)],
    },
  }

  const command = new BatchWriteItemCommand(params)
  const data = await client.send(command)

  console.log(data)

  return data
}

export function teardownDatabase(TableName: string) {
  return async () => {
    console.log(`Deleting ${TableName}`)

    await deleteTable({ TableName })

    console.log(`${TableName} is deleted`)
  }
}

export function populateDatabase(TableName: string) {
  return async () => {
    console.log(`Populating ${TableName}`)

    await batchWriteItems({
      TableName,
      writeItems: Array.from({ length: 10 }).map(() => ({
        PK: { S: faker.datatype.uuid() },
        SK: { S: faker.internet.userName() },
        no: { N: 0 },
        list: { L: [1] },
      })),
    })

    console.log(`${TableName} populated`)
  }
}

export function setupDatabase(TableName: string) {
  return async () => {
    console.log(`Setup ${TableName}`)

    try {
      await createTable({
        TableName,
        StreamSpecification: {
          StreamEnabled: false,
        },
        ProvisionedThroughput: {
          ReadCapacityUnits: 1,
          WriteCapacityUnits: 1,
        },
        ...generateTableSchema([
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
        ]),
      })
    } catch (e) {
      console.warn(e)
    }

    console.log(`${TableName} setup`)
  }
}

export async function dump(TableName: string) {
  const params = { TableName }
  const command = new ScanCommand(params)
  return await client.send(command)
}

// ;(async () => {
//   if (require.main !== module) return
//   // const createData = await createTable(createParams)
//   // console.log(createData)
//   const deleteData = await deleteTable({
//     TableName: "Table",
//   })
//   console.log(deleteData)
// })()
