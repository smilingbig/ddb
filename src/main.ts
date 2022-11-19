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
import { unmarshall } from "@aws-sdk/util-dynamodb"

const client = new DynamoDBClient({ endpoint: "http://localhost:8000" })

export interface TableAttributes {
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

export async function send(command: any) {
  return await client.send(command)
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
  // TODO
  // I don't think this works for delete, because if I remember correctly the
  // 'Item' structure is slightly different
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
  const params: BatchWriteItemCommandInput = {
    RequestItems: {
      [TableName]: [...generateBatchWriteRequests(writeItems)],
    },
  }

  const command = new BatchWriteItemCommand(params)
  const data = await client.send(command)

  return data
}

export function teardownDatabase(TableName: string) {
  return async () => {
    await deleteTable({ TableName })
  }
}

// TODO
// This should be a reusable method that takes different data schemas or
// something like that for populating different structures for testing
export function populateDatabase<T>(TableName: string, writeItems: T[]) {
  return async () => {
    await batchWriteItems({
      TableName,
      writeItems,
    })
  }
}

export function setupDatabase(
  TableName: string,
  tableAttributes: TableAttributes[],
) {
  return async () => {
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
        ...generateTableSchema(tableAttributes),
      })
    } catch (e) {
      console.warn("Database already created")
    }
  }
}

export async function dump(TableName: string) {
  const params = { TableName }

  const command = new ScanCommand(params)
  const result = await client.send(command)

  return {
    ...result,
    Items: result.Items.map((x) => unmarshall(x)),
  }
}
