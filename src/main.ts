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
  GlobalSecondaryIndex,
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

interface AttrProperties {
  AttributeDefinitions: AttributeDefinition[]
  KeySchema: KeySchemaElement[]
}

export function addGlobalSecondaryIndexAttributeDefinitions(
  attributes: AttrProperties,
  globalSecondaryIndexes: GlobalSecondaryIndex[] = [],
): AttrProperties {
  if (!globalSecondaryIndexes.length) return attributes
  return {
    ...attributes,
    AttributeDefinitions: [
      ...attributes.AttributeDefinitions,
      ...globalSecondaryIndexes.reduce(
        (accu: any, current: any) => [...accu, ...current.KeySchema],
        [],
      ),
    ],
  }
}

export function generateTableSchema(
  tableAttributes: TableAttributes[],
): AttrProperties {
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
  globalSecondaryIndexes?: GlobalSecondaryIndex[],
) {
  const settings = {
    TableName,
    StreamSpecification: {
      StreamEnabled: false,
    },
    ProvisionedThroughput: {
      ReadCapacityUnits: 1,
      WriteCapacityUnits: 1,
    },
    ...(globalSecondaryIndexes && {
      GlobalSecondaryIndexes: globalSecondaryIndexes,
    }),
    ...addGlobalSecondaryIndexAttributeDefinitions(
      generateTableSchema(tableAttributes),
      globalSecondaryIndexes,
    ),
  }

  console.log("settings", JSON.stringify(settings, null, 4))

  return async () => {
    try {
      await createTable(settings)
    } catch (e) {
      console.warn("Issues creating database")
      console.error(e)
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
