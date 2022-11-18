#!/bin/env node

import { deleteTable } from "../src/main"

const [, , ...args] = process.argv

;(async () => {
  const result = await deleteTable({
    TableName: args[0],
  })

  console.log(JSON.stringify(result, null, 4))
})()
