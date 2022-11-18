import {
  dump,
  populateDatabase,
  setupDatabase,
  // teardownDatabase
} from "../src/main"

const TABLE_NAME = "Table"

beforeAll(setupDatabase(TABLE_NAME))
beforeAll(populateDatabase(TABLE_NAME))
// afterAll(teardownDatabase(TABLE_NAME))

// TODO
// setup db with a complex attribute an array of something maybe, and with another field as the amount field of the complex attribute.
// on insert only insert if the amount field is less than a certain amount
// on successful insert increment the amount field as well
// throw an error if we can't insert saying to remove before inserting
// on remove could do the same except only if the amount if more than 0 otherwise not
// tap.test("Denormalization by using a complex attribute", (t) => {
test("That we create a database", async () => {
  console.log("test")
  console.log(await dump(TABLE_NAME))

  expect(true).toBeTruthy()
})
