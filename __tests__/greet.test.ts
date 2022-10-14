import tap from "tap"
import { greet } from "../src/main"

tap.test('testing', t => {
  t.plan(1)
  t.ok(greet("hello world"), "hello world")
  t.end()
})
