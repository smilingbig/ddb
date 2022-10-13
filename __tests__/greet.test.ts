import tap from "tap"
import { greet } from "../src/main"

tap.equal(greet("hello world"), "hello world")
