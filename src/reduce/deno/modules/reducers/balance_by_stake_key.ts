import type { JsonValue } from "npm:@bufbuild/protobuf";

export function apply(blockJson: JsonValue) {
  return {
    command: "PNCounter",
    key: "stake_key",
    value: 1
  }
}

export function undo(blockJson: JsonValue) {
  return {
    command: "PNCounter",
    key: "stake_key",
    value: -1
  }
}